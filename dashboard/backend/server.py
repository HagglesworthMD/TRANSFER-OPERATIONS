"""FastAPI app — all endpoints + static file serving."""

import logging
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .data_reader import get_file_info, load_csv, load_json
from .kpi_engine import compute_dashboard
from .staff_manager import add_staff, read_staff, remove_staff

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Transfer-Bot Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Disable caching for all responses
@app.middleware("http")
async def disable_cache(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── Global exception handler ──
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s", request.url)
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "detail": "Internal server error"},
    )


# ── Endpoints ──

@app.get("/api/dashboard")
async def dashboard_endpoint(date_start: str | None = None, date_end: str | None = None):
    """Unified dashboard data — single call for all read-only data.

    Query params:
        date_start: YYYY-MM-DD (optional, defaults to today)
        date_end:   YYYY-MM-DD (optional, defaults to today)
    """
    rows, csv_err = load_csv(config.DAILY_STATS_CSV)
    roster, _ = load_json(config.ROSTER_STATE_JSON)
    settings, _ = load_json(config.SETTINGS_OVERRIDES_JSON)
    hib_state, _ = load_json(config.HIB_WATCHDOG_JSON)
    staff_list = read_staff(config.STAFF_TXT)

    payload = compute_dashboard(rows, roster, settings, staff_list, hib_state,
                                date_start=date_start, date_end=date_end)
    if csv_err:
        payload["warning"] = csv_err
    return payload


@app.get("/api/staff")
async def get_staff():
    return {"staff": read_staff(config.STAFF_TXT)}


class StaffRequest(BaseModel):
    email: str


@app.post("/api/staff")
async def post_staff(body: StaffRequest):
    ok, msg = add_staff(config.STAFF_TXT, body.email)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg, "staff": read_staff(config.STAFF_TXT)}


@app.delete("/api/staff/{email}")
async def delete_staff(email: str):
    ok, msg = remove_staff(config.STAFF_TXT, email)
    if not ok:
        raise HTTPException(status_code=404, detail=msg)
    return {"message": msg, "staff": read_staff(config.STAFF_TXT)}


@app.get("/api/managers")
async def get_managers():
    return {"managers": read_staff(config.MANAGERS_TXT)}


@app.post("/api/managers")
async def post_manager(body: StaffRequest):
    ok, msg = add_staff(config.MANAGERS_TXT, body.email)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg, "managers": read_staff(config.MANAGERS_TXT)}


@app.delete("/api/managers/{email}")
async def delete_manager(email: str):
    ok, msg = remove_staff(config.MANAGERS_TXT, email)
    if not ok:
        raise HTTPException(status_code=404, detail=msg)
    return {"message": msg, "managers": read_staff(config.MANAGERS_TXT)}


@app.get("/api/apps")
async def get_apps():
    return {"apps": read_staff(config.APPS_TXT)}


@app.post("/api/apps")
async def post_apps(body: StaffRequest):
    ok, msg = add_staff(config.APPS_TXT, body.email)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg, "apps": read_staff(config.APPS_TXT)}


@app.delete("/api/apps/{email}")
async def delete_apps(email: str):
    ok, msg = remove_staff(config.APPS_TXT, email)
    if not ok:
        raise HTTPException(status_code=404, detail=msg)
    return {"message": msg, "apps": read_staff(config.APPS_TXT)}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "csv": get_file_info(config.DAILY_STATS_CSV),
        "staff": get_file_info(config.STAFF_TXT),
        "roster_state": get_file_info(config.ROSTER_STATE_JSON),
    }


@app.get("/api/settings")
async def get_settings():
    settings, _ = load_json(config.SETTINGS_OVERRIDES_JSON)
    return {
        "manager_cc_addr": settings.get("manager_cc_addr", "") if settings else "",
        "apps_cc_addr": settings.get("apps_cc_addr", "") if settings else "",
    }


class SettingUpdate(BaseModel):
    key: str
    value: str


@app.post("/api/settings")
async def update_setting(body: SettingUpdate):
    import json
    from pathlib import Path

    # Validate key
    if body.key not in ["manager_cc_addr", "apps_cc_addr"]:
        raise HTTPException(status_code=400, detail="Invalid setting key")

    # Validate email format
    if body.value and "@" not in body.value:
        raise HTTPException(status_code=400, detail="Invalid email format")

    # Load current settings
    settings, _ = load_json(config.SETTINGS_OVERRIDES_JSON)
    if settings is None:
        settings = {}

    # Update setting
    settings[body.key] = body.value

    # Save back
    try:
        with open(config.SETTINGS_OVERRIDES_JSON, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=2)
        logger.info(f"Updated setting {body.key} = {body.value}")
        return {"message": f"Updated {body.key}", "settings": settings}
    except Exception as e:
        logger.exception("Failed to save settings")
        raise HTTPException(status_code=500, detail=str(e))


# ── Static files (frontend) — mounted last so API routes take priority ──
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


def main():
    logger.info("Starting Transfer-Bot Dashboard on http://localhost:%s", config.PORT)
    logger.info("CSV path: %s", config.DAILY_STATS_CSV)
    uvicorn.run(app, host=config.HOST, port=config.PORT, log_level="info")


if __name__ == "__main__":
    # Allow running as `python -m dashboard.backend.server` or directly
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    main()
