"""FastAPI app — all endpoints + static file serving."""

import csv
import io
import json
import logging
import os
import re
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .data_reader import get_file_info, load_csv, load_json
from .kpi_engine import compute_dashboard, export_active_events, export_staff_events
from .reconciliation import load_reconciled, load_reconciled_set, add_reconciled, add_reconciled_bulk, remove_reconciled

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

_FOLDER_KEYS = (
    "completed",
    "non_actionable",
    "quarantine",
    "hold",
    "system_notification",
)

_DEFAULT_FOLDERS: dict[str, str] = {
    "completed": "01_COMPLETED",
    "non_actionable": "02_PROCESSED",
    "quarantine": "03_QUARANTINE",
    "hold": "04_HIB",
    "system_notification": "05_SYSTEM_NOTIFICATIONS",
}


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _safe_load_json_direct(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, f"Missing file: {path.name}"
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON in {path.name}: {e}"
    except OSError as e:
        return None, f"Read failed for {path.name}: {e}"


def _atomic_write_json(path: Path, obj) -> tuple[bool, str | None]:
    """Atomic write (Windows-safe): write temp then os.replace()."""
    tmp_path = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(path.name + f".tmp.{os.getpid()}")
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(obj, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, path)
        return True, None
    except Exception as e:
        try:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False, str(e)


def _atomic_write_json_if_changed(path: Path, obj) -> tuple[bool, str | None, bool]:
    existing, err = _safe_load_json_direct(path)
    if err is None and existing == obj:
        return True, None, False
    ok, werr = _atomic_write_json(path, obj)
    return ok, werr, ok


def _load_list_from_legacy_txt(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except Exception:
        return []
    out: list[str] = []
    for line in text.splitlines():
        s = (line or "").strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def _resolve_stats_csv_path() -> Path:
    """Prefer v2 stats file when present, else legacy file."""
    if config.DAILY_STATS_V2_CSV.exists():
        return config.DAILY_STATS_V2_CSV
    return config.DAILY_STATS_CSV


def _normalize_email(raw) -> tuple[str | None, str | None]:
    if not isinstance(raw, str):
        return None, "Email must be a string"
    s = raw.strip().lower()
    if not s:
        return None, "Email cannot be empty"
    if any(ch.isspace() for ch in s):
        return None, "Email must not contain spaces"
    if s.count("@") != 1:
        return None, "Email must contain exactly one '@'"
    local, domain = s.split("@", 1)
    if not local or not domain:
        return None, "Email must include local and domain parts"
    if "." not in domain:
        return None, "Email domain must contain a dot"
    return s, None


def _normalize_domain(raw) -> tuple[str | None, str | None]:
    if not isinstance(raw, str):
        return None, "Domain must be a string"
    s = raw.strip().lower()
    if s.startswith("@"):
        s = s[1:]
    if s.startswith("http://"):
        s = s[len("http://"):]
    elif s.startswith("https://"):
        s = s[len("https://"):]
    s = s.strip().strip("/")
    if not s:
        return None, "Domain cannot be empty"
    if any(ch.isspace() for ch in s):
        return None, "Domain must not contain spaces"
    if "/" in s or "\\" in s:
        return None, "Domain must not include a path"
    if ":" in s:
        return None, "Domain must not include a port"
    if "." not in s:
        return None, "Domain must contain at least one dot"
    return s, None


def _normalize_list(raw, *, kind: str, field_name: str) -> tuple[list[str] | None, str | None]:
    if raw is None:
        return [], None
    if not isinstance(raw, list):
        return None, f"{field_name} must be a list"
    normalized: list[str] = []
    for item in raw:
        if kind == "email":
            val, err = _normalize_email(item)
        else:
            val, err = _normalize_domain(item)
        if err:
            return None, f"{field_name}: {err}"
        assert val is not None
        normalized.append(val)
    return _dedupe_preserve_order(normalized), None


def _validate_folders(raw) -> tuple[dict[str, str] | None, str | None]:
    if raw is None:
        return dict(_DEFAULT_FOLDERS), None
    if not isinstance(raw, dict):
        return None, "folders must be an object"
    out: dict[str, str] = {}
    for key in _FOLDER_KEYS:
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip():
            return None, f"folders.{key} must be a non-empty string"
        out[key] = value.strip()
    return out, None


def _load_staff_json() -> tuple[dict | None, str | None]:
    data, err = _safe_load_json_direct(config.STAFF_JSON)
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, "staff.json must be an object"
    for key in ("staff", "off_rotation", "leave"):
        if key not in data:
            return None, f"staff.json missing key: {key}"
        if not isinstance(data.get(key), list):
            return None, f"staff.json key {key} must be a list"
    staff, err = _normalize_list(data.get("staff"), kind="email", field_name="staff.staff")
    if err:
        return None, err
    off_rotation, err = _normalize_list(data.get("off_rotation"), kind="email", field_name="staff.off_rotation")
    if err:
        return None, err
    leave, err = _normalize_list(data.get("leave"), kind="email", field_name="staff.leave")
    if err:
        return None, err
    return {"staff": staff, "off_rotation": off_rotation, "leave": leave}, None


def _load_recipients_json(path: Path, *, name: str) -> tuple[dict | None, str | None]:
    data, err = _safe_load_json_direct(path)
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, f"{name} must be an object"
    if "recipients" not in data or not isinstance(data.get("recipients"), list):
        return None, f"{name} missing key: recipients"
    recipients, err = _normalize_list(data.get("recipients"), kind="email", field_name=f"{name}.recipients")
    if err:
        return None, err
    return {"recipients": recipients}, None


def _load_system_buckets_json() -> tuple[dict | None, str | None]:
    data, err = _safe_load_json_direct(config.SYSTEM_BUCKETS_JSON)
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, "system_buckets.json must be an object"
    required_lists = (
        "transfer_domains",
        "system_notification_domains",
        "quarantine_domains",
        "held_domains",
    )
    for key in required_lists:
        if key not in data:
            return None, f"system_buckets.json missing key: {key}"
        if not isinstance(data.get(key), list):
            return None, f"system_buckets.json key {key} must be a list"
    if "folders" not in data:
        return None, "system_buckets.json missing key: folders"
    transfer_domains, err = _normalize_list(data.get("transfer_domains"), kind="domain", field_name="transfer_domains")
    if err:
        return None, err
    system_notification_domains, err = _normalize_list(
        data.get("system_notification_domains"),
        kind="domain",
        field_name="system_notification_domains",
    )
    if err:
        return None, err
    quarantine_domains, err = _normalize_list(data.get("quarantine_domains"), kind="domain", field_name="quarantine_domains")
    if err:
        return None, err
    held_domains, err = _normalize_list(data.get("held_domains"), kind="domain", field_name="held_domains")
    if err:
        return None, err
    # Sender override lists (optional — backward compatible)
    transfer_senders, err = _normalize_list(data.get("transfer_senders"), kind="email", field_name="transfer_senders")
    if err:
        return None, err
    system_notification_senders, err = _normalize_list(data.get("system_notification_senders"), kind="email", field_name="system_notification_senders")
    if err:
        return None, err
    quarantine_senders, err = _normalize_list(data.get("quarantine_senders"), kind="email", field_name="quarantine_senders")
    if err:
        return None, err
    held_senders, err = _normalize_list(data.get("held_senders"), kind="email", field_name="held_senders")
    if err:
        return None, err
    folders, err = _validate_folders(data.get("folders"))
    if err:
        return None, err
    return {
        "transfer_domains": transfer_domains,
        "system_notification_domains": system_notification_domains,
        "quarantine_domains": quarantine_domains,
        "held_domains": held_domains,
        "transfer_senders": transfer_senders,
        "system_notification_senders": system_notification_senders,
        "quarantine_senders": quarantine_senders,
        "held_senders": held_senders,
        "folders": folders,
    }, None


def _read_domain_policy_legacy() -> dict:
    """Read domain_policy.json directly (bypass mtime cache). Read-only legacy fallback."""
    data, _ = _safe_load_json_direct(config.DOMAIN_POLICY_JSON)
    return data if isinstance(data, dict) else {}


def _build_domain_policy_payload() -> dict:
    # Staff
    staff_cfg, _ = _load_staff_json()
    if staff_cfg:
        staff_rr = staff_cfg.get("staff", [])
    else:
        legacy_staff = _load_list_from_legacy_txt(config.STAFF_TXT)
        staff_rr, _ = _normalize_list(legacy_staff, kind="email", field_name="staff_round_robin")
        staff_rr = staff_rr or []

    # Apps / managers
    apps_cfg, _ = _load_recipients_json(config.APPS_TEAM_JSON, name="apps_team.json")
    apps_recipients = apps_cfg.get("recipients", []) if apps_cfg else []
    if not apps_cfg:
        legacy_apps = _load_list_from_legacy_txt(config.APPS_TXT)
        apps_recipients, _ = _normalize_list(legacy_apps, kind="email", field_name="apps_team_recipients")
        apps_recipients = apps_recipients or []

    mgr_cfg, _ = _load_recipients_json(config.MANAGER_CONFIG_JSON, name="manager_config.json")
    manager_recipients = mgr_cfg.get("recipients", []) if mgr_cfg else []
    if not mgr_cfg:
        legacy_mgrs = _load_list_from_legacy_txt(config.MANAGERS_TXT)
        manager_recipients, _ = _normalize_list(legacy_mgrs, kind="email", field_name="manager_recipients")
        manager_recipients = manager_recipients or []

    # Buckets + folders
    buckets_cfg, _ = _load_system_buckets_json()
    if buckets_cfg:
        transfer_domains = buckets_cfg.get("transfer_domains", [])
        system_notification_domains = buckets_cfg.get("system_notification_domains", [])
        quarantine_domains = buckets_cfg.get("quarantine_domains", [])
        held_domains = buckets_cfg.get("held_domains", [])
        transfer_senders = buckets_cfg.get("transfer_senders", [])
        system_notification_senders = buckets_cfg.get("system_notification_senders", [])
        quarantine_senders = buckets_cfg.get("quarantine_senders", [])
        held_senders = buckets_cfg.get("held_senders", [])
        folders = buckets_cfg.get("folders", dict(_DEFAULT_FOLDERS))
    else:
        legacy = _read_domain_policy_legacy()
        transfer_domains, _ = _normalize_list(legacy.get("external_image_request_domains", []), kind="domain", field_name="transfer_domains")
        transfer_domains = transfer_domains or []
        system_notification_domains, _ = _normalize_list(legacy.get("system_notification_domains", []), kind="domain", field_name="system_notification_domains")
        system_notification_domains = system_notification_domains or []
        quarantine_domains, _ = _normalize_list(legacy.get("quarantine_domains", []), kind="domain", field_name="quarantine_domains")
        quarantine_domains = quarantine_domains or []
        held_domains, _ = _normalize_list(legacy.get("always_hold_domains", []), kind="domain", field_name="held_domains")
        held_domains = held_domains or []
        transfer_senders = []
        system_notification_senders = []
        quarantine_senders = []
        held_senders = []
        folders = dict(_DEFAULT_FOLDERS)

    return {
        "transfer_domains": transfer_domains,
        "system_notification_domains": system_notification_domains,
        "quarantine_domains": quarantine_domains,
        "held_domains": held_domains,
        "transfer_senders": transfer_senders,
        "system_notification_senders": system_notification_senders,
        "quarantine_senders": quarantine_senders,
        "held_senders": held_senders,
        "staff_round_robin": staff_rr,
        "apps_team_recipients": apps_recipients,
        "manager_recipients": manager_recipients,
        "folders": folders,
    }


def _save_domain_policy_payload(payload: dict) -> tuple[bool, str | None]:
    # Extract + validate
    staff_rr, err = _normalize_list(payload.get("staff_round_robin"), kind="email", field_name="staff_round_robin")
    if err:
        return False, err
    apps_recipients, err = _normalize_list(payload.get("apps_team_recipients"), kind="email", field_name="apps_team_recipients")
    if err:
        return False, err
    manager_recipients, err = _normalize_list(payload.get("manager_recipients"), kind="email", field_name="manager_recipients")
    if err:
        return False, err
    transfer_domains, err = _normalize_list(payload.get("transfer_domains"), kind="domain", field_name="transfer_domains")
    if err:
        return False, err
    system_notification_domains, err = _normalize_list(
        payload.get("system_notification_domains"),
        kind="domain",
        field_name="system_notification_domains",
    )
    if err:
        return False, err
    quarantine_domains, err = _normalize_list(payload.get("quarantine_domains"), kind="domain", field_name="quarantine_domains")
    if err:
        return False, err
    held_domains, err = _normalize_list(payload.get("held_domains"), kind="domain", field_name="held_domains")
    if err:
        return False, err
    transfer_senders, err = _normalize_list(payload.get("transfer_senders"), kind="email", field_name="transfer_senders")
    if err:
        return False, err
    system_notification_senders, err = _normalize_list(payload.get("system_notification_senders"), kind="email", field_name="system_notification_senders")
    if err:
        return False, err
    quarantine_senders, err = _normalize_list(payload.get("quarantine_senders"), kind="email", field_name="quarantine_senders")
    if err:
        return False, err
    held_senders, err = _normalize_list(payload.get("held_senders"), kind="email", field_name="held_senders")
    if err:
        return False, err
    folders, err = _validate_folders(payload.get("folders"))
    if err:
        return False, err

    staff_obj = {"staff": staff_rr, "off_rotation": [], "leave": []}
    apps_obj = {"recipients": apps_recipients}
    manager_obj = {"recipients": manager_recipients}
    system_buckets_obj = {
        "transfer_domains": transfer_domains,
        "system_notification_domains": system_notification_domains,
        "quarantine_domains": quarantine_domains,
        "held_domains": held_domains,
        "transfer_senders": transfer_senders,
        "system_notification_senders": system_notification_senders,
        "quarantine_senders": quarantine_senders,
        "held_senders": held_senders,
        "folders": folders,
    }

    for path, obj in (
        (config.STAFF_JSON, staff_obj),
        (config.APPS_TEAM_JSON, apps_obj),
        (config.MANAGER_CONFIG_JSON, manager_obj),
        (config.SYSTEM_BUCKETS_JSON, system_buckets_obj),
    ):
        ok, werr, _changed = _atomic_write_json_if_changed(path, obj)
        if not ok:
            return False, f"Failed to write {path.name}: {werr}"

    # Keep legacy staff.txt in sync so fallback path is always correct
    try:
        with open(config.STAFF_TXT, "w", encoding="utf-8") as f:
            for email in staff_rr:
                f.write(email + "\n")
    except Exception:
        pass  # non-fatal — staff.json is the canonical source

    return True, None

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
async def dashboard_endpoint(date_start: str | None = None, date_end: str | None = None,
                             staff: str | None = None):
    """Unified dashboard data — single call for all read-only data.

    Query params:
        date_start: YYYY-MM-DD (optional, defaults to today)
        date_end:   YYYY-MM-DD (optional, defaults to today)
        staff:      display name to filter activity feed (optional)
    """
    rows, csv_err = load_csv(_resolve_stats_csv_path())
    roster, _ = load_json(config.ROSTER_STATE_JSON)
    settings, _ = load_json(config.SETTINGS_OVERRIDES_JSON)
    hib_state, _ = load_json(config.HIB_WATCHDOG_JSON)
    # Prefer canonical staff.json; fall back to legacy staff.txt if missing/invalid.
    staff_cfg, _ = _load_staff_json()
    if staff_cfg:
        staff_list = staff_cfg.get("staff", [])
    else:
        staff_list = _load_list_from_legacy_txt(config.STAFF_TXT)
        staff_list, _ = _normalize_list(staff_list, kind="email", field_name="staff_round_robin")
        staff_list = staff_list or []

    rec_set = load_reconciled_set()
    payload = compute_dashboard(rows, roster, settings, staff_list, hib_state,
                                date_start=date_start, date_end=date_end,
                                staff_filter=staff, reconciled_set=rec_set)
    if csv_err:
        payload["warning"] = csv_err
    return payload


@app.get("/api/staff-export")
async def staff_export(name: str, date_start: str | None = None, date_end: str | None = None):
    """Download a CSV of all events for a given staff member in the date range."""
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="name parameter is required")

    rows, csv_err = load_csv(_resolve_stats_csv_path())
    if not rows:
        raise HTTPException(status_code=404, detail="No data available")

    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    ds = date_start or today
    de = date_end or today

    events = export_staff_events(rows, name, ds, de)

    # Build CSV in memory
    buf = io.StringIO()
    fieldnames = ["Date", "Time", "Type", "Subject", "Sender", "Source", "Risk Level", "Domain", "Duration"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(events)

    # Build filename: replace spaces with underscores
    safe_name = name.strip().replace(" ", "_")
    filename = f"{safe_name}_{ds}.csv"

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )




@app.get("/api/active")
async def active_rows(date_start: str | None = None, date_end: str | None = None,
                      staff: str | None = None):
    """Return likely-open ASSIGNED tickets for modal display."""
    rows, _ = load_csv(_resolve_stats_csv_path())

    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    ds = date_start or today
    de = date_end or today

    rec_set = load_reconciled_set()
    active = export_active_events(rows, ds, de, staff_name=staff,
                                  reconciled_set=rec_set)
    payload_rows = [
        {
            "date": r.get("Date", ""),
            "time": r.get("Time", ""),
            "sami_ref": r.get("SAMI Ref", ""),
            "staff": r.get("Staff", ""),
            "sender": r.get("Sender", ""),
            "domain": r.get("Domain", ""),
            "risk_level": r.get("Risk Level", ""),
            "subject": r.get("Subject", ""),
            "msg_key": r.get("Message Key", ""),
            "identity": r.get("Identity", ""),
        }
        for r in active
    ]

    # Additive: include reconciled entries for this context
    rec_state = load_reconciled()
    rec_entries = rec_state.get("reconciled", [])
    if staff:
        staff_lower = staff.strip().lower()
        rec_entries = [
            e for e in rec_entries
            if (e.get("staff_email") or "").strip().lower() == staff_lower
            or staff_lower in (e.get("staff_email") or "").lower()
        ]

    return {
        "rows": payload_rows,
        "count": len(payload_rows),
        "date_start": ds,
        "date_end": de,
        "reconciled": rec_entries,
    }


@app.get("/api/active-export")
async def active_export(date_start: str | None = None, date_end: str | None = None,
                        staff: str | None = None):
    """Download active-ticket CSV including SAMI reference codes."""
    rows, _ = load_csv(_resolve_stats_csv_path())

    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    ds = date_start or today
    de = date_end or today

    rec_set = load_reconciled_set()
    active = export_active_events(rows, ds, de, staff_name=staff,
                                  reconciled_set=rec_set)

    buf = io.StringIO()
    fieldnames = ["Date", "Time", "SAMI Ref", "Staff", "Sender", "Domain", "Risk Level", "Subject", "Message Key"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(active)

    if staff and staff.strip():
        safe_staff = re.sub(r"[^a-zA-Z0-9_\-]+", "_", staff.strip())
        filename = f"active_{safe_staff}_{ds}_{de}.csv"
    else:
        filename = f"active_{ds}_{de}.csv"

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# ── Reconciliation endpoints ──

@app.get("/api/staff/{email}/active")
async def staff_active(email: str, date_start: str | None = None,
                       date_end: str | None = None):
    """Return active tickets for a specific staff member, filtered by reconciliation."""
    rows, _ = load_csv(_resolve_stats_csv_path())

    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    ds = date_start or today
    de = date_end or today

    rec_set = load_reconciled_set()
    active = export_active_events(rows, ds, de, staff_name=email,
                                  reconciled_set=rec_set)
    payload_rows = [
        {
            "date": r.get("Date", ""),
            "time": r.get("Time", ""),
            "sami_ref": r.get("SAMI Ref", ""),
            "staff": r.get("Staff", ""),
            "sender": r.get("Sender", ""),
            "domain": r.get("Domain", ""),
            "risk_level": r.get("Risk Level", ""),
            "subject": r.get("Subject", ""),
            "msg_key": r.get("Message Key", ""),
            "identity": r.get("Identity", ""),
        }
        for r in active
    ]

    rec_state = load_reconciled()
    email_lower = email.strip().lower()
    rec_entries = [
        e for e in rec_state.get("reconciled", [])
        if (e.get("staff_email") or "").strip().lower() == email_lower
    ]

    return {
        "rows": payload_rows,
        "count": len(payload_rows),
        "date_start": ds,
        "date_end": de,
        "reconciled": rec_entries,
    }


@app.post("/api/reconcile")
async def reconcile_add(request: Request):
    """Mark an active item as reconciled."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    identity = (body.get("identity") or "").strip()
    staff_email = (body.get("staff_email") or "").strip()
    if not identity:
        raise HTTPException(status_code=400, detail="identity is required")
    if not staff_email:
        raise HTTPException(status_code=400, detail="staff_email is required")

    from datetime import datetime as _dt, timezone as _tz
    entry = {
        "identity": identity,
        "staff_email": staff_email.lower(),
        "ts": _dt.now(_tz.utc).isoformat(),
    }
    if body.get("sami_ref"):
        entry["sami_ref"] = body["sami_ref"].strip()
    if body.get("msg_key_norm"):
        entry["msg_key_norm"] = body["msg_key_norm"].strip().lower()
    if body.get("reason"):
        entry["reason"] = body["reason"].strip()

    ok, err = add_reconciled(entry)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to write reconciliation: {err}")

    return {"ok": True, "identity": identity}


@app.post("/api/reconcile/remove")
async def reconcile_remove(request: Request):
    """Remove a reconciled entry (undo)."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    identity = (body.get("identity") or "").strip()
    if not identity:
        raise HTTPException(status_code=400, detail="identity is required")

    ok, err = remove_reconciled(identity)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to update reconciliation: {err}")

    return {"ok": True, "identity": identity}


@app.post("/api/reconcile/all")
async def reconcile_all(request: Request):
    """Reconcile all currently active items (bulk). Zeroes out active count."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    reason = (body.get("reason") or "Reconcile all").strip()

    rows, _ = load_csv(_resolve_stats_csv_path())
    from datetime import datetime as _dt, timezone as _tz
    today = _dt.now().strftime("%Y-%m-%d")
    ds = body.get("date_start") or today
    de = body.get("date_end") or today
    staff_filter = body.get("staff") or None

    # Get current active rows (unfiltered by reconciliation)
    active = export_active_events(rows, ds, de, staff_name=staff_filter)

    ts = _dt.now(_tz.utc).isoformat()
    entries = []
    for r in active:
        identity = r.get("Identity", "")
        if not identity:
            continue
        entry = {
            "identity": identity,
            "staff_email": (r.get("Staff Email") or "").strip().lower(),
            "reason": reason,
            "ts": ts,
        }
        sami = r.get("SAMI Ref", "")
        if sami:
            entry["sami_ref"] = sami
        msg_key = r.get("Message Key", "")
        if msg_key:
            entry["msg_key_norm"] = msg_key
        entries.append(entry)

    if not entries:
        return {"ok": True, "count": 0}

    ok, err = add_reconciled_bulk(entries)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to bulk reconcile: {err}")

    return {"ok": True, "count": len(entries)}


@app.get("/api/config/domain_policy")
async def get_domain_policy_config():
    # Canonical merged view used by the Domain Policy & Staff panel.
    return _build_domain_policy_payload()


@app.post("/api/config/domain_policy")
async def post_domain_policy_config(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid JSON body"})
    if not isinstance(payload, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Body must be an object"})
    ok, err = _save_domain_policy_payload(payload)
    if not ok:
        return JSONResponse(status_code=400, content={"ok": False, "error": err or "Invalid config"})
    return {"ok": True}


@app.get("/api/staff")
async def get_staff():
    payload = _build_domain_policy_payload()
    return {"staff": payload.get("staff_round_robin", [])}


class StaffRequest(BaseModel):
    email: str


@app.post("/api/staff")
async def post_staff(body: StaffRequest):
    email, err = _normalize_email(body.email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    staff = payload.get("staff_round_robin", [])
    if email in staff:
        raise HTTPException(status_code=400, detail=f"Already exists: {email}")
    staff.append(email)
    payload["staff_round_robin"] = staff
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Added: {email}", "staff": _build_domain_policy_payload().get("staff_round_robin", [])}


@app.delete("/api/staff/{email}")
async def delete_staff(email: str):
    email, err = _normalize_email(email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    staff = payload.get("staff_round_robin", [])
    if email not in staff:
        raise HTTPException(status_code=404, detail=f"Not found: {email}")
    staff = [e for e in staff if e != email]
    payload["staff_round_robin"] = staff
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Removed: {email}", "staff": _build_domain_policy_payload().get("staff_round_robin", [])}


@app.get("/api/managers")
async def get_managers():
    payload = _build_domain_policy_payload()
    return {"managers": payload.get("manager_recipients", [])}


@app.post("/api/managers")
async def post_manager(body: StaffRequest):
    email, err = _normalize_email(body.email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    managers = payload.get("manager_recipients", [])
    if email in managers:
        raise HTTPException(status_code=400, detail=f"Already exists: {email}")
    managers.append(email)
    payload["manager_recipients"] = managers
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Added: {email}", "managers": _build_domain_policy_payload().get("manager_recipients", [])}


@app.delete("/api/managers/{email}")
async def delete_manager(email: str):
    email, err = _normalize_email(email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    managers = payload.get("manager_recipients", [])
    if email not in managers:
        raise HTTPException(status_code=404, detail=f"Not found: {email}")
    managers = [e for e in managers if e != email]
    payload["manager_recipients"] = managers
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Removed: {email}", "managers": _build_domain_policy_payload().get("manager_recipients", [])}


@app.get("/api/apps")
async def get_apps():
    payload = _build_domain_policy_payload()
    return {"apps": payload.get("apps_team_recipients", [])}


@app.post("/api/apps")
async def post_apps(body: StaffRequest):
    email, err = _normalize_email(body.email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    apps = payload.get("apps_team_recipients", [])
    if email in apps:
        raise HTTPException(status_code=400, detail=f"Already exists: {email}")
    apps.append(email)
    payload["apps_team_recipients"] = apps
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Added: {email}", "apps": _build_domain_policy_payload().get("apps_team_recipients", [])}


@app.delete("/api/apps/{email}")
async def delete_apps(email: str):
    email, err = _normalize_email(email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    payload = _build_domain_policy_payload()
    apps = payload.get("apps_team_recipients", [])
    if email not in apps:
        raise HTTPException(status_code=404, detail=f"Not found: {email}")
    apps = [e for e in apps if e != email]
    payload["apps_team_recipients"] = apps
    ok, serr = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=serr or "Failed to save config")
    return {"message": f"Removed: {email}", "apps": _build_domain_policy_payload().get("apps_team_recipients", [])}


# ── Domain policy endpoints ──

_DOMAIN_BUCKETS = {
    "external_image_request": "transfer_domains",
    "system_notification": "system_notification_domains",
    "always_hold": "held_domains",
    "quarantine": "quarantine_domains",
}

_SENDER_BUCKETS = {
    "external_image_request": "transfer_senders",
    "system_notification": "system_notification_senders",
    "always_hold": "held_senders",
    "quarantine": "quarantine_senders",
}


class DomainRequest(BaseModel):
    domain: str


class SenderRequest(BaseModel):
    sender: str


def _read_domain_policy() -> dict:
    """Legacy shim (read-only). Prefer canonical system_buckets.json via _build_domain_policy_payload()."""
    return _read_domain_policy_legacy()


def _write_domain_policy(data: dict) -> None:
    """Deprecated: dashboard must not write domain_policy.json."""
    raise HTTPException(status_code=400, detail="domain_policy.json is bot-owned (write via canonical JSON configs)")


def _validate_bucket(bucket: str) -> str:
    """Return the JSON key for the bucket, or raise 404."""
    key = _DOMAIN_BUCKETS.get(bucket)
    if not key:
        raise HTTPException(status_code=404, detail=f"Unknown bucket: {bucket}")
    return key


def _validate_domain(domain: str) -> str:
    """Normalise and validate a domain string."""
    val, err = _normalize_domain(domain)
    if err:
        raise HTTPException(status_code=400, detail=err)
    assert val is not None
    return val


def _validate_sender_bucket(bucket: str) -> str:
    """Return the JSON key for the sender bucket, or raise 404."""
    key = _SENDER_BUCKETS.get(bucket)
    if not key:
        raise HTTPException(status_code=404, detail=f"Unknown bucket: {bucket}")
    return key


def _validate_sender(sender: str) -> str:
    """Normalise and validate a sender email string."""
    val, err = _normalize_email(sender)
    if err:
        raise HTTPException(status_code=400, detail=err)
    assert val is not None
    return val


@app.get("/api/domains/{bucket}")
async def get_domains(bucket: str):
    key = _validate_bucket(bucket)
    payload = _build_domain_policy_payload()
    return {"domains": payload.get(key, [])}


@app.post("/api/domains/{bucket}")
async def add_domain(bucket: str, body: DomainRequest):
    key = _validate_bucket(bucket)
    domain = _validate_domain(body.domain)
    payload = _build_domain_policy_payload()
    domains = payload.get(key, [])
    if domain in domains:
        raise HTTPException(status_code=400, detail=f"{domain} already in {bucket}")
    domains.append(domain)
    payload[key] = domains
    ok, err = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to save config")
    logger.info("Added domain %s to %s", domain, bucket)
    return {"domains": _build_domain_policy_payload().get(key, [])}


@app.delete("/api/domains/{bucket}/{domain:path}")
async def remove_domain(bucket: str, domain: str):
    key = _validate_bucket(bucket)
    domain = _validate_domain(domain)
    payload = _build_domain_policy_payload()
    domains = payload.get(key, [])
    if domain not in domains:
        raise HTTPException(status_code=404, detail=f"{domain} not found in {bucket}")
    domains = [d for d in domains if d != domain]
    payload[key] = domains
    ok, err = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to save config")
    logger.info("Removed domain %s from %s", domain, bucket)
    return {"domains": _build_domain_policy_payload().get(key, [])}


# ── Sender override endpoints ──

@app.get("/api/senders/{bucket}")
async def get_senders(bucket: str):
    key = _validate_sender_bucket(bucket)
    payload = _build_domain_policy_payload()
    return {"senders": payload.get(key, [])}


@app.post("/api/senders/{bucket}")
async def add_sender(bucket: str, body: SenderRequest):
    key = _validate_sender_bucket(bucket)
    sender = _validate_sender(body.sender)
    payload = _build_domain_policy_payload()
    senders = payload.get(key, [])
    if sender in senders:
        raise HTTPException(status_code=400, detail=f"{sender} already in {bucket}")
    senders.append(sender)
    payload[key] = senders
    ok, err = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to save config")
    logger.info("Added sender %s to %s", sender, bucket)
    return {"senders": _build_domain_policy_payload().get(key, [])}


@app.delete("/api/senders/{bucket}/{sender:path}")
async def remove_sender(bucket: str, sender: str):
    key = _validate_sender_bucket(bucket)
    sender = _validate_sender(sender)
    payload = _build_domain_policy_payload()
    senders = payload.get(key, [])
    if sender not in senders:
        raise HTTPException(status_code=404, detail=f"{sender} not found in {bucket}")
    senders = [s for s in senders if s != sender]
    payload[key] = senders
    ok, err = _save_domain_policy_payload(payload)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to save config")
    logger.info("Removed sender %s from %s", sender, bucket)
    return {"senders": _build_domain_policy_payload().get(key, [])}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "csv": get_file_info(config.DAILY_STATS_CSV),
        "staff_json": get_file_info(config.STAFF_JSON),
        "apps_team_json": get_file_info(config.APPS_TEAM_JSON),
        "manager_config_json": get_file_info(config.MANAGER_CONFIG_JSON),
        "system_buckets_json": get_file_info(config.SYSTEM_BUCKETS_JSON),
        "settings_overrides": get_file_info(config.SETTINGS_OVERRIDES_JSON),
        "staff_legacy_txt": get_file_info(config.STAFF_TXT),
        "roster_state": get_file_info(config.ROSTER_STATE_JSON),
    }


@app.get("/api/settings")
async def get_settings():
    settings, _ = _safe_load_json_direct(config.SETTINGS_OVERRIDES_JSON)
    if not isinstance(settings, dict):
        settings = {}
    return {
        "manager_cc_addr": settings.get("manager_cc_addr", "") if settings else "",
        "apps_cc_addr": settings.get("apps_cc_addr", "") if settings else "",
    }


class SettingUpdate(BaseModel):
    key: str
    value: str


@app.post("/api/settings")
async def update_setting(body: SettingUpdate):
    # Validate key
    if body.key not in ["manager_cc_addr", "apps_cc_addr"]:
        raise HTTPException(status_code=400, detail="Invalid setting key")

    # Validate email format
    if body.value and "@" not in body.value:
        raise HTTPException(status_code=400, detail="Invalid email format")

    # Load current settings (no cache)
    settings, _ = _safe_load_json_direct(config.SETTINGS_OVERRIDES_JSON)
    if not isinstance(settings, dict):
        settings = {}

    # Update setting
    settings[body.key] = body.value

    # Save back (atomic)
    ok, err = _atomic_write_json(config.SETTINGS_OVERRIDES_JSON, settings)
    if not ok:
        logger.error("Failed to save settings: %s", err)
        raise HTTPException(status_code=500, detail=err or "Failed to save settings")
    logger.info("Updated setting %s", body.key)
    return {"message": f"Updated {body.key}", "settings": settings}


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
