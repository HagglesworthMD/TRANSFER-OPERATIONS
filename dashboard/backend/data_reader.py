"""CSV / JSON file loading with mtime-based caching."""

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── mtime cache ──
_cache: dict[str, dict[str, Any]] = {}


def _read_with_cache(path: Path, parser):
    """Return cached data if file mtime hasn't changed, else re-parse."""
    key = str(path)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        logger.warning("File not found: %s", path)
        return None, f"File not found: {path}"

    cached = _cache.get(key)
    if cached and cached["mtime"] == mtime:
        return cached["data"], None

    try:
        data = parser(path)
        _cache[key] = {"mtime": mtime, "data": data}
        return data, None
    except Exception as e:
        logger.exception("Error reading %s", path)
        # Return last cached data if available
        if cached:
            return cached["data"], f"Using stale cache: {e}"
        return None, str(e)


def load_csv(path: Path) -> tuple[list[dict] | None, str | None]:
    """Load CSV as list of row dicts. Returns (rows, error)."""
    def parser(p):
        with open(p, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    return _read_with_cache(path, parser)


def load_json(path: Path) -> tuple[dict | None, str | None]:
    """Load JSON file. Returns (data, error)."""
    def parser(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return _read_with_cache(path, parser)


def get_file_info(path: Path) -> dict:
    """Return mtime and existence info for health checks."""
    try:
        stat = os.stat(path)
        return {
            "exists": True,
            "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "size_bytes": stat.st_size,
        }
    except OSError:
        return {"exists": False, "mtime": None, "size_bytes": 0}
