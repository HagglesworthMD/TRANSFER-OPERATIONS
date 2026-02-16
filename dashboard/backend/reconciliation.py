"""Dashboard-managed reconciliation state for active-job balancing.

State file: reconciled_identities.json
Schema:
  {
    "version": 1,
    "reconciled": [
      {
        "identity": "SAMI-ABC123" or "msg:<key>",
        "staff_email": "<email>",
        "sami_ref": "SAMI-ABC123",      # optional
        "msg_key_norm": "<lower>",       # optional
        "reason": "<short text>",        # optional
        "ts": "<iso8601>"
      }
    ]
  }
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)

_EMPTY_STATE: dict = {"version": 1, "reconciled": []}


# ── Atomic I/O (mirrors server.py helpers) ──────────────────────────

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


# ── Validation ───────────────────────────────────────────────────────

def _validate(data) -> bool:
    """Minimal schema check: version == 1 and reconciled is a list."""
    if not isinstance(data, dict):
        return False
    if data.get("version") != 1:
        return False
    if not isinstance(data.get("reconciled"), list):
        return False
    return True


# ── Public API ───────────────────────────────────────────────────────

def load_reconciled() -> dict:
    """Load reconciled state. Returns empty state if missing or corrupt."""
    data, err = _safe_load_json_direct(config.RECONCILED_JSON)
    if err is not None:
        if "Missing file" not in err:
            logger.warning("Reconciled state load error: %s", err)
        return {"version": 1, "reconciled": []}
    if not _validate(data):
        logger.warning("Reconciled state corrupt/invalid schema, returning empty")
        return {"version": 1, "reconciled": []}
    return data


def load_reconciled_set() -> set[str]:
    """Return set of reconciled identity strings."""
    state = load_reconciled()
    return {
        entry["identity"]
        for entry in state["reconciled"]
        if isinstance(entry, dict) and "identity" in entry
    }


def add_reconciled(entry: dict) -> tuple[bool, str | None]:
    """Add a reconciled entry (deduplicate by identity). Atomic write."""
    identity = entry.get("identity")
    if not identity:
        return False, "Missing identity"

    state = load_reconciled()

    # Deduplicate: replace existing entry with same identity
    state["reconciled"] = [
        e for e in state["reconciled"]
        if not (isinstance(e, dict) and e.get("identity") == identity)
    ]

    # Ensure timestamp
    if "ts" not in entry:
        entry["ts"] = datetime.now(timezone.utc).isoformat()

    state["reconciled"].append(entry)
    return _atomic_write_json(config.RECONCILED_JSON, state)


def remove_reconciled(identity: str) -> tuple[bool, str | None]:
    """Remove a reconciled entry by identity. Atomic write."""
    if not identity:
        return False, "Missing identity"

    state = load_reconciled()
    before = len(state["reconciled"])
    state["reconciled"] = [
        e for e in state["reconciled"]
        if not (isinstance(e, dict) and e.get("identity") == identity)
    ]
    if len(state["reconciled"]) == before:
        return True, None  # Not found, no-op is fine
    return _atomic_write_json(config.RECONCILED_JSON, state)
