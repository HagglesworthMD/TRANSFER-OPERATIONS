import argparse
import copy
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta


TERMINAL_ASSIGNEES = {
    "applications_direct",
    "bot",
    "completed",
    "error",
    "hib",
    "hold",
    "manager_review",
    "non_actionable",
    "quarantined",
    "skipped",
    "system_notification",
}


def _atomic_write_json(path, data):
    dir_name = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=dir_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


def _load_ledger(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Ledger file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("processed_ledger.json is not a JSON object")
    return data


def _find_candidate(ledger):
    for key in sorted(ledger.keys()):
        entry = ledger.get(key)
        if not isinstance(entry, dict):
            continue

        assigned_to = str(entry.get("assigned_to") or "").strip().lower()
        if not assigned_to:
            continue
        if assigned_to in TERMINAL_ASSIGNEES:
            continue
        if "@" not in assigned_to:
            continue
        if str(entry.get("stale_last_reloop_at") or "").strip():
            continue

        # Outlook move-back in stale reloop requires resolvable item identity.
        entry_id = str(entry.get("entry_id") or "").strip()
        if not entry_id:
            continue

        return key, entry
    return None, None


def main():
    parser = argparse.ArgumentParser(description="Inject one stale assignment test record.")
    parser.add_argument("--ledger", default="processed_ledger.json")
    parser.add_argument("--backup", default="stale_reloop_test_backup.json")
    parser.add_argument("--hours", type=int, default=14)
    args = parser.parse_args()

    try:
        if os.path.exists(args.backup):
            print(f"Backup already exists: {args.backup}")
            print("Refusing to continue. Restore or remove the backup file first.")
            return 2

        ledger = _load_ledger(args.ledger)
        key, entry = _find_candidate(ledger)
        if not key:
            print("No eligible staff-owned ledger entry found for stale injection.")
            print("Criteria: assigned_to has '@', not terminal, no stale_last_reloop_at, has entry_id.")
            return 3

        original_entry = copy.deepcopy(entry)
        injected_ts = (datetime.now() - timedelta(hours=args.hours)).isoformat()
        entry["ts"] = injected_ts
        ledger[key] = entry

        backup_obj = {
            "created_at": datetime.now().isoformat(),
            "ledger_path": args.ledger,
            "modified_key": key,
            "sami_id": str(entry.get("sami_id") or "").strip(),
            "original_entry": original_entry,
            "injected_ts": injected_ts,
        }

        _atomic_write_json(args.backup, backup_obj)
        _atomic_write_json(args.ledger, ledger)

        print("Injected stale test job:")
        print(f"key={key}")
        print(f"sami_id={str(entry.get('sami_id') or '').strip()}")
        print(f"assigned_to={str(entry.get('assigned_to') or '').strip()}")
        print(f"ts={injected_ts}")
        return 0
    except Exception as e:
        print(f"Injection failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
