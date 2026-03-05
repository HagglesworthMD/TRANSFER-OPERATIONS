import argparse
import json
import os
import sys
import tempfile


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


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Restore stale reloop test backup.")
    parser.add_argument("--ledger", default="processed_ledger.json")
    parser.add_argument("--backup", default="stale_reloop_test_backup.json")
    args = parser.parse_args()

    try:
        if not os.path.exists(args.backup):
            print(f"Backup file not found: {args.backup}")
            return 2

        backup = _load_json(args.backup)
        if not isinstance(backup, dict):
            print("Backup file is not a JSON object.")
            return 3

        modified_key = backup.get("modified_key")
        original_entry = backup.get("original_entry")
        sami_id = str(backup.get("sami_id") or "").strip()
        if not modified_key or not isinstance(original_entry, dict):
            print("Backup missing required fields: modified_key/original_entry.")
            return 4

        if not os.path.exists(args.ledger):
            print(f"Ledger file not found: {args.ledger}")
            return 5

        ledger = _load_json(args.ledger)
        if not isinstance(ledger, dict):
            print("Ledger file is not a JSON object.")
            return 6

        ledger[modified_key] = original_entry
        _atomic_write_json(args.ledger, ledger)
        os.remove(args.backup)

        print("Restored stale test job:")
        print(f"key={modified_key}")
        print(f"sami_id={sami_id}")
        return 0
    except Exception as e:
        print(f"Restore failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
