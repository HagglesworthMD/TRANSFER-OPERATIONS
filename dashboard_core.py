import json
from datetime import datetime
from pathlib import Path

import pandas as pd

EXPECTED_DAILY_STATS_COLUMNS = [
    "Date",
    "Time",
    "Subject",
    "Assigned To",
    "Sender",
    "Risk Level",
]


def load_daily_stats_csv(csv_path):
    empty = pd.DataFrame(columns=EXPECTED_DAILY_STATS_COLUMNS)
    try:
        if not Path(csv_path).exists():
            empty.attrs["error"] = f"missing: {csv_path}"
            return empty
        df = pd.read_csv(csv_path)
        missing = [c for c in EXPECTED_DAILY_STATS_COLUMNS if c not in df.columns]
        if missing:
            empty.attrs["error"] = f"schema_mismatch: missing {missing} in {csv_path}"
            return empty
        df.attrs["error"] = None
        return df
    except Exception as e:
        empty.attrs["error"] = f"{e}"
        return empty


def load_json_safe(path):
    if not Path(path).exists():
        return None, "missing"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        return None, str(e)


def load_state_snapshot(base_dir):
    base = Path(base_dir)
    data = {}
    errors = {}
    for name in ["processed_ledger.json", "roster_state.json", "urgent_watchdog.json", "settings_overrides.json"]:
        value, err = load_json_safe(base / name)
        data[name] = value
        errors[name] = err
    return {"data": data, "errors": errors}


def compute_summary(stats_df, snapshot):
    summary = {"total_rows": 0, "today_rows": 0, "critical_count": 0, "urgent_count": 0}
    if stats_df is None or stats_df.empty:
        return summary
    summary["total_rows"] = len(stats_df)
    today = datetime.now().strftime("%Y-%m-%d")
    if "Date" in stats_df.columns:
        summary["today_rows"] = len(stats_df[stats_df["Date"] == today])
    if "Risk Level" in stats_df.columns:
        summary["critical_count"] = len(stats_df[stats_df["Risk Level"] == "critical"])
        summary["urgent_count"] = len(stats_df[stats_df["Risk Level"] == "urgent"])
    return summary


def compute_completions_today(stats_df, now_dt):
    if stats_df is None or "Date" not in stats_df.columns or "Assigned To" not in stats_df.columns:
        return 0
    try:
        today_str = now_dt.strftime("%Y-%m-%d")
        df_today = stats_df[stats_df["Date"] == today_str]
        return len(df_today[df_today["Assigned To"] == "completed"])
    except Exception:
        return 0
