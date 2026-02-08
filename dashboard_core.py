import json
from datetime import datetime, timedelta
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


def format_duration_human(seconds_value):
    try:
        total = int(float(seconds_value))
    except Exception:
        return ""
    if total < 0:
        total = 0
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def prepare_event_frame(df):
    if df is None:
        return pd.DataFrame(), 0
    events = df.copy()
    required_cols = ["event_type", "msg_key", "assigned_ts", "completed_ts", "duration_sec", "assigned_to"]
    for col in required_cols:
        if col not in events.columns:
            events[col] = ""
    if "Assigned To" not in events.columns:
        events["Assigned To"] = ""
    if "domain_bucket" not in events.columns:
        events["domain_bucket"] = ""
    if "action" not in events.columns:
        events["action"] = ""
    events["event_type"] = events["event_type"].fillna("").astype(str).str.strip()
    events["event_type_norm"] = events["event_type"].str.upper()
    events["msg_key"] = events["msg_key"].fillna("").astype(str).str.strip().str.lower()
    events["assigned_to"] = events["assigned_to"].fillna("").astype(str).str.strip().str.lower()
    assigned_to_fallback = events["Assigned To"].fillna("").astype(str).str.strip().str.lower()
    events.loc[events["assigned_to"] == "", "assigned_to"] = assigned_to_fallback
    events["assigned_ts_dt"] = pd.to_datetime(events["assigned_ts"], errors="coerce")
    events["completed_ts_dt"] = pd.to_datetime(events["completed_ts"], errors="coerce")
    events["duration_sec_num"] = pd.to_numeric(events["duration_sec"], errors="coerce")
    fill_duration = events["duration_sec_num"].isna() & events["assigned_ts_dt"].notna() & events["completed_ts_dt"].notna()
    if fill_duration.any():
        computed = (events.loc[fill_duration, "completed_ts_dt"] - events.loc[fill_duration, "assigned_ts_dt"]).dt.total_seconds()
        events.loc[fill_duration, "duration_sec_num"] = computed.clip(lower=0)
    events["event_ts_dt"] = events["assigned_ts_dt"]
    completed_with_ts = events["event_type_norm"].eq("COMPLETED") & events["completed_ts_dt"].notna()
    events.loc[completed_with_ts, "event_ts_dt"] = events.loc[completed_with_ts, "completed_ts_dt"]
    dropped_bad_rows = int((events["msg_key"] == "").sum())
    events = events[events["msg_key"] != ""].copy()
    return events, dropped_bad_rows


def apply_time_window(events, window_value, now_dt):
    if events is None or events.empty:
        return events
    if window_value == "All":
        return events
    if window_value == "Today":
        start_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif window_value == "Last 4h":
        start_dt = now_dt - timedelta(hours=4)
    else:
        start_dt = now_dt - timedelta(hours=24)
    return events[(events["event_ts_dt"].notna()) & (events["event_ts_dt"] >= start_dt)].copy()


def build_work_queue_views(events, now_dt):
    if events is None or events.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    assigned = events[(events["event_type_norm"].eq("ASSIGNED")) & (events["assigned_ts_dt"].notna())].copy()
    if assigned.empty:
        assigned_latest = assigned
    else:
        assigned_latest = assigned.sort_values("assigned_ts_dt").drop_duplicates(subset=["msg_key"], keep="last").copy()
    completed = events[events["event_type_norm"].eq("COMPLETED")].copy()
    completed_keys = set(completed["msg_key"].dropna().astype(str).tolist())
    active = assigned_latest[~assigned_latest["msg_key"].isin(completed_keys)].copy()
    if not active.empty:
        active["live_age_sec"] = (now_dt - active["assigned_ts_dt"]).dt.total_seconds().clip(lower=0)
    else:
        active["live_age_sec"] = pd.Series(dtype=float)
    if not completed.empty:
        lookup = assigned_latest[["msg_key", "assigned_to", "assigned_ts_dt"]].rename(
            columns={"assigned_to": "assigned_to_lookup", "assigned_ts_dt": "assigned_ts_lookup"}
        )
        completed = completed.merge(lookup, on="msg_key", how="left")
        completed["assigned_to"] = completed["assigned_to"].where(completed["assigned_to"] != "", completed["assigned_to_lookup"])
        completed["assigned_ts_dt"] = completed["assigned_ts_dt"].where(completed["assigned_ts_dt"].notna(), completed["assigned_ts_lookup"])
        fill_duration = completed["duration_sec_num"].isna() & completed["assigned_ts_dt"].notna() & completed["completed_ts_dt"].notna()
        if fill_duration.any():
            computed = (completed.loc[fill_duration, "completed_ts_dt"] - completed.loc[fill_duration, "assigned_ts_dt"]).dt.total_seconds()
            completed.loc[fill_duration, "duration_sec_num"] = computed.clip(lower=0)
        completed["duration_human"] = completed["duration_sec_num"].apply(format_duration_human)
        completed = completed.sort_values("completed_ts_dt", ascending=False, na_position="last")
    return active, completed, assigned_latest


def compute_per_staff_kpis(source_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Staff Member", "Assigned", "Completed", "Active", "Median (min)", "P90 (min)"]
    empty = pd.DataFrame(columns=columns)
    if source_df is None or source_df.empty:
        return empty

    has_event_schema = all(
        col in source_df.columns
        for col in ["event_type", "msg_key", "assigned_ts", "completed_ts", "duration_sec", "assigned_to"]
    )
    if has_event_schema:
        events_df, _dropped_bad_rows = prepare_event_frame(source_df)
    else:
        events_df = source_df.copy()
    if events_df is None or events_df.empty:
        return empty

    events_df = events_df.copy()
    event_type = (
        events_df["event_type"]
        if "event_type" in events_df.columns
        else pd.Series("", index=events_df.index, dtype="object")
    )
    events_df["event_type_norm"] = event_type.fillna("").astype(str).str.strip().str.upper()
    action_norm = (
        events_df["Action"].fillna("").astype(str).str.strip().str.upper()
        if "Action" in events_df.columns
        else pd.Series("", index=events_df.index, dtype="object")
    )
    blank_et = events_df["event_type_norm"].eq("")
    is_completion_action = action_norm.isin(["STAFF_COMPLETED_CONFIRMATION", "COMPLETION_SUBJECT_KEYWORD"])
    events_df.loc[blank_et & is_completion_action, "event_type_norm"] = "COMPLETED"
    events_df = events_df[events_df["event_type_norm"].isin(["ASSIGNED", "COMPLETED"])].copy()
    if events_df.empty:
        return empty

    assigned_to_norm = (
        events_df["assigned_to"]
        if "assigned_to" in events_df.columns
        else pd.Series("", index=events_df.index, dtype="object")
    )
    assigned_to_norm = assigned_to_norm.fillna("").astype(str).str.strip().str.lower()
    sender_norm = (
        events_df["Sender"]
        if "Sender" in events_df.columns
        else pd.Series("", index=events_df.index, dtype="object")
    )
    sender_norm = sender_norm.fillna("").astype(str).str.strip().str.lower()

    events_df["staff_email"] = assigned_to_norm
    completed_mask = events_df["event_type_norm"].eq("COMPLETED")
    sender_has_email = sender_norm.str.contains("@", na=False)
    needs_sender = completed_mask & (events_df["staff_email"] == "")
    events_df.loc[needs_sender & sender_has_email, "staff_email"] = sender_norm[needs_sender & sender_has_email]
    events_df = events_df[events_df["staff_email"] != ""].copy()
    if events_df.empty:
        return empty

    grouped = (
        events_df.groupby(["staff_email", "event_type_norm"])
        .size()
        .unstack(fill_value=0)
    )
    if grouped.empty:
        return empty

    kpi_df = pd.DataFrame(index=grouped.index)
    kpi_df["Assigned"] = grouped["ASSIGNED"] if "ASSIGNED" in grouped.columns else 0
    kpi_df["Completed"] = grouped["COMPLETED"] if "COMPLETED" in grouped.columns else 0
    kpi_df["Assigned"] = kpi_df["Assigned"].astype(int)
    kpi_df["Completed"] = kpi_df["Completed"].astype(int)
    kpi_df["Active"] = (kpi_df["Assigned"] - kpi_df["Completed"]).clip(lower=0).astype(int)

    completed_df = events_df[events_df["event_type_norm"] == "COMPLETED"].copy()
    if "duration_sec" in completed_df.columns:
        completed_df["duration_sec_num"] = pd.to_numeric(completed_df["duration_sec"], errors="coerce")
    else:
        completed_df["duration_sec_num"] = pd.Series(index=completed_df.index, dtype=float)
    completed_df["duration_min"] = completed_df["duration_sec_num"] / 60.0
    duration_stats = (
        completed_df.groupby("staff_email")["duration_min"].agg(
            **{"Median (min)": "median", "P90 (min)": lambda s: s.quantile(0.9)}
        ).round(1)
        if not completed_df.empty
        else pd.DataFrame(columns=["Median (min)", "P90 (min)"])
    )
    kpi_df = kpi_df.join(duration_stats, how="left")

    kpi_df["Staff Member"] = kpi_df.index.map(
        lambda email: str(email).split("@", 1)[0].replace(".", " ").replace("_", " ").strip().title()
    )
    kpi_df = kpi_df.reset_index(drop=True)
    kpi_df = kpi_df[columns]
    kpi_df = kpi_df.sort_values(by=["Assigned", "Staff Member"], ascending=[False, True], kind="stable").reset_index(drop=True)
    return kpi_df
