"""KPI computation — pure stdlib, no pandas.

Ported from dashboard_core.py: prepare_event_frame, compute_per_staff_kpis,
format_duration_human, plus new helpers for charts/feeds.
"""

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from . import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def format_duration_human(seconds_value) -> str:
    try:
        total = int(float(seconds_value))
    except (TypeError, ValueError):
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


def _parse_ts(val: str | None) -> datetime | None:
    if not val or not isinstance(val, str) or not val.strip():
        return None
    val = val.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Simple percentile on a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    k = (n - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def _median(vals: list[float]) -> float:
    return _percentile(vals, 0.5)


# ── Business-hours constants ──
_BH_START_H, _BH_START_M = 8, 30   # 08:30
_BH_END_H,   _BH_END_M   = 17, 0  # 17:00
_BH_DAY_SEC = ((_BH_END_H * 60 + _BH_END_M) - (_BH_START_H * 60 + _BH_START_M)) * 60  # 30600


def _business_seconds(start: datetime, end: datetime) -> float:
    """Return seconds between *start* and *end* that fall inside
    business hours (08:30-17:00 Mon-Fri).  Returns 0 if end <= start
    or no business time elapsed."""
    if end <= start:
        return 0.0

    from datetime import time as _time, timedelta as _td

    bh_open  = _time(_BH_START_H, _BH_START_M)
    bh_close = _time(_BH_END_H,   _BH_END_M)
    total = 0.0
    cur = start

    # Cap to a reasonable max (30 calendar days) to avoid runaway loops
    if (end - start).days > 30:
        return 0.0

    while cur < end:
        # Skip weekends
        if cur.weekday() >= 5:
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
            continue

        day_open  = cur.replace(hour=_BH_START_H, minute=_BH_START_M, second=0, microsecond=0)
        day_close = cur.replace(hour=_BH_END_H,   minute=_BH_END_M,   second=0, microsecond=0)

        # Effective window for this day
        win_start = max(cur, day_open)
        win_end   = min(end, day_close)

        if win_start < win_end:
            total += (win_end - win_start).total_seconds()

        # Advance to next calendar day 00:00
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)

    return total


def _staff_display_name(email: str) -> str:
    local = email.split("@")[0] if "@" in email else email
    return local.replace(".", " ").replace("_", " ").strip().title()


# ── Sender → source label mapping ──
_SENDER_DOMAIN_MAP = {
    "jonesradiology.com.au": "Jones",
    "drjones.com.au": "Jones",
    "jonesradiology.atlassian.net": "Jones",
    "bensonradiology.com.au": "Bensons",
    "radiologysa.com.au": "RadSA",
    "i-med.com.au": "I-MED",
}

_system_notification_domains: set[str] | None = None


def _load_system_domains() -> set[str]:
    global _system_notification_domains
    if _system_notification_domains is not None:
        return _system_notification_domains
    from . import data_reader
    data, _ = data_reader.load_json(config.SYSTEM_BUCKETS_JSON)
    if data:
        _system_notification_domains = {
            d.lower().strip() for d in data.get("system_notification_domains", [])
            if "@" not in d  # skip full-address entries like quarantine@...
        }
    else:
        _system_notification_domains = set()
    return _system_notification_domains


def _sender_display_name(email: str) -> str:
    """Map sender email to a friendly source label."""
    if not email or "@" not in email:
        return "Unknown"
    domain = email.split("@")[1].lower().strip()

    # Known domains
    label = _SENDER_DOMAIN_MAP.get(domain)
    if label:
        return label

    # System notification domains
    sys_domains = _load_system_domains()
    if domain in sys_domains:
        return "System"

    # *.gov.au → Internal
    if domain.endswith(".gov.au"):
        return "Internal"

    # Fallback: titlecase first part of domain
    return domain.split(".")[0].title()


def _compute_hib_burst_status(hib_state: dict | None) -> dict:
    """Compute HIB burst status from hib_watchdog.json state.

    Returns dict with:
    - count: number of HIB messages in 30-min window
    - status: "normal" (0-9), "elevated" (10-14), "burst" (15+)
    - threshold: 15
    - window_min: 30
    - last_alert_human: time since last alert (if any)
    """
    now = datetime.now()
    window_min = 30
    threshold = 15

    if not hib_state:
        return {
            "count": 0,
            "status": "normal",
            "threshold": threshold,
            "window_min": window_min,
            "last_alert_human": None,
        }

    # Parse hib_events and filter to 30-minute window
    hib_events = hib_state.get("hib_events", [])
    count = 0
    if hib_events:
        window_start = now - timedelta(minutes=window_min)
        for ts_str in hib_events:
            ts = _parse_ts(ts_str)
            if ts and ts >= window_start:
                count += 1

    # Determine status
    if count >= threshold:
        status = "burst"
    elif count >= 10:
        status = "elevated"
    else:
        status = "normal"

    # Calculate time since last alert
    last_alert_human = None
    last_alert_iso = hib_state.get("last_alert_iso")
    if last_alert_iso:
        last_alert_ts = _parse_ts(last_alert_iso)
        if last_alert_ts:
            delta = now - last_alert_ts
            minutes_ago = int(delta.total_seconds() / 60)
            if minutes_ago < 60:
                last_alert_human = f"{minutes_ago}m ago"
            elif minutes_ago < 1440:
                hours_ago = minutes_ago // 60
                last_alert_human = f"{hours_ago}h ago"
            else:
                days_ago = minutes_ago // 1440
                last_alert_human = f"{days_ago}d ago"

    return {
        "count": count,
        "status": status,
        "threshold": threshold,
        "window_min": window_min,
        "last_alert_human": last_alert_human,
    }


# ─────────────────────────────────────────────
# Event normalisation (port of prepare_event_frame)
# ─────────────────────────────────────────────

def _normalise_rows(rows: list[dict]) -> list[dict]:
    """Normalise raw CSV rows into event dicts with computed fields."""
    out = []
    for row in rows:
        event_type_raw = (row.get("event_type") or "").strip().upper()
        action_raw = (row.get("Action") or "").strip().upper()
        risk_level = (row.get("Risk Level") or "").strip().lower()
        msg_key = (row.get("msg_key") or "").strip().lower()

        # Skip heartbeats
        if risk_level == "heartbeat" or action_raw == "HEARTBEAT":
            continue

        # Legacy completion labels were written to Risk Level in older runs.
        # Normalize them so they do not appear as fake risk categories.
        if risk_level in ("completion_matched", "completion_unmatched"):
            risk_level = "normal"

        # Completion detection from Action column
        if not event_type_raw and action_raw in (
            "STAFF_COMPLETED_CONFIRMATION",
            "COMPLETION_SUBJECT_KEYWORD",
            "COMPLETION_MATCHED",
            "COMPLETION_UNMATCHED",
            "COMPLETION_LINKED_TO_ASSIGNMENT",
            "COMPLETION_NOT_LINKED_TO_ASSIGNMENT",
        ):
            event_type_raw = "COMPLETED"

        # assigned_to with fallback to "Assigned To"
        assigned_to = (row.get("assigned_to") or "").strip().lower()
        if not assigned_to:
            assigned_to = (row.get("Assigned To") or "").strip().lower()

        # Filter non-staff
        if assigned_to in config.NON_STAFF_ASSIGNEES:
            # Keep for activity feed but mark
            pass

        # Timestamps
        assigned_ts = _parse_ts(row.get("assigned_ts"))
        completed_ts = _parse_ts(row.get("completed_ts"))

        # Duration
        duration_sec = _safe_float(row.get("duration_sec"))
        if duration_sec is None and assigned_ts and completed_ts:
            duration_sec = max(0.0, _business_seconds(assigned_ts, completed_ts))

        # Event timestamp for sorting
        if event_type_raw == "COMPLETED" and completed_ts:
            event_ts = completed_ts
        elif assigned_ts:
            event_ts = assigned_ts
        else:
            # Try to build from Date + Time columns
            date_str = (row.get("Date") or "").strip()
            time_str = (row.get("Time") or "").strip()
            event_ts = _parse_ts(f"{date_str}T{time_str}") if date_str and time_str else None

        out.append({
            "date": (row.get("Date") or "").strip(),
            "time": (row.get("Time") or "").strip(),
            "subject": (row.get("Subject") or "").strip(),
            "assigned_to": assigned_to,
            "sender": (row.get("Sender") or "").strip().lower(),
            "risk_level": risk_level,
            "domain_bucket": (row.get("Domain Bucket") or "").strip(),
            "action": action_raw,
            "event_type": event_type_raw,
            "msg_key": msg_key,
            "assigned_ts": assigned_ts,
            "completed_ts": completed_ts,
            "duration_sec": duration_sec,
            "event_ts": event_ts,
        })
    return out


def _is_staff(email: str) -> bool:
    return bool(email) and email not in config.NON_STAFF_ASSIGNEES and "@" in email


# ─────────────────────────────────────────────
# Main computation
# ─────────────────────────────────────────────

def compute_dashboard(rows: list[dict] | None, roster_state: dict | None,
                      settings: dict | None, staff_list: list[str] | None = None,
                      hib_state: dict | None = None,
                      date_start: str | None = None,
                      date_end: str | None = None,
                      staff_filter: str | None = None) -> dict[str, Any]:
    """Compute the full unified dashboard payload.

    date_start/date_end: optional YYYY-MM-DD strings to filter events.
    When omitted, defaults to today only.
    """
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    if not rows:
        return _empty_dashboard(now, roster_state, hib_state)

    # Resolve date range
    if not date_start and not date_end:
        # Default: all time
        ds, de = "2000-01-01", "2099-12-31"
    else:
        ds = date_start or "2000-01-01"
        de = date_end or "2099-12-31"

    events = _normalise_rows(rows)
    filtered = [e for e in events if ds <= (e["date"] or "") <= de]

    # ── Summary cards ──
    processed = len([e for e in filtered if e["event_type"] == "ASSIGNED"])
    completions = len([e for e in filtered if e["event_type"] == "COMPLETED"])

    # Build active count from filtered events to match date range
    # Use count-based approach (msg_keys differ between ASSIGNED/COMPLETED).
    total_assigned_by_staff: dict[str, int] = defaultdict(int)
    total_completed_by_staff: dict[str, int] = defaultdict(int)

    for e in filtered:
        if e["event_type"] == "ASSIGNED":
            email = (e.get("assigned_to") or "").strip().lower()
            if _is_staff(email):
                total_assigned_by_staff[email] += 1
        elif e["event_type"] == "COMPLETED":
            email = (e.get("sender") or "").strip().lower()
            if not _is_staff(email):
                email = (e.get("assigned_to") or "").strip().lower()
            if _is_staff(email):
                total_completed_by_staff[email] += 1

    active_count = 0
    for staff in set(total_assigned_by_staff) | set(total_completed_by_staff):
        active_count += max(0, total_assigned_by_staff.get(staff, 0) - total_completed_by_staff.get(staff, 0))

    active_staff = len(staff_list) if staff_list else 0

    # Avg completion time — infer durations via per-staff FIFO queues
    # (mirrors _compute_staff_kpis logic: match each COMPLETED to the
    #  earliest unmatched ASSIGNED for the same staff member)
    _avg_staff_queues: dict[str, list[datetime]] = defaultdict(list)
    for e in events:
        if e["event_type"] != "ASSIGNED":
            continue
        email = (e.get("assigned_to") or "").strip().lower()
        if not _is_staff(email):
            continue
        ts = e.get("assigned_ts") or e.get("event_ts")
        if ts:
            _avg_staff_queues[email].append(ts)
    for q in _avg_staff_queues.values():
        q.sort()

    # Pre-consume queue with completions before the filtered date range
    if ds and ds > "2000-01-01":
        range_start_dt = _parse_ts(f"{ds}T00:00:00")
        for e in events:
            if e["event_type"] != "COMPLETED":
                continue
            if (e.get("date") or "") >= ds:
                continue
            email = (e.get("assigned_to") or "").strip().lower()
            if not _is_staff(email):
                sender = (e.get("sender") or "").strip().lower()
                email = sender if _is_staff(sender) else ""
            if not email or not _avg_staff_queues.get(email):
                continue
            if range_start_dt and _avg_staff_queues[email][0] >= range_start_dt:
                continue
            _avg_staff_queues[email].pop(0)

    durations = []
    for e in filtered:
        if e["event_type"] != "COMPLETED":
            continue

        # Try explicit duration_sec first
        if e["duration_sec"] is not None and e["duration_sec"] > 0:
            durations.append(e["duration_sec"])
            continue

        # Resolve staff email for this completion
        email = (e.get("assigned_to") or "").strip().lower()
        if not _is_staff(email):
            sender = (e.get("sender") or "").strip().lower()
            email = sender if _is_staff(sender) else ""

        completed_ts = e.get("completed_ts") or e.get("event_ts")
        if not email or not completed_ts or not _avg_staff_queues.get(email):
            continue

        # Pop earliest ASSIGNED timestamp that precedes this completion
        while _avg_staff_queues[email]:
            a_ts = _avg_staff_queues[email][0]
            if a_ts >= completed_ts:
                break
            _avg_staff_queues[email].pop(0)
            delta = _business_seconds(a_ts, completed_ts)
            if 0 < delta <= _BH_DAY_SEC * 10:
                durations.append(delta)
                break

    avg_time_sec = sum(durations) / len(durations) if durations else 0

    # Uptime: find earliest HEARTBEAT in raw rows for today
    first_hb = None
    for r in rows:
        action_raw = (r.get("Action") or "").strip().upper()
        risk_raw = (r.get("Risk Level") or "").strip().lower()
        if action_raw == "HEARTBEAT" or risk_raw == "heartbeat":
            date_str = (r.get("Date") or "").strip()
            time_str = (r.get("Time") or "").strip()
            if date_str and time_str:
                dt_str = f"{date_str} {time_str}"
                try:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    if first_hb is None or dt < first_hb:
                        first_hb = dt
                except ValueError:
                    continue

    uptime_str = None
    if first_hb:
        uptime_sec = (now - first_hb).total_seconds()
        if uptime_sec >= 0:
            uptime_str = format_duration_human(uptime_sec)

    # Next staff member from roster
    total_processed = None
    next_staff = None
    if roster_state:
        total_processed = roster_state.get("total_processed")
        roster_index = roster_state.get("current_index")
        if staff_list and roster_index is not None:
            next_idx = roster_index % len(staff_list)
            next_email = staff_list[next_idx]
            next_staff = _staff_display_name(next_email)

    # Compute HIB burst status
    hib_burst = _compute_hib_burst_status(hib_state)

    summary = {
        "processed_today": processed,
        "completions_today": completions,
        "active_count": active_count,
        "active_staff": active_staff,
        "avg_time_sec": round(avg_time_sec, 1),
        "avg_time_human": format_duration_human(avg_time_sec) if durations else "N/A",
        "uptime": uptime_str,
        "total_processed": total_processed,
        "next_staff": next_staff,
        "hib_burst": hib_burst,
    }

    # ── Per-staff KPIs (filtered range for counts, all events for active) ──
    staff_kpis = _compute_staff_kpis(filtered, events, date_start=ds, date_end=de)

    # ── Hourly activity ──
    hourly = _compute_hourly(filtered)
    hourly_detail = _compute_hourly_detail(filtered)

    # ── Risk level distribution ──
    risk_dist = _compute_distribution(filtered, "risk_level",
                                       exclude={"heartbeat", ""})

    # ── Domain bucket distribution ──
    domain_dist = _compute_distribution(filtered, "domain_bucket",
                                         exclude={""})

    # ── Assignment pie (staff only) ──
    assignment_pie = _compute_distribution(
        [e for e in filtered if e["event_type"] == "ASSIGNED" and _is_staff(e["assigned_to"])],
        "assigned_to"
    )
    assignment_pie = {
        _staff_display_name(k): v for k, v in assignment_pie.items()
    }

    # ── Requestor (sender) breakdown — top 15 + Other ──
    requestor_dist = _compute_requestor_distribution(filtered)

    # ── Recent activity feed (last 50 in range) ──
    activity_feed = _build_activity_feed(filtered, events, limit=50,
                                         staff_filter=staff_filter)

    return {
        "summary": summary,
        "staff_kpis": staff_kpis,
        "hourly": hourly,
        "hourly_detail": hourly_detail,
        "risk_distribution": risk_dist,
        "domain_distribution": domain_dist,
        "assignment_pie": assignment_pie,
        "requestor_distribution": requestor_dist,
        "activity_feed": activity_feed,
        "date_start": ds,
        "date_end": de,
        "last_updated": now.isoformat(),
        "csv_rows": len(rows),
    }


def _empty_dashboard(now: datetime, roster_state: dict | None, hib_state: dict | None = None) -> dict:
    hib_burst = _compute_hib_burst_status(hib_state)
    return {
        "summary": {
            "processed_today": 0, "completions_today": 0, "active_count": 0,
            "active_staff": 0, "avg_time_sec": 0, "avg_time_human": "N/A",
            "uptime": None,
            "total_processed": roster_state.get("total_processed") if roster_state else None,
            "next_staff": None,
            "hib_burst": hib_burst,
        },
        "staff_kpis": [],
        "hourly": {},
        "hourly_detail": {"hours": {}, "all_sources": []},
        "risk_distribution": {},
        "domain_distribution": {},
        "assignment_pie": {},
        "requestor_distribution": {},
        "activity_feed": [],
        "date_start": now.strftime("%Y-%m-%d"),
        "date_end": now.strftime("%Y-%m-%d"),
        "last_updated": now.isoformat(),
        "csv_rows": 0,
    }


def _compute_requestor_distribution(events: list[dict], top_n: int = 15) -> dict[str, int]:
    """Top N requestor domains by count, with 'Other' bucket."""
    counts: dict[str, int] = defaultdict(int)
    exclude_senders = config.NON_STAFF_ASSIGNEES | {"", "system"}
    for e in events:
        if e["event_type"] != "ASSIGNED":
            continue
        sender = e.get("sender", "") or ""
        if sender in exclude_senders or not sender:
            continue
        # Group by domain
        domain = sender.split("@")[1] if "@" in sender else sender
        counts[domain] += 1

    if not counts:
        return {}

    sorted_items = sorted(counts.items(), key=lambda x: -x[1])
    top = dict(sorted_items[:top_n])
    other = sum(v for _, v in sorted_items[top_n:])
    if other > 0:
        top["Other"] = other
    return top


def _compute_staff_kpis(filtered: list[dict], all_events: list[dict] | None = None,
                        date_start: str | None = None,
                        date_end: str | None = None) -> list[dict]:
    """Per-staff KPI table data.

    *filtered* is the date-range subset (for assigned/completed counts).
    *all_events* is the full dataset (for truly-open active count).
    *date_start* is the YYYY-MM-DD lower bound so we can pre-consume the
    FIFO queue with completions before the filtered range.
    If all_events is None, falls back to filtered for backwards compat.
    """
    # Group by staff email — counts from filtered range
    staff_assigned: dict[str, int] = defaultdict(int)
    staff_completed: dict[str, int] = defaultdict(int)
    staff_durations: dict[str, list[float]] = defaultdict(list)

    def _resolve_received_ts(e: dict, kind: str) -> datetime | None:
        """Resolve best available timestamp for assignment/completion receive time."""
        if kind == "assigned":
            ts = e.get("assigned_ts") or e.get("event_ts")
        else:
            ts = e.get("completed_ts") or e.get("event_ts")
        if ts:
            return ts

        date_raw = e.get("date") or e.get("Date")
        time_raw = e.get("time") or e.get("Time")
        date_str = date_raw.strip() if isinstance(date_raw, str) else ""
        time_str = time_raw.strip() if isinstance(time_raw, str) else ""
        if not date_str or not time_str:
            return None

        return _parse_ts(f"{date_str}T{time_str}") or _parse_ts(f"{date_str} {time_str}")

    def _resolve_staff_email(e, et):
        assigned_email = (e.get("assigned_to") or "").strip().lower()
        if _is_staff(assigned_email):
            return assigned_email
        if et == "COMPLETED":
            sender_email = (e.get("sender") or e.get("Sender") or "").strip().lower()
            if _is_staff(sender_email):
                return sender_email
        return None

    # For inferred durations on COMPLETED events that don't have duration_sec:
    # earliest ASSIGNED timestamp per msg_key, sourced from all_events when available.
    earliest_assigned_ts_by_key: dict[str, datetime] = {}
    for e in (all_events if all_events is not None else filtered):
        if e["event_type"] != "ASSIGNED":
            continue
        key = e.get("msg_key") or ""
        if not key:
            continue
        ts = _resolve_received_ts(e, "assigned")
        if not ts:
            continue
        prev = earliest_assigned_ts_by_key.get(key)
        if prev is None or ts < prev:
            earliest_assigned_ts_by_key[key] = ts

    # Fallback: per-staff ASSIGNED timestamps for FIFO matching
    # when msg_key matching is not possible.
    staff_assigned_ts_queue: dict[str, list[datetime]] = defaultdict(list)
    for e in (all_events if all_events is not None else filtered):
        if e["event_type"] != "ASSIGNED":
            continue
        a_email = _resolve_staff_email(e, "ASSIGNED")
        if not a_email:
            continue
        ts = _resolve_received_ts(e, "assigned")
        if ts:
            staff_assigned_ts_queue[a_email].append(ts)
    for q in staff_assigned_ts_queue.values():
        q.sort()

    # Pre-consume FIFO queue with completions BEFORE the filtered date range.
    # Without this, a narrow filter (e.g. TODAY) leaves the queue full of
    # old assignments, causing today's completions to match stale entries.
    # Only pop assignment entries that are themselves before the range so we
    # never steal today's assignments for yesterday's completions.
    if date_start and all_events is not None:
        range_start_dt = _parse_ts(f"{date_start}T00:00:00")
        for e in all_events:
            if e["event_type"] != "COMPLETED":
                continue
            if (e.get("date") or "") >= date_start:
                continue  # inside or after the range — leave for the main loop
            email = _resolve_staff_email(e, "COMPLETED")
            if not email:
                continue
            # Try msg_key match first (no queue consumption needed)
            key = e.get("msg_key") or ""
            if key and earliest_assigned_ts_by_key.get(key):
                continue
            # Consume one FIFO entry, but only if it's before the date range.
            # This prevents yesterday's excess completions from eating today's
            # assignments.
            if staff_assigned_ts_queue.get(email):
                if range_start_dt and staff_assigned_ts_queue[email][0] >= range_start_dt:
                    continue  # next queue entry is inside the range — don't consume
                staff_assigned_ts_queue[email].pop(0)

    for e in filtered:
        et = e["event_type"]
        email = _resolve_staff_email(e, et)
        if not email:
            continue

        if et == "ASSIGNED":
            staff_assigned[email] += 1
        elif et == "COMPLETED":
            staff_completed[email] += 1
            if e["duration_sec"] is not None and e["duration_sec"] > 0:
                staff_durations[email].append(e["duration_sec"])
            else:
                key = e.get("msg_key") or ""
                assigned_ts = earliest_assigned_ts_by_key.get(key) if key else None
                completed_ts = _resolve_received_ts(e, "completed")
                if assigned_ts and completed_ts:
                    inferred_sec = _business_seconds(assigned_ts, completed_ts)
                    if inferred_sec > 0:
                        staff_durations[email].append(inferred_sec)
                elif completed_ts and staff_assigned_ts_queue.get(email):
                    while staff_assigned_ts_queue[email]:
                        a_ts = staff_assigned_ts_queue[email][0]
                        if a_ts >= completed_ts:
                            break  # assignment is after completion — stop, don't waste entries
                        staff_assigned_ts_queue[email].pop(0)
                        delta = _business_seconds(a_ts, completed_ts)
                        if 0 < delta <= _BH_DAY_SEC * 10:
                            staff_durations[email].append(delta)
                            break

    # Track active count from ALL events using simple counts.
    # (msg_keys differ between ASSIGNED and COMPLETED events so set
    #  subtraction doesn't work — use count-based approach instead.)
    staff_all_assigned: dict[str, int] = defaultdict(int)
    staff_all_completed: dict[str, int] = defaultdict(int)

    for e in (all_events if all_events is not None else filtered):
        et = e["event_type"]
        email = _resolve_staff_email(e, et)
        if not email:
            continue

        if et == "ASSIGNED":
            staff_all_assigned[email] += 1
        elif et == "COMPLETED":
            staff_all_completed[email] += 1

    all_emails = set(staff_assigned) | set(staff_completed) | set(staff_all_assigned)
    result = []
    for email in sorted(all_emails):
        assigned = staff_assigned.get(email, 0)
        completed = staff_completed.get(email, 0)
        active = max(0, assigned - completed)

        durations_min = sorted([d / 60.0 for d in staff_durations.get(email, [])])
        median_min = round(_median(durations_min), 1) if durations_min else None
        p90_min = round(_percentile(durations_min, 0.9), 1) if durations_min else None
        low_confidence = False

        result.append({
            "email": email,
            "name": _staff_display_name(email),
            "assigned": assigned,
            "completed": completed,
            "active": active,
            "median_min": median_min,
            "p90_min": p90_min,
            "low_confidence": low_confidence,
            "median_human": format_duration_human((median_min or 0) * 60) if median_min else None,
            "p90_human": format_duration_human((p90_min or 0) * 60) if p90_min else None,
        })

    result.sort(key=lambda x: (-x["assigned"], x["name"]))
    return result


def _compute_hourly(events: list[dict]) -> dict[str, dict[str, int]]:
    """Hourly breakdown: {hour: {assigned: N, completed: N}}."""
    hourly: dict[str, dict[str, int]] = {}
    for h in range(24):
        key = f"{h:02d}:00"
        hourly[key] = {"assigned": 0, "completed": 0}

    for e in events:
        ts = e.get("event_ts")
        if not ts:
            continue
        key = f"{ts.hour:02d}:00"
        if e["event_type"] == "ASSIGNED":
            hourly[key]["assigned"] += 1
        elif e["event_type"] == "COMPLETED":
            hourly[key]["completed"] += 1
    return hourly


def _compute_hourly_detail(events: list[dict]) -> dict[str, Any]:
    """Detailed hourly breakdown with source attribution and per-event data."""
    hours: dict[str, dict] = {}
    all_sources: set[str] = set()

    # Initialise all 24 hours
    for h in range(24):
        key = f"{h:02d}:00"
        hours[key] = {"sources": {}, "events": [], "total": 0}

    # Build msg_key → staff lookup from ASSIGNED events so COMPLETED
    # events can show who originally handled the request.
    assigned_staff: dict[str, str] = {}
    for e in events:
        if e["event_type"] == "ASSIGNED" and _is_staff(e.get("assigned_to", "")) and e.get("msg_key"):
            assigned_staff[e["msg_key"]] = e["assigned_to"]

    for e in events:
        ts = e.get("event_ts")
        if not ts:
            continue
        if e["event_type"] not in ("ASSIGNED", "COMPLETED"):
            continue

        key = f"{ts.hour:02d}:00"
        sender = e.get("sender", "") or ""
        source = _sender_display_name(sender)

        bucket = hours[key]

        # Source counts only for ASSIGNED (drives the stacked chart)
        if e["event_type"] == "ASSIGNED":
            all_sources.add(source)
            bucket["sources"][source] = bucket["sources"].get(source, 0) + 1
            bucket["total"] += 1

        # Resolve staff display name
        staff_email = (e.get("assigned_to") or "").strip().lower()
        if e["event_type"] == "COMPLETED" and not _is_staff(staff_email):
            # Try msg_key lookup to find original assignee
            msg_key = e.get("msg_key") or ""
            if msg_key:
                staff_email = assigned_staff.get(msg_key, staff_email)
            # Fall back to sender (COMPLETED rows often have staff as sender)
            if not _is_staff(staff_email):
                sender_email = (e.get("sender") or "").strip().lower()
                if _is_staff(sender_email):
                    staff_email = sender_email
        staff_name = _staff_display_name(staff_email) if _is_staff(staff_email) else staff_email

        bucket["events"].append({
            "time": ts.strftime("%H:%M:%S"),
            "sender": sender,
            "source": source,
            "type": e["event_type"],
            "action": e.get("action", ""),
            "subject": (e.get("subject") or "")[:80],
            "staff": staff_name,
            "risk": e.get("risk_level", "normal") or "normal",
        })

    # Sort events within each hour descending by time
    for bucket in hours.values():
        bucket["events"].sort(key=lambda x: x["time"], reverse=True)

    return {
        "hours": hours,
        "all_sources": sorted(all_sources),
    }


def _compute_distribution(events: list[dict], field: str,
                          exclude: set | None = None) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    exclude = exclude or set()
    for e in events:
        val = e.get(field, "") or ""
        if val in exclude:
            continue
        counts[val] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def _build_activity_feed(filtered: list[dict], all_events: list[dict], limit: int = 50,
                         staff_filter: str | None = None) -> list[dict]:
    """Recent activity — most recent first."""
    # Build lookup: msg_key → staff email from ASSIGNED events (all events, not just filtered)
    # so COMPLETED rows can show who originally handled the ticket.
    assigned_staff: dict[str, str] = {}
    for e in all_events:
        if e["event_type"] == "ASSIGNED" and _is_staff(e["assigned_to"]) and e["msg_key"]:
            assigned_staff[e["msg_key"]] = e["assigned_to"]

    # ------------------------------------------------------------------
    # Duration inference for COMPLETED events (mirrors _compute_staff_kpis logic)
    # ------------------------------------------------------------------

    def _resolve_ts(e: dict, kind: str) -> datetime | None:
        """Best available timestamp for an event."""
        if kind == "assigned":
            ts = e.get("assigned_ts") or e.get("event_ts")
        else:
            ts = e.get("completed_ts") or e.get("event_ts")
        if ts:
            return ts
        date_raw = e.get("date") or e.get("Date")
        time_raw = e.get("time") or e.get("Time")
        date_str = date_raw.strip() if isinstance(date_raw, str) else ""
        time_str = time_raw.strip() if isinstance(time_raw, str) else ""
        if not date_str or not time_str:
            return None
        return _parse_ts(f"{date_str}T{time_str}") or _parse_ts(f"{date_str} {time_str}")

    def _resolve_email(e: dict) -> str | None:
        email = (e.get("assigned_to") or "").strip().lower()
        if _is_staff(email):
            return email
        if e["event_type"] == "COMPLETED":
            sender = (e.get("sender") or e.get("Sender") or "").strip().lower()
            if _is_staff(sender):
                return sender
        return None

    # 1. msg_key → earliest ASSIGNED event_ts
    earliest_assigned_ts: dict[str, datetime] = {}
    for e in all_events:
        if e["event_type"] != "ASSIGNED":
            continue
        key = e.get("msg_key") or ""
        if not key:
            continue
        ts = _resolve_ts(e, "assigned")
        if not ts:
            continue
        prev = earliest_assigned_ts.get(key)
        if prev is None or ts < prev:
            earliest_assigned_ts[key] = ts

    # 2. Per-staff FIFO queues of ASSIGNED timestamps
    staff_queues: dict[str, list[datetime]] = defaultdict(list)
    for e in all_events:
        if e["event_type"] != "ASSIGNED":
            continue
        email = _resolve_email(e)
        if not email:
            continue
        ts = _resolve_ts(e, "assigned")
        if ts:
            staff_queues[email].append(ts)
    for q in staff_queues.values():
        q.sort()

    # Pre-consume FIFO queue with COMPLETED events that precede the filtered set,
    # so that today's completions don't match stale assignments.
    filtered_set = set(id(e) for e in filtered)
    for e in all_events:
        if e["event_type"] != "COMPLETED" or id(e) in filtered_set:
            continue
        email = _resolve_email(e)
        if not email:
            continue
        key = e.get("msg_key") or ""
        if key and earliest_assigned_ts.get(key):
            continue
        completed_ts = _resolve_ts(e, "completed")
        if completed_ts and staff_queues.get(email):
            if staff_queues[email][0] < completed_ts:
                staff_queues[email].pop(0)

    # ------------------------------------------------------------------

    # Only include events with a timestamp and a meaningful event type
    feed_events = [
        e for e in filtered
        if e["event_ts"] and e["event_type"] in ("ASSIGNED", "COMPLETED")
    ]
    feed_events.sort(key=lambda e: e["event_ts"], reverse=True)

    # Resolve display name for each event up-front so staff filtering works
    def _display_staff(e: dict) -> str:
        staff = e["assigned_to"]
        if e["event_type"] == "COMPLETED" and not _is_staff(staff):
            # Try msg_key lookup first
            if e["msg_key"]:
                staff = assigned_staff.get(e["msg_key"], staff)
            # Fall back to sender field (COMPLETED rows often have
            # the staff email as sender rather than assigned_to)
            if not _is_staff(staff):
                sender = (e.get("sender") or e.get("Sender") or "").strip().lower()
                if _is_staff(sender):
                    staff = sender
        return staff

    # When a staff filter is active, keep only that person's events before
    # applying the limit so the user sees all their recent jobs.
    if staff_filter:
        sf_lower = staff_filter.strip().lower()
        feed_events = [
            e for e in feed_events
            if _staff_display_name(_display_staff(e)).lower() == sf_lower
            or _display_staff(e).lower() == sf_lower
        ]

    result = []
    for e in feed_events[:limit]:
        staff = _display_staff(e)

        # Infer duration for COMPLETED events
        dur_sec = e["duration_sec"]
        if e["event_type"] == "COMPLETED" and not dur_sec:
            completed_ts = _resolve_ts(e, "completed")
            key = e.get("msg_key") or ""
            assigned_ts = earliest_assigned_ts.get(key) if key else None
            email = _resolve_email(e)
            if assigned_ts and completed_ts:
                dur_sec = _business_seconds(assigned_ts, completed_ts)
            elif completed_ts and email and staff_queues.get(email):
                while staff_queues[email]:
                    a_ts = staff_queues[email][0]
                    if a_ts >= completed_ts:
                        break
                    staff_queues[email].pop(0)
                    delta = _business_seconds(a_ts, completed_ts)
                    if delta > 0:
                        dur_sec = delta
                        break

        result.append({
            "time": e["event_ts"].strftime("%H:%M:%S") if e["event_ts"] else "",
            "date": e["date"],
            "type": e["event_type"],
            "subject": e["subject"][:80],
            "assigned_to": _staff_display_name(staff) if _is_staff(staff) else staff,
            "duration_human": format_duration_human(dur_sec) if dur_sec else "",
            "duration_sec": dur_sec if dur_sec else None,
            "risk_level": e["risk_level"],
        })
    return result
