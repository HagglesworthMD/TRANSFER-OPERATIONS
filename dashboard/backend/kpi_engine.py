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


def _staff_display_name(email: str) -> str:
    local = email.split("@")[0] if "@" in email else email
    return local.replace(".", " ").replace("_", " ").strip().title()


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
            duration_sec = max(0.0, (completed_ts - assigned_ts).total_seconds())

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
                      date_end: str | None = None) -> dict[str, Any]:
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

    # Build active set from ALL events (not date-filtered) so we track
    # truly open tickets regardless of when they were assigned.
    assigned_keys: dict[str, dict] = {}
    completed_keys: set[str] = set()
    assigned_by_staff: dict[str, int] = {}
    completed_by_staff: dict[str, int] = {}

    for e in events:
        if e["event_type"] == "ASSIGNED":
            if e["msg_key"]:
                assigned_keys[e["msg_key"]] = e
            elif _is_staff(e["assigned_to"]):
                assigned_by_staff[e["assigned_to"]] = assigned_by_staff.get(e["assigned_to"], 0) + 1
        if e["event_type"] == "COMPLETED":
            if e["msg_key"]:
                completed_keys.add(e["msg_key"])
            elif _is_staff(e["sender"]):
                completed_by_staff[e["sender"]] = completed_by_staff.get(e["sender"], 0) + 1

    active_items = {k: v for k, v in assigned_keys.items() if k not in completed_keys}
    active_count = len(active_items)

    # Add legacy count (assigned - completed per staff, but can't go negative)
    for staff, count in assigned_by_staff.items():
        legacy_active = max(0, count - completed_by_staff.get(staff, 0))
        active_count += legacy_active

    errors = len([e for e in filtered if e["assigned_to"] == "error" or e["risk_level"] == "error"])

    # Avg completion time
    durations = [
        e["duration_sec"] for e in filtered
        if e["event_type"] == "COMPLETED" and e["duration_sec"] is not None
    ]
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
        "errors_today": errors,
        "avg_time_sec": round(avg_time_sec, 1),
        "avg_time_human": format_duration_human(avg_time_sec) if avg_time_sec else "N/A",
        "uptime": uptime_str,
        "total_processed": total_processed,
        "next_staff": next_staff,
        "hib_burst": hib_burst,
    }

    # ── Per-staff KPIs (filtered range for counts, all events for active) ──
    staff_kpis = _compute_staff_kpis(filtered, events)

    # ── Hourly activity ──
    hourly = _compute_hourly(filtered)

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
    activity_feed = _build_activity_feed(filtered, events, limit=50)

    return {
        "summary": summary,
        "staff_kpis": staff_kpis,
        "hourly": hourly,
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
            "errors_today": 0, "avg_time_sec": 0, "avg_time_human": "N/A",
            "uptime": None,
            "total_processed": roster_state.get("total_processed") if roster_state else None,
            "next_staff": None,
            "hib_burst": hib_burst,
        },
        "staff_kpis": [],
        "hourly": {},
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


def _compute_staff_kpis(filtered: list[dict], all_events: list[dict] | None = None) -> list[dict]:
    """Per-staff KPI table data.

    *filtered* is the date-range subset (for assigned/completed counts).
    *all_events* is the full dataset (for truly-open active count).
    If all_events is None, falls back to filtered for backwards compat.
    """
    # Group by staff email — counts from filtered range
    staff_assigned: dict[str, int] = defaultdict(int)
    staff_completed: dict[str, int] = defaultdict(int)
    staff_durations: dict[str, list[float]] = defaultdict(list)

    for e in filtered:
        et = e["event_type"]
        email = e["assigned_to"]

        if not _is_staff(email):
            if et == "COMPLETED" and _is_staff(e["sender"]):
                email = e["sender"]
            else:
                continue

        if et == "ASSIGNED":
            staff_assigned[email] += 1
        elif et == "COMPLETED":
            staff_completed[email] += 1
            if e["duration_sec"] is not None and e["duration_sec"] > 0:
                staff_durations[email].append(e["duration_sec"])

    # Track msg_keys for active count from ALL events
    staff_assigned_keys: dict[str, set] = defaultdict(set)
    staff_completed_keys: dict[str, set] = defaultdict(set)

    for e in (all_events if all_events is not None else filtered):
        et = e["event_type"]
        email = e["assigned_to"]

        if not _is_staff(email):
            if et == "COMPLETED" and _is_staff(e["sender"]):
                email = e["sender"]
            else:
                continue

        if et == "ASSIGNED" and e["msg_key"]:
            staff_assigned_keys[email].add(e["msg_key"])
        elif et == "COMPLETED" and e["msg_key"]:
            staff_completed_keys[email].add(e["msg_key"])

    all_emails = set(staff_assigned) | set(staff_completed) | set(staff_assigned_keys)
    result = []
    for email in sorted(all_emails):
        assigned = staff_assigned.get(email, 0)
        completed = staff_completed.get(email, 0)
        active = len(staff_assigned_keys.get(email, set()) - staff_completed_keys.get(email, set()))

        durations_min = sorted([d / 60.0 for d in staff_durations.get(email, [])])
        median_min = round(_median(durations_min), 1) if durations_min else None
        p90_min = round(_percentile(durations_min, 0.9), 1) if durations_min else None
        low_confidence = len(durations_min) < config.LOW_CONFIDENCE_MIN_COMPLETIONS

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


def _build_activity_feed(filtered: list[dict], all_events: list[dict], limit: int = 50) -> list[dict]:
    """Recent activity — most recent first."""
    # Build lookup: msg_key → staff email from ASSIGNED events (all events, not just filtered)
    # so COMPLETED rows can show who originally handled the ticket.
    assigned_staff: dict[str, str] = {}
    for e in all_events:
        if e["event_type"] == "ASSIGNED" and _is_staff(e["assigned_to"]) and e["msg_key"]:
            assigned_staff[e["msg_key"]] = e["assigned_to"]

    # Only include events with a timestamp and a meaningful event type
    feed_events = [
        e for e in filtered
        if e["event_ts"] and e["event_type"] in ("ASSIGNED", "COMPLETED")
    ]
    feed_events.sort(key=lambda e: e["event_ts"], reverse=True)

    result = []
    for e in feed_events[:limit]:
        # For COMPLETED events, look up the staff from the original ASSIGNED event
        staff = e["assigned_to"]
        if e["event_type"] == "COMPLETED" and not _is_staff(staff) and e["msg_key"]:
            staff = assigned_staff.get(e["msg_key"], staff)

        result.append({
            "time": e["event_ts"].strftime("%H:%M:%S") if e["event_ts"] else "",
            "date": e["date"],
            "type": e["event_type"],
            "subject": e["subject"][:80],
            "assigned_to": _staff_display_name(staff) if _is_staff(staff) else staff,
            "duration_human": format_duration_human(e["duration_sec"]) if e["duration_sec"] else "",
            "risk_level": e["risk_level"],
        })
    return result
