"""KPI computation — pure stdlib, no pandas.

Ported from dashboard_core.py: prepare_event_frame, compute_per_staff_kpis,
format_duration_human, plus new helpers for charts/feeds.
"""

import bisect
import logging
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from . import config

logger = logging.getLogger(__name__)

_SAMI_REF_RE = re.compile(r"\[SAMI-([A-Z0-9]+)\]", re.IGNORECASE)
_SAMI_TAG_RE = re.compile(r"\[SAMI-[A-Z0-9]+\]", re.IGNORECASE)
_COMPLETED_TAG_RE = re.compile(r"\[COMPLETED\]", re.IGNORECASE)



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
_DURATION_HARD_CAP_SEC = _BH_DAY_SEC * 10   # 10 business days — absolute max (garbage data)
_DURATION_MISMATCH_SEC = _BH_DAY_SEC * 2    # 2 business days — suspected mismatch threshold


def _event_key(e: dict) -> tuple:
    """Stable identity tuple for duration pre-computation lookup."""
    return (e.get("event_type") or "", e.get("date") or "", e.get("time") or "",
            e.get("assigned_to") or "", e.get("sender") or "",
            e.get("subject") or "")


def _pop_nearest_preceding(sorted_times: list[datetime], before: datetime) -> datetime | None:
    """Pop and return the latest timestamp in *sorted_times* that is < *before*.
    Returns None if no such timestamp exists."""
    idx = bisect.bisect_left(sorted_times, before)
    if idx > 0:
        return sorted_times.pop(idx - 1)
    return None


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


def _load_processed_ledger_by_sami() -> dict[str, dict]:
    from . import data_reader

    ledger_path = config.BASE_DIR / "processed_ledger.json"
    data, _ = data_reader.load_json(ledger_path)
    if not isinstance(data, dict):
        return {}

    by_sami: dict[str, dict] = {}
    for entry in data.values():
        if not isinstance(entry, dict):
            continue
        sami_id = (entry.get("sami_id") or "").strip().upper()
        if not sami_id:
            continue
        by_sami[sami_id] = entry
    return by_sami


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
            "COMPLETION_SWEEP",
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
            "sami_id": (row.get("sami_id") or "").strip() if "sami_id" in row else "",
            "assigned_ts": assigned_ts,
            "completed_ts": completed_ts,
            "duration_sec": duration_sec,
            "event_ts": event_ts,
        })
    return out


def _is_staff(email: str) -> bool:
    return bool(email) and email not in config.NON_STAFF_ASSIGNEES and "@" in email


# ─────────────────────────────────────────────
# Staff CSV export
# ─────────────────────────────────────────────

def export_staff_events(rows: list[dict], staff_name: str,
                        date_start: str, date_end: str) -> list[dict]:
    """Return a list of dicts (ready for CSV) for *staff_name* in the date range.

    Each dict has: Date, Time, Type, Subject, Sender, Source, Risk Level,
    Domain, Duration.
    """
    events = _normalise_rows(rows)
    filtered = [e for e in events if date_start <= (e["date"] or "") <= date_end]

    # Build msg_key → assigned_to lookup so COMPLETED events can resolve staff
    assigned_staff: dict[str, str] = {}
    for e in events:
        if e["event_type"] == "ASSIGNED" and _is_staff(e["assigned_to"]) and e["msg_key"]:
            assigned_staff[e["msg_key"]] = e["assigned_to"]

    target = staff_name.strip().lower()
    result: list[dict] = []

    for e in filtered:
        et = e["event_type"]
        if et not in ("ASSIGNED", "COMPLETED"):
            continue

        # Resolve staff email for this event
        staff_email = (e.get("assigned_to") or "").strip().lower()

        if et == "COMPLETED" and not _is_staff(staff_email):
            # 3-step fallback: assigned_to → msg_key lookup → sender
            if e.get("msg_key"):
                staff_email = assigned_staff.get(e["msg_key"], staff_email)
            if not _is_staff(staff_email):
                sender = (e.get("sender") or "").strip().lower()
                if _is_staff(sender):
                    staff_email = sender

        # Match against display name
        if _staff_display_name(staff_email).lower() != target:
            continue

        dur = ""
        if e["duration_sec"] is not None and e["duration_sec"] > 0:
            dur = format_duration_human(e["duration_sec"])

        result.append({
            "Date": e["date"],
            "Time": e["time"],
            "Type": et,
            "Subject": e["subject"],
            "Sender": _sender_display_name(e.get("sender") or ""),
            "Source": e.get("domain_bucket") or "",
            "Risk Level": e["risk_level"],
            "Domain": (e.get("sender") or "").split("@")[1] if "@" in (e.get("sender") or "") else "",
            "Duration": dur,
        })

    # Sort by date then time
    result.sort(key=lambda r: (r["Date"], r["Time"]))
    return result


# ─────────────────────────────────────────────
# Main computation
# ─────────────────────────────────────────────



def _extract_sami_ref(subject: str) -> str:
    if not subject:
        return ""
    m = _SAMI_REF_RE.search(subject)
    if not m:
        return ""
    return f"SAMI-{m.group(1).upper()}"


def _display_sami_ref(e: dict) -> str:
    sami_id = (e.get("sami_id") or "").strip().upper()
    if sami_id:
        return sami_id
    return _extract_sami_ref((e.get("subject") or "").strip())


def _normalise_subject_for_completion_match(subject: str) -> str:
    """Normalise subject for fallback assignment↔completion matching."""
    s = (subject or "").strip()
    if not s:
        return ""
    # Strip common reply/forward prefixes.
    while True:
        lowered = s.lower()
        if lowered.startswith("re:"):
            s = s[3:].strip()
            continue
        if lowered.startswith("fw:"):
            s = s[3:].strip()
            continue
        if lowered.startswith("fwd:"):
            s = s[4:].strip()
            continue
        break
    s = _COMPLETED_TAG_RE.sub(" ", s)
    s = _SAMI_TAG_RE.sub(" ", s)
    return " ".join(s.lower().split())


def _legacy_group_key(e: dict) -> str:
    return (e.get("msg_key") or "").strip().lower()


def _resolve_group_key(e: dict) -> str:
    if "sami_id" in e:
        sami_id = (e.get("sami_id") or "").strip().lower()
        if sami_id:
            return sami_id
    return _legacy_group_key(e)


def _resolve_sami_group_key(e: dict) -> str:
    sami_id = (e.get("sami_id") or "").strip().lower()
    if sami_id:
        return sami_id
    return _extract_sami_ref((e.get("subject") or "").strip()).lower()


def _active_identity_key(e: dict) -> str:
    sami_ref = _display_sami_ref(e)
    if sami_ref:
        return sami_ref
    msg_key = (e.get("msg_key") or "").strip().lower()
    if msg_key:
        return f"msg:{msg_key}"
    return ""


def _collect_active_identity_rows(rows: list[dict], date_end: str, staff_name: str | None = None,
                                  date_start: str | None = None) -> list[dict]:
    """Return likely-open ticket identities as of *date_end* before reconciliation filtering."""
    events = _normalise_rows(rows)
    candidates = [
        e for e in events
        if e.get("event_type") in ("ASSIGNED", "REASSIGN_MANUAL", "STALE_RELOOP") and (not (e.get("date") or "") or (e.get("date") or "") <= date_end)
    ]

    staff_target = (staff_name or "").strip().lower()
    start_cutoff = _parse_ts(f"{date_start}T00:00:00") if date_start else None

    completed_sami_keys: set[str] = set()
    for e in events:
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type not in ("COMPLETED", "FILTER_JONES_COMPLETION", "COMPLETION_SWEEP"):
            continue
        if (e.get("date") or "") > date_end:
            continue
        sami_key = _resolve_sami_group_key(e)
        if sami_key:
            completed_sami_keys.add(sami_key)

    manual_release_by_identity_ts: dict[str, datetime | None] = {}
    for e in events:
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type != "MANUAL_STALE_RELEASE":
            continue
        if (e.get("date") or "") > date_end:
            continue
        identity = _active_identity_key(e)
        if not identity:
            continue
        current_ts = e.get("event_ts")
        previous_ts = manual_release_by_identity_ts.get(identity)
        if previous_ts is None or (current_ts and current_ts >= previous_ts):
            manual_release_by_identity_ts[identity] = current_ts

    latest_by_identity: dict[str, dict] = {}
    for e in candidates:
        staff_email = (e.get("assigned_to") or "").strip().lower()
        if not _is_staff(staff_email):
            continue

        staff_display = _staff_display_name(staff_email)

        subject = e.get("subject") or ""
        sami_ref = _display_sami_ref(e)
        msg_key = (e.get("msg_key") or "").strip().lower()
        sami_key = _resolve_sami_group_key(e)
        current_ts = e.get("event_ts")
        event_type = (e.get("event_type") or "").strip().upper()

        # STALE_RELOOP rows are operational reloop artifacts, not active-job ownership rows.
        if event_type == "STALE_RELOOP":
            continue

        if start_cutoff:
            if current_ts and current_ts < start_cutoff:
                continue
            if not current_ts and (e.get("date") or "") and (e.get("date") or "") < (date_start or ""):
                continue

        if sami_key and sami_key in completed_sami_keys:
            continue

        identity = sami_ref or (f"msg:{msg_key}" if msg_key else f"{e.get('date','')}|{e.get('time','')}|{staff_email}|{subject[:40]}")
        manual_release_ts = manual_release_by_identity_ts.get(identity)
        if manual_release_ts is not None:
            if current_ts is None or current_ts <= manual_release_ts:
                continue

        existing = latest_by_identity.get(identity)
        if existing is not None:
            prev_ts = existing.get("event_ts")
            if prev_ts and current_ts and prev_ts >= current_ts:
                continue

        sender = (e.get("sender") or "").strip().lower()
        domain = sender.split("@", 1)[1] if "@" in sender else ""

        latest_by_identity[identity] = {
            "Date": e.get("date") or "",
            "Time": e.get("time") or "",
            "SAMI Ref": sami_ref,
            "Staff": staff_display,
            "Staff Email": staff_email,
            "Sender": sender,
            "Domain": domain,
            "Risk Level": e.get("risk_level") or "",
            "Subject": subject,
            "Message Key": msg_key,
            "Identity": identity,
            "event_ts": current_ts,
        }

    rows_out = list(latest_by_identity.values())
    ledger_by_sami = _load_processed_ledger_by_sami()
    filtered_rows: list[dict] = []
    for row in rows_out:
        sami_ref = (row.get("SAMI Ref") or "").strip().upper()
        if not sami_ref:
            owner_email = (row.get("Staff Email") or "").strip().lower()
            owner_display = _staff_display_name(owner_email)
            if staff_target and staff_target not in (owner_email, owner_display.lower()):
                continue
            filtered_rows.append(row)
            continue
        entry = ledger_by_sami.get(sami_ref)
        if not entry:
            continue
        assigned_to = entry.get("assigned_to")
        if assigned_to is None:
            continue
        assigned_to_norm = str(assigned_to).strip().lower()
        if assigned_to_norm in ("", "completed"):
            continue
        row["Staff Email"] = assigned_to_norm
        row["Staff"] = _staff_display_name(assigned_to_norm)
        if staff_target and staff_target not in (assigned_to_norm, row["Staff"].lower()):
            continue
        filtered_rows.append(row)

    rows_out = filtered_rows
    rows_out.sort(key=lambda r: (r.get("event_ts") is not None, r.get("event_ts"), r.get("Date", ""), r.get("Time", "")), reverse=True)
    return rows_out


def export_active_events(rows: list[dict], date_start: str, date_end: str,
                         staff_name: str | None = None,
                         reconciled_set: set[str] | None = None) -> list[dict]:
    """Return likely-open ASSIGNED tickets with SAMI references for CSV/export.

    Active scope is constrained to assignments within date_start/date_end and
    excludes tickets completed by date_end.
    """
    rows_out = _collect_active_identity_rows(rows, date_end, staff_name=staff_name, date_start=date_start)

    # Filter out reconciled identities BEFORE aggregation/output
    if reconciled_set:
        rows_out = [r for r in rows_out if r.get("Identity") not in reconciled_set]
    for r in rows_out:
        r.pop("event_ts", None)
    return rows_out

def compute_dashboard(rows: list[dict] | None, roster_state: dict | None,
                      settings: dict | None, staff_list: list[str] | None = None,
                      hib_state: dict | None = None,
                      date_start: str | None = None,
                      date_end: str | None = None,
                      staff_filter: str | None = None,
                      reconciled_set: set[str] | None = None,
                      activity_mode: str | None = None,
                      activity_staff: str | None = None) -> dict[str, Any]:
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

    # ── Summary cards (canonical SAMI lifecycle) ──
    def _resolve_received_ts(e: dict, kind: str) -> datetime | None:
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

    def _is_reconciliation_only(e: dict) -> bool:
        event_type = (e.get("event_type") or "").strip().upper()
        action = (e.get("action") or e.get("Action") or "").strip().upper()
        return event_type.startswith("RECON") or action.startswith("RECON")

    def _is_canonical_kpi_event(e: dict) -> bool:
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type == "CONFIG_CHANGED":
            return False
        if event_type not in ("ASSIGNED", "COMPLETED"):
            return False
        if _is_reconciliation_only(e):
            return False
        return bool(_resolve_sami_group_key(e))

    jobs: dict[str, dict[str, Any]] = {}
    for e in events:
        if not _is_canonical_kpi_event(e):
            continue
        key = _resolve_sami_group_key(e)
        event_type = (e.get("event_type") or "").strip().upper()
        job = jobs.setdefault(
            key,
            {
                "has_assigned": False,
                "has_completed": False,
                "assigned_ts": None,
                "completed_ts": None,
                "assigned_event": None,
                "initial_assigned_event": None,
                "is_jira_followup": False,
            },
        )

        if event_type == "ASSIGNED":
            job["has_assigned"] = True
            assigned_ts = _resolve_received_ts(e, "assigned")
            if assigned_ts and (job["assigned_ts"] is None or assigned_ts < job["assigned_ts"]):
                job["assigned_ts"] = assigned_ts
                job["assigned_event"] = e
        elif event_type == "COMPLETED":
            job["has_completed"] = True
            completed_ts = _resolve_received_ts(e, "completed")
            if completed_ts and (job["completed_ts"] is None or completed_ts < job["completed_ts"]):
                job["completed_ts"] = completed_ts

    assigned_keys_in_range: set[str] = set()
    completed_keys_in_range: set[str] = set()
    latest_assignment_events_in_range: set[tuple] = set()
    for e in filtered:
        if not _is_canonical_kpi_event(e):
            continue
        key = _resolve_sami_group_key(e)
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type == "ASSIGNED":
            assigned_keys_in_range.add(key)
        elif event_type == "COMPLETED":
            completed_keys_in_range.add(key)
        if event_type in ("REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
            latest_assignment_events_in_range.add(_event_key(e))

    processed_keys: set[str] = set()
    processed_in_range_keys: set[str] = set()
    completed_matched_keys: set[str] = set()
    for key, job in jobs.items():
        assigned_event = job.get("assigned_event")
        if not assigned_event:
            continue
        assigned_email = (assigned_event.get("assigned_to") or "").strip().lower()
        if not _is_staff(assigned_email):
            continue
        if job.get("has_assigned"):
            processed_keys.add(key)
        if key in assigned_keys_in_range:
            processed_in_range_keys.add(key)
        if key in completed_keys_in_range and job.get("has_completed"):
            completed_matched_keys.add(key)

    processed = len(processed_keys)

    # Keep summary active_count in parity with /api/active modal output.
    active_rows_all = _collect_active_identity_rows(rows, de, staff_name=staff_filter, date_start=ds)
    reconciled_completed_rows = []
    if reconciled_set:
        reconciled_completed_rows = [
            row for row in active_rows_all
            if row.get("Identity") in reconciled_set and ds <= (row.get("Date") or "") <= de
        ]
    reconciled_completed_count = len(reconciled_completed_rows)
    completions = len(completed_matched_keys) + reconciled_completed_count
    completions_unmatched = len(completed_keys_in_range - completed_matched_keys)

    active_rows = [row for row in active_rows_all if row.get("Identity") not in (reconciled_set or set())]
    active_count = len(active_rows)
    active_by_staff: dict[str, int] = defaultdict(int)
    for row in active_rows:
        email = (row.get("Staff Email") or "").strip().lower()
        if _is_staff(email):
            active_by_staff[email] += 1

    active_staff = len(staff_list) if staff_list else 0

    # Avg completion time — canonical SAMI lifecycle durations
    # (identical algorithm to per-staff KPIs)
    durations = []
    suppressed_count = 0
    for key in completed_matched_keys:
        job = jobs.get(key)
        if not job:
            continue
        assigned_ts = job.get("assigned_ts")
        completed_ts = job.get("completed_ts")
        if not assigned_ts or not completed_ts:
            continue
        dur = _business_seconds(assigned_ts, completed_ts)
        if dur <= 0:
            continue
        if dur > _DURATION_HARD_CAP_SEC:
            continue
        if dur > _DURATION_MISMATCH_SEC:
            suppressed_count += 1
            continue
        durations.append(dur)

    avg_time_sec = sum(durations) / len(durations) if durations else 0

    # Canonical SAMI durations lookup — shared with activity feed
    _canonical_sami_durations: dict[str, float] = {}
    for key in completed_matched_keys:
        job = jobs.get(key)
        if not job:
            continue
        a_ts, c_ts = job.get("assigned_ts"), job.get("completed_ts")
        if a_ts and c_ts:
            d = _business_seconds(a_ts, c_ts)
            if 0 < d <= _DURATION_HARD_CAP_SEC and d <= _DURATION_MISMATCH_SEC:
                _canonical_sami_durations[key] = d

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
        "processed_in_range": len(processed_in_range_keys),
        "completions_today": completions,
        "completions_matched": completions,
        "completions_unmatched": completions_unmatched,
        "active_count": active_count,
        "active_staff": active_staff,
        "avg_time_sec": round(avg_time_sec, 1),
        "avg_time_human": format_duration_human(avg_time_sec) if durations else "N/A",
        "uptime": uptime_str,
        "total_processed": total_processed,
        "next_staff": next_staff,
        "hib_burst": hib_burst,
        "durations_suppressed": suppressed_count,
        "kpi_mode": "sami_lifecycle_v1",
    }

    # ── Per-staff KPIs (filtered range for counts, all events for active) ──
    _reconciled_per_staff: dict[str, int] | None = None
    if reconciled_set:
        _rps: dict[str, int] = defaultdict(int)
        for row in reconciled_completed_rows:
            email = (row.get("Staff Email") or "").strip().lower()
            if email:
                _rps[email] += 1
        _reconciled_per_staff = _rps

    staff_kpis = _compute_staff_kpis(
        filtered,
        events,
        date_start=ds,
        date_end=de,
        reconciled_per_staff=_reconciled_per_staff,
        active_by_staff=active_by_staff,
    )

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
                                         staff_filter=staff_filter,
                                         canonical_durations=_canonical_sami_durations,
                                         activity_mode=activity_mode,
                                         activity_staff=activity_staff)

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
            "completions_matched": 0, "completions_unmatched": 0,
            "active_staff": 0, "avg_time_sec": 0, "avg_time_human": "N/A",
            "uptime": None,
            "total_processed": roster_state.get("total_processed") if roster_state else None,
            "next_staff": None,
            "hib_burst": hib_burst,
            "kpi_mode": "sami_lifecycle_v1",
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
                        date_end: str | None = None,
                        reconciled_per_staff: dict[str, int] | None = None,
                        active_by_staff: dict[str, int] | None = None) -> list[dict]:
    """Per-staff KPI table data.

    *filtered* is the date-range subset (for assigned/completed counts).
    *all_events* is the full dataset used for canonical SAMI lifecycle grouping.
    If all_events is None, falls back to filtered.
    """
    # Keep signature stable while this analytical path no longer consumes
    # legacy reconciliation/active overrides.
    _ = date_start
    _ = date_end
    external_active_by_staff = active_by_staff is not None
    active_by_staff = active_by_staff or {}

    # Group by staff email (canonical per-SAMI job metrics)
    staff_assigned: dict[str, int] = defaultdict(int)
    staff_assigned_in_range: dict[str, int] = defaultdict(int)
    staff_jira_followups: dict[str, int] = defaultdict(int)
    staff_completed: dict[str, int] = defaultdict(int)
    staff_active: dict[str, int] = defaultdict(int)
    staff_durations: dict[str, list[float]] = defaultdict(list)
    canonical_staff_emails: set[str] = set()

    def _resolve_received_ts(e: dict, kind: str) -> datetime | None:
        """Resolve best available timestamp for assignment/completion time."""
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

    def _is_reconciliation_only(e: dict) -> bool:
        event_type = (e.get("event_type") or "").strip().upper()
        action = (e.get("action") or e.get("Action") or "").strip().upper()
        return event_type.startswith("RECON") or action.startswith("RECON")

    def _is_canonical_kpi_event(e: dict) -> bool:
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type == "CONFIG_CHANGED":
            return False
        if event_type not in ("ASSIGNED", "COMPLETED", "REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
            return False
        if _is_reconciliation_only(e):
            return False
        return bool(_resolve_sami_group_key(e))

    source_events = all_events if all_events is not None else filtered
    source_events = [
        e for e in source_events
        if not (date_end and (e.get("date") or "") and (e.get("date") or "") > date_end)
    ]

    # Canonical job lifecycle by SAMI: latest owner as of date_end + earliest COMPLETED.
    jobs: dict[str, dict[str, Any]] = {}
    for e in source_events:
        if not _is_canonical_kpi_event(e):
            continue
        key = _resolve_sami_group_key(e)
        event_type = (e.get("event_type") or "").strip().upper()
        job = jobs.setdefault(
            key,
            {
                "has_assigned": False,
                "has_completed": False,
                "assigned_ts": None,
                "completed_ts": None,
                "assigned_event": None,
            },
        )

        action = (e.get("action") or e.get("Action") or "").strip().upper()
        if event_type == "JIRA_FOLLOWUP_ASSIGNED" or action == "JIRA_FOLLOWUP":
            job["is_jira_followup"] = True

        if event_type in ("ASSIGNED", "REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
            job["has_assigned"] = True
            assigned_ts = _resolve_received_ts(e, "assigned")
            if event_type in ("ASSIGNED", "JIRA_FOLLOWUP_ASSIGNED") and job.get("initial_assigned_event") is None:
                job["initial_assigned_event"] = e
            if assigned_ts and (job["assigned_ts"] is None or assigned_ts >= job["assigned_ts"]):
                job["assigned_ts"] = assigned_ts
                job["assigned_event"] = e
        elif event_type == "COMPLETED":
            job["has_completed"] = True
            completed_ts = _resolve_received_ts(e, "completed")
            if completed_ts and (job["completed_ts"] is None or completed_ts < job["completed_ts"]):
                job["completed_ts"] = completed_ts

    # Per-range dedupe: a SAMI contributes at most once per metric in filtered range.
    assigned_keys_in_range: set[str] = set()
    completed_keys_in_range: set[str] = set()
    latest_assignment_events_in_range: set[tuple] = set()
    for e in filtered:
        if not _is_canonical_kpi_event(e):
            continue
        key = _resolve_sami_group_key(e)
        event_type = (e.get("event_type") or "").strip().upper()
        if event_type == "ASSIGNED":
            assigned_keys_in_range.add(key)
        elif event_type == "COMPLETED":
            completed_keys_in_range.add(key)
        if event_type in ("REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
            latest_assignment_events_in_range.add(_event_key(e))

    for key, job in jobs.items():
        assigned_event = job.get("assigned_event")
        if not assigned_event:
            continue

        assigned_email = (assigned_event.get("assigned_to") or "").strip().lower()
        if not _is_staff(assigned_email):
            continue
        canonical_staff_emails.add(assigned_email)

        initial_assigned_event = job.get("initial_assigned_event")
        initial_email = (initial_assigned_event.get("assigned_to") or "").strip().lower() if initial_assigned_event else assigned_email

        if job.get("has_assigned"):
            staff_assigned[assigned_email] += 1
        if key in assigned_keys_in_range:
            staff_assigned_in_range[assigned_email] += 1
        if job.get("is_jira_followup") and initial_email and _is_staff(initial_email) and (assigned_event.get("event_type") or "").strip().upper() == "JIRA_FOLLOWUP_ASSIGNED" and _event_key(assigned_event) in latest_assignment_events_in_range:
            staff_jira_followups[initial_email] += 1
        if key in completed_keys_in_range and job.get("has_completed"):
            staff_completed[assigned_email] += 1
        if job.get("has_assigned") and not job.get("has_completed"):
            staff_active[assigned_email] += 1

        if key in completed_keys_in_range:
            assigned_ts = job.get("assigned_ts")
            completed_ts = job.get("completed_ts")
            if assigned_ts and completed_ts:
                dur = _business_seconds(assigned_ts, completed_ts)
                if 0 < dur <= _DURATION_HARD_CAP_SEC and dur <= _DURATION_MISMATCH_SEC:
                    staff_durations[assigned_email].append(dur)

    canonical_active_by_staff: dict[str, int] = {}
    if external_active_by_staff:
        canonical_active_by_staff = {
            email: count
            for email, count in active_by_staff.items()
            if count > 0 and email in canonical_staff_emails
        }

    all_emails = set(staff_assigned) | set(staff_completed) | set(staff_jira_followups) | set(reconciled_per_staff or {})
    if external_active_by_staff:
        all_emails |= set(canonical_active_by_staff)
    else:
        all_emails |= set(staff_active)
    result = []
    for email in sorted(all_emails):
        assigned = staff_assigned.get(email, 0)
        completed = staff_completed.get(email, 0) + (reconciled_per_staff.get(email, 0) if reconciled_per_staff else 0)
        if external_active_by_staff:
            active = canonical_active_by_staff.get(email, 0)
        else:
            active = staff_active.get(email, 0)

        durations_min = sorted([d / 60.0 for d in staff_durations.get(email, [])])
        median_min = round(_median(durations_min), 1) if durations_min else None
        p90_min = round(_percentile(durations_min, 0.9), 1) if durations_min else None
        low_confidence = False

        result.append({
            "email": email,
            "name": _staff_display_name(email),
            "assigned": assigned,
            "assigned_in_range": staff_assigned_in_range.get(email, 0),
            "jira_followups": staff_jira_followups.get(email, 0),
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
                         staff_filter: str | None = None, *,
                         canonical_durations: dict[str, float] | None = None,
                         activity_mode: str | None = None,
                         activity_staff: str | None = None) -> list[dict]:
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
        key = _resolve_group_key(e)
        if not key:
            continue
        ts = _resolve_ts(e, "assigned")
        if not ts:
            continue
        prev = earliest_assigned_ts.get(key)
        if prev is None or ts < prev:
            earliest_assigned_ts[key] = ts

    # 2. Per-staff sorted lists of ASSIGNED timestamps (for nearest-preceding matching)
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

    # Pre-consume queue with COMPLETED events that precede the filtered set,
    # so that today's completions don't match stale assignments.
    filtered_set = set(_event_key(e) for e in filtered)
    for e in all_events:
        if e["event_type"] != "COMPLETED" or _event_key(e) in filtered_set:
            continue
        email = _resolve_email(e)
        if not email:
            continue
        key = _resolve_group_key(e)
        if key and earliest_assigned_ts.get(key):
            continue
        completed_ts = _resolve_ts(e, "completed")
        if completed_ts and staff_queues.get(email):
            _pop_nearest_preceding(staff_queues[email], completed_ts)

    # Pre-compute durations for ALL filtered COMPLETED events in chronological
    # order.  Nearest-preceding matching must happen chronologically so that
    # newer completions don't steal the nearest ASSIGNED from earlier ones.
    _precomputed_dur: dict[tuple, float | None] = {}
    _completed_chrono = [
        e for e in filtered
        if e["event_type"] == "COMPLETED" and not (e["duration_sec"] and e["duration_sec"] > 0)
    ]
    _completed_chrono.sort(key=lambda e: e.get("completed_ts") or e.get("event_ts") or datetime.min)
    for e in _completed_chrono:
        completed_ts = _resolve_ts(e, "completed")
        key = _resolve_group_key(e)
        assigned_ts = earliest_assigned_ts.get(key) if key else None
        email = _resolve_email(e)
        dur = None
        if assigned_ts and completed_ts:
            dur = _business_seconds(assigned_ts, completed_ts)
        elif completed_ts and email and staff_queues.get(email):
            a_ts = _pop_nearest_preceding(staff_queues[email], completed_ts)
            if a_ts is not None:
                dur = _business_seconds(a_ts, completed_ts)
        if dur is not None and (dur <= 0 or dur > _DURATION_HARD_CAP_SEC):
            dur = None
        # Suppress display for suspected mismatches (> 2 BD) but keep in dict as None
        if dur is not None and dur > _DURATION_MISMATCH_SEC:
            dur = None
        _precomputed_dur[_event_key(e)] = dur

    # ------------------------------------------------------------------

    jira_followup_keys: set[str] = set()
    if activity_mode == "jira_followups" and activity_staff:
        target_staff = activity_staff.strip().lower()
        jobs: dict[str, dict[str, Any]] = {}
        for e in all_events:
            key = _resolve_sami_group_key(e)
            if not key:
                continue
            event_type = (e.get("event_type") or "").strip().upper()
            action = (e.get("action") or e.get("Action") or "").strip().upper()
            if event_type == "CONFIG_CHANGED":
                continue
            if event_type not in ("ASSIGNED", "COMPLETED", "REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
                continue
            job = jobs.setdefault(key, {"initial_email": None, "latest_email": None, "is_jira_followup": False, "latest_event_type": None})
            if event_type == "JIRA_FOLLOWUP_ASSIGNED" or action == "JIRA_FOLLOWUP":
                job["is_jira_followup"] = True
            if event_type in ("ASSIGNED", "JIRA_FOLLOWUP_ASSIGNED") and job.get("initial_email") is None:
                email = (e.get("assigned_to") or "").strip().lower()
                if _is_staff(email):
                    job["initial_email"] = email
            if event_type in ("ASSIGNED", "REASSIGN_MANUAL", "JIRA_FOLLOWUP_ASSIGNED"):
                email = (e.get("assigned_to") or "").strip().lower()
                if _is_staff(email):
                    job["latest_email"] = email
                    job["latest_event_type"] = event_type
        jira_followup_keys = {
            key for key, job in jobs.items()
            if job.get("is_jira_followup")
            and job.get("initial_email")
            and (
                job.get("initial_email") == target_staff
                or _staff_display_name(job.get("initial_email") or "").lower() == target_staff
            )
        }

    # Only include events with a timestamp and a meaningful event type
    feed_events = [
        e for e in filtered
        if e["event_ts"] and e["event_type"] in (("JIRA_FOLLOWUP_ASSIGNED",) if activity_mode == "jira_followups" else ("ASSIGNED", "COMPLETED"))
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
    if activity_mode == "jira_followups" and activity_staff:
        filtered_followups = [
            e for e in feed_events
            if _resolve_sami_group_key(e) in jira_followup_keys
        ]
        deduped_followups = []
        seen_followup_keys: set[str] = set()
        for e in filtered_followups:
            key = _resolve_sami_group_key(e)
            if not key or key in seen_followup_keys:
                continue
            seen_followup_keys.add(key)
            deduped_followups.append(e)
        feed_events = deduped_followups
    elif staff_filter:
        sf_lower = staff_filter.strip().lower()
        feed_events = [
            e for e in feed_events
            if _staff_display_name(_display_staff(e)).lower() == sf_lower
            or _display_staff(e).lower() == sf_lower
        ]

    result = []
    for e in feed_events[:limit]:
        staff = _display_staff(e)

        # Infer duration for COMPLETED events — prefer canonical SAMI duration,
        # fall back to queue-based inference for non-SAMI events
        dur_sec = e["duration_sec"]
        if e["event_type"] == "COMPLETED" and not dur_sec:
            sami_key = _resolve_sami_group_key(e)
            if sami_key and canonical_durations and sami_key in canonical_durations:
                dur_sec = canonical_durations[sami_key]
            else:
                dur_sec = _precomputed_dur.get(_event_key(e))

        sender = (e.get("sender") or e.get("Sender") or "").strip().lower()
        result.append({
            "time": e["event_ts"].strftime("%H:%M:%S") if e["event_ts"] else "",
            "date": e["date"],
            "type": e["event_type"],
            "action": e.get("action", "") or e.get("Action", ""),
            "subject": e["subject"][:80],
            "sami_ref": _display_sami_ref(e),
            "assigned_to": _staff_display_name(staff) if _is_staff(staff) else staff,
            "sender": _staff_display_name(sender) if _is_staff(sender) else (e.get("sender") or e.get("Sender") or ""),
            "domain": e.get("domain_bucket", "") or e.get("Domain Bucket", ""),
            "duration_human": format_duration_human(dur_sec) if dur_sec else "",
            "duration_sec": dur_sec if dur_sec else None,
            "risk_level": e["risk_level"],
        })
    return result

