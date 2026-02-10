"""Paths and constants for the Transfer-Bot dashboard."""

from pathlib import Path

# ── Base directory (parent of dashboard/) ──
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# ── Data files (read-only except staff.txt) ──
DAILY_STATS_CSV = BASE_DIR / "daily_stats.csv"
STAFF_TXT = BASE_DIR / "staff.txt"
MANAGERS_TXT = BASE_DIR / "managers.txt"
APPS_TXT = BASE_DIR / "apps.txt"
ROSTER_STATE_JSON = BASE_DIR / "roster_state.json"
SETTINGS_OVERRIDES_JSON = BASE_DIR / "settings_overrides.json"
HIB_WATCHDOG_JSON = BASE_DIR / "hib_watchdog.json"
DOMAIN_POLICY_JSON = BASE_DIR / "domain_policy.json"

# ── Server ──
HOST = "0.0.0.0"
PORT = 3000

# ── Polling / caching ──
REFRESH_INTERVAL_SEC = 15

# ── CSV schema ──
EXPECTED_COLUMNS = [
    "Date", "Time", "Subject", "Assigned To", "Sender", "Risk Level",
    "Domain Bucket", "Action", "Policy Source", "event_type", "msg_key",
    "status_after", "assigned_to", "assigned_ts", "completed_ts", "duration_sec",
]

# ── Assignees to exclude from staff KPIs ──
NON_STAFF_ASSIGNEES = frozenset([
    "bot", "completed", "hib", "hold", "error",
    "quarantined", "system_notification",
])

# ── Thresholds ──
P90_WARNING_MINUTES = 15.0
ACTIVE_AMBER_THRESHOLD = 3
LOW_CONFIDENCE_MIN_COMPLETIONS = 10
