"""
Helpdesk Clinical Safety Bot v2.2
Risk-Aware Clinical Dispatcher with SLA Watchdog

Features:
- Fair round-robin distribution
- Semantic risk detection (deletions, urgent requests)
- 20-minute SLA enforcement
- Manager escalation on breach
- Robust error handling (never crashes)
"""

import os
import sys
import time
import json
import csv
import schedule
import atexit
import subprocess
import traceback
import re
import html
import hashlib
from urllib.parse import quote
from datetime import datetime, timedelta

# Windows-specific imports (graceful fallback for Linux/Mac)
try:
    import win32com.client
    OUTLOOK_AVAILABLE = True
except ImportError:
    OUTLOOK_AVAILABLE = False
    print("⚠️ pywin32 not available - running in demo mode")

# ==================== CONFIGURATION ====================
CONFIG = {
    "mailbox": "Brian.Shaw@sa.gov.au",
    "inbox_folder": "Transfer Bot Test Received",
    "manager": "manager@example.com",
    "sla_minutes": 20,
    "check_interval_seconds": 60,
    "processed_folder": "Transfer Bot Test",
    "send_urgency_notifications": False,
    "enable_completion_cc": False,
    "enable_completion_workflow": False,
    "quarantine_folder": "Inbox/03_QUARANTINE",
    "completed_folder": "Inbox/01_COMPLETED"
}

RISK_FILTER_ENABLED = False

FILES = {
    "staff": "staff.txt",
    "state": "roster_state.json",
    "log": "daily_stats.csv",
    "watchdog": "urgent_watchdog.json"
}

PROCESSED_LEDGER_PATH = "processed_ledger.json"
POISON_COUNTS_PATH = "poison_counts.json"
LOCK_PATH = "bot.lock"
SETTINGS_OVERRIDES_PATH = "settings_overrides.json"
DOMAIN_POLICY_PATH = "domain_policy.json"
STAFF_PATH = os.path.join(os.path.dirname(__file__), "staff.txt")
STAFF_JSON_PATH = os.path.join(os.path.dirname(__file__), "staff.json")
APPS_TEAM_JSON_PATH = os.path.join(os.path.dirname(__file__), "apps_team.json")
MANAGER_CONFIG_JSON_PATH = os.path.join(os.path.dirname(__file__), "manager_config.json")
SYSTEM_BUCKETS_JSON_PATH = os.path.join(os.path.dirname(__file__), "system_buckets.json")
MANAGERS_TXT_PATH = os.path.join(os.path.dirname(__file__), "managers.txt")
APPS_TXT_PATH = os.path.join(os.path.dirname(__file__), "apps.txt")
COMPLETION_CC_ADDR = "completion.placeholder@example.invalid"
SAMI_SHARED_INBOX = "health.samisupportteam@sa.gov.au"
COMPLETION_SUBJECT_KEYWORD = "[COMPLETED]"
COMPLETION_SUBJECT_PREFIX = "[COMPLETED] "
SAMI_SUPPORT_MAILBOX = "health.samisupportteam@sa.gov.au"
COMPLETION_FOOTER_TEMPLATE = (
    "If this request has not been resolved in a timely manner, "
    "please email {mailbox} and quote reference {ref}."
)
HEARTBEAT_INTERVAL_SECONDS = 300
JIRA_FOLLOW_UP_FOLDER_PATH = "Inbox/06_JIRA_FOLLOW_UP"
JIRA_FOLLOW_UP_SUBJECT_PREFIX = "[JIRA FOLLOW-UP] "
JIRA_FOLLOW_UP_BANNER = (
    "\u26A0 JIRA FOLLOW-UP REQUEST\n\n"
    "A comment has been added in Jira indicating the transfer may not have completed correctly.\n\n"
    "Please review the original job and verify transfer status before marking complete.\n\n"
    "--- Original Jira Email Below ---\n\n"
)

# HIB routing and burst detection
HIB_FOLDER_NAME = "04_HIB"
HIB_WATCHDOG_PATH = "hib_watchdog.json"
HIB_BURST_WINDOW_MIN = 30
HIB_BURST_THRESHOLD = 15
HIB_BURST_COOLDOWN_MIN = 60
_staff_list_cache = None
_safe_mode_cache = None
_safe_mode_inbox = None
_live_test_override = False
_jira_followup_folder_error_logged = False

# Hot-reloaded dashboard-managed config (last-known-good per file).
_hot_config_state = {
    "staff": {"seen_fp": None, "seen_sha": None, "lkg": None, "lkg_sha": None},
    "apps_team": {"seen_fp": None, "seen_sha": None, "lkg": None, "lkg_sha": None},
    "manager_config": {"seen_fp": None, "seen_sha": None, "lkg": None, "lkg_sha": None},
    "system_buckets": {"seen_fp": None, "seen_sha": None, "lkg": None, "lkg_sha": None},
}

def is_valid_completion_cc(value):
    if not isinstance(value, str):
        return False
    addr = value.strip()
    if not addr or " " in addr or len(addr) < 6 or len(addr) > 254:
        return False
    if addr.count("@") != 1:
        return False
    local, domain = addr.split("@")
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    return True

def is_valid_email(value):
    """Validate email address format (for apps_cc_addr, manager_cc_addr)"""
    if not isinstance(value, str):
        return False
    raw = value.strip()
    if not raw:
        return False
    parts = [part.strip() for part in raw.split(";")]
    for part in parts:
        if not part:
            return False
        if any(ch.isspace() for ch in part):
            return False
        if len(part) < 6 or len(part) > 254:
            return False
        if part.count("@") != 1:
            return False
        local, domain = part.split("@")
        if not local or not domain:
            return False
        if "." not in domain:
            return False
    return True

def is_valid_unknown_domain_mode(value):
    """Validate unknown_domain_mode enum"""
    if not isinstance(value, str):
        return False
    valid_modes = {"hold_manager", "hold_apps", "hold_both"}
    return value.strip() in valid_modes

def is_urgent_watchdog_disabled(overrides):
    return (not RISK_FILTER_ENABLED) or bool(overrides.get("disable_urgent_watchdog", False))

def get_override_addr(overrides, key):
    if not isinstance(overrides, dict):
        return None
    value = overrides.get(key)
    if not isinstance(value, str):
        return None
    addr = value.strip()
    if not addr:
        return None
    if "@" not in addr or "." not in addr:
        return None
    return addr

ALLOWED_OVERRIDES = {
    "inbox_folder": lambda v: isinstance(v, str) and v.strip(),
    "processed_folder": lambda v: isinstance(v, str) and v.strip(),
    "completion_cc_addr": is_valid_completion_cc,
    "apps_cc_addr": is_valid_email,
    "manager_cc_addr": is_valid_email,
    "unknown_domain_mode": is_valid_unknown_domain_mode,
    "target_mailbox_store": lambda v: isinstance(v, str) and v.strip(),
    "disable_urgent_watchdog": lambda v: isinstance(v, bool)
}

# ==================== SAFE_MODE ====================
def determine_safe_mode(inbox_folder):
    """
    Determine SAFE_MODE status using a specific inbox folder value.

    Returns: (is_safe, reason, live_test_override)
    """
    env_value = os.environ.get("TRANSFER_BOT_LIVE", "").strip().lower()
    if env_value != "true":
        return (True, "env_missing", False)

    inbox_value = inbox_folder or ""
    if "test" in inbox_value.lower():
        test_ok = os.environ.get("TRANSFER_BOT_ALLOW_TEST_FOLDER", "").strip().lower()
        if test_ok != "true":
            legacy_ok = os.environ.get("TRANSFER_BOT_LIVE_TEST_OK", "").strip().lower()
            if legacy_ok == "true":
                test_ok = "true"
        if test_ok == "true":
            return (False, "live_test_override", True)
        return (True, "test_folder", False)

    return (False, "live_mode_armed", False)

def is_safe_mode():
    """
    Check if SAFE_MODE is active (prevents sending emails).

    Returns: (is_safe, reason)
        is_safe: True if SAFE_MODE active (no sending)
        reason: String explaining why SAFE_MODE is active
    """
    if _safe_mode_cache is not None:
        return _safe_mode_cache
    is_safe, reason, _ = determine_safe_mode(CONFIG.get("inbox_folder", ""))
    return (is_safe, reason)

def log_safe_mode_status(inbox_folder=None):
    """Log SAFE_MODE status"""
    inbox_value = inbox_folder if inbox_folder is not None else CONFIG.get("inbox_folder", "")
    is_safe, reason, override_active = determine_safe_mode(inbox_value)
    if override_active:
        log(f"LIVE_TEST_OVERRIDE_ENABLED inbox_folder={inbox_value}", "INFO")
    if is_safe:
        if reason == "env_missing":
            log("SAFE_MODE_ACTIVE reason=env_missing (TRANSFER_BOT_LIVE not set to 'true')", "WARN")
        elif reason == "test_folder":
            log(f"SAFE_MODE_ACTIVE reason=test_folder inbox_folder={inbox_value} (set TRANSFER_BOT_ALLOW_TEST_FOLDER=true to allow)", "WARN")
        log("*** NO EMAILS WILL BE SENT IN SAFE_MODE ***", "WARN")
    else:
        log("LIVE_MODE_ARMED - emails will be sent", "WARN")

# ==================== SEMANTIC DICTIONARY ====================
# Risk Detection: (Action + Context) OR (Urgency + Action) OR (High Importance)

RISK_ACTIONS = [
    "delete", "deletion", "remove", "unlink", "purge", "erase", "destroy",
    "cancel", "void", "nullify", "terminate", 
    "merge", "merging", "merged", "split", "splitting",
    "combine", "duplicate", "dedupe", "dedup"
]

RISK_CONTEXT = [
    "patient", "scan", "accession", "study", "exam", "report",
    "imaging", "dicom", "mri", "ct", "ultrasound", "xray", "x-ray",
    "record", "data", "file", "prior", "comparison"
]

URGENCY_WORDS = [
    "stat", "asap", "urgent", "emergency", "critical", "immediate",
    "now", "rush", "priority", "life-threatening", "code"
]

CRITICAL_BANNER_HEADER = "CRITICAL RISK TICKET"
MANAGER_NOTIFICATION_BANNER = (
    "[ Manager Notification ]\n"
    "This request has been forwarded to the Manager for visibility and oversight.\n\n"
)
APPS_TEAM_NOTIFICATION_BANNER = (
    "[ Applications Team Notification ]\n"
    "This request has been forwarded to the Applications Team for action.\n\n"
)

# ==================== HELPERS ====================
def dedupe_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out

def build_critical_one_liner(orig_subject, sla_minutes, reasons):
    reasons_u = dedupe_preserve_order(reasons)
    reason_str = "; ".join(reasons_u) if reasons_u else "Unspecified"
    return f"CRITICAL | SLA {sla_minutes}m | {reason_str} | Subject: {orig_subject}"

_re_assigned = re.compile(r"\[Assigned:\s*[^]]+\]", re.IGNORECASE)
_re_critical = re.compile(r"\[CRITICAL\]", re.IGNORECASE)

def strip_bot_subject_tags(subject):
    if not subject:
        return ""
    cleaned = subject
    for _ in range(5):
        cleaned = _re_assigned.sub("", cleaned)
        cleaned = _re_critical.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def is_completion_subject(subject):
    if not subject:
        return False
    return COMPLETION_SUBJECT_KEYWORD.lower() in str(subject).lower()

def is_jira_candidate(subject, body, sender):
    subject_lower = (subject or "").lower()
    body_lower = (body or "").lower()
    sender_lower = (sender or "").lower()
    return (
        ("comment" in subject_lower)
        or ("atlassian" in body_lower)
        or ("view request" in body_lower)
        or ("jira" in sender_lower)
    )

def is_jira_comment_email(body):
    return "request comments:" in (body or "").lower()

def build_completion_subject(base_subject, is_jira_followup=False):
    subject_text = (base_subject or "").strip()
    if is_completion_subject(subject_text):
        return subject_text
    if is_jira_followup:
        return f"{COMPLETION_SUBJECT_KEYWORD}[JIRA] {subject_text}".strip()
    return f"{COMPLETION_SUBJECT_PREFIX}{subject_text}".strip()

def is_staff_completed_confirmation(sender_email, subject, staff_set):
    """Return True if sender is staff and subject contains [COMPLETED]."""
    if not sender_email or not subject:
        return False
    return sender_email.lower().strip() in staff_set and is_completion_subject(subject)

def compute_sami_id(msg):
    try:
        entry_id = getattr(msg, "EntryID", "") or ""
    except Exception:
        entry_id = ""
    seed = str(entry_id).strip()
    if not seed:
        try:
            received_time = getattr(msg, "ReceivedTime", None)
            received_iso = received_time.isoformat() if received_time else ""
        except Exception:
            received_iso = ""
        try:
            sender = getattr(msg, "SenderEmailAddress", "") or ""
        except Exception:
            sender = ""
        try:
            message_class = getattr(msg, "MessageClass", "") or ""
        except Exception:
            message_class = ""
        seed = f"{received_iso}|{sender}|{message_class}|fallback"
    if not seed:
        return ""
    try:
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest().upper()
        return f"SAMI-{digest[:6]}"
    except Exception:
        log("SAMI_ID_COMPUTE_FAIL", "WARN")
        return ""

def ensure_sami_id_in_subject(subject: str, msg) -> str:
    text = "" if subject is None else str(subject)
    if "[sami-" in text.lower():
        return text
    sami_id = compute_sami_id(msg)
    if not sami_id:
        return text
    return f"[{sami_id}] {text}".strip()

def prepend_banner(existing_body, banner):
    body = existing_body or ""
    return (banner + body) if banner else body

def build_completion_mailto(to_addr, cc_addr, subject):
    to_value = str(to_addr).strip() if to_addr else ""
    if not to_value or "@" not in to_value:
        return ""
    cc_value = str(cc_addr).strip() if cc_addr else ""
    subject_value = "" if subject is None else str(subject)
    params = []
    if cc_value:
        params.append(f"cc={cc_value}")
    params.append(f"subject={quote(subject_value)}")
    return f"mailto:{to_value}?{'&'.join(params)}"

def prepend_completion_hotlink_html(html, mailto_url):
    html_notice = (
        '<p><b>Mark job complete:</b> '
        f'<a href="{mailto_url}">Click to notify requester (CC SAMI)</a></p>'
        "<hr/>"
    )
    return html_notice + (html or "")

COMPLETION_MAILTO_BODY_MAX_LEN = 1800
COMPLETION_MAILTO_MAX_URL_LEN = 1800
COMPLETION_MAILTO_URL_MAX_LEN = 1800
COMPLETION_MAILTO_STRIP_SAFELINKS = True
COMPLETION_MAILTO_STRIP_DISCLAIMER = True

def _html_to_text_minimal(html_str):
    if not html_str:
        return ""
    text = html_str
    for tag in ("<br>", "<br/>", "<br />", "</p>", "</div>", "</tr>", "</li>"):
        text = re.sub(re.escape(tag), "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def get_completion_source_body_text(msg):
    try:
        body = getattr(msg, "Body", "") or ""
    except Exception:
        body = ""
    body_text = body.strip()
    if body_text:
        return body_text, "body"
    try:
        html_body = getattr(msg, "HTMLBody", "") or ""
    except Exception:
        html_body = ""
    html_text = _html_to_text_minimal(html_body)
    if html_text:
        return html_text, "html"
    return "", "none"

def sanitize_completion_excerpt(text):
    if not text:
        return ""
    lines = text.splitlines()
    out = []
    blank_run = 0
    for line in lines:
        raw = line.strip()
        lower = raw.lower()
        if COMPLETION_MAILTO_STRIP_DISCLAIMER and "this email and any attachments are confidential" in lower:
            break
        if COMPLETION_MAILTO_STRIP_SAFELINKS and "safelinks.protection.outlook.com" in lower:
            continue
        if lower.startswith("http://") or lower.startswith("https://"):
            continue
        if "<tel:" in lower or lower.startswith("tel:"):
            continue
        if raw == "":
            blank_run += 1
            if blank_run > 2:
                continue
            out.append("")
            continue
        blank_run = 0
        out.append(raw)
    while out and out[0] == "":
        out.pop(0)
    while out and out[-1] == "":
        out.pop()
    return "\r\n".join(out)

def build_completion_mailto_body(msg):
    """Build plain-text body for completion mailto with original email context."""
    if msg is None:
        return "", False
    try:
        sender_name = getattr(msg, "SenderName", "") or ""
        sender_email = resolve_sender_smtp(msg) or getattr(msg, "SenderEmailAddress", "") or ""
        subject = getattr(msg, "Subject", "") or ""
        try:
            received_time = msg.ReceivedTime
            received_str = received_time.strftime("%d %b %Y %H:%M") if received_time else ""
        except Exception:
            received_str = ""
        original_body, source = get_completion_source_body_text(msg)
        original_body = sanitize_completion_excerpt(original_body)
        header_block = (
            f"From: {sender_name} <{sender_email}>\r\n"
            f"Received: {received_str}\r\n"
            f"Subject: {subject}\r\n"
            "----- Original request -----\r\n"
        )
        full_body = header_block + original_body
        truncated = False
        if len(full_body) > COMPLETION_MAILTO_BODY_MAX_LEN:
            full_body = full_body[:COMPLETION_MAILTO_BODY_MAX_LEN] + "\r\n...(truncated)"
            truncated = True
        return full_body, truncated
    except Exception:
        return "", False

def build_completion_mailto_url(to_email, cc_email, subject, body=None):
    to_value = str(to_email).strip() if to_email else ""
    if not to_value or "@" not in to_value:
        return ""
    cc_value = str(cc_email).strip() if cc_email else ""
    subject_value = "" if subject is None else str(subject)
    if not subject_value.lstrip().lower().startswith(COMPLETION_SUBJECT_KEYWORD.lower()):
        subject_value = f"{COMPLETION_SUBJECT_PREFIX}{subject_value}".strip()
        log("COMPLETION_SUBJECT_PREFIXED added=1", "INFO")
    # Extract SAMI reference for completion footer
    sami_match = re.search(r'\bSAMI-\d+\b', subject_value)
    sami_ref = sami_match.group(0) if sami_match else "the reference in the subject"
    # Build completion footer
    completion_footer = COMPLETION_FOOTER_TEMPLATE.format(
        mailbox=SAMI_SUPPORT_MAILBOX,
        ref=sami_ref
    )
    # Append footer to body (or create body with footer if none exists)
    if not body:
        body = completion_footer
    else:
        body = body + "\n\n" + completion_footer
    base_params = []
    if cc_value:
        base_params.append(f"cc={quote(cc_value, safe='@')}")
    base_params.append(f"subject={quote(subject_value, safe='')}")
    base_url = f"mailto:{to_value}?{'&'.join(base_params)}"
    if not body:
        log(f"COMPLETION_MAILTO url_len={len(base_url)} body_included=no reason=no_body", "INFO")
        return base_url
    encoded_body = quote(body, safe='')
    full_url = base_url + "&body=" + encoded_body
    if len(full_url) <= COMPLETION_MAILTO_URL_MAX_LEN:
        log(f"COMPLETION_MAILTO url_len={len(full_url)} body_included=yes", "INFO")
        return full_url
    header_part = ""
    excerpt_part = body
    lines = body.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "-----" or stripped == "----- Original request -----":
            header_part = "\r\n".join(lines[:i + 1]) + "\r\n"
            excerpt_part = "\r\n".join(lines[i + 1:])
            break
    trimmed = False
    for _ in range(8):
        if len(full_url) <= COMPLETION_MAILTO_URL_MAX_LEN:
            break
        if not excerpt_part:
            break
        keep_len = int(len(excerpt_part) * 0.8)
        if keep_len >= len(excerpt_part):
            keep_len = max(0, len(excerpt_part) - 200)
        excerpt_part = excerpt_part[:keep_len]
        body = header_part + excerpt_part
        encoded_body = quote(body, safe='')
        full_url = base_url + "&body=" + encoded_body
        trimmed = True
    if len(full_url) <= COMPLETION_MAILTO_URL_MAX_LEN:
        if trimmed:
            log(f"COMPLETION_MAILTO_TRIM url_len={len(full_url)} max={COMPLETION_MAILTO_URL_MAX_LEN} reason=url_too_long", "INFO")
        log(f"COMPLETION_MAILTO url_len={len(full_url)} body_included=yes", "INFO")
        return full_url
    if header_part:
        body = header_part
        encoded_body = quote(body, safe='')
        full_url = base_url + "&body=" + encoded_body
        if len(full_url) <= COMPLETION_MAILTO_URL_MAX_LEN:
            log(f"COMPLETION_MAILTO_TRIM url_len={len(full_url)} max={COMPLETION_MAILTO_URL_MAX_LEN} reason=url_too_long", "INFO")
            log(f"COMPLETION_MAILTO url_len={len(full_url)} body_included=yes", "INFO")
            return full_url
    if trimmed:
        log(f"COMPLETION_MAILTO_TRIM url_len={len(base_url)} max={COMPLETION_MAILTO_URL_MAX_LEN} reason=url_too_long", "INFO")
    log(f"COMPLETION_MAILTO url_len={len(base_url)} body_included=no reason=too_long", "INFO")
    return base_url

def inject_completion_hotlink(fwd, original_sender_email, original_subject, sami_inbox, mode_out=None, \
        original_msg=None, is_jira_followup=False):
    if fwd is None:
        return False
    body_text, truncated = build_completion_mailto_body(original_msg)
    if body_text:
        log(f"COMPLETE_MAILTO_BODY len={len(body_text)} truncated={truncated}", "INFO")
    body_param = body_text or None
    mailto_subject = build_completion_subject(original_subject, is_jira_followup=is_jira_followup)
    mailto_url = build_completion_mailto_url(
        original_sender_email,
        sami_inbox,
        mailto_subject,
        body=body_param,
    )
    if not mailto_url:
        return False
    mailto_url_html = mailto_url.replace("&", "&amp;")
    href_amp = "yes" if "&amp;" in mailto_url_html else "no"
    log(f"COMPLETION_HOTLINK href_amp={href_amp}", "INFO")
    html_notice = (
        '<p><b>Mark job complete:</b> '
        f'<a href="{mailto_url_html}">Click to notify requester (CC SAMI)</a></p>'
        "<hr/>"
    )
    text_notice = (
        "Mark job complete:\n"
        f"{mailto_url}\n"
        "-----\n"
    )
    use_html = False
    try:
        if getattr(fwd, "BodyFormat", None) == 2:
            use_html = True
    except Exception:
        pass
    if fwd.HTMLBody:
        use_html = True
    if use_html:
        try:
            fwd.BodyFormat = 2
        except Exception:
            pass
        fwd.HTMLBody = html_notice + (fwd.HTMLBody or "")
        mode = "HTML"
    else:
        fwd.Body = text_notice + (fwd.Body or "")
        mode = "TEXT"
    if mode_out is not None:
        mode_out.append(mode)
    return True

def extract_subject_from_body(body_text):
    if not body_text:
        return ""
    match = re.search(r"^Subject:\s*(.+)$", body_text, re.IGNORECASE | re.MULTILINE)
    if match:
        return match.group(1).strip()
    return ""

def message_has_completion_cc(msg, target_addr):
    target = (target_addr or "").lower()
    if not target:
        return False
    try:
        cc_line = getattr(msg, "CC", "") or ""
        if target in cc_line.lower():
            return True
    except Exception:
        pass
    try:
        to_line = getattr(msg, "To", "") or ""
        if target in to_line.lower():
            return True
    except Exception:
        pass
    try:
        for rec in msg.Recipients:
            try:
                addr = rec.Address or rec.Name or ""
            except Exception:
                addr = ""
            if target in str(addr).lower():
                return True
    except Exception:
        pass
    return False

def find_ledger_key_by_conversation_id(ledger, conversation_id):
    if not conversation_id:
        return None
    for key, entry in ledger.items():
        if isinstance(entry, dict) and entry.get("conversation_id") == conversation_id:
            return key
    return None

def compute_message_identity(msg, sender_email, subject, received_iso):
    entry_id = None
    store_id = None
    internet_message_id = None
    try:
        entry_id = msg.EntryID
    except Exception:
        entry_id = None
    try:
        store_id = msg.StoreID
    except Exception:
        try:
            store_id = msg.Parent.Store.StoreID
        except Exception:
            store_id = None
    try:
        internet_message_id = msg.InternetMessageID
    except Exception:
        internet_message_id = None
    if store_id and entry_id:
        message_key = f"store:{store_id}|entry:{entry_id}"
    elif internet_message_id:
        message_key = f"internet:{internet_message_id}"
    elif entry_id:
        message_key = entry_id
    else:
        message_key = f"fallback:{sender_email}|{subject}|{received_iso}"
    return message_key, {
        "entry_id": entry_id,
        "store_id": store_id,
        "internet_message_id": internet_message_id
    }

# ==================== LOGGING ====================
def log(msg, level="INFO"):
    """Timestamped logging (encoding-safe for Windows console)"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # ASCII-only symbols to prevent Windows console encoding crashes
    symbol = {"INFO": "[INFO]", "WARN": "[WARN]", "ERROR": "[ERROR]", "CRITICAL": "[CRIT]", "SUCCESS": "[OK]"}.get(level, "[LOG]")
    # Sanitize message to ASCII to prevent encoding crashes
    safe_msg = str(msg).encode("ascii", "backslashreplace").decode("ascii")
    print(f"[{timestamp}] {symbol} {safe_msg}")

    # Also append to log file (UTF-8 safe)
    try:
        with open("bot_activity.log", "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [{level}] {msg}\n")
    except:
        pass


_lock_acquired = False
_last_heartbeat_ts = 0

def maybe_emit_heartbeat(mailbox, inbox_folder, processed_folder):
    global _last_heartbeat_ts
    now_ts = time.time()
    if now_ts - _last_heartbeat_ts >= HEARTBEAT_INTERVAL_SECONDS:
        append_stats(
            f"HEARTBEAT inbox={inbox_folder} processed={processed_folder}",
            "bot",
            "system",
            "HEARTBEAT",
            "",
            "HEARTBEAT",
            ""
        )
        _last_heartbeat_ts = now_ts

def acquire_lock():
    # Acquire single-instance lock
    global _lock_acquired
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(f"{os.getpid()}\n")
        _lock_acquired = True
        log("LOCK_ACQUIRED", "INFO")
        return True
    except FileExistsError:
        if sys.platform.startswith("win"):
            if not is_bot_running_windows(os.path.abspath(".")):
                try:
                    os.remove(LOCK_PATH)
                    log(f"LOCK_STALE_CLEARED path={LOCK_PATH}", "WARN")
                    return acquire_lock()
                except Exception:
                    pass
        log("LOCK_EXISTS_EXIT", "WARN")
        try:
            stat = os.stat(LOCK_PATH)
            log(f"LOCK_FILE_PRESENT path={LOCK_PATH} mtime={stat.st_mtime} size={stat.st_size}", "WARN")
        except Exception:
            pass
        log(f"INSTANCE_ALREADY_RUNNING lock_path={LOCK_PATH}", "WARN")
        return False
    except Exception as e:
        log(f"Lock error for {LOCK_PATH}: {e}", "ERROR")
        return False

def release_lock():
    # Release single-instance lock best-effort
    if not _lock_acquired:
        return
    try:
        os.remove(LOCK_PATH)
    except Exception as e:
        log(f"Lock release warning for {LOCK_PATH}: {e}", "WARN")

def is_bot_running_windows(repo_path):
    # Best-effort process check for distributor.py in this repo path
    try:
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False
        )
        if result.returncode != 0:
            return True
        output = (result.stdout or "").lower()
        repo_lower = repo_path.lower()
        for line in output.splitlines():
            if "distributor.py" in line and repo_lower in line:
                return True
        return False
    except Exception:
        return True

def safe_load_json(path, default, *, required=False, state_name=""):
    # Load JSON with warning on missing/invalid
    try:
        if not os.path.exists(path):
            log(f"STATE_MISSING state={state_name} path={path}", "WARN")
            return None if required else default
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        log(f"STATE_CORRUPT state={state_name} path={path} error={e}", "WARN")
        return None if required else default

def atomic_write_json(path, data, *, state_name=""):
    # Atomic JSON write via temp file + replace
    try:
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        tmp_path = f"{path}.tmp.{os.getpid()}"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=4, default=str)
            f.flush()
            os.fsync(f.fileno())
        for attempt in range(3):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                time.sleep(0.05 * (attempt + 1))
        else:
            raise PermissionError("replace_failed")
        return True
    except Exception as e:
        log(f"STATE_WRITE_FAIL state={state_name} path={path} error={e}", "ERROR")
        return False

def normalize_email(raw):
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    if any(ch.isspace() for ch in s):
        return None
    if s.count("@") != 1:
        return None
    local, domain = s.split("@", 1)
    if not local or not domain:
        return None
    if "." not in domain:
        return None
    return s

def normalize_domain(raw):
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    if s.startswith("@"):
        s = s[1:]
    if s.startswith("http://"):
        s = s[len("http://"):]
    elif s.startswith("https://"):
        s = s[len("https://"):]
    s = s.strip().rstrip("/")
    if not s:
        return None
    if any(ch.isspace() for ch in s):
        return None
    if "/" in s or "\\" in s:
        return None
    if "@" in s:
        return None
    if ":" in s:
        return None
    if "." not in s:
        return None
    if s.startswith(".") or s.endswith(".") or ".." in s:
        return None
    for ch in s:
        if ch.isalnum() or ch in ".-":
            continue
        return None
    return s

def _dedupe_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out

def _file_fingerprint(path):
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    except OSError:
        return None
    mtime_ns = getattr(st, "st_mtime_ns", None)
    if mtime_ns is None:
        mtime_ns = int(st.st_mtime * 1_000_000_000)
    return (mtime_ns, st.st_size)

def _parse_staff_json(obj):
    if not isinstance(obj, dict):
        return None, "staff.json must be a JSON object"
    for key in ("staff", "off_rotation", "leave"):
        if key not in obj:
            return None, f"staff.json missing key: {key}"
        if not isinstance(obj.get(key), list):
            return None, f"staff.json key not a list: {key}"
    staff = []
    for item in obj.get("staff", []):
        email = normalize_email(item)
        if not email:
            return None, "staff.json contains invalid email in staff"
        staff.append(email)
    off_rotation = []
    for item in obj.get("off_rotation", []):
        email = normalize_email(item)
        if not email:
            return None, "staff.json contains invalid email in off_rotation"
        off_rotation.append(email)
    leave = []
    for item in obj.get("leave", []):
        email = normalize_email(item)
        if not email:
            return None, "staff.json contains invalid email in leave"
        leave.append(email)
    return {
        "staff": _dedupe_preserve_order(staff),
        "off_rotation": _dedupe_preserve_order(off_rotation),
        "leave": _dedupe_preserve_order(leave),
    }, None

def _parse_recipients_json(obj, name="recipients"):
    if not isinstance(obj, dict):
        return None, f"{name}.json must be a JSON object"
    if "recipients" not in obj:
        return None, f"{name}.json missing key: recipients"
    if not isinstance(obj.get("recipients"), list):
        return None, f"{name}.json key not a list: recipients"
    recipients = []
    for item in obj.get("recipients", []):
        email = normalize_email(item)
        if not email:
            return None, f"{name}.json contains invalid email in recipients"
        recipients.append(email)
    return {"recipients": _dedupe_preserve_order(recipients)}, None

def _parse_system_buckets_json(obj):
    if not isinstance(obj, dict):
        return None, "system_buckets.json must be a JSON object"
    required_list_keys = (
        "transfer_domains",
        "system_notification_domains",
        "quarantine_domains",
        "held_domains",
    )
    for key in required_list_keys:
        if key not in obj:
            return None, f"system_buckets.json missing key: {key}"
        if not isinstance(obj.get(key), list):
            return None, f"system_buckets.json key not a list: {key}"
    if "folders" not in obj:
        return None, "system_buckets.json missing key: folders"
    if not isinstance(obj.get("folders"), dict):
        return None, "system_buckets.json key not an object: folders"

    def _parse_domains(key):
        domains = []
        for item in obj.get(key, []):
            dom = normalize_domain(item)
            if not dom:
                return None, f"system_buckets.json contains invalid domain in {key}"
            domains.append(dom)
        return _dedupe_preserve_order(domains), None

    transfer_domains, err = _parse_domains("transfer_domains")
    if err:
        return None, err
    system_notification_domains, err = _parse_domains("system_notification_domains")
    if err:
        return None, err
    quarantine_domains, err = _parse_domains("quarantine_domains")
    if err:
        return None, err
    held_domains, err = _parse_domains("held_domains")
    if err:
        return None, err

    # Optional sender override lists (backward compatible — missing keys default to [])
    def _parse_senders(key):
        raw = obj.get(key)
        if raw is None:
            return [], None
        if not isinstance(raw, list):
            return None, f"system_buckets.json key not a list: {key}"
        senders = []
        for item in raw:
            email = normalize_email(item)
            if not email:
                return None, f"system_buckets.json contains invalid email in {key}"
            senders.append(email)
        return _dedupe_preserve_order(senders), None

    transfer_senders, err = _parse_senders("transfer_senders")
    if err:
        return None, err
    system_notification_senders, err = _parse_senders("system_notification_senders")
    if err:
        return None, err
    quarantine_senders, err = _parse_senders("quarantine_senders")
    if err:
        return None, err
    held_senders, err = _parse_senders("held_senders")
    if err:
        return None, err

    folders_in = obj.get("folders", {})
    allowed_folder_keys = {
        "completed",
        "non_actionable",
        "quarantine",
        "hold",
        "system_notification",
    }
    folders = {}
    for key, value in folders_in.items():
        if key not in allowed_folder_keys:
            continue
        if not isinstance(value, str) or not value.strip():
            return None, f"system_buckets.json invalid folder name for {key}"
        folders[key] = value.strip()

    return {
        "transfer_domains": transfer_domains,
        "system_notification_domains": system_notification_domains,
        "quarantine_domains": quarantine_domains,
        "held_domains": held_domains,
        "transfer_senders": transfer_senders,
        "system_notification_senders": system_notification_senders,
        "quarantine_senders": quarantine_senders,
        "held_senders": held_senders,
        "folders": folders,
    }, None

def _reload_hot_json(name, path, parse_fn):
    state = _hot_config_state.get(name)
    if not isinstance(state, dict):
        return None, None

    fp = _file_fingerprint(path)
    if fp is None:
        return state.get("lkg"), None
    if state.get("seen_fp") == fp:
        return state.get("lkg"), None

    raw = None
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except Exception as e:
        state["seen_fp"] = fp
        return state.get("lkg"), {"event_type": "CONFIG_INVALID", "config_name": name, "error": f"read_failed:{type(e).__name__}"}

    try:
        text = raw.decode("utf-8")
    except Exception:
        try:
            text = raw.decode("utf-8-sig")
        except Exception:
            state["seen_fp"] = fp
            state["seen_sha"] = hashlib.sha256(raw).hexdigest()
            return state.get("lkg"), {"event_type": "CONFIG_INVALID", "config_name": name, "error": "decode_failed"}

    try:
        obj = json.loads(text)
    except Exception as e:
        state["seen_fp"] = fp
        state["seen_sha"] = hashlib.sha256(raw).hexdigest()
        return state.get("lkg"), {"event_type": "CONFIG_INVALID", "config_name": name, "error": f"invalid_json:{type(e).__name__}"}

    parsed, err = parse_fn(obj)
    if err:
        state["seen_fp"] = fp
        state["seen_sha"] = hashlib.sha256(raw).hexdigest()
        return state.get("lkg"), {"event_type": "CONFIG_INVALID", "config_name": name, "error": err}

    try:
        stable = json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    except Exception:
        stable = repr(parsed)
    sha = hashlib.sha256(stable.encode("utf-8")).hexdigest()

    state["seen_fp"] = fp
    state["seen_sha"] = sha
    if state.get("lkg_sha") == sha:
        return state.get("lkg"), None

    state["lkg"] = parsed
    state["lkg_sha"] = sha
    return parsed, {"event_type": "CONFIG_CHANGED", "config_name": name}

def load_config_files_each_tick():
    events = []

    staff_cfg, evt = _reload_hot_json("staff", STAFF_JSON_PATH, _parse_staff_json)
    if evt:
        events.append(evt)
    if staff_cfg is None:
        staff_cfg = {"staff": get_staff_list(), "off_rotation": [], "leave": []}

    apps_cfg, evt = _reload_hot_json(
        "apps_team",
        APPS_TEAM_JSON_PATH,
        lambda obj: _parse_recipients_json(obj, name="apps_team"),
    )
    if evt:
        events.append(evt)
    if apps_cfg is None:
        apps = []
        if os.path.exists(APPS_TXT_PATH):
            try:
                with open(APPS_TXT_PATH, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        s = line.strip()
                        if not s or s.startswith("#"):
                            continue
                        email = normalize_email(s)
                        if email:
                            apps.append(email)
            except Exception:
                pass
        apps_cfg = {"recipients": _dedupe_preserve_order(apps)}

    mgr_cfg, evt = _reload_hot_json(
        "manager_config",
        MANAGER_CONFIG_JSON_PATH,
        lambda obj: _parse_recipients_json(obj, name="manager_config"),
    )
    if evt:
        events.append(evt)
    if mgr_cfg is None:
        mgrs = []
        if os.path.exists(MANAGERS_TXT_PATH):
            try:
                with open(MANAGERS_TXT_PATH, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        s = line.strip()
                        if not s or s.startswith("#"):
                            continue
                        email = normalize_email(s)
                        if email:
                            mgrs.append(email)
            except Exception:
                pass
        mgr_cfg = {"recipients": _dedupe_preserve_order(mgrs)}

    buckets_cfg, evt = _reload_hot_json("system_buckets", SYSTEM_BUCKETS_JSON_PATH, _parse_system_buckets_json)
    if evt:
        events.append(evt)
    if buckets_cfg is None:
        # Legacy fallback (bot-owned domain_policy.json). No CONFIG_CHANGED logs for legacy.
        fallback = safe_load_json(DOMAIN_POLICY_PATH, {}, required=False, state_name="domain_policy")
        if not isinstance(fallback, dict):
            fallback = {}
        buckets_cfg = {
            "transfer_domains": [normalize_domain(d) for d in fallback.get("external_image_request_domains", []) if normalize_domain(d)],
            "system_notification_domains": [normalize_domain(d) for d in fallback.get("system_notification_domains", []) if normalize_domain(d)],
            "quarantine_domains": [],
            "held_domains": [normalize_domain(d) for d in fallback.get("always_hold_domains", []) if normalize_domain(d)],
            "folders": {},
        }
        for k in ("transfer_domains", "system_notification_domains", "held_domains"):
            buckets_cfg[k] = _dedupe_preserve_order(buckets_cfg[k])

    return {
        "staff": staff_cfg,
        "apps_team": apps_cfg,
        "manager_config": mgr_cfg,
        "system_buckets": buckets_cfg,
    }, events

def _parse_iso_safe(iso_str):
    if not isinstance(iso_str, str) or not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str)
    except Exception:
        return None

def _add_and_resolve_recipients(mail, addrs, *, kind):
    recips = mail.Recipients
    added = 0
    for a in (addrs or []):
        if not isinstance(a, str):
            continue
        a2 = a.strip()
        if not a2:
            continue
        recips.Add(a2)
        added += 1
    if added == 0:
        return True
    ok = True
    try:
        ok = bool(recips.ResolveAll())
    except Exception:
        ok = False
    if not ok:
        log(f"RECIPIENTS_RESOLVE_FAIL kind={kind} count={added}", "ERROR")
        # best-effort unresolved listing
        try:
            bad = []
            for i in range(1, recips.Count + 1):
                r = recips.Item(i)
                if hasattr(r, "Resolved") and not r.Resolved:
                    bad.append(getattr(r, "Name", "unknown"))
            if bad:
                log(f"RECIPIENTS_UNRESOLVED kind={kind} names={';'.join(bad)}", "ERROR")
        except Exception:
            pass
    return ok

def _send_hib_burst_alert(outlook_app, to_email, subject, body):
    if not outlook_app or not to_email:
        return False
    try:
        is_safe, safe_reason = is_safe_mode()
        if is_safe:
            log(f"HIB_BURST_ALERT_SUPPRESSED to={to_email} reason={safe_reason}", "WARN")
            return False
        mail = outlook_app.CreateItem(0)
        to_addrs = []
        if isinstance(to_email, str):
            to_addrs = [p.strip() for p in to_email.split(";") if p.strip()]
        elif isinstance(to_email, (list, tuple)):
            to_addrs = list(to_email)
        ok = _add_and_resolve_recipients(mail, to_addrs, kind="hib_burst")
        if not ok:
            raise Exception("ResolveAll failed")
        mail.Subject = subject
        mail.Body = body
        mail.Send()
        log(f"HIB_BURST_ALERT_SENT to={to_email}", "INFO")
        return True
    except Exception as e:
        log(f"HIB_BURST_ALERT_FAIL to={to_email} error={e}", "ERROR")
        return False

def hib_watchdog_record_and_maybe_alert(now_dt, outlook_app, manager_email, apps_email):
    try:
        state = safe_load_json(HIB_WATCHDOG_PATH, {}, required=False, state_name="hib_watchdog")
        if not isinstance(state, dict):
            log("HIB_WATCHDOG_RESET reason=not_object", "WARN")
            state = {}
        hib_events = state.get("hib_events")
        if not isinstance(hib_events, list):
            log("HIB_WATCHDOG_RESET reason=bad_hib_events", "WARN")
            hib_events = []
        hib_events.append(now_dt.isoformat())
        cutoff = now_dt - timedelta(minutes=HIB_BURST_WINDOW_MIN)
        trimmed = []
        for ts in hib_events:
            parsed = _parse_iso_safe(ts)
            if parsed is not None and parsed >= cutoff:
                trimmed.append(ts)
        hib_events = trimmed
        count = len(hib_events)
        last_alert_dt = _parse_iso_safe(state.get("last_alert_iso"))
        cooldown_ok = True
        if last_alert_dt is not None:
            if (now_dt - last_alert_dt) < timedelta(minutes=HIB_BURST_COOLDOWN_MIN):
                cooldown_ok = False
        if count >= HIB_BURST_THRESHOLD and cooldown_ok:
            subj = f"HIB Spike: {HIB_BURST_THRESHOLD}+ in {HIB_BURST_WINDOW_MIN}min"
            time_str = now_dt.strftime("%d %b %Y %H:%M")
            body_lines = []
            body_lines.append(f"Time: {time_str}")
            body_lines.append(f"Count: {count}")
            body_lines.append(f"Window: {HIB_BURST_WINDOW_MIN} min")
            body_lines.append(f"Folder: {HIB_FOLDER_NAME}")
            body_lines.append("")
            body_lines.append("High HIB volume detected. Investigate.")
            body = "\n".join(body_lines)
            _send_hib_burst_alert(outlook_app, manager_email, subj, body)
            _send_hib_burst_alert(outlook_app, apps_email, subj, body)
            state["last_alert_iso"] = now_dt.isoformat()
            log(f"HIB_BURST_ALERT count={count} window={HIB_BURST_WINDOW_MIN}", "INFO")
        state["hib_events"] = hib_events
        atomic_write_json(HIB_WATCHDOG_PATH, state, state_name="hib_watchdog")
    except Exception as e:
        log(f"HIB_WATCHDOG_ERROR error={e}", "ERROR")

def is_hib_notification(msg):
    try:
        to_line = getattr(msg, "To", "") or ""
    except Exception:
        to_line = ""
    try:
        cc_line = getattr(msg, "CC", "") or ""
    except Exception:
        cc_line = ""
    to_cc = (to_line + " " + cc_line).lower()
    if "@chib.had.sa.gov.au" in to_cc:
        return True
    try:
        body = (getattr(msg, "Body", "") or "")[:4000]
    except Exception:
        body = ""
    body_lower = body.lower()
    if "whib.had.sa.gov.au" in body_lower:
        return True
    try:
        subject = getattr(msg, "Subject", "") or ""
    except Exception:
        subject = ""
    subject_lower = subject.lower()
    if subject_lower.startswith("error:"):
        if ("ensportal.visualtrace" in body_lower) or ("imgproduction" in body_lower):
            return True
    return False

def hib_contains_16110(msg):
    """Check if HIB message contains '16110' in subject or body (best-effort)"""
    try:
        subject = getattr(msg, "Subject", "") or ""
    except Exception:
        subject = ""
    if "16110" in subject:
        return True
    try:
        body = (getattr(msg, "Body", "") or "")[:4000]
    except Exception:
        body = ""
    if "16110" in body:
        return True
    return False

# ==================== FILE OPERATIONS ====================
def get_staff_list():
    """Load staff list from file, preferring staff.json over staff.txt.

    staff.json is the canonical source (written by the dashboard).
    staff.txt is the legacy fallback. When staff.json is used the
    off_rotation and leave lists are respected so dashboard changes
    take effect immediately.
    """
    # Prefer staff.json (hot-reloaded by dashboard) over legacy staff.txt
    try:
        json_path = os.path.abspath(STAFF_JSON_PATH)
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding="utf-8") as f:
                scfg = json.load(f)
            if isinstance(scfg, dict) and isinstance(scfg.get("staff"), list) and scfg["staff"]:
                all_staff = [e.strip().lower() for e in scfg["staff"] if e.strip()]
                off = set((e.strip().lower() for e in (scfg.get("off_rotation") or [])))
                leave = set((e.strip().lower() for e in (scfg.get("leave") or [])))
                staff = [e for e in all_staff if e not in off and e not in leave]
                log(
                    f"STAFF_LOADED members={len(staff)} source=staff.json "
                    f"off_rotation={len(off)} leave={len(leave)}",
                    "INFO",
                )
                return staff
    except Exception as e:
        log(f"STAFF_JSON_ERROR path={STAFF_JSON_PATH} error={e} falling_back=staff.txt", "WARN")

    # Fallback to legacy staff.txt
    try:
        staff_path = os.path.abspath(STAFF_PATH)
        log(f"STAFF_FILE_PATH path={staff_path}", "INFO")
        if not os.path.exists(staff_path):
            log(f"STAFF_FILE_MISSING path={staff_path}", "WARN")
            return []
        with open(staff_path, 'r', encoding="utf-8", errors="replace") as f:
            raw_lines = f.readlines()
        staff = []
        for line in raw_lines:
            cleaned = line.strip()
            if not cleaned or cleaned.startswith('#'):
                continue
            staff.append(cleaned.lower())
        log(
            f"STAFF_LOADED members={len(staff)} raw_lines={len(raw_lines)} source=staff.txt "
            f"path={staff_path}",
            "INFO",
        )
        return staff
    except Exception as e:
        log(f"STAFF_FILE_ERROR path={STAFF_PATH} error={e}", "ERROR")
        return []

def find_child_folder(parent_folder, child_name):
    """Return child folder by name or None"""
    try:
        return parent_folder.Folders[child_name]
    except Exception as e:
        log(f"Folder lookup failed {child_name}: {e}", "WARN")
        return None

def find_mailbox_root(namespace, mailbox_name):
    """Return mailbox root by name or None"""
    try:
        return namespace.Folders.Item(mailbox_name)
    except Exception:
        pass
    try:
        for i in range(namespace.Folders.Count):
            try:
                folder = namespace.Folders.Item(i + 1)
                if folder.Name.lower().strip() == mailbox_name.lower().strip():
                    return folder
            except Exception:
                continue
    except Exception:
        pass
    log("FOLDER_NOT_FOUND mailbox=(configured)", "ERROR")
    return None

def find_mailbox_root_robust(namespace, mailbox_spec):
    """Return mailbox root by name/path match or None"""
    try:
        folder = namespace.Folders.Item(mailbox_spec)
        if folder:
            return folder
    except Exception:
        pass
    top_level_names = []
    try:
        for i in range(namespace.Folders.Count):
            try:
                folder = namespace.Folders.Item(i + 1)
                name = (folder.Name or "").strip()
                top_level_names.append(name)
                if name.lower() == mailbox_spec.lower().strip():
                    return folder
                try:
                    folder_path = folder.FolderPath or ""
                except Exception:
                    folder_path = ""
                if mailbox_spec.lower().strip() in folder_path.lower():
                    return folder
            except Exception:
                continue
    except Exception:
        pass
    log("FOLDER_NOT_FOUND mailbox=(configured)", "ERROR")
    if top_level_names:
        log(f"MAILBOX_ENUM top_level={','.join(top_level_names)}", "INFO")
    return None

def resolve_folder_by_path(root, path_spec):
    """Resolve a folder by path segments under root"""
    current = root
    segments = [seg for seg in path_spec.replace("/", "\\").split("\\") if seg]
    for seg in segments:
        try:
            current = current.Folders.Item(seg)
        except Exception:
            return None
    return current

def resolve_folder_recursive(root, target_name, max_depth=6, max_nodes=2500):
    """Resolve a folder by name using deterministic BFS"""
    try:
        root_name = (root.Name or "").strip()
        if root_name.lower() == target_name.lower().strip():
            return root
    except Exception:
        pass
    queue = [(root, 0)]
    visited = 0
    while queue:
        node, depth = queue.pop(0)
        if depth >= max_depth:
            continue
        try:
            count = node.Folders.Count
        except Exception:
            continue
        for i in range(count):
            if visited >= max_nodes:
                return None
            visited += 1
            try:
                child = node.Folders.Item(i + 1)
            except Exception:
                continue
            try:
                child_name = (child.Name or "").strip()
                if child_name.lower() == target_name.lower().strip():
                    return child
            except Exception:
                pass
            queue.append((child, depth + 1))
    return None

def resolve_folder(root, folder_spec):
    """Resolve folder by path or name under root"""
    if "\\" in folder_spec or "/" in folder_spec:
        return resolve_folder_by_path(root, folder_spec), "PATH_RESOLVE"
    return resolve_folder_recursive(root, folder_spec), "RECURSIVE_SEARCH"

def get_or_create_subfolder(parent_folder, name):
    if not parent_folder or not name:
        return None
    try:
        for folder in parent_folder.Folders:
            try:
                if folder.Name == name:
                    return folder
            except Exception:
                continue
        return parent_folder.Folders.Add(name)
    except Exception:
        return None

def get_folder_path_safe(folder):
    """Best-effort folder path for logs"""
    try:
        return folder.FolderPath
    except Exception:
        return ""

def load_settings_overrides(path):
    """Load and validate settings overrides"""
    overrides = safe_load_json(path, {}, required=False, state_name="settings_overrides")
    if not isinstance(overrides, dict):
        log(f"OVERRIDE_REJECT reason=not_object path={path}", "WARN")
        return {}
    accepted = {}
    for key, value in overrides.items():
        validator = ALLOWED_OVERRIDES.get(key)
        if not validator:
            log(f"OVERRIDE_REJECT key={key} reason=not_allowed", "WARN")
            continue
        if not validator(value):
            log(f"OVERRIDE_REJECT key={key} reason=invalid_value", "WARN")
            continue
        # Never log addresses; log only "value=set"
        if key in ("completion_cc_addr", "apps_cc_addr", "manager_cc_addr"):
            log(f"OVERRIDE_ACCEPT key={key} value=set", "INFO")
            accepted[key] = value.strip()
        else:
            log(f"OVERRIDE_ACCEPT key={key} value={value}", "INFO")
            accepted[key] = value
    return accepted

def load_domain_policy(path=None):
    """
    Load and validate domain policy.
    If missing/invalid, fail safe: treat non-internal as unknown/hold and log POLICY_INVALID.

    Returns: (policy_dict, is_valid)
    """
    if path is None:
        path = DOMAIN_POLICY_PATH

    policy = safe_load_json(path, None, required=False, state_name="domain_policy")

    # Validate structure
    if policy is None or not isinstance(policy, dict):
        log("POLICY_INVALID reason=missing_or_corrupt path=" + path, "ERROR")
        return {
            "internal_domains": [],
            "vendor_domains": [],
            "always_hold_domains": []
        }, False

    # Validate required keys (minimal set for backward compatibility)
    required_keys = {"internal_domains"}
    if not required_keys.issubset(policy.keys()):
        log("POLICY_INVALID reason=missing_keys path=" + path, "ERROR")
        return {
            "internal_domains": [],
            "external_image_request_domains": [],
            "system_notification_domains": [],
            "sami_support_staff": [],
            "apps_specialists": [],
            "manager_email": "",
            "always_hold_domains": []
        }, False

    # Validate types for all list fields
    list_fields = [
        "internal_domains",
        "external_image_request_domains",
        "system_notification_domains",
        "sami_support_staff",
        "apps_specialists",
        "always_hold_domains"
    ]
    for key in list_fields:
        if key in policy and not isinstance(policy[key], list):
            log(f"POLICY_INVALID reason=key_not_list key={key} path=" + path, "ERROR")
            return {
                "internal_domains": [],
                "external_image_request_domains": [],
                "system_notification_domains": [],
                "sami_support_staff": [],
                "apps_specialists": [],
                "manager_email": "",
                "always_hold_domains": []
            }, False

    # Ensure all optional fields have defaults
    if "external_image_request_domains" not in policy:
        policy["external_image_request_domains"] = []
    if "system_notification_domains" not in policy:
        policy["system_notification_domains"] = []
    if "sami_support_staff" not in policy:
        policy["sami_support_staff"] = []
    if "apps_specialists" not in policy:
        policy["apps_specialists"] = []
    if "manager_email" not in policy:
        policy["manager_email"] = ""
    if "always_hold_domains" not in policy:
        policy["always_hold_domains"] = []

    log(f"POLICY_LOADED path={path}", "INFO")
    return policy, True

def get_known_domains(policy):
    if not isinstance(policy, dict):
        return set()
    domains = []
    for key in (
        "internal_domains",
        "external_image_request_domains",
        "system_notification_domains",
        "quarantine_domains",
        "always_hold_domains",
        "vendor_domains"
    ):
        value = policy.get(key, [])
        if isinstance(value, list):
            domains.extend(value)
    return {d.lower().strip() for d in domains if isinstance(d, str) and d.strip()}

def is_domain_known(sender_email, known_domains):
    if not sender_email or not isinstance(known_domains, set) or not known_domains:
        return False
    email_str = str(sender_email).strip()
    if "<" in email_str and ">" in email_str:
        try:
            email_str = email_str.split("<", 1)[1].split(">", 1)[0].strip()
        except Exception:
            return False
    if "@" not in email_str:
        return False
    domain = email_str.split("@")[-1].strip().lower()
    if not domain:
        return False
    return domain in known_domains

def classify_sender(sender_email, sender_domain, policy):
    """
    Unified sender classification: exact sender match first, then domain match.

    Returns (bucket, match_level) if an explicit policy bucket matches,
    or (None, None) if no match — caller falls back to existing internal/unknown logic.

    Priority (explicit, deterministic — no dict iteration):
      1. quarantine  (sender, then domain)
      2. hold        (sender, then domain)
      3. system_notification (sender, then domain)
      4. external_image_request (sender, then domain)
    """
    email_lower = normalize_sender_for_policy(sender_email) or ""
    domain_lower = sender_domain.lower().strip() if sender_domain else ""

    # Build sets for O(1) membership (lists stored in JSON, sets built at runtime)
    q_senders = _build_sender_override_set(policy, "quarantine_senders")
    q_domains = set(d.lower().strip() for d in policy.get("quarantine_domains", []))
    h_senders = _build_sender_override_set(policy, "held_senders")
    h_domains = set(d.lower().strip() for d in policy.get("held_domains", []))
    sn_senders = _build_sender_override_set(policy, "system_notification_senders")
    sn_domains = set(d.lower().strip() for d in policy.get("system_notification_domains", []))
    eir_senders = _build_sender_override_set(policy, "transfer_senders")
    eir_domains = set(d.lower().strip() for d in policy.get("transfer_domains", []))

    # 1. Quarantine
    if email_lower and email_lower in q_senders:
        return "quarantine", "sender"
    if domain_lower and domain_lower in q_domains:
        return "quarantine", "domain"

    # 2. Hold
    if email_lower and email_lower in h_senders:
        return "hold", "sender"
    if domain_lower and domain_lower in h_domains:
        return "hold", "domain"

    # 3. System notification
    if email_lower and email_lower in sn_senders:
        return "system_notification", "sender"
    if domain_lower and domain_lower in sn_domains:
        return "system_notification", "domain"

    # 4. External image request
    if email_lower and email_lower in eir_senders:
        return "external_image_request", "sender"
    if domain_lower and domain_lower in eir_domains:
        return "external_image_request", "domain"

    # No explicit policy match
    return None, None




def normalize_sender_for_policy(raw):
    """Normalize sender strings for exact override matching."""
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    if s.startswith("smtp:"):
        s = s[5:].strip()
    match = re.search(r"<([^>]+)>", s)
    if match:
        s = match.group(1).strip()
    if "@" not in s:
        fallback = re.search(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", s)
        if fallback:
            s = fallback.group(0).strip()
    return normalize_email(s)


def _build_sender_override_set(policy, key):
    values = policy.get(key, []) if isinstance(policy, dict) else []
    if not isinstance(values, list):
        return set()
    result = set()
    for item in values:
        norm = normalize_sender_for_policy(item)
        if norm:
            result.add(norm)
    return result


def get_sender_override_bucket(sender_email, policy):
    """Return sender override bucket for this sender, or None if no override."""
    email_lower = normalize_sender_for_policy(sender_email)
    if not email_lower:
        return None
    if email_lower in _build_sender_override_set(policy, "quarantine_senders"):
        return "quarantine"
    if email_lower in _build_sender_override_set(policy, "held_senders"):
        return "hold"
    if email_lower in _build_sender_override_set(policy, "system_notification_senders"):
        return "system_notification"
    if email_lower in _build_sender_override_set(policy, "transfer_senders"):
        return "external_image_request"
    return None

# Superseded by classify_sender() for process_inbox routing — kept for backward compatibility.
def classify_sender_domain(domain, policy):
    """
    Classify sender domain based on policy.

    Returns: quarantine | hold | system_notification | external_image_request | internal | unknown
    """
    if not domain:
        return "unknown"

    domain_lower = domain.lower().strip()

    # Check quarantine domains (must run before any assignment/routing)
    quarantine_domains = [d.lower().strip() for d in policy.get("quarantine_domains", [])]
    if domain_lower in quarantine_domains:
        return "quarantine"

    # Check always hold domains (held overrides system_notification)
    hold_domains = [d.lower().strip() for d in policy.get("always_hold_domains", [])]
    if domain_lower in hold_domains:
        return "hold"

    # Check system notification domains (Class 3)
    system_notification_domains = [d.lower().strip() for d in policy.get("system_notification_domains", [])]
    if domain_lower in system_notification_domains:
        return "system_notification"

    # Check external image request domains (Class 1)
    external_image_domains = [d.lower().strip() for d in policy.get("external_image_request_domains", [])]
    if domain_lower in external_image_domains:
        return "external_image_request"

    # Check internal domains
    internal_domains = [d.lower().strip() for d in policy.get("internal_domains", [])]
    if domain_lower in internal_domains:
        return "internal"

    # Unknown domain
    return "unknown"

def is_sami_support_staff(sender_email, policy):
    """
    Check if sender is in SAMI support staff list (completion authorities).
    Returns True if sender is SAMI staff, False otherwise.
    """
    if not sender_email:
        return False

    sender_lower = sender_email.lower().strip()
    sami_staff = [email.lower().strip() for email in policy.get("sami_support_staff", [])]

    return sender_lower in sami_staff

def send_manager_hold_notification(outlook_app, manager_email, original_msg, reason, quarantine_folder_name):
    if not outlook_app:
        log("MANAGER_HOLD_NOTIFY_ERROR OutlookUnavailable", "ERROR")
        return False
    if not manager_email:
        log("MANAGER_HOLD_NOTIFY_SKIPPED_NO_MANAGER", "ERROR")
        return False
    try:
        mail = outlook_app.CreateItem(0)
        mgr_addrs = []
        if isinstance(manager_email, str):
            mgr_addrs = [p.strip() for p in manager_email.split(";") if p.strip()]
        elif isinstance(manager_email, (list, tuple)):
            mgr_addrs = list(manager_email)
        ok = _add_and_resolve_recipients(mail, mgr_addrs, kind="manager_hold_notify")
        if not ok:
            raise Exception("ResolveAll failed")
        try:
            subject = original_msg.Subject or ""
        except Exception:
            subject = ""
        mail.Subject = f"HOLD – Unknown Domain: {subject}"
        try:
            sender_email = original_msg.SenderEmailAddress or ""
        except Exception:
            sender_email = ""
        sender_domain = ""
        if "@" in sender_email:
            sender_domain = sender_email.split("@")[-1].lower().strip()
        try:
            received_time = original_msg.ReceivedTime
            received_str = received_time.strftime("%d %b %Y %H:%M") if received_time else ""
        except Exception:
            received_str = ""
        mail.Body = (
            f"From: {sender_email}\n"
            f"Domain: {sender_domain}\n"
            f"Received: {received_str}\n"
            f"Subject: {subject}\n"
            f"Action: moved to Inbox/{quarantine_folder_name}\n"
            f"Reason: {reason}\n\n"
            "If this sender is legitimate, please contact the system administrator to whitelist this domain."
        )
        is_safe, safe_reason = is_safe_mode()
        if is_safe:
            log("MANAGER_HOLD_NOTIFY_SUPPRESSED_SAFE_MODE", "WARN")
            return False
        mail.Send()
        log("MANAGER_HOLD_NOTIFY_SENT", "INFO")
        return True
    except Exception as e:
        log(f"MANAGER_HOLD_NOTIFY_ERROR {type(e).__name__}", "ERROR")
        return False

def get_roster_state():
    """Load roster state from JSON"""
    return safe_load_json(
        FILES["state"],
        {"current_index": 0, "total_processed": 0},
        required=False,
        state_name="roster_state"
    )

def save_roster_state(state):
    """Save roster state to JSON"""
    atomic_write_json(FILES["state"], state, state_name="roster_state")


def load_processed_ledger():
    """Load processed ledger from JSON"""
    return safe_load_json(PROCESSED_LEDGER_PATH, {}, required=True, state_name="processed_ledger")


def save_processed_ledger(ledger):
    """Save processed ledger to JSON"""
    return atomic_write_json(PROCESSED_LEDGER_PATH, ledger, state_name="processed_ledger")

def mark_processed(entry_id, reason, ledger=None):
    """Record a processed entry id with timestamp and reason"""
    try:
        ledger_data = ledger if isinstance(ledger, dict) else load_processed_ledger()
        if ledger_data is None:
            log("MARK_PROCESSED_FAILED state=processed_ledger", "ERROR")
            return None
        existing = ledger_data.get(entry_id, {})
        existing.update({
            "ts": datetime.now().isoformat(),
            "reason": reason
        })
        ledger_data[entry_id] = existing
        if not atomic_write_json(PROCESSED_LEDGER_PATH, ledger_data, state_name="processed_ledger"):
            log("MARK_PROCESSED_FAILED state=processed_ledger", "ERROR")
            return None
        return ledger_data
    except Exception:
        log("MARK_PROCESSED_FAILED state=processed_ledger", "ERROR")
        return None

def ensure_processed_ledger_exists(path):
    """Ensure processed ledger file exists with default schema"""
    try:
        if os.path.exists(path):
            return True
        default_ledger = {}
        if atomic_write_json(path, default_ledger, state_name="processed_ledger"):
            log(f"STATE_BOOTSTRAP_CREATED state=processed_ledger path={path}", "INFO")
            return True
        log("STATE_BOOTSTRAP_FAILED state=processed_ledger error=write_failed", "ERROR")
        return False
    except Exception as e:
        log(f"STATE_BOOTSTRAP_FAILED state=processed_ledger error={e}", "ERROR")
        return False

def load_poison_counts():
    return safe_load_json(POISON_COUNTS_PATH, {}, required=False, state_name="poison_counts")

def save_poison_counts(counts):
    return atomic_write_json(POISON_COUNTS_PATH, counts, state_name="poison_counts")

def get_next_staff():
    """Get next staff member in rotation"""
    staff = _staff_list_cache if _staff_list_cache is not None else get_staff_list()
    if not staff:
        return None
    
    state = get_roster_state()
    idx = state.get("current_index", 0)
    
    person = staff[idx % len(staff)]
    
    # Update state
    state["current_index"] = idx + 1
    state["total_processed"] = state.get("total_processed", 0) + 1
    save_roster_state(state)
    
    return person

def append_stats(subject, assigned_to, sender="unknown", risk_level="normal", domain_bucket="", action="", policy_source="", event_type="", msg_key="", status_after="", assigned_ts="", completed_ts="", duration_sec=""):
    """Append entry to daily stats CSV with full 16-column schema"""
    try:
        file_exists = os.path.isfile(FILES["log"])

        # Determine column count from existing file header
        use_old_schema = False
        if file_exists:
            try:
                with open(FILES["log"], 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line:
                        col_count = len(first_line.split(','))
                        use_old_schema = (col_count <= 6)
            except Exception:
                pass

        with open(FILES["log"], 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                # New file: write full 16-column header
                writer.writerow([
                    'Date', 'Time', 'Subject', 'Assigned To', 'Sender', 'Risk Level',
                    'Domain Bucket', 'Action', 'Policy Source', 'event_type', 'msg_key',
                    'status_after', 'assigned_to', 'assigned_ts', 'completed_ts', 'duration_sec'
                ])
                use_old_schema = False

            now = datetime.now()

            if use_old_schema:
                # Old schema: write only 6 fields (backward compatible)
                writer.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    subject,
                    assigned_to,
                    sender,
                    risk_level
                ])
            else:
                # Full 16-column schema
                writer.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    subject,
                    assigned_to,
                    sender,
                    risk_level,
                    domain_bucket,
                    action,
                    policy_source,
                    event_type,
                    msg_key,
                    status_after,
                    assigned_to,
                    assigned_ts,
                    completed_ts,
                    duration_sec
                ])
    except Exception as e:
        log(f"Error writing stats: {e}", "ERROR")

def maybe_rotate_daily_stats_to_new_schema():
    """
    Safe CSV schema rotation: if existing daily_stats.csv has old 6-col header,
    archive it and create fresh file with new 9-col header.
    Preserves all existing data. No rewrites.
    """
    log_path = FILES["log"]

    # Check if file exists
    if not os.path.exists(log_path):
        log("CSV_SCHEMA_CHECK file_not_found", "INFO")
        return

    try:
        # Read first line to check schema
        with open(log_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()

        if not first_line:
            log("CSV_SCHEMA_CHECK file_empty", "INFO")
            return

        # Determine column count
        col_count = len(first_line.split(','))

        if col_count > 6:
            # Already on new schema
            log(f"CSV_SCHEMA_CHECK current_cols={col_count} status=already_new", "INFO")
            return

        # Old schema detected, need to rotate
        log(f"CSV_SCHEMA_ROTATE old_cols={col_count} action=archiving", "WARN")

        # Find available archive filename (deterministic, no timestamps)
        log_dir = os.path.dirname(log_path) or "."
        log_basename = os.path.basename(log_path).replace('.csv', '')

        archive_path = None
        for i in range(1, 1000):
            candidate = os.path.join(log_dir, f"{log_basename}_legacy_{i}.csv")
            if not os.path.exists(candidate):
                archive_path = candidate
                break

        if not archive_path:
            log("CSV_SCHEMA_ROTATE error=no_available_archive_name", "ERROR")
            return

        # Atomic rename old file to archive
        os.replace(log_path, archive_path)
        log(f"CSV_SCHEMA_ROTATE archived={os.path.basename(archive_path)}", "INFO")

        # Create new file with 9-column header (atomic write)
        temp_path = log_path + ".tmp"
        try:
            with open(temp_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Date', 'Time', 'Subject', 'Assigned To', 'Sender', 'Risk Level',
                    'Domain Bucket', 'Action', 'Policy Source'
                ])
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace
            os.replace(temp_path, log_path)
            log(f"CSV_SCHEMA_ROTATE old_cols=6 new_cols=9 archived={os.path.basename(archive_path)}", "WARN")

        except Exception as e:
            log(f"CSV_SCHEMA_ROTATE error={e}", "ERROR")
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            raise

    except Exception as e:
        log(f"CSV_SCHEMA_ROTATE_FAILED error={e}", "ERROR")

# ==================== WATCHDOG OPERATIONS ====================
def load_watchdog(overrides):
    """Load urgent watchdog from JSON"""
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
        return {}
    return safe_load_json(FILES["watchdog"], {}, required=False, state_name="urgent_watchdog")

def save_watchdog(data, overrides):
    """Save urgent watchdog to JSON"""
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
        return
    atomic_write_json(FILES["watchdog"], data, state_name="urgent_watchdog")

def add_to_watchdog(msg_id, subject, assigned_to, sender, risk_type, overrides):
    """Add urgent ticket to watchdog"""
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
        return
    watchdog = load_watchdog(overrides)
    watchdog[msg_id] = {
        "subject": subject[:100],
        "assigned_to": assigned_to,
        "sender": sender,
        "risk_type": risk_type,
        "timestamp": datetime.now().isoformat(),
        "escalation_count": 0
    }
    save_watchdog(watchdog, overrides)
    log(f"WATCHDOG_ADDED msg_id={msg_id}", "CRITICAL")

def remove_from_watchdog(msg_id, overrides):
    """Remove completed ticket from watchdog"""
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
        return
    watchdog = load_watchdog(overrides)
    if msg_id in watchdog:
        del watchdog[msg_id]
        save_watchdog(watchdog, overrides)
        log(f"✅ Removed from watchdog: {msg_id}", "SUCCESS")

# ==================== RISK DETECTION ====================
def detect_risk(subject, body="", high_importance=False):
    """
    Semantic risk detection using (Action + Context) OR (Urgency + Action) logic.
    
    Returns: ("normal", "urgent", or "critical"), risk_reason
    """
    text = (subject + " " + body).lower()
    
    # Check for risk actions
    found_actions = [a for a in RISK_ACTIONS if a in text]
    found_context = [c for c in RISK_CONTEXT if c in text]
    found_urgency = [u for u in URGENCY_WORDS if u in text]
    
    # Rule 1: High Importance Flag (Outlook) = CRITICAL
    if high_importance:
        return "critical", "Outlook High Importance Flag"
    
    # Rule 2: (Action + Context) = CRITICAL (e.g., "delete patient scan")
    if found_actions and found_context:
        return "critical", f"Action+Context: {found_actions[0]}+{found_context[0]}"
    
    # Rule 3: (Urgency + Action) = CRITICAL (e.g., "STAT delete request")
    if found_urgency and found_actions:
        return "critical", f"Urgency+Action: {found_urgency[0]}+{found_actions[0]}"
    
    # Rule 4: Urgency words alone = URGENT
    if found_urgency:
        return "urgent", f"Urgency: {found_urgency[0]}"
    
    # Rule 5: Risk actions alone (without context) = WARN but not critical
    if found_actions:
        return "urgent", f"Action detected: {found_actions[0]}"
    
    return "normal", None

# ==================== SMART FILTER ====================
def resolve_sender_smtp(msg):
    """
    Resolve sender SMTP address from Outlook message.
    Handles Exchange (EX) users by extracting PrimarySmtpAddress.
    Returns lowercased SMTP address or empty string.
    """
    try:
        email_type = getattr(msg, "SenderEmailType", "") or ""
        if email_type.upper() == "EX":
            # Exchange user - try GetExchangeUser first
            try:
                sender_obj = msg.Sender
                if sender_obj:
                    exchange_user = sender_obj.GetExchangeUser()
                    if exchange_user:
                        smtp = exchange_user.PrimarySmtpAddress
                        if smtp:
                            return smtp.lower().strip()
            except Exception:
                pass
            # Fallback: PropertyAccessor for PR_SMTP_ADDRESS
            try:
                PR_SMTP_ADDRESS = "http://schemas.microsoft.com/mapi/proptag/0x39FE001E"
                smtp = msg.PropertyAccessor.GetProperty(PR_SMTP_ADDRESS)
                if smtp:
                    return smtp.lower().strip()
            except Exception:
                pass
        # Non-Exchange or fallback to SenderEmailAddress
        raw = getattr(msg, "SenderEmailAddress", "") or ""
        return raw.lower().strip()
    except Exception:
        return ""

def is_internal_sender(smtp_addr):
    """Check if sender is internal (@sa.gov.au)."""
    if not smtp_addr or not isinstance(smtp_addr, str):
        return False
    return smtp_addr.lower().strip().endswith("@sa.gov.au")

def is_staff_sender(smtp_addr, staff_list):
    """Check if sender is in staff list. Accepts list or set."""
    if not smtp_addr or not isinstance(smtp_addr, str):
        return False
    if not staff_list:
        return False
    addr_lower = smtp_addr.lower().strip()
    if isinstance(staff_list, set):
        return addr_lower in staff_list
    return addr_lower in {s.lower().strip() for s in staff_list}

def extract_sender_domain(sender_email):
    """
    Extract domain from sender email address.
    Handles: "Name <user@domain>" and bare "user@domain"
    Returns domain or None
    """
    if not sender_email:
        return None

    email_str = str(sender_email).strip()

    # Handle "Name <user@domain>" format
    import re
    match = re.search(r'<([^>]+)>', email_str)
    if match:
        email_str = match.group(1)

    # Extract domain from email
    if '@' in email_str:
        try:
            domain = email_str.split('@')[-1].strip().lower()
            return domain if domain else None
        except Exception:
            return None

    return None

def is_internal_reply(sender_email, subject, staff_list):
    """
    Smart Filter: Only skip if:
    1. Sender IS in staff.txt AND
    2. Subject indicates a REPLY (RE:, Accepted:, etc.) OR contains bot tags
    """
    is_staff = sender_email.lower() in staff_list

    reply_prefixes = ('re:', 'accepted:', 'declined:', 'fw:', 'fwd:')
    is_reply = subject.lower().strip().startswith(reply_prefixes)
    is_bot_tagged = '[assigned:' in subject.lower() or '[completed:' in subject.lower()

    return is_staff and (is_reply or is_bot_tagged)

def is_jira_automation_notification(sender_domain, subject, msg):
    """
    Detect automated Jira notifications from Jones Radiology.
    Returns True if:
    - Sender domain contains "jonesradiology.atlassian.net"
    - Body contains Jira Service Desk boilerplate patterns
    Catches: completion, confirmation, received, status updates.
    """
    if not sender_domain:
        return False
    domain_lower = sender_domain.lower()
    if "jonesradiology.atlassian.net" not in domain_lower:
        return False
    try:
        body_text = (msg.Body or "")[:3000].lower()
    except Exception:
        return False
    # Escape hatch: if body contains human-reply markers, do not filter
    human_markers = [
        "?", "can you", "could you", "please", "urgent", "asap",
        "call", "phone", "ring", "not received", "follow up",
        "chasing", "still need", "not working", "failed", "error",
    ]
    if any(m in body_text for m in human_markers):
        return False
    # Jira Service Desk boilerplate indicators
    jira_patterns = [
        "reply above this line",
        "view request",
        "service desk",
        "has been resolved",
        "re-open the ticket",
        "confirmation received",
        "your request has been received",
    ]
    matches = sum(1 for p in jira_patterns if p in body_text)
    return matches >= 2

def is_jones_completion_notification(msg):
    sender_email = ""
    try:
        sender_email = resolve_sender_smtp(msg) or getattr(msg, "SenderEmailAddress", "") or ""
    except Exception:
        sender_email = ""
    try:
        sender_name = getattr(msg, "SenderName", "") or ""
    except Exception:
        sender_name = ""
    try:
        subject = getattr(msg, "Subject", "") or ""
    except Exception:
        subject = ""
    try:
        body = (getattr(msg, "Body", "") or "")[:4000]
    except Exception:
        body = ""

    sender_email_lower = sender_email.lower()
    sender_name_lower = sender_name.lower()
    subject_lower = subject.lower()
    body_lower = body.lower()

    sender_jones = ("jones" in sender_email_lower) or ("jones" in sender_name_lower)
    subject_hit = ("completed" in subject_lower) or ("completion" in subject_lower)
    body_hit = ("has been completed" in body_lower) or ("completed" in body_lower)

    return sender_jones and (subject_hit or body_hit)

def build_unknown_notice_block():
    return (
        "\n\n"
        "────────────────────────────────\n"
        "Automated notice – action required\n\n"
        "This message was held because the sender or domain is not currently approved.\n\n"
        "If this sender/domain should be:\n"
        "• approved for normal distribution, or\n"
        "• routed to a specific team (e.g. Apps visibility), or\n"
        "• left on hold,\n\n"
        "please email the system administrator with your decision.\n\n"
        "(Do not include patient or clinical information in your reply.)\n"
        "────────────────────────────────\n"
    )

# ==================== SLA WATCHDOG CHECK ====================
def check_sla_breaches(overrides):
    """
    Review-only mode: SLA enforcement disabled.
    """
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
        return
    log("SLA_WATCHDOG_DISABLED review_only=true", "INFO")
    return
    watchdog = load_watchdog(overrides)
    if not watchdog:
        return
    
    now = datetime.now()
    sla_limit = timedelta(minutes=CONFIG["sla_minutes"])
    
    for msg_id, ticket in list(watchdog.items()):
        try:
            ticket_time = datetime.fromisoformat(ticket["timestamp"])
            elapsed = now - ticket_time
            
            if elapsed > sla_limit:
                # SLA BREACH!
                log(f"🚨 SLA BREACH: {ticket['subject'][:50]}... ({elapsed.seconds // 60}m elapsed)", "CRITICAL")
                
                # Re-assign to next staff member
                new_assignee = get_next_staff()
                if new_assignee and new_assignee != ticket["assigned_to"]:
                    log(f"🔄 Re-assigning from {ticket['assigned_to']} to {new_assignee}", "WARN")
                
                # Escalate to manager (would send email in real implementation)
                escalate_to_manager(ticket, elapsed)
                
                # Update watchdog with reset timer and escalation count
                watchdog[msg_id]["timestamp"] = now.isoformat()
                watchdog[msg_id]["escalation_count"] = ticket.get("escalation_count", 0) + 1
                watchdog[msg_id]["assigned_to"] = new_assignee or ticket["assigned_to"]
                
                # Log SLA failure
                append_stats(
                    f"[SLA_FAIL] {ticket['subject'][:50]}",
                    ticket["assigned_to"],
                    ticket["sender"],
                    "SLA_BREACH",
                    "",
                    "SLA_BREACH",
                    ""
                )
                
        except Exception as e:
            log(f"Error checking SLA for {msg_id}: {e}", "ERROR")
    
    save_watchdog(watchdog, overrides)

def escalate_to_manager(ticket, elapsed):
    """Send escalation email to manager"""
    manager = CONFIG["manager"]
    log(f"📧 Escalating to manager ({manager}): {ticket['subject'][:30]}...", "CRITICAL")
    
    # In production, this would send an actual email
    # For now, we log the escalation
    try:
        with open("escalations.log", "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().isoformat()}] ESCALATION\n")
            f.write(f"  Manager: {manager}\n")
            f.write(f"  Subject: {ticket['subject']}\n")
            f.write(f"  Original Assignee: {ticket['assigned_to']}\n")
            f.write(f"  Risk Type: {ticket['risk_type']}\n")
            f.write(f"  Time Elapsed: {elapsed.seconds // 60} minutes\n")
            f.write(f"  Escalation Count: {ticket.get('escalation_count', 0) + 1}\n")
            f.write("-" * 50 + "\n")
    except:
        pass

# ==================== MAILBOX STORE GUARD ====================
def get_store_root_by_display_name(namespace, store_name):
    """Return the root folder of the Outlook store matching store_name, or None."""
    try:
        for s in namespace.Stores:
            if (s.DisplayName or "").lower().strip() == store_name.lower().strip():
                return s.GetRootFolder()
    except Exception:
        pass
    return None

def check_msg_mailbox_store(msg, expected_store):
    """
    Safety guard: verify msg belongs to the expected mailbox store.
    Returns (ok, actual_store_name).
    If expected_store is None/empty, always returns (True, "").
    """
    if not expected_store:
        return (True, "")
    try:
        actual = msg.Parent.Store.DisplayName or ""
    except Exception:
        actual = ""
    ok = actual.lower().strip() == expected_store.lower().strip()
    return (ok, actual)

# ==================== MAIN EMAIL PROCESSING ====================
def process_inbox():
    """Main email processing loop with risk detection"""
    tick_id = datetime.now().strftime('%Y%m%dT%H%M%S')
    start_time = time.perf_counter()
    scanned_count = 0
    candidates_unread_count = 0
    processed_count = 0
    skipped_count = 0
    errors_count = 0
    _store_warned = False
    effective_config = CONFIG.copy()
    overrides = load_settings_overrides(SETTINGS_OVERRIDES_PATH)
    if overrides:
        effective_config.update(overrides)
        applied_keys = [k for k, v in overrides.items() if v != CONFIG.get(k)]
        if applied_keys:
            log(f"OVERRIDE_APPLIED keys={','.join(sorted(applied_keys))}", "INFO")
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED", "INFO")

    hot_cfg, hot_events = load_config_files_each_tick()
    for ev in hot_events:
        et = ev.get("event_type")
        cname = ev.get("config_name", "")
        if et == "CONFIG_CHANGED":
            log(f"CONFIG_CHANGED config={cname}", "INFO")
            append_stats("", "", "", "normal", "", cname, "", event_type="CONFIG_CHANGED", status_after="loaded")
        elif et == "CONFIG_INVALID":
            err = ev.get("error", "")
            log(f"CONFIG_INVALID config={cname} error={err}", "WARN")
            append_stats("", "", "", "normal", "", cname, "", event_type="CONFIG_INVALID", status_after="rejected")

    staff_cfg = hot_cfg.get("staff") if isinstance(hot_cfg, dict) else {}
    rr_staff_list = []
    if isinstance(staff_cfg, dict):
        staff_all = staff_cfg.get("staff", []) if isinstance(staff_cfg.get("staff"), list) else []
        off_rotation = staff_cfg.get("off_rotation", []) if isinstance(staff_cfg.get("off_rotation"), list) else []
        leave = staff_cfg.get("leave", []) if isinstance(staff_cfg.get("leave"), list) else []
        off_set = set(off_rotation)
        leave_set = set(leave)
        rr_staff_list = [e for e in staff_all if e not in off_set and e not in leave_set]

    # Extract domain routing settings (prefer dashboard-managed canonical recipients)
    apps_cc_addr_override = get_override_addr(overrides, "apps_cc_addr")
    manager_cc_addr_override = get_override_addr(overrides, "manager_cc_addr")
    apps_override_list = [part for part in (p.strip() for p in (apps_cc_addr_override or "").split(";")) if part]
    manager_override_list = [part for part in (p.strip() for p in (manager_cc_addr_override or "").split(";")) if part]

    apps_team_cfg = hot_cfg.get("apps_team") if isinstance(hot_cfg, dict) else {}
    apps_team_recipients = (
        apps_team_cfg.get("recipients", [])
        if isinstance(apps_team_cfg, dict) and isinstance(apps_team_cfg.get("recipients"), list)
        else []
    )
    if not apps_team_recipients:
        apps_team_recipients = apps_override_list

    manager_cfg = hot_cfg.get("manager_config") if isinstance(hot_cfg, dict) else {}
    manager_recipients = (
        manager_cfg.get("recipients", [])
        if isinstance(manager_cfg, dict) and isinstance(manager_cfg.get("recipients"), list)
        else []
    )
    if not manager_recipients:
        manager_recipients = manager_override_list

    apps_cc_addr = ";".join(apps_team_recipients) if apps_team_recipients else ""
    manager_cc_addr = ";".join(manager_recipients) if manager_recipients else ""
    if len(apps_team_recipients) > 1:
        log(f"CC_MULTI_RECIPIENTS key=apps_cc_addr count={len(apps_team_recipients)}", "INFO")
    if len(manager_recipients) > 1:
        log(f"CC_MULTI_RECIPIENTS key=manager_cc_addr count={len(manager_recipients)}", "INFO")

    apps_cc_list = list(apps_team_recipients)

    buckets_cfg = hot_cfg.get("system_buckets") if isinstance(hot_cfg, dict) else {}
    folders_cfg = buckets_cfg.get("folders", {}) if isinstance(buckets_cfg, dict) and isinstance(buckets_cfg.get("folders"), dict) else {}
    default_folders = {
        "completed": "01_COMPLETED",
        "non_actionable": "02_PROCESSED",
        "quarantine": "03_QUARANTINE",
        "hold": "04_HIB",
        "system_notification": "05_SYSTEM_NOTIFICATIONS",
    }
    effective_config["completed_folder"] = f"Inbox/{folders_cfg.get('completed', default_folders['completed'])}"
    effective_config["quarantine_folder"] = f"Inbox/{folders_cfg.get('quarantine', default_folders['quarantine'])}"
    effective_config["system_notification_folder"] = f"Inbox/{folders_cfg.get('system_notification', default_folders['system_notification'])}"
    effective_config["jira_follow_up_folder"] = JIRA_FOLLOW_UP_FOLDER_PATH

    # Load domain policy
    domain_policy, policy_valid = load_domain_policy()
    if isinstance(buckets_cfg, dict):
        # Canonical domain keys (from _parse_system_buckets_json)
        domain_policy["transfer_domains"] = buckets_cfg.get("transfer_domains", domain_policy.get("transfer_domains", []))
        domain_policy["system_notification_domains"] = buckets_cfg.get("system_notification_domains", domain_policy.get("system_notification_domains", []))
        domain_policy["quarantine_domains"] = buckets_cfg.get("quarantine_domains", domain_policy.get("quarantine_domains", []))
        domain_policy["held_domains"] = buckets_cfg.get("held_domains", domain_policy.get("held_domains", []))
        # Legacy aliases (other code may reference these names)
        domain_policy["external_image_request_domains"] = domain_policy.get("transfer_domains", [])
        domain_policy["always_hold_domains"] = domain_policy.get("held_domains", [])
        # Sender override lists
        domain_policy["transfer_senders"] = buckets_cfg.get("transfer_senders", [])
        domain_policy["system_notification_senders"] = buckets_cfg.get("system_notification_senders", [])
        domain_policy["quarantine_senders"] = buckets_cfg.get("quarantine_senders", [])
        domain_policy["held_senders"] = buckets_cfg.get("held_senders", [])
    policy_source = "valid" if policy_valid else "invalid_fallback"
    known_domains = get_known_domains(domain_policy) if policy_valid else set()
    allowlist_valid = bool(known_domains)
    if not allowlist_valid:
        log("ALLOWLIST_INVALID_FAILSAFE", "ERROR")
    hib_noise_rule = domain_policy.get("hib_noise") if isinstance(domain_policy.get("hib_noise"), dict) else {}
    unknown_domain_mode = overrides.get("unknown_domain_mode", "hold_manager")
    target_store = overrides.get("target_mailbox_store") or ""
    completion_workflow_enabled = CONFIG.get("enable_completion_workflow", False)
    completion_cc_enabled = completion_workflow_enabled and CONFIG.get("enable_completion_cc", True)
    effective_completion_cc = overrides.get("completion_cc_addr", COMPLETION_CC_ADDR) if overrides else COMPLETION_CC_ADDR
    if overrides and overrides.get("completion_cc_addr") and effective_completion_cc != COMPLETION_CC_ADDR:
        log("OVERRIDE_APPLIED key=completion_cc_addr", "INFO")
    global _safe_mode_cache, _safe_mode_inbox, _live_test_override, _jira_followup_folder_error_logged
    is_safe, safe_reason, override_active = determine_safe_mode(effective_config.get("inbox_folder", ""))
    _safe_mode_cache = (is_safe, safe_reason)
    _safe_mode_inbox = effective_config.get("inbox_folder", "")
    _live_test_override = override_active
    log_safe_mode_status(_safe_mode_inbox)
    log(
        f"TICK_START tick_id={tick_id} mailbox=(configured) "
        f"inbox_folder={effective_config['inbox_folder']} processed_folder={effective_config['processed_folder']}",
        "INFO"
    )
    maybe_emit_heartbeat(
        effective_config["mailbox"],
        effective_config["inbox_folder"],
        effective_config["processed_folder"]
    )
    try:
        if not OUTLOOK_AVAILABLE:
            log("Outlook not available - skipping inbox check", "WARN")
            log(f"TICK_SKIP tick_id={tick_id} reason=OUTLOOK_NOT_AVAILABLE", "WARN")
            return
        
        try:
            namespace = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
            
            # Find shared mailbox
            if target_store:
                mailbox = get_store_root_by_display_name(namespace, target_store)
                if not mailbox:
                    log(f"STORE_NOT_FOUND expected_store={target_store}", "ERROR")
                    log(f"TICK_SKIP tick_id={tick_id} reason=STORE_NOT_FOUND", "ERROR")
                    return
                log(f"STORE_SELECTED expected_store={target_store}", "INFO")
            else:
                mailbox = find_mailbox_root_robust(namespace, effective_config["mailbox"])
            if not mailbox:
                log(f"TICK_SKIP tick_id={tick_id} reason=MAILBOX_NOT_FOUND", "ERROR")
                return
            
            inbox, inbox_method = resolve_folder(mailbox, effective_config["inbox_folder"])
            if not inbox:
                log(f"FOLDER_NOT_FOUND inbox_folder={effective_config['inbox_folder']} mailbox=(configured)", "ERROR")
                log(f"FOLDER_RESOLVE_FAILED kind=inbox method={inbox_method} tried_roots=mailbox", "ERROR")
                log(f"TICK_SKIP tick_id={tick_id} reason=INBOX_FOLDER_NOT_FOUND", "ERROR")
                return
            log(f"FOLDER_RESOLVED kind=inbox path={get_folder_path_safe(inbox)}", "INFO")

            processed = None
            processed_method = "RECURSIVE_SEARCH"
            tried_roots = ["mailbox", "inbox_parent", "inbox"]
            root_candidates = [("mailbox_root", mailbox)]
            try:
                root_candidates.append(("inbox_parent", inbox.Parent))
            except Exception:
                pass
            root_candidates.append(("inbox", inbox))
            for _, root in root_candidates:
                processed, processed_method = resolve_folder(root, effective_config["processed_folder"])
                if processed:
                    break
            if not processed:
                log(f"FOLDER_NOT_FOUND processed_folder={effective_config['processed_folder']} mailbox=(configured)", "ERROR")
                log(f"FOLDER_RESOLVE_FAILED kind=processed method={processed_method} tried_roots={','.join(tried_roots)}", "ERROR")
                log(f"TICK_SKIP tick_id={tick_id} reason=PROCESSED_FOLDER_NOT_FOUND", "ERROR")
                return
            log(f"FOLDER_RESOLVED kind=processed path={get_folder_path_safe(processed)}", "INFO")
            resolved_root = "unknown"
            try:
                processed_path = processed.FolderPath or ""
            except Exception:
                processed_path = ""
            for label, root in root_candidates:
                try:
                    root_path = root.FolderPath or ""
                except Exception:
                    root_path = ""
                if processed_path and root_path and processed_path.startswith(root_path):
                    resolved_root = label
                    break
            log(f"FOLDER_RESOLVED_ROOT kind=processed root={resolved_root}", "INFO")

            quarantine = None
            quarantine_method = "RECURSIVE_SEARCH"
            for _, root in root_candidates:
                quarantine, quarantine_method = resolve_folder(root, effective_config["quarantine_folder"])
                if quarantine:
                    break
            if quarantine:
                log(f"FOLDER_RESOLVED kind=quarantine path={get_folder_path_safe(quarantine)}", "INFO")
            else:
                log(f"FOLDER_NOT_FOUND quarantine_folder={effective_config['quarantine_folder']} mailbox=(configured)", "WARN")
                quarantine = get_or_create_subfolder(inbox, effective_config["quarantine_folder"])
                if quarantine:
                    log(f"FOLDER_CREATED kind=quarantine path={get_folder_path_safe(quarantine)}", "INFO")
                else:
                    log(f"FOLDER_CREATE_FAIL kind=quarantine name={effective_config['quarantine_folder']}", "ERROR")
            
            system_notification_folder = None
            sn_path = effective_config.get("system_notification_folder", "Inbox/05_SYSTEM_NOTIFICATIONS")
            for _, root in root_candidates:
                system_notification_folder, _ = resolve_folder(root, sn_path)
                if system_notification_folder:
                    break
            if system_notification_folder:
                log(f"FOLDER_RESOLVED kind=system_notification path={get_folder_path_safe(system_notification_folder)}", "INFO")
            else:
                log(f"FOLDER_NOT_FOUND system_notification_folder={sn_path} mailbox=(configured)", "WARN")

            completed_dest = None
            for _, root in root_candidates:
                completed_dest, _ = resolve_folder(root, effective_config["completed_folder"])
                if completed_dest:
                    break
            if completed_dest:
                log(f"FOLDER_RESOLVED kind=completed path={get_folder_path_safe(completed_dest)}", "INFO")
            else:
                log(f"FOLDER_NOT_FOUND completed_folder={effective_config['completed_folder']}", "WARN")

            jira_follow_up_folder = None
            jira_follow_up_enabled = False
            jira_follow_up_path = effective_config.get("jira_follow_up_folder", JIRA_FOLLOW_UP_FOLDER_PATH)
            for _, root in root_candidates:
                jira_follow_up_folder, _ = resolve_folder(root, jira_follow_up_path)
                if jira_follow_up_folder:
                    break
            if jira_follow_up_folder:
                jira_follow_up_enabled = True
                log(f"FOLDER_RESOLVED kind=jira_follow_up path={get_folder_path_safe(jira_follow_up_folder)}", "INFO")
            else:
                if not _jira_followup_folder_error_logged:
                    log(f"FOLDER_RESOLVE_FAIL kind=jira_follow_up path={jira_follow_up_path} feature=disabled", "ERROR")
                    _jira_followup_folder_error_logged = True

            hib_folder = get_or_create_subfolder(inbox, HIB_FOLDER_NAME)
            if hib_folder:
                log(f"FOLDER_RESOLVED kind=hib path={get_folder_path_safe(hib_folder)}", "INFO")
            else:
                log(f"FOLDER_CREATE_FAIL kind=hib name={HIB_FOLDER_NAME}", "ERROR")

            items_total = 0
            unread_count = 0
            default_item_type = "?"
            try:
                items_total = inbox.Items.Count
            except Exception:
                items_total = 0
            try:
                unread_count = inbox.Items.Restrict("[UnRead] = True").Count
            except Exception:
                unread_count = 0
            try:
                default_item_type = inbox.DefaultItemType
            except Exception:
                default_item_type = "?"
            log(
                f"INBOX_COUNTS folder_path={get_folder_path_safe(inbox)} items_total={items_total} "
                f"unread_count={unread_count} default_item_type={default_item_type}",
                "INFO"
            )
            if items_total > 0 and unread_count == 0:
                try:
                    for idx in range(min(items_total, 3)):
                        try:
                            item = inbox.Items.Item(idx + 1)
                        except Exception:
                            continue
                        try:
                            message_class = getattr(item, "MessageClass", "?")
                        except Exception:
                            message_class = "?"
                        try:
                            unread = getattr(item, "UnRead", "?")
                        except Exception:
                            unread = "?"
                        try:
                            received = getattr(item, "ReceivedTime", "?")
                        except Exception:
                            received = "?"
                        try:
                            entry_id = getattr(item, "EntryID", "")
                            entryid_tail = entry_id[-6:] if entry_id else "?"
                        except Exception:
                            entryid_tail = "?"
                        log(
                            f"INBOX_SAMPLE idx={idx} message_class={message_class} unread={unread} "
                            f"received={received} entryid_tail={entryid_tail}",
                            "INFO"
                        )
                except Exception:
                    pass
            
            # Get unread messages
            msgs = list(inbox.Items.Restrict("[UnRead] = True"))
            scanned_count = len(msgs)
            candidates_unread_count = len(msgs)
            if items_total > 0 and scanned_count == 0:
                log(
                    f"ITEMS_ENUM_ANOMALY items_total={items_total} note=\"Items.Count>0 but scan loop saw 0\"",
                    "WARN"
                )
            if not msgs:
                return  # No new messages
            
            global _staff_list_cache
            staff_list = get_staff_list()
            _staff_list_cache = staff_list
            if not ensure_processed_ledger_exists(PROCESSED_LEDGER_PATH):
                log("STATE_REQUIRED_SKIP state=processed_ledger", "ERROR")
                log(f"TICK_SKIP tick_id={tick_id} reason=STATE_REQUIRED_MISSING", "ERROR")
                return
            processed_ledger = load_processed_ledger()
            if processed_ledger is None:
                log("STATE_REQUIRED_SKIP state=processed_ledger", "ERROR")
                log(f"TICK_SKIP tick_id={tick_id} reason=STATE_REQUIRED_MISSING", "ERROR")
                return
            
            for msg in msgs:
                staff_sender_flag = False
                try:
                    # Store mismatch warning (once per tick)
                    if target_store and not _store_warned:
                        try:
                            _actual_store = msg.Parent.Store.DisplayName or ""
                        except Exception:
                            _actual_store = ""
                        if _actual_store and _actual_store.lower().strip() != target_store.lower().strip():
                            log(f"CONFIG_MISMATCH expected_store={target_store} actual_store={_actual_store}", "WARN")
                        _store_warned = True

                    # Extract email details (resolve SMTP for Exchange users)
                    sender_email = resolve_sender_smtp(msg) or "unknown"

                    # Extract and classify sender (sender override first, then domain)
                    sender_domain = extract_sender_domain(sender_email)
                    domain_bucket, match_level = classify_sender(sender_email, sender_domain, domain_policy)
                    if domain_bucket is None:
                        # No explicit policy match — fall through to existing internal/unknown logic
                        domain_bucket = classify_sender_domain(sender_domain, domain_policy) if sender_domain else "unknown"
                        # Only set match_level for explicit policy buckets, not defaults
                        if domain_bucket in ("quarantine", "hold", "system_notification", "external_image_request"):
                            match_level = "domain"
                        else:
                            match_level = None
                    # Safety: if sender override matches, always treat it as explicit sender match.
                    if match_level != "sender":
                        sender_bucket = get_sender_override_bucket(sender_email, domain_policy)
                        if sender_bucket:
                            domain_bucket = sender_bucket
                            match_level = "sender"

                    if match_level == "sender":
                        log(f"POLICY_MATCH level=sender bucket={domain_bucket} sender={sender_email}", "INFO")
                    elif match_level == "domain":
                        log(f"POLICY_MATCH level=domain bucket={domain_bucket} domain={sender_domain}", "INFO")

                    if domain_bucket == "quarantine":
                        log(f"ROUTE_QUARANTINE domain={sender_domain}", "WARN")
                        if not quarantine:
                            log("ROUTE_QUARANTINE_FAIL reason=folder_missing", "ERROR")
                            continue
                        try:
                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if not _sb_ok:
                                log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                continue
                            msg.UnRead = False
                            msg.Move(quarantine)
                            log("MOVE_OK kind=quarantine", "INFO")
                        except Exception as e:
                            log(f"MOVE_FAIL kind=quarantine error={e}", "ERROR")
                        continue

                    try:
                        subject = msg.Subject.strip()
                    except:
                        subject = ""
                    subject_with_id = ensure_sami_id_in_subject(subject, msg)
                    
                    try:
                        body = msg.Body[:500] if msg.Body else ""  # First 500 chars
                    except:
                        body = ""
                    
                    try:
                        high_importance = (msg.Importance == 2)  # 2 = High
                    except:
                        high_importance = False
                    
                    try:
                        received_time = msg.ReceivedTime
                        received_iso = received_time.isoformat() if received_time else ""
                    except:
                        received_iso = ""
                    
                    try:
                        conversation_id = msg.ConversationID
                    except Exception:
                        conversation_id = None

                    message_key, identity = compute_message_identity(msg, sender_email, subject, received_iso)
                    entry_id = identity.get("entry_id")
                    if entry_id:
                        msg_id = entry_id
                    else:
                        msg_id = str(hash(subject + sender_email))
                    if message_key.startswith("fallback:"):
                        msg_id = identity.get("entry_id") or identity.get("conversation_id") or ""
                        log(f"LEDGER_FALLBACK_KEY msg_id={msg_id}", "WARN")
                    
                    if message_key in processed_ledger:
                        log(f"LEDGER_SKIP {message_key}", "WARN")
                        skipped_count += 1
                        continue

                    if is_hib_notification(msg):
                        subject_prefix = re.sub(r"\d", "X", subject or "")[:60]
                        log(f"HIB_MOVE msg_id={msg_id} sender={sender_email} subject_prefix={subject_prefix}", "INFO")
                        hib_moved = False
                        if not hib_folder:
                            log("HIB_MOVE_SKIP reason=hib_folder_missing", "WARN")
                        else:
                            try:
                                _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                                if not _sb_ok:
                                    log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                else:
                                    msg.UnRead = False
                                    msg.Move(hib_folder)
                                    hib_now = locals().get("now_dt") or datetime.now()
                                    processed_ledger[message_key] = {
                                        "ts": hib_now.isoformat(),
                                        "assigned_to": "hib",
                                        "risk": "normal",
                                        "route": "HIB"
                                    }
                                    if identity.get("entry_id"):
                                        processed_ledger[message_key]["entry_id"] = identity["entry_id"]
                                    # Check for 16110 escalation before saving ledger
                                    if hib_contains_16110(msg) and apps_cc_addr and not processed_ledger[message_key].get("apps_fwd"):
                                        try:
                                            fwd = msg.Forward()
                                            ok = _add_and_resolve_recipients(fwd, apps_cc_list, kind="apps_team")
                                            if not ok:
                                                raise Exception("ResolveAll failed")
                                            is_safe, _ = is_safe_mode()
                                            if not is_safe:
                                                fwd.Send()
                                            processed_ledger[message_key]["apps_fwd"] = True
                                            eid = processed_ledger[message_key].get("entry_id")
                                            if not eid:
                                                try:
                                                    eid = getattr(msg, "EntryID", "")
                                                except:
                                                    eid = ""
                                            entry_tail = (eid or "")[-8:]
                                            log(f"HIB_16110_FORWARD apps_team=yes entryid_tail={entry_tail}", "INFO")
                                        except Exception as e:
                                            log(f"HIB_16110_FORWARD_ERROR error={e}", "ERROR")
                                    save_processed_ledger(processed_ledger)
                                    append_stats(subject, "hib", sender_email, "normal", "hib", "ROUTE_HIB", policy_source)
                                    hib_outlook = locals().get("outlook_app")
                                    hib_watchdog_record_and_maybe_alert(hib_now, hib_outlook, manager_cc_addr, apps_cc_addr)
                                    processed_count += 1
                                    hib_moved = True
                            except Exception as e:
                                log(f"HIB_ROUTE_ERROR error={e}", "ERROR")
                        if hib_moved:
                            continue
                    # Internal staff guard - skip round-robin but allow completion
                    sender_override_matched = (match_level == "sender")
                    if sender_override_matched and is_internal_sender(sender_email) and (not is_staff_sender(sender_email, staff_list)):
                        log(f"INTERNAL_NON_STAFF_BYPASS reason=sender_override sender={sender_email} bucket={domain_bucket}", "INFO")
                    if (not sender_override_matched) and is_internal_sender(sender_email) and is_staff_sender(sender_email, staff_list):
                        if not is_completion_subject(subject):
                            log(f"INTERNAL_STAFF_EMAIL skip_new_job sender={sender_email}", "INFO")
                            continue

                    # Internal non-staff safety guard
                    if (not sender_override_matched) and is_internal_sender(sender_email) and (not is_staff_sender(sender_email, staff_list)):
                        log(f"ROUTE manager reason=internal_sender_not_in_staff sender={sender_email}", "INFO")
                        try:
                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if _sb_ok and manager_cc_addr:
                                fwd = msg.Forward()
                                ok = _add_and_resolve_recipients(fwd, manager_recipients, kind="manager")
                                if not ok:
                                    raise Exception("ResolveAll failed")
                                fwd.Subject = f"[REVIEW] Internal non-staff: {subject_with_id}"
                                fwd.Body = f"Internal sender not in staff.txt.\nSender: {sender_email}\n\n" + (fwd.Body or "")
                                is_safe, _ = is_safe_mode()
                                if not is_safe:
                                    fwd.Send()
                                msg.UnRead = False
                                msg.Move(processed)
                                processed_ledger[message_key] = {
                                    "ts": datetime.now().isoformat(),
                                    "assigned_to": "manager_review",
                                    "risk": "normal",
                                    "route": "internal_non_staff"
                                }
                                if identity.get("entry_id"):
                                    processed_ledger[message_key]["entry_id"] = identity["entry_id"]
                                save_processed_ledger(processed_ledger)
                                append_stats(subject, "manager_review", sender_email, "normal", domain_bucket, "INTERNAL_NON_STAFF", policy_source)
                                processed_count += 1
                        except Exception as e:
                            log(f"INTERNAL_NON_STAFF_ERROR error={e}", "ERROR")
                        continue

                    hib_noise_match = False
                    hib_cc_override = ""
                    if hib_noise_rule:
                        hib_sender = str(hib_noise_rule.get("sender_equals", "")).strip().lower()
                        if hib_sender and sender_email == hib_sender:
                            subject_lower = subject.lower()
                            require_all = hib_noise_rule.get("subject_contains_all", [])
                            require_any = hib_noise_rule.get("subject_contains_any", [])
                            all_ok = all(term.lower() in subject_lower for term in require_all) if require_all else True
                            any_ok = any(term.lower() in subject_lower for term in require_any) if require_any else False
                            if all_ok and any_ok:
                                hib_noise_match = True
                    if hib_noise_match:
                        log(f"ROUTE=HIB subject={subject[:50]}", "INFO")
                        if hib_folder:
                            try:
                                _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                                if not _sb_ok:
                                    log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                else:
                                    msg.UnRead = False
                                    msg.Move(hib_folder)
                                    processed_ledger[message_key] = {
                                        "ts": datetime.now().isoformat(),
                                        "assigned_to": "hib",
                                        "risk": "normal",
                                        "route": "HIB"
                                    }
                                    if identity.get("entry_id"):
                                        processed_ledger[message_key]["entry_id"] = identity["entry_id"]
                                    # Check for 16110 escalation before saving ledger
                                    if hib_contains_16110(msg) and apps_cc_addr and not processed_ledger[message_key].get("apps_fwd"):
                                        try:
                                            fwd = msg.Forward()
                                            ok = _add_and_resolve_recipients(fwd, apps_cc_list, kind="apps_team")
                                            if not ok:
                                                raise Exception("ResolveAll failed")
                                            is_safe, _ = is_safe_mode()
                                            if not is_safe:
                                                fwd.Send()
                                            processed_ledger[message_key]["apps_fwd"] = True
                                            eid = processed_ledger[message_key].get("entry_id")
                                            if not eid:
                                                try:
                                                    eid = getattr(msg, "EntryID", "")
                                                except:
                                                    eid = ""
                                            entry_tail = (eid or "")[-8:]
                                            log(f"HIB_16110_FORWARD apps_team=yes entryid_tail={entry_tail}", "INFO")
                                        except Exception as e:
                                            log(f"HIB_16110_FORWARD_ERROR error={e}", "ERROR")
                                    save_processed_ledger(processed_ledger)
                                    append_stats(subject, "hib", sender_email, "normal", "hib", "ROUTE_HIB", policy_source)
                                    try:
                                        hib_outlook = win32com.client.Dispatch("Outlook.Application")
                                    except Exception:
                                        hib_outlook = None
                                    hib_watchdog_record_and_maybe_alert(datetime.now(), hib_outlook, manager_cc_addr, apps_cc_addr)
                                    processed_count += 1
                            except Exception as e:
                                log(f"HIB_ROUTE_ERROR error={e}", "ERROR")
                        continue

                    jira_candidate = is_jira_candidate(subject, body, sender_email)
                    jira_comment_email = is_jira_comment_email(body)

                    if jira_candidate and jira_comment_email:
                        jira_followup_key = f"{message_key}::JIRA_FOLLOWUP"
                        if jira_followup_key in processed_ledger:
                            log(f"JIRA_FOLLOWUP_DUP_SKIP msg_key={message_key}", "WARN")
                            skipped_count += 1
                            continue
                        try:
                            if not jira_follow_up_enabled or not jira_follow_up_folder:
                                log(f"JIRA_FOLLOWUP_FAIL msg_key={message_key} error=feature_disabled", "ERROR")
                                errors_count += 1
                                continue

                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if not _sb_ok:
                                log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                                continue

                            msg.UnRead = False
                            moved_msg = msg.Move(jira_follow_up_folder)
                            jira_msg = moved_msg if moved_msg is not None else msg
                            try:
                                jira_msg.Importance = 2
                            except Exception:
                                pass

                            assignee = get_next_staff()
                            if not assignee:
                                log(f"JIRA_FOLLOWUP_FAIL msg_key={message_key} error=no_staff_available", "ERROR")
                                errors_count += 1
                                continue

                            fwd = jira_msg.Forward()
                            fwd.Recipients.Add(assignee)
                            fwd.Subject = f"{JIRA_FOLLOW_UP_SUBJECT_PREFIX}{subject_with_id}"
                            fwd.Body = JIRA_FOLLOW_UP_BANNER + (fwd.Body or "")
                            fwd.SentOnBehalfOfName = CONFIG["mailbox"]

                            try:
                                requester = sender_email.strip() if isinstance(sender_email, str) else ""
                                if not requester or "@" not in requester:
                                    requester = CONFIG["mailbox"]
                                assignee_email = assignee if isinstance(assignee, str) else ""
                                staff_set = {s.lower() for s in staff_list}
                                if assignee_email.lower() not in staff_set:
                                    skip_reason = "assignee_not_staff"
                                else:
                                    skip_reason = ""
                                if skip_reason:
                                    log(f"COMPLETION_HOTLINK_SKIPPED reason={skip_reason} msg_id={msg_id}", "INFO")
                                else:
                                    mode_out = []
                                    injected = inject_completion_hotlink(
                                        fwd,
                                        requester,
                                        subject_with_id,
                                        SAMI_SHARED_INBOX,
                                        mode_out,
                                        original_msg=jira_msg,
                                        is_jira_followup=True,
                                    )
                                    if injected:
                                        mode = mode_out[0] if mode_out else "HTML"
                                        log(f"COMPLETION_HOTLINK_ADDED mode={mode} msg_id={msg_id}", "INFO")
                                    else:
                                        log(f"COMPLETION_HOTLINK_SKIPPED reason=inject_failed msg_id={msg_id}", "WARN")
                            except Exception:
                                log("COMPLETION_HOTLINK_FAIL", "WARN")

                            is_safe, safe_reason = is_safe_mode()
                            if is_safe:
                                log(f"SAFE_MODE_SUPPRESS_SEND action=JIRA_FOLLOWUP bucket={domain_bucket} assignee={assignee} reason={safe_reason}", "WARN")
                            else:
                                fwd.Send()

                            assigned_now = datetime.now().isoformat()
                            processed_ledger[message_key] = {
                                "ts": assigned_now,
                                "assigned_to": assignee,
                                "risk": "normal",
                                "route": "JIRA_FOLLOWUP"
                            }
                            if identity.get("entry_id"):
                                processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                            if identity.get("store_id"):
                                processed_ledger[message_key]["store_id"] = identity.get("store_id")
                            if identity.get("internet_message_id"):
                                processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                            if conversation_id:
                                processed_ledger[message_key]["conversation_id"] = conversation_id
                            processed_ledger[jira_followup_key] = {
                                "ts": assigned_now,
                                "assigned_to": assignee,
                                "route": "JIRA_FOLLOWUP",
                                "msg_key": message_key
                            }
                            if not save_processed_ledger(processed_ledger):
                                log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                                errors_count += 1
                                continue
                            append_stats(
                                subject,
                                assignee,
                                sender_email,
                                "normal",
                                domain_bucket,
                                "JIRA_FOLLOWUP",
                                policy_source,
                                event_type="JIRA_FOLLOWUP_ASSIGNED",
                                msg_key=message_key,
                                status_after="jira_follow_up",
                                assigned_ts=assigned_now,
                            )
                            processed_count += 1
                        except Exception as e:
                            log(f"JIRA_FOLLOWUP_FAIL msg_key={message_key} error={e}", "ERROR")
                            errors_count += 1
                        continue

                    # ===== COMPLETION DETECTION =====
                    try:
                        staff_sender_flag = sender_email in staff_list
                        keyword_hit = is_completion_subject(subject)
                        if staff_sender_flag and keyword_hit:
                            if conversation_id:
                                match_key = find_ledger_key_by_conversation_id(processed_ledger, conversation_id)
                            else:
                                match_key = None
                            if match_key:
                                entry = processed_ledger.get(match_key, {})
                                entry["completed_at"] = datetime.now().isoformat()
                                entry["completed_by"] = sender_email
                                entry["completion_source"] = "subject_keyword"
                                processed_ledger[match_key] = entry
                                append_stats(
                                    subject,
                                    "completed",
                                    sender_email,
                                    "COMPLETION_MATCHED",
                                    domain_bucket,
                                    "COMPLETION_SUBJECT_KEYWORD",
                                    policy_source
                                )
                                if not save_processed_ledger(processed_ledger):
                                    log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                                    errors_count += 1
                                    continue
                                msg.UnRead = False
                                _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                                if not _sb_ok:
                                    log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                    append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                                else:
                                    msg.Move(processed)
                                processed_count += 1
                                continue
                            else:
                                # Staff [COMPLETED] with no prior ledger entry — bypass quarantine
                                log(f"BYPASS_QUARANTINE_STAFF_COMPLETED_CONFIRMATION msg_id={msg_id} sender={sender_email}", "INFO")
                                processed_ledger[message_key] = {
                                    "ts": datetime.now().isoformat(),
                                    "assigned_to": "completed",
                                    "risk": "normal",
                                    "completion_source": "staff_completed_confirmation"
                                }
                                if identity.get("entry_id"):
                                    processed_ledger[message_key]["entry_id"] = identity["entry_id"]
                                if identity.get("store_id"):
                                    processed_ledger[message_key]["store_id"] = identity["store_id"]
                                if identity.get("internet_message_id"):
                                    processed_ledger[message_key]["internet_message_id"] = identity["internet_message_id"]
                                if conversation_id:
                                    processed_ledger[message_key]["conversation_id"] = conversation_id
                                append_stats(subject, "completed", sender_email, "normal", domain_bucket, "STAFF_COMPLETED_CONFIRMATION", policy_source, event_type="COMPLETED", msg_key=message_key)
                                if not save_processed_ledger(processed_ledger):
                                    log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                                    errors_count += 1
                                    continue
                                msg.UnRead = False
                                _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                                if not _sb_ok:
                                    log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                    append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                                elif completed_dest:
                                    msg.Move(completed_dest)
                                else:
                                    msg.Move(processed)
                                processed_count += 1
                                continue
                        is_reply = subject.lower().strip().startswith("re:")
                        if completion_cc_enabled and staff_sender_flag and is_reply and message_has_completion_cc(msg, effective_completion_cc):
                            if conversation_id:
                                match_key = find_ledger_key_by_conversation_id(processed_ledger, conversation_id)
                            else:
                                match_key = None
                            if match_key:
                                entry = processed_ledger.get(match_key, {})
                                entry["completed_at"] = datetime.now().isoformat()
                                entry["completed_by"] = sender_email
                                entry["completion_source"] = "reply_all_cc"
                                entry["completion_subject"] = subject
                                processed_ledger[match_key] = entry
                                append_stats(subject, "completed", sender_email, "COMPLETION_MATCHED", domain_bucket, "COMPLETION_MATCHED", policy_source, event_type="COMPLETED")
                            else:
                                append_stats(subject, "completed", sender_email, "COMPLETION_UNMATCHED", domain_bucket, "COMPLETION_UNMATCHED", policy_source, event_type="COMPLETED")
                            if not save_processed_ledger(processed_ledger):
                                log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                                errors_count += 1
                                continue
                            msg.UnRead = False
                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if not _sb_ok:
                                log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                            else:
                                msg.Move(processed)
                            processed_count += 1
                            continue
                    except Exception as e:
                        log(f"COMPLETION_ERROR {e}", "ERROR")
                        append_stats(subject, "completed", sender_email, "COMPLETION_ERROR", domain_bucket, "COMPLETION_ERROR", policy_source, event_type="COMPLETED")
                        try:
                            msg.UnRead = False
                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if not _sb_ok:
                                log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                append_stats(subject, "skipped", sender_email, "COMPLETION_ERROR", domain_bucket, "WRONG_MAILBOX", policy_source)
                            else:
                                msg.Move(processed)
                        except Exception:
                            pass
                        processed_count += 1
                        continue
                    
                    # ===== SMART FILTER =====
                    if is_internal_reply(sender_email, subject, staff_list):
                        msg_id = getattr(msg, "EntryID", "") or getattr(msg, "ConversationID", "") or ""
                        log(f"SMART_FILTER_SKIP msg_id={msg_id}", "INFO")
                        append_stats(subject, "completed", sender_email, "normal", domain_bucket, "SMART_FILTER_COMPLETION", policy_source, event_type="COMPLETED")
                        processed_ledger[message_key] = {
                            "ts": datetime.now().isoformat(),
                            "assigned_to": "completed",
                            "risk": "normal"
                        }
                        if identity.get("entry_id"):
                            processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                        if identity.get("store_id"):
                            processed_ledger[message_key]["store_id"] = identity.get("store_id")
                        if identity.get("internet_message_id"):
                            processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                        if conversation_id:
                            processed_ledger[message_key]["conversation_id"] = conversation_id
                        if not save_processed_ledger(processed_ledger):
                            log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                            errors_count += 1
                            continue
                        msg.UnRead = False
                        _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                        if not _sb_ok:
                            log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                            append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                        else:
                            msg.Move(processed)
                        processed_count += 1
                        continue

                    # ===== JIRA AUTOMATION NOTIFICATION FILTER =====
                    if is_jira_automation_notification(sender_domain, subject, msg):
                        log(f"JIRA_AUTOMATION_SKIP msg_id={msg_id} sender={sender_email}", "INFO")
                        append_stats(subject, "non_actionable", sender_email, "normal", domain_bucket, "JIRA_AUTOMATION_NOTIFICATION", policy_source)
                        processed_ledger[message_key] = {
                            "ts": datetime.now().isoformat(),
                            "assigned_to": "non_actionable",
                            "risk": "normal",
                            "reason": "JIRA_AUTOMATION_NOTIFICATION"
                        }
                        if identity.get("entry_id"):
                            processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                        if identity.get("store_id"):
                            processed_ledger[message_key]["store_id"] = identity.get("store_id")
                        if identity.get("internet_message_id"):
                            processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                        if conversation_id:
                            processed_ledger[message_key]["conversation_id"] = conversation_id
                        if not save_processed_ledger(processed_ledger):
                            log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                            errors_count += 1
                            continue
                        msg.UnRead = False
                        _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                        if not _sb_ok:
                            log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                            append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                        else:
                            msg.Move(processed)
                        processed_count += 1
                        continue

                    # ===== UNKNOWN DOMAIN HOLD =====
                    allowlist_reason = "ALLOWLIST_INVALID_FAILSAFE" if not allowlist_valid else "UNKNOWN_DOMAIN"
                    if not allowlist_valid or not is_domain_known(sender_email, known_domains):
                        if not quarantine:
                            log("HOLD_UNKNOWN_DOMAIN_FAIL reason=quarantine_missing", "ERROR")
                            errors_count += 1
                            continue
                        processed_ledger[message_key] = {
                            "ts": datetime.now().isoformat(),
                            "assigned_to": "hold",
                            "risk": "normal",
                            "reason": "HOLD_UNKNOWN_DOMAIN"
                        }
                        if identity.get("entry_id"):
                            processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                        if identity.get("store_id"):
                            processed_ledger[message_key]["store_id"] = identity.get("store_id")
                        if identity.get("internet_message_id"):
                            processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                        if conversation_id:
                            processed_ledger[message_key]["conversation_id"] = conversation_id
                        if not save_processed_ledger(processed_ledger):
                            log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                            errors_count += 1
                            continue
                        try:
                            msg.UnRead = False
                            _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                            if not _sb_ok:
                                log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                                continue
                            msg.Move(quarantine)
                        except Exception:
                            log("HOLD_UNKNOWN_DOMAIN_FAIL reason=move_failed", "ERROR")
                            errors_count += 1
                            continue
                        manager_email = manager_cc_addr
                        outlook_app = None
                        try:
                            outlook_app = win32com.client.Dispatch("Outlook.Application")
                        except Exception:
                            outlook_app = None
                        notified = send_manager_hold_notification(
                            outlook_app,
                            manager_email,
                            msg,
                            allowlist_reason,
                            effective_config["quarantine_folder"]
                        )
                        append_stats(
                            subject,
                            "hold",
                            sender_email,
                            "normal",
                            "unknown",
                            "HOLD_UNKNOWN_DOMAIN",
                            policy_source
                        )
                        processed_count += 1
                        continue
                    
                    # ===== RISK DETECTION =====
                    if hib_noise_match or not RISK_FILTER_ENABLED:
                        risk_level = "normal"
                        risk_reason = None
                    else:
                        risk_level, risk_reason = detect_risk(subject, body, high_importance)

                        if risk_level != "normal":
                            log(f"\u26A0\uFE0F Risk detected [{risk_level.upper()}]: {risk_reason}", "WARN")

                    # ===== AUTHORITATIVE ROUTING POLICY =====
                    action_taken = ""
                    assignee = None
                    hold_recipients = []
                    cc_manager = False
                    cc_apps = False
                    is_completion = False

                    # Get policy addresses
                    policy_manager = manager_cc_addr or ""
                    policy_apps_specialists = apps_cc_list

                    if hib_noise_match:
                        action_taken = "hib_noise_suppressed"
                        assignee = "hib_noise"
                        hold_recipients = []
                        cc_manager = False
                        cc_apps = False
                        domain_bucket = "hib_noise"
                    elif domain_bucket == "external_image_request":
                        # Class 1: External image requests - round-robin to staff, NO CC
                        assignee = get_next_staff()
                        action_taken = "IMAGE_REQUEST_EXTERNAL"
                        cc_manager = False
                        cc_apps = False
                        log(f"IMAGE_REQUEST_EXTERNAL domain={sender_domain} cc_manager=False cc_apps=False", "INFO")

                    elif domain_bucket == "internal":
                        # Internal domain: check if SAMI support staff
                        if is_sami_support_staff(sender_email, domain_policy):
                            # SAMI support staff - treat as COMPLETION
                            is_completion = True
                            action_taken = "COMPLETION"
                            assignee = "completed"
                            msg_id = getattr(msg, "EntryID", "") or getattr(msg, "ConversationID", "") or ""
                            log(f"COMPLETION reason=SAMI_SUPPORT_STAFF msg_id={msg_id}", "INFO")
                        else:
                            # Non-SAMI internal sender - round-robin to staff
                            assignee = get_next_staff()
                            action_taken = "INTERNAL_QUERY"
                            cc_manager = False
                            cc_apps = False
                            log(f"INTERNAL_QUERY domain={sender_domain}", "INFO")

                    elif domain_bucket == "system_notification":
                        # Class 3: System notifications - silent move, no email
                        action_taken = "SYSTEM_NOTIFICATION"
                        assignee = "system_notification"
                        cc_manager = False
                        cc_apps = False
                        log(f"SYSTEM_NOTIFICATION domain={sender_domain} silent_move=True", "INFO")

                    elif domain_bucket in ("unknown", "hold"):
                        # Unknown domain: To manager, no CC
                        action_taken = "UNKNOWN_DOMAIN"
                        assignee = "hold"
                        cc_manager = False
                        cc_apps = False
                        # Manager as To recipient
                        if policy_manager:
                            hold_recipients.append(policy_manager)
                        log(f"UNKNOWN_DOMAIN domain={sender_domain} to_manager=True cc=False", "WARN")

                    else:
                        # Fallback for any other bucket
                        assignee = get_next_staff()
                        action_taken = "FALLBACK_ROUTING"
                        log(f"FALLBACK_ROUTING domain_bucket={domain_bucket}", "WARN")

                    # Append match_level to action for audit trail (e.g. IMAGE_REQUEST_EXTERNAL/sender)
                    if match_level and action_taken and action_taken != "hib_noise_suppressed":
                        action_taken = f"{action_taken}/{match_level}"

                    # Handle SAMI completion early
                    if is_completion:
                        append_stats(subject, "completed", sender_email, "normal", domain_bucket, action_taken, policy_source, event_type="COMPLETED")
                        msg.UnRead = False
                        _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                        if not _sb_ok:
                            log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                            append_stats(subject, "skipped", sender_email, "normal", domain_bucket, "WRONG_MAILBOX", policy_source)
                        else:
                            msg.Move(processed)

                        processed_ledger[message_key] = {
                            "ts": datetime.now().isoformat(),
                            "assigned_to": "completed",
                            "risk": "normal",
                            "completion_source": "sami_support_staff"
                        }
                        if identity.get("entry_id"):
                            processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                        if identity.get("store_id"):
                            processed_ledger[message_key]["store_id"] = identity.get("store_id")
                        if identity.get("internet_message_id"):
                            processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                        if conversation_id:
                            processed_ledger[message_key]["conversation_id"] = conversation_id
                        if not save_processed_ledger(processed_ledger):
                            log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                            errors_count += 1
                            continue
                        processed_count += 1
                        continue

                    if not assignee:
                        log("No staff available for assignment!", "ERROR")
                        errors_count += 1
                        continue

                    if risk_level == "critical" and message_key in processed_ledger:
                        log("CRITICAL_ALREADY_PROCESSED", "WARN")
                        skipped_count += 1
                        continue

                    processed_ledger[message_key] = {
                        "ts": datetime.now().isoformat(),
                        "assigned_to": assignee,
                        "risk": risk_level
                    }
                    if action_taken == "hib_noise_suppressed":
                        processed_ledger[message_key]["reason"] = "hib_noise_suppressed"
                    if match_level:
                        processed_ledger[message_key]["match_level"] = match_level
                    if identity.get("entry_id"):
                        processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                    if identity.get("store_id"):
                        processed_ledger[message_key]["store_id"] = identity.get("store_id")
                    if identity.get("internet_message_id"):
                        processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                    if conversation_id:
                        processed_ledger[message_key]["conversation_id"] = conversation_id
                    if not save_processed_ledger(processed_ledger):
                        log("STATE_WRITE_FAIL state=processed_ledger", "ERROR")
                        errors_count += 1
                        continue

                    if domain_bucket == "system_notification":
                        # Silent move to system_notification folder — no email sent
                        if is_jones_completion_notification(msg):
                            log("FILTER_JONES_COMPLETION action=move_processed", "INFO")
                        else:
                            log(f"SYSTEM_NOTIFICATION_SILENT_MOVE domain={sender_domain}", "INFO")
                        append_stats(subject, assignee, sender_email, risk_level, domain_bucket, action_taken, policy_source)
                        msg.UnRead = False
                        _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                        if not _sb_ok:
                            log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                            append_stats(subject, "skipped", sender_email, risk_level, domain_bucket, "WRONG_MAILBOX", policy_source)
                        else:
                            if system_notification_folder:
                                msg.Move(system_notification_folder)
                            else:
                                msg.Move(processed)
                        processed_count += 1
                        continue
                    # Forward email
                    fwd = msg.Forward()

                    # Add recipients based on routing decision
                    if action_taken == "hib_noise_suppressed":
                        if hib_cc_override:
                            try:
                                fwd.CC = hib_cc_override
                            except Exception:
                                log("HIB_NOISE_CC_SET_FAIL", "WARN")
                    elif hold_recipients:
                        # HOLD or SYSTEM_NOTIFICATION routing: add hold recipients
                        for recipient in hold_recipients:
                            fwd.Recipients.Add(recipient)
                        log(f"HOLD_FORWARD count={len(hold_recipients)} action={action_taken}", "INFO")
                    else:
                        # Normal routing: add assignee
                        fwd.Recipients.Add(assignee)

                    # Add CC recipients based on policy flags
                    if cc_manager and policy_manager:
                        try:
                            cc_recipient = fwd.Recipients.Add(policy_manager)
                            cc_recipient.Type = 2  # CC
                            log("CC_MANAGER_ADDED value=set", "INFO")
                        except Exception as e:
                            log(f"CC_MANAGER_ADD_FAIL {e}", "WARN")

                    if cc_apps and policy_apps_specialists:
                        for apps_email in policy_apps_specialists:
                            try:
                                cc_recipient = fwd.Recipients.Add(apps_email)
                                cc_recipient.Type = 2  # CC
                                log("CC_APPS_ADDED value=set", "INFO")
                            except Exception as e:
                                log(f"CC_APPS_ADD_FAIL {e}", "WARN")
                    if completion_cc_enabled:
                        try:
                            cc_recipient = fwd.Recipients.Add(effective_completion_cc)
                            try:
                                cc_recipient.Type = 2
                            except Exception:
                                pass
                            try:
                                fwd.Recipients.ResolveAll()
                            except Exception:
                                pass
                            log("FORWARD_CC_ADDED completion_cc_addr=set", "INFO")
                        except Exception as e:
                            log(f"FORWARD_CC_ADD_FAIL {e}", "WARN")
                    original_body = fwd.Body
                    
                    # Add risk warning if applicable
                    if risk_level in ("urgent", "critical"):
                        if not CONFIG.get("send_urgency_notifications", False):
                            log(f"URGENCY_NOTIFICATION_SUPPRESSED risk={risk_level}", "INFO")
                        elif risk_level == "critical":
                            raw_subject = msg.Subject or "(no subject)"
                            clean_subject = strip_bot_subject_tags(raw_subject)
                            try:
                                received_time = msg.ReceivedTime
                            except Exception:
                                received_time = None
                            received_str = received_time.strftime("%d %b %Y %H:%M") if received_time else "Unknown"
                            try:
                                sender_name = msg.SenderName or ""
                            except Exception:
                                sender_name = ""
                            try:
                                sender_email = msg.SenderEmailAddress or ""
                            except Exception:
                                sender_email = ""
                            orig_body = msg.Body or ""
                            max_chars = 12000
                            if len(orig_body) > max_chars:
                                orig_body = orig_body[:max_chars] + "\r\n...[truncated]"
                            if clean_subject in ("", "[C", "[CRITICAL]"):
                                extracted_subject = extract_subject_from_body(orig_body)
                                if extracted_subject:
                                    clean_subject = extracted_subject
                            body_text = (
                                "CRITICAL INCIDENT - ACTION REQUIRED\n\n"
                                f"Reason: {risk_reason}\n"
                                f"Assigned to: {assignee}\n\n"
                                f"Received: {received_str}\n"
                                f"Original subject: {clean_subject}\n"
                                f"From: {sender_name} {sender_email}\n\n"
                                "--- Original message ---\n"
                                f"{orig_body}\n"
                            )
                            fwd.BodyFormat = 1
                            fwd.Body = body_text + "\r\n"
                            fwd.Subject = subject_with_id
                        else:
                            banner_header = f"{risk_level.upper()} RISK TICKET"
                            risk_banner = (
                                "\u26A0" * 60 + "\n"
                                f"\U0001F6A8 {banner_header} \U0001F6A8\n"
                                f"Reason: {risk_reason}\n"
                                "\u26A0" * 60 + "\n\n"
                            )
                            fwd.Body = risk_banner + (original_body or "")
                        # Add to watchdog review register
                        add_to_watchdog(msg_id, subject, assignee, sender_email, risk_reason, overrides)
                    else:
                        if action_taken not in ("hib_noise_suppressed", "UNKNOWN_DOMAIN"):
                            fwd.Body = f"--- \U0001F3E5 AUTO-ASSIGNED TO {assignee} ---\n\n" + fwd.Body
                        if action_taken == "UNKNOWN_DOMAIN":
                            fwd.Body = (fwd.Body or "") + build_unknown_notice_block()
                            log("UNKNOWN_NOTICE_BLOCK_ADDED action=UNKNOWN_DOMAIN", "INFO")
                    
                    if action_taken != "manager_review":
                        fwd.Subject = subject_with_id
                    fwd.SentOnBehalfOfName = CONFIG["mailbox"]

                    try:
                        requester = sender_email.strip() if isinstance(sender_email, str) else ""
                        assignee_email = assignee if isinstance(assignee, str) else ""
                        staff_set = {s.lower() for s in staff_list}
                        if is_completion_subject(subject):
                            skip_reason = "completion_email"
                        elif assignee_email.lower() not in staff_set:
                            skip_reason = "assignee_not_staff"
                        elif not requester or "@" not in requester:
                            skip_reason = "requester_unavailable"
                        else:
                            skip_reason = ""
                        msg_id = getattr(msg, "EntryID", "") or getattr(msg, "ConversationID", "") or ""
                        if skip_reason:
                            log(f"COMPLETION_HOTLINK_SKIPPED reason={skip_reason} msg_id={msg_id}", "INFO")
                        else:
                            mode_out = []
                            injected = inject_completion_hotlink(
                                fwd,
                                requester,
                                subject_with_id,
                                SAMI_SHARED_INBOX,
                                mode_out,
                                original_msg=msg,
                            )
                            if injected:
                                mode = mode_out[0] if mode_out else "HTML"
                                log(f"COMPLETION_HOTLINK_ADDED mode={mode} msg_id={msg_id}", "INFO")
                            else:
                                log(f"COMPLETION_HOTLINK_SKIPPED reason=inject_failed msg_id={msg_id}", "WARN")
                    except Exception:
                        log("COMPLETION_HOTLINK_FAIL", "WARN")

                    # MAILBOX STORE GUARD (forward)
                    _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                    if not _sb_ok:
                        log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                        append_stats(subject, "skipped", sender_email, risk_level, domain_bucket, "WRONG_MAILBOX", policy_source)
                    else:
                        # SAFE_MODE enforcement
                        is_safe, safe_reason = is_safe_mode()
                        if is_safe:
                            log(f"SAFE_MODE_SUPPRESS_SEND action={action_taken} bucket={domain_bucket} assignee={assignee} reason={safe_reason}", "WARN")
                        else:
                            fwd.Send()

                    if risk_level == "critical":
                        updated_ledger = mark_processed(message_key, "critical_forwarded", processed_ledger)
                        if updated_ledger is not None:
                            processed_ledger = updated_ledger
                    
                    if action_taken != "hib_noise_suppressed":
                        log(f"ASSIGNED msg_id={msg_id} risk={risk_level}", "INFO")

                    # Archive original (no subject mutation per constraints)
                    # Set event_type=ASSIGNED when assignee is staff (contains @ and not system/bot)
                    _non_staff = {"bot", "completed", "error", "hib", "hold", "manager_review",
                                  "non_actionable", "quarantined", "skipped", "system_notification"}
                    a_norm = (assignee or "").strip().lower()
                    evt_type = "ASSIGNED" if ("@" in a_norm and a_norm not in _non_staff) else ""
                    append_stats(subject, assignee, sender_email, risk_level, domain_bucket, action_taken, policy_source, event_type=evt_type, msg_key=message_key)
                    msg.UnRead = False
                    _sb_ok2, _sb_actual2 = check_msg_mailbox_store(msg, target_store)
                    if not _sb_ok2:
                        log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual2}", "WARN")
                        append_stats(subject, "skipped", sender_email, risk_level, domain_bucket, "WRONG_MAILBOX", policy_source)
                    else:
                        msg.Move(processed)
                    processed_count += 1

                except Exception as e:
                    stack = "".join(traceback.format_tb(e.__traceback__))
                    log(f"Error processing email: exc_type={type(e).__name__} stack={stack}", "ERROR")
                    append_stats(subject, "error", sender_email, "PROCESSING_ERROR", domain_bucket if 'domain_bucket' in locals() else "", "PROCESSING_ERROR", policy_source if 'policy_source' in locals() else "")
                    errors_count += 1
                    poison_counts = load_poison_counts() or {}
                    poison_count = poison_counts.get(message_key, 0) + 1
                    poison_counts[message_key] = poison_count
                    if not save_poison_counts(poison_counts):
                        log("STATE_WRITE_FAIL state=poison_counts", "ERROR")
                    if poison_count >= 3:
                        log(f"QUARANTINE_TRIGGER key={message_key} count={poison_count}", "ERROR")
                        if quarantine:
                            try:
                                msg.UnRead = False
                                _sb_ok, _sb_actual = check_msg_mailbox_store(msg, target_store)
                                if not _sb_ok:
                                    log(f"WRONG_MAILBOX expected={target_store} actual={_sb_actual}", "WARN")
                                    append_stats(subject, "skipped", sender_email, "QUARANTINED", domain_bucket if 'domain_bucket' in locals() else "", "WRONG_MAILBOX", policy_source if 'policy_source' in locals() else "")
                                    continue
                                msg.Move(quarantine)
                                append_stats(subject, "quarantined", sender_email, "QUARANTINED", domain_bucket if 'domain_bucket' in locals() else "", "QUARANTINED", policy_source if 'policy_source' in locals() else "")
                                processed_count += 1
                                continue
                            except Exception as qe:
                                log(f"QUARANTINE_FAILED key={message_key} error={qe}", "ERROR")
                        else:
                            log(f"QUARANTINE_FAILED key={message_key} reason=folder_not_found", "ERROR")
                    continue  # Don't crash - continue to next email
            
        except Exception as e:
            log(f"Outlook connection error: {e}", "ERROR")
            errors_count += 1
            # Don't crash - will retry next cycle
    finally:
        _staff_list_cache = None
        _safe_mode_cache = None
        _safe_mode_inbox = None
        _live_test_override = False
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        log(
            f"TICK_END tick_id={tick_id} scanned={scanned_count} candidates_unread={candidates_unread_count} "
            f"processed={processed_count} skipped={skipped_count} errors={errors_count} duration_ms={duration_ms}",
            "INFO"
        )

def run_job():
    """Main job: Process inbox"""
    try:
        process_inbox()
    except Exception as e:
        log(f"Error in process_inbox: {e}", "ERROR")

# ==================== MAIN ENTRY POINT ====================
if __name__ == "__main__":
    if not acquire_lock():
        sys.exit(0)
    atexit.register(release_lock)
    log("=" * 60)
    log("🏥 Helpdesk Clinical Safety Bot v2.2")
    log("=" * 60)
    overrides = load_settings_overrides(SETTINGS_OVERRIDES_PATH)
    manager_override = get_override_addr(overrides, "manager_cc_addr")
    apps_override = get_override_addr(overrides, "apps_cc_addr")
    log("Mailbox: (configured)")
    log(f"Manager: ({'override set' if manager_override else 'not set'})")
    log(f"Apps CC: ({'override set' if apps_override else 'not set'})")
    log(f"SLA Limit (review-only): {CONFIG['sla_minutes']} minutes")
    log(f"Staff loaded: {len(get_staff_list())} members")
    log("=" * 60)

    # Initialize watchdog file if needed
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED_SKIP", "INFO")
    elif not os.path.exists(FILES["watchdog"]):
        save_watchdog({}, overrides)
        log("Initialized empty watchdog file")

    # Rotate daily_stats.csv to new schema if needed
    maybe_rotate_daily_stats_to_new_schema()

    # Run immediately
    try:
        run_job()
    except KeyboardInterrupt:
        log("Bot stopped by user", "INFO")
        sys.exit(0)

    # Schedule to run every minute
    schedule.every(CONFIG["check_interval_seconds"]).seconds.do(run_job)
    
    log("🔄 Entering main loop (Ctrl+C to stop)")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            log("Bot stopped by user", "INFO")
            break
        except Exception as e:
            log(f"Unexpected error in main loop: {e}", "ERROR")
            time.sleep(5)  # Wait before retry
