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
import re
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
    "quarantine_folder": "Transfer Bot Quarantine"
}

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
COMPLETION_CC_ADDR = "completion.placeholder@example.invalid"
SAMI_SHARED_INBOX = "health.samisupportteam@sa.gov.au"
COMPLETION_SUBJECT_KEYWORD = "[COMPLETED]"
HEARTBEAT_INTERVAL_SECONDS = 300
_staff_list_cache = None
_safe_mode_cache = None
_safe_mode_inbox = None
_live_test_override = False

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
    return bool(overrides.get("disable_urgent_watchdog", False))

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
        test_ok = os.environ.get("TRANSFER_BOT_LIVE_TEST_OK", "").strip().lower()
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
            log(f"SAFE_MODE_ACTIVE reason=test_folder inbox_folder={inbox_value}", "WARN")
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

def build_completion_mailto(to_addr, cc_addr, subject):
    to_value = str(to_addr).strip() if to_addr else ""
    if not to_value or "@" not in to_value:
        return ""
    cc_value = str(cc_addr).strip() if cc_addr else ""
    subject_value = "" if subject is None else str(subject)
    params = []
    if cc_value:
        params.append(f"cc={quote(cc_value)}")
    params.append(f"subject={quote(subject_value)}")
    return f"mailto:{to_value}?{'&'.join(params)}"

def prepend_completion_hotlink_html(html, mailto_url):
    html_notice = (
        '<p><b>Mark job complete:</b> '
        f'<a href="{mailto_url}">Click to notify requester (CC SAMI)</a></p>'
        "<hr/>"
    )
    return html_notice + (html or "")

def build_completion_mailto_url(to_email, cc_email, subject):
    to_value = str(to_email).strip() if to_email else ""
    if not to_value or "@" not in to_value:
        return ""
    cc_value = str(cc_email).strip() if cc_email else ""
    subject_value = "" if subject is None else str(subject)
    params = []
    if cc_value:
        params.append(f"cc={quote(cc_value)}")
    params.append(f"subject={quote(subject_value)}")
    return f"mailto:{to_value}?{'&'.join(params)}"

def inject_completion_hotlink(fwd, original_sender_email, original_subject, sami_inbox, mode_out=None):
    if fwd is None:
        return False
    mailto_subject = f"{COMPLETION_SUBJECT_KEYWORD} {original_subject}".strip()
    mailto_url = build_completion_mailto_url(original_sender_email, sami_inbox, mailto_subject)
    if not mailto_url:
        return False
    html_notice = (
        '<p><b>Mark job complete:</b> '
        f'<a href="{mailto_url}">Click to notify requester (CC SAMI)</a></p>'
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

# ==================== FILE OPERATIONS ====================
def get_staff_list():
    """Load staff list from file"""
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
        log(f"STAFF_LOADED members={len(staff)} raw_lines={len(raw_lines)} path={staff_path}", "INFO")
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

def classify_sender_domain(domain, policy):
    """
    Classify sender domain based on policy.

    Returns: external_image_request | system_notification | internal | hold | unknown
    """
    if not domain:
        return "unknown"

    domain_lower = domain.lower().strip()

    # Check external image request domains (Class 1)
    external_image_domains = [d.lower().strip() for d in policy.get("external_image_request_domains", [])]
    if domain_lower in external_image_domains:
        return "external_image_request"

    # Check system notification domains (Class 3)
    system_notification_domains = [d.lower().strip() for d in policy.get("system_notification_domains", [])]
    if domain_lower in system_notification_domains:
        return "system_notification"

    # Check internal domains
    internal_domains = [d.lower().strip() for d in policy.get("internal_domains", [])]
    if domain_lower in internal_domains:
        return "internal"

    # Check always hold domains
    hold_domains = [d.lower().strip() for d in policy.get("always_hold_domains", [])]
    if domain_lower in hold_domains:
        return "hold"

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
        mail.To = manager_email
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

def append_stats(subject, assigned_to, sender="unknown", risk_level="normal", domain_bucket="", action="", policy_source=""):
    """Append entry to daily stats CSV (backward compatible with old 6-col schema)"""
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
                # New file: write new 9-column header
                writer.writerow([
                    'Date', 'Time', 'Subject', 'Assigned To', 'Sender', 'Risk Level',
                    'Domain Bucket', 'Action', 'Policy Source'
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
                # New schema: write all 9 fields
                writer.writerow([
                    now.strftime('%Y-%m-%d'),
                    now.strftime('%H:%M:%S'),
                    subject,
                    assigned_to,
                    sender,
                    risk_level,
                    domain_bucket,
                    action,
                    policy_source
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
    effective_config = CONFIG.copy()
    overrides = load_settings_overrides(SETTINGS_OVERRIDES_PATH)
    if overrides:
        effective_config.update(overrides)
        applied_keys = [k for k, v in overrides.items() if v != CONFIG.get(k)]
        if applied_keys:
            log(f"OVERRIDE_APPLIED keys={','.join(sorted(applied_keys))}", "INFO")
    if is_urgent_watchdog_disabled(overrides):
        log("URGENT_WATCHDOG_DISABLED", "INFO")

    # Load domain policy
    domain_policy, policy_valid = load_domain_policy()
    policy_source = "valid" if policy_valid else "invalid_fallback"
    known_domains = get_known_domains(domain_policy) if policy_valid else set()
    allowlist_valid = bool(known_domains)
    if not allowlist_valid:
        log("ALLOWLIST_INVALID_FAILSAFE", "ERROR")
    hib_noise_rule = domain_policy.get("hib_noise") if isinstance(domain_policy.get("hib_noise"), dict) else {}

    # Extract domain routing settings from overrides
    apps_cc_addr = get_override_addr(overrides, "apps_cc_addr")
    manager_cc_addr = get_override_addr(overrides, "manager_cc_addr")
    if isinstance(apps_cc_addr, str) and ";" in apps_cc_addr:
        apps_cc_count = len([part for part in (p.strip() for p in apps_cc_addr.split(";")) if part])
        log(f"CC_MULTI_RECIPIENTS key=apps_cc_addr count={apps_cc_count}", "INFO")
    if isinstance(manager_cc_addr, str) and ";" in manager_cc_addr:
        manager_cc_count = len([part for part in (p.strip() for p in manager_cc_addr.split(";")) if part])
        log(f"CC_MULTI_RECIPIENTS key=manager_cc_addr count={manager_cc_count}", "INFO")
    apps_cc_list = [part for part in (p.strip() for p in (apps_cc_addr or "").split(";")) if part]
    unknown_domain_mode = overrides.get("unknown_domain_mode", "hold_manager")
    completion_workflow_enabled = CONFIG.get("enable_completion_workflow", False)
    completion_cc_enabled = completion_workflow_enabled and CONFIG.get("enable_completion_cc", True)
    effective_completion_cc = overrides.get("completion_cc_addr", COMPLETION_CC_ADDR) if overrides else COMPLETION_CC_ADDR
    if overrides and overrides.get("completion_cc_addr") and effective_completion_cc != COMPLETION_CC_ADDR:
        log("OVERRIDE_APPLIED key=completion_cc_addr", "INFO")
    global _safe_mode_cache, _safe_mode_inbox, _live_test_override
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
                try:
                    # Extract email details
                    try:
                        sender_email = msg.SenderEmailAddress.lower()
                    except:
                        sender_email = "unknown"

                    # Extract and classify sender domain
                    sender_domain = extract_sender_domain(sender_email)
                    domain_bucket = classify_sender_domain(sender_domain, domain_policy) if sender_domain else "unknown"

                    try:
                        subject = msg.Subject.strip()
                    except:
                        subject = ""
                    
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
                        log("HIB_NOISE_MATCH action=hib_noise_suppressed", "INFO")
                        notify_apps = bool(hib_noise_rule.get("notify_apps", False))
                        notify_manager = bool(hib_noise_rule.get("notify_manager", False))
                        cc_value = ""
                        if notify_apps and isinstance(apps_cc_addr, str) and apps_cc_addr.strip():
                            cc_value = apps_cc_addr
                        if notify_manager and isinstance(manager_cc_addr, str) and manager_cc_addr.strip():
                            cc_value = f"{cc_value};{manager_cc_addr}" if cc_value else manager_cc_addr
                        hib_cc_override = cc_value

                    # ===== COMPLETION DETECTION =====
                    try:
                        is_staff_sender = sender_email in staff_list
                        keyword_hit = is_completion_subject(subject)
                        if is_staff_sender and keyword_hit:
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
                                    log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                                    log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                                    return
                                msg.UnRead = False
                                msg.Move(processed)
                                processed_count += 1
                                continue
                        is_reply = subject.lower().strip().startswith("re:")
                        if completion_cc_enabled and is_staff_sender and is_reply and message_has_completion_cc(msg, effective_completion_cc):
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
                                append_stats(subject, "completed", sender_email, "COMPLETION_MATCHED", domain_bucket, "COMPLETION_MATCHED", policy_source)
                            else:
                                append_stats(subject, "completed", sender_email, "COMPLETION_UNMATCHED", domain_bucket, "COMPLETION_UNMATCHED", policy_source)
                            if not save_processed_ledger(processed_ledger):
                                log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                                log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                                return
                            msg.UnRead = False
                            msg.Move(processed)
                            processed_count += 1
                            continue
                    except Exception as e:
                        log(f"COMPLETION_ERROR {e}", "ERROR")
                        append_stats(subject, "completed", sender_email, "COMPLETION_ERROR", domain_bucket, "COMPLETION_ERROR", policy_source)
                        try:
                            msg.UnRead = False
                            msg.Move(processed)
                        except Exception:
                            pass
                        processed_count += 1
                        continue
                    
                    # ===== SMART FILTER =====
                    if is_internal_reply(sender_email, subject, staff_list):
                        msg_id = getattr(msg, "EntryID", "") or getattr(msg, "ConversationID", "") or ""
                        log(f"SMART_FILTER_SKIP msg_id={msg_id}", "INFO")
                        append_stats(subject, "completed", sender_email, "normal", domain_bucket, "SMART_FILTER_COMPLETION", policy_source)
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
                            log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                            log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                            return
                        msg.UnRead = False
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
                            log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                            log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                            return
                        try:
                            msg.UnRead = False
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
                    if hib_noise_match:
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
                        # Class 3: System notifications - To apps, CC manager, no assignment
                        action_taken = "SYSTEM_NOTIFICATION"
                        assignee = "system_notification"
                        cc_manager = True
                        cc_apps = False
                        # Apps specialists as To recipients
                        for apps_email in policy_apps_specialists:
                            hold_recipients.append(apps_email)
                        # Manager will be added as CC via cc_manager flag
                        log(f"SYSTEM_NOTIFICATION domain={sender_domain} to_apps={len(policy_apps_specialists)} cc_manager=True", "WARN")

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

                    # Handle SAMI completion early
                    if is_completion:
                        append_stats(subject, "completed", sender_email, "normal", domain_bucket, action_taken, policy_source)
                        msg.UnRead = False
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
                            log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                            log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                            return
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
                    if identity.get("entry_id"):
                        processed_ledger[message_key]["entry_id"] = identity.get("entry_id")
                    if identity.get("store_id"):
                        processed_ledger[message_key]["store_id"] = identity.get("store_id")
                    if identity.get("internet_message_id"):
                        processed_ledger[message_key]["internet_message_id"] = identity.get("internet_message_id")
                    if conversation_id:
                        processed_ledger[message_key]["conversation_id"] = conversation_id
                    if not save_processed_ledger(processed_ledger):
                        log("STATE_WRITE_FAIL_SKIP state=processed_ledger", "ERROR")
                        log(f"TICK_SKIP tick_id={tick_id} reason=STATE_WRITE_FAIL", "ERROR")
                        return

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
                            fwd.Subject = f"CRITICAL | {clean_subject}"
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
                                subject,
                                SAMI_SHARED_INBOX,
                                mode_out
                            )
                            if injected:
                                mode = mode_out[0] if mode_out else "HTML"
                                log(f"COMPLETION_HOTLINK_ADDED mode={mode} msg_id={msg_id}", "INFO")
                            else:
                                log(f"COMPLETION_HOTLINK_SKIPPED reason=inject_failed msg_id={msg_id}", "WARN")
                    except Exception:
                        log("COMPLETION_HOTLINK_FAIL", "WARN")

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
                    append_stats(subject, assignee, sender_email, risk_level, domain_bucket, action_taken, policy_source)
                    msg.UnRead = False
                    msg.Move(processed)
                    processed_count += 1
                    
                except Exception as e:
                    log(f"Error processing email: {e}", "ERROR")
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
    run_job()
    
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
