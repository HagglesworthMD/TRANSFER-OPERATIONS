import csv
import hashlib
import os
import secrets
import sys
from datetime import datetime
from pathlib import Path

import pythoncom
import win32com.client

INCLUDE_RAW_SUBJECT = False
LOG_EVERY_N = 500
LOG_EVERY_N_SEEN = 200
ENABLE_SORT = False
ENABLE_ATTACHMENTS_COUNT = False
MAX_FOLDERS = 0
MAX_ITEMS_PER_FOLDER = 0

KEYWORDS = [
    "stat",
    "asap",
    "urgent",
    "emergency",
    "critical",
    "immediate",
    "now",
    "rush",
    "priority",
    "life-threatening",
    "code",
]

SALT_FILE = Path("data/snapshot_salt.txt")
OUTPUT_DIR = Path("data/snapshots")


def _hash_with_salt(value, salt):
    if not value:
        return ""
    payload = f"{value}{salt}".encode("utf-8", "ignore")
    return hashlib.sha256(payload).hexdigest()


def _ensure_salt():
    if SALT_FILE.exists():
        try:
            content = SALT_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            content = ""
        if content:
            return content
    SALT_FILE.parent.mkdir(parents=True, exist_ok=True)
    salt = secrets.token_hex(32)
    tmp_path = SALT_FILE.with_suffix(".snapshot.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(salt)
    os.replace(tmp_path, SALT_FILE)
    return salt


def _extract_domain(address):
    if not address or "@" not in address:
        return ""
    return address.split("@", 1)[1].lower()


def _sender_domain(item):
    try:
        email_type = getattr(item, "SenderEmailType", "") or ""
        if email_type.lower() in ("ex", "exchange"):
            sender = getattr(item, "Sender", None)
            if sender is not None:
                ex_user = sender.GetExchangeUser()
                if ex_user is not None:
                    return _extract_domain(ex_user.PrimarySmtpAddress or "")
            return ""
        address = getattr(item, "SenderEmailAddress", "") or ""
        return _extract_domain(address)
    except Exception:
        return ""


def _is_mail_item(item):
    try:
        if getattr(item, "Class", None) == 43:
            return True
    except Exception:
        return False
    try:
        message_class = getattr(item, "MessageClass", "") or ""
        return isinstance(message_class, str) and message_class.startswith("IPM.Note")
    except Exception:
        return False


def _safe_iso(dt_value):
    if not dt_value:
        return ""
    try:
        return dt_value.isoformat()
    except Exception:
        return ""


def _iter_folders(root_folder, root_path):
    yield root_folder, root_path
    try:
        subfolders = list(root_folder.Folders)
    except Exception:
        subfolders = []
    for subfolder in subfolders:
        name = getattr(subfolder, "Name", "") or ""
        if not name:
            continue
        path = f"{root_path}\\{name}"
        yield from _iter_folders(subfolder, path)


def main():
    pythoncom.CoInitialize()
    try:
        salt = _ensure_salt()

        print("START connect_outlook")
        try:
            app = win32com.client.Dispatch("Outlook.Application")
            ns = app.GetNamespace("MAPI")
        except Exception as exc:
            print(f"Failed to connect to Outlook: {exc}")
            return 1
        print("OK connect_outlook")

        print("START resolve_mailbox")
        recip = ns.CreateRecipient("health.samisupportteam@sa.gov.au")
        if not recip.Resolve():
            print("Could not resolve shared mailbox: health.samisupportteam@sa.gov.au")
            return 1
        print("OK resolve_mailbox")

        try:
            inbox = ns.GetSharedDefaultFolder(recip, 6)
        except Exception as exc:
            print(f"Failed to access shared Inbox: {exc}")
            return 1
        print("OK get_inbox")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        filename = f"outlook_snapshot_health.samisupportteam_FULL_{timestamp}.csv"
        output_path = OUTPUT_DIR / filename
        tmp_path = output_path.with_suffix(".csv.tmp")

        keyword_fields = [f"keyword_hit_{kw}" for kw in KEYWORDS]
        columns = [
            "folder_path",
            "received_time",
            "message_class",
            "unread",
            "importance",
            "flag_status",
            "has_attachments",
            "attachments_count",
            "sender_domain",
            "subject_len",
            "subject_hash",
        ]
        if INCLUDE_RAW_SUBJECT:
            columns.append("subject_raw")
        columns += keyword_fields
        columns += [
            "conversation_id_hash",
            "internet_message_id_hash",
            "categories_present",
        ]

        folders_scanned = 0
        items_seen = 0
        mailitems_processed = 0
        skipped_nonmail = 0
        errors = 0
        attachments_errors = 0
        folder_counts = {}

        write_success = False
        first_row_written = False
        try:
            with open(tmp_path, "w", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=columns)
                writer.writeheader()
                csv_file.flush()
                try:
                    os.fsync(csv_file.fileno())
                except Exception:
                    pass

                for folder, folder_path in _iter_folders(inbox, "Inbox"):
                    if MAX_FOLDERS and folders_scanned >= MAX_FOLDERS:
                        break
                    folders_scanned += 1
                    print(f"FOLDER_START path={folder_path}")
                    folder_processed = 0
                    folder_errors = 0
                    try:
                        items = folder.Items
                    except Exception:
                        print(f"FOLDER_ITEMS path={folder_path} count=0")
                        print(
                            f"FOLDER_DONE path={folder_path} processed=0 errors={folder_errors}"
                        )
                        continue
                    if ENABLE_SORT:
                        try:
                            items.Sort("[ReceivedTime]", True)
                        except Exception:
                            pass
                    try:
                        item_count = int(items.Count)
                    except Exception:
                        item_count = 0
                    print(f"FOLDER_ITEMS path={folder_path} count={item_count}")
                    if MAX_ITEMS_PER_FOLDER and item_count > MAX_ITEMS_PER_FOLDER:
                        stop_index = item_count - MAX_ITEMS_PER_FOLDER + 1
                    else:
                        stop_index = 1
                    for i in range(item_count, stop_index - 1, -1):
                        try:
                            item = items.Item(i)
                        except Exception:
                            errors += 1
                            folder_errors += 1
                            continue
                        items_seen += 1
                        if LOG_EVERY_N_SEEN and items_seen % LOG_EVERY_N_SEEN == 0:
                            print(
                                f"PROGRESS folder={folder_path} i={i}/{item_count} "
                                f"seen={items_seen} processed={mailitems_processed} "
                                f"errors={errors}"
                            )
                        try:
                            if item is None or not _is_mail_item(item):
                                skipped_nonmail += 1
                                continue

                            subject = getattr(item, "Subject", "") or ""
                            subject_lower = subject.lower()
                            subject_len = len(subject)

                            attachments_count = ""
                            try:
                                has_attachments = (
                                    1 if getattr(item, "HasAttachments", False) else 0
                                )
                            except Exception:
                                has_attachments = 0
                            if ENABLE_ATTACHMENTS_COUNT and has_attachments == 1:
                                try:
                                    attachments_count = int(item.Attachments.Count)
                                except Exception:
                                    attachments_count = ""
                                    attachments_errors += 1

                            flag_status = ""
                            try:
                                flag_status = int(getattr(item, "FlagStatus"))
                            except Exception:
                                flag_status = ""

                            categories_present = 0
                            try:
                                categories = getattr(item, "Categories", "") or ""
                                categories_present = 1 if categories else 0
                            except Exception:
                                categories_present = 0

                            row = {
                                "folder_path": folder_path,
                                "received_time": _safe_iso(
                                    getattr(item, "ReceivedTime", None)
                                ),
                                "message_class": getattr(item, "MessageClass", "") or "",
                                "unread": 1 if getattr(item, "UnRead", False) else 0,
                                "importance": int(getattr(item, "Importance", 0) or 0),
                                "flag_status": flag_status,
                                "has_attachments": has_attachments,
                                "attachments_count": attachments_count,
                                "sender_domain": _sender_domain(item),
                                "subject_len": subject_len,
                                "subject_hash": _hash_with_salt(subject, salt),
                                "conversation_id_hash": _hash_with_salt(
                                    getattr(item, "ConversationID", "") or "", salt
                                ),
                                "internet_message_id_hash": _hash_with_salt(
                                    getattr(item, "InternetMessageID", "") or "", salt
                                ),
                                "categories_present": categories_present,
                            }
                            if INCLUDE_RAW_SUBJECT:
                                row["subject_raw"] = subject

                            for kw in KEYWORDS:
                                row[f"keyword_hit_{kw}"] = subject_lower.count(kw)

                            writer.writerow(row)
                            mailitems_processed += 1
                            folder_processed += 1
                            if not first_row_written:
                                first_row_written = True
                                print(f"FIRST_ROW_WRITTEN tmp={tmp_path.name}")
                            folder_counts[folder_path] = (
                                folder_counts.get(folder_path, 0) + 1
                            )
                            if LOG_EVERY_N and mailitems_processed % LOG_EVERY_N == 0:
                                print(
                                    "PROGRESS processed="
                                    f"{mailitems_processed} seen={items_seen} "
                                    f"errors={errors} folder={folder_path}"
                                )
                        except Exception:
                            errors += 1
                            folder_errors += 1
                            continue
                    print(
                        f"FOLDER_DONE path={folder_path} processed={folder_processed} "
                        f"errors={folder_errors}"
                    )
            write_success = True
        finally:
            if write_success:
                os.replace(tmp_path, output_path)
            else:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass

        print(f"folders_scanned: {folders_scanned}")
        print(f"items_seen: {items_seen}")
        print(f"mailitems_processed: {mailitems_processed}")
        print(f"skipped_nonmail: {skipped_nonmail}")
        print(f"errors: {errors}")
        print(f"attachments_errors: {attachments_errors}")
        print("Top 10 folders by item count:")
        for folder_path, count in sorted(
            folder_counts.items(), key=lambda kv: kv[1], reverse=True
        )[:10]:
            print(f"  {folder_path} -> {count}")

        return 0
    finally:
        pythoncom.CoUninitialize()


if __name__ == "__main__":
    # Verification steps:
    # 1) python tools\outlook_snapshot.py
    # 2) git status
    # 3) inspect CSV header (confirm no forbidden fields)
    sys.exit(main())
