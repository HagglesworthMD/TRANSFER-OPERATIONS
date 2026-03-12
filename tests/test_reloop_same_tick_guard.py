import copy
import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import distributor


STAT_KEYS = (
    "subject",
    "assigned_to",
    "sender",
    "risk_level",
    "domain_bucket",
    "action",
    "policy_source",
    "event_type",
    "msg_key",
    "status_after",
    "assigned_ts",
    "completed_ts",
    "duration_sec",
    "sami_id",
)


class DummyRecipient:
    def __init__(self, value):
        self.Name = value
        self.Address = value
        self.Type = 1
        self.Resolved = True


class DummyRecipients:
    def __init__(self):
        self._items = []

    def Add(self, value):
        recipient = DummyRecipient(value)
        self._items.append(recipient)
        return recipient

    def ResolveAll(self):
        return True

    def __iter__(self):
        return iter(self._items)


class DummyForward:
    def __init__(self, observed, source_message):
        self._observed = observed
        self.source_message = source_message
        self.Recipients = DummyRecipients()
        self.CC = ""
        self.Body = source_message.Body or ""
        self.HTMLBody = source_message.HTMLBody or ""
        self.Subject = source_message.Subject or ""
        self.BodyFormat = None
        self.SentOnBehalfOfName = None

    def Send(self):
        self._observed["send_calls"] += 1


class DummyStore:
    def __init__(self, display_name="UnitTest Mailbox", store_id="STORE-1"):
        self.DisplayName = display_name
        self.StoreID = store_id


class DummyFolder:
    def __init__(self, name, path=None, parent=None, store=None):
        self.Name = name
        self.Parent = parent
        self.Store = store if store is not None else (parent.Store if parent is not None else DummyStore())
        self.FolderPath = path or name
        self.Items = DummyItems([])


class DummyItems:
    def __init__(self, items):
        self._items = list(items)

    @property
    def Count(self):
        return len(self._items)

    def Restrict(self, query):
        if query == "[UnRead] = True":
            return DummyItems([item for item in self._items if getattr(item, "UnRead", False)])
        if "[UnRead] = False" in query:
            return DummyItems([item for item in self._items if not getattr(item, "UnRead", False)])
        return DummyItems(self._items)

    def Sort(self, *_args, **_kwargs):
        return None

    def Item(self, index):
        return self._items[index - 1]

    def __iter__(self):
        return iter(self._items)


class DummyMessage:
    def __init__(self, sender, subject, parent, conversation_id="CONV-1", entry_id="ENTRY-1", body="Body"):
        self.Subject = subject
        self.Body = body
        self.HTMLBody = body
        self.Importance = 1
        self.UnRead = True
        self.ReceivedTime = distributor.datetime(2026, 3, 4, 8, 0, 0)
        self.ConversationID = conversation_id
        self.EntryID = entry_id
        self.StoreID = parent.Store.StoreID
        self.InternetMessageID = f"<{entry_id}@unit.test>"
        self.SenderEmailType = ""
        self.SenderEmailAddress = sender
        self.SenderName = sender
        self.MessageClass = "IPM.Note"
        self.Parent = parent
        self.forwards = []
        self.moved_to = None

    def Forward(self):
        forward = DummyForward(self.Parent._observed, self)
        self.forwards.append(forward)
        self.Parent._observed["forwards"].append(forward)
        return forward

    def Move(self, folder):
        self.moved_to = folder
        self.Parent._observed["moves"].append(folder.Name)
        self.UnRead = False
        return self


class DummyNamespace:
    def __init__(self, mailbox):
        self.mailbox = mailbox

    def GetDefaultFolder(self, _):
        return self.mailbox


class DummyOutlookApp:
    def __init__(self, namespace):
        self._namespace = namespace

    def GetNamespace(self, _):
        return self._namespace


class ReloopSameTickGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = json.loads(Path("system_buckets.json").read_text(encoding="utf-8"))
        parsed, err = distributor._parse_system_buckets_json(data)
        if err:
            raise AssertionError(f"system_buckets.json parse failed: {err}")
        cls.base_buckets = parsed

    def _stats_recorder(self, observed):
        def _record(*args, **kwargs):
            record = {key: "" for key in STAT_KEYS}
            for key, value in zip(STAT_KEYS, args):
                record[key] = value
            record.update(kwargs)
            observed["stats"].append(record)
        return _record

    def _base_domain_policy(self):
        return {
            "internal_domains": ["sa.gov.au"],
            "vendor_domains": [],
            "external_image_request_domains": [],
            "system_notification_domains": [],
            "quarantine_domains": [],
            "always_hold_domains": [],
            "hib_noise": {},
        }

    def _build_hot_cfg(self, buckets):
        return {
            "staff": {
                "staff": ["staff.one@sa.gov.au"],
                "off_rotation": [],
                "leave": [],
            },
            "apps_team": {"recipients": ["apps.team@sa.gov.au"]},
            "manager_config": {"recipients": ["manager.review@sa.gov.au"]},
            "system_buckets": buckets,
        }

    def _make_folders(self, observed):
        store = DummyStore()
        mailbox = DummyFolder("Mailbox", path="Mailbox", store=store)
        inbox = DummyFolder("Transfer Bot Test Received", path="Mailbox\\Transfer Bot Test Received", parent=mailbox)
        processed = DummyFolder("Transfer Bot Test", path="Mailbox\\Transfer Bot Test", parent=mailbox)
        quarantine = DummyFolder("03_QUARANTINE", path="Mailbox\\Inbox\\03_QUARANTINE", parent=mailbox)
        hib = DummyFolder("04_HIB", path="Mailbox\\Inbox\\04_HIB", parent=mailbox)
        system_notification = DummyFolder("05_SYSTEM_NOTIFICATIONS", path="Mailbox\\Inbox\\05_SYSTEM_NOTIFICATIONS", parent=mailbox)
        completed = DummyFolder("01_COMPLETED", path="Mailbox\\Inbox\\01_COMPLETED", parent=mailbox)
        for folder in (mailbox, inbox, processed, quarantine, hib, system_notification, completed):
            folder._observed = observed
        return {
            "mailbox": mailbox,
            "inbox": inbox,
            "processed": processed,
            "quarantine": quarantine,
            "hib": hib,
            "system_notification": system_notification,
            "completed": completed,
        }

    def _resolve_folder_factory(self, folders):
        def _resolve_folder(_root, path):
            mapping = {
                distributor.CONFIG["inbox_folder"]: folders["inbox"],
                distributor.CONFIG["processed_folder"]: folders["processed"],
                "Inbox/03_QUARANTINE": folders["quarantine"],
                "Inbox/04_HIB": folders["hib"],
                "Inbox/05_SYSTEM_NOTIFICATIONS": folders["system_notification"],
                "Inbox/01_COMPLETED": folders["completed"],
                distributor.JIRA_FOLLOW_UP_FOLDER_PATH: folders["processed"],
            }
            return mapping.get(path), "unit_test"
        return _resolve_folder

    def _run_single_message(self, stale_age_seconds):
        observed = {
            "stats": [],
            "forwards": [],
            "send_calls": 0,
            "moves": [],
            "saved_ledgers": [],
            "get_next_staff_calls": 0,
        }
        buckets = copy.deepcopy(self.base_buckets)
        hot_cfg = self._build_hot_cfg(buckets)
        folders = self._make_folders(observed)
        mailbox = folders["mailbox"]
        inbox = folders["inbox"]
        message = DummyMessage("someone@bensonradiology.com.au", "Image transfer request", inbox)
        inbox.Items = DummyItems([message])
        namespace = DummyNamespace(mailbox)
        outlook = DummyOutlookApp(namespace)
        win32_stub = SimpleNamespace(client=SimpleNamespace(Dispatch=lambda _name: outlook))
        message_key, _identity = distributor.compute_message_identity(
            message,
            message.SenderEmailAddress,
            message.Subject,
            message.ReceivedTime.isoformat(),
        )
        message_sami = distributor.compute_sami_id(message)
        ledger_data = {
            message_key: {
                "assigned_to": "",
                "risk": "normal",
                "sami_id": message_sami,
                "ts": (datetime.now() - timedelta(hours=13)).isoformat(),
                "stale_last_reloop_at": (datetime.now() - timedelta(seconds=stale_age_seconds)).isoformat(),
                "stale_reloop_count": 1,
                "entry_id": message.EntryID,
                "store_id": message.StoreID,
            }
        }

        def _save_processed_ledger(data):
            observed["saved_ledgers"].append(copy.deepcopy(data))
            return True

        def _get_next_staff():
            observed["get_next_staff_calls"] += 1
            return "staff.one@sa.gov.au"

        patches = [
            patch.object(distributor, "OUTLOOK_AVAILABLE", True),
            patch.object(distributor, "win32com", win32_stub, create=True),
            patch.object(distributor, "find_mailbox_root_robust", return_value=mailbox),
            patch.object(distributor, "resolve_folder", side_effect=self._resolve_folder_factory(folders)),
            patch.object(distributor, "get_or_create_subfolder", side_effect=lambda _parent, path: folders["quarantine"] if "03_QUARANTINE" in path else folders["hib"]),
            patch.object(distributor, "get_folder_path_safe", side_effect=lambda folder: folder.FolderPath if folder else ""),
            patch.object(distributor, "check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox")),
            patch.object(distributor, "load_settings_overrides", return_value={}),
            patch.object(distributor, "load_config_files_each_tick", return_value=(hot_cfg, [])),
            patch.object(distributor, "load_domain_policy", return_value=(self._base_domain_policy(), True)),
            patch.object(distributor, "get_staff_list", return_value=["staff.one@sa.gov.au"]),
            patch.object(distributor, "ensure_processed_ledger_exists", return_value=True),
            patch.object(distributor, "load_processed_ledger", return_value=ledger_data),
            patch.object(distributor, "save_processed_ledger", side_effect=_save_processed_ledger),
            patch.object(distributor, "append_stats", side_effect=self._stats_recorder(observed)),
            patch.object(distributor, "get_next_staff", side_effect=_get_next_staff),
            patch.object(
                distributor,
                "_get_normal_assignment_business_context",
                return_value=(datetime(2026, 3, 10, 9, 0), True, None),
            ),
            patch.object(distributor, "inject_completion_hotlink", return_value=False),
            patch.object(distributor, "send_manager_hold_notification", return_value=True),
            patch.object(distributor, "detect_risk", return_value=("normal", None)),
            patch.object(distributor, "is_hib_notification", return_value=False),
            patch.object(distributor, "hib_contains_16110", return_value=False),
            patch.object(distributor, "is_jira_candidate", return_value=False),
            patch.object(distributor, "is_jira_comment_email", return_value=False),
            patch.object(distributor, "is_staff_completed_confirmation", return_value=False),
            patch.object(distributor, "is_jones_completion_notification", return_value=False),
            patch.object(distributor, "maybe_emit_heartbeat", return_value=None),
            patch.object(distributor, "log_safe_mode_status", return_value=None),
            patch.object(distributor, "log", return_value=None),
            patch.object(distributor, "determine_safe_mode", return_value=(False, "unit_test", False)),
        ]

        for active in patches:
            active.start()
            self.addCleanup(active.stop)

        distributor.process_inbox()
        return observed

    def test_relooped_item_skipped_within_protection_window(self):
        observed = self._run_single_message(stale_age_seconds=10)
        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], [])
        row = observed["stats"][-1]
        self.assertEqual(row["event_type"], "RELOOP_SKIP_SAME_TICK")
        self.assertEqual(row["action"], "RELOOP_SKIP_SAME_TICK")

    def test_relooped_item_processed_after_protection_window(self):
        observed = self._run_single_message(stale_age_seconds=70)
        self.assertEqual(observed["get_next_staff_calls"], 1)
        self.assertEqual(observed["send_calls"], 1)
        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        self.assertTrue(any(row.get("event_type") == "ASSIGNED" for row in observed["stats"]))


if __name__ == "__main__":
    unittest.main()
