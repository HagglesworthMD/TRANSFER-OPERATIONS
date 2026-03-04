
import copy
import json
import unittest
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


class ReplyChainCompletionTests(unittest.TestCase):
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

    def _run_single_message(self, sender, subject, *, ledger=None, overrides=None, bucket_overlay=None):
        observed = {
            "stats": [],
            "forwards": [],
            "send_calls": 0,
            "moves": [],
            "saved_ledgers": [],
        }
        buckets = copy.deepcopy(self.base_buckets)
        if bucket_overlay:
            for key, value in bucket_overlay.items():
                buckets[key] = value
        hot_cfg = self._build_hot_cfg(buckets)
        folders = self._make_folders(observed)
        mailbox = folders["mailbox"]
        inbox = folders["inbox"]
        message = DummyMessage(sender, subject, inbox)
        inbox.Items = DummyItems([message])
        namespace = DummyNamespace(mailbox)
        outlook = DummyOutlookApp(namespace)
        win32_stub = SimpleNamespace(client=SimpleNamespace(Dispatch=lambda _name: outlook))
        ledger_data = copy.deepcopy(ledger or {})

        def _save_processed_ledger(data):
            observed["saved_ledgers"].append(copy.deepcopy(data))
            return True

        patches = [
            patch.object(distributor, "OUTLOOK_AVAILABLE", True),
            patch.object(distributor, "win32com", win32_stub, create=True),
            patch.object(distributor, "find_mailbox_root_robust", return_value=mailbox),
            patch.object(distributor, "resolve_folder", side_effect=self._resolve_folder_factory(folders)),
            patch.object(distributor, "get_or_create_subfolder", side_effect=lambda _parent, path: folders["quarantine"] if "03_QUARANTINE" in path else folders["hib"]),
            patch.object(distributor, "get_folder_path_safe", side_effect=lambda folder: folder.FolderPath if folder else ""),
            patch.object(distributor, "check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox")),
            patch.object(distributor, "load_settings_overrides", return_value=(overrides or {})),
            patch.object(distributor, "load_config_files_each_tick", return_value=(hot_cfg, [])),
            patch.object(distributor, "load_domain_policy", return_value=(self._base_domain_policy(), True)),
            patch.object(distributor, "get_staff_list", return_value=["staff.one@sa.gov.au"]),
            patch.object(distributor, "ensure_processed_ledger_exists", return_value=True),
            patch.object(distributor, "load_processed_ledger", return_value=ledger_data),
            patch.object(distributor, "save_processed_ledger", side_effect=_save_processed_ledger),
            patch.object(distributor, "append_stats", side_effect=self._stats_recorder(observed)),
            patch.object(distributor, "get_next_staff", side_effect=AssertionError("round robin should not run")),
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
        observed["message"] = message
        observed["ledger"] = ledger_data
        return observed

    def test_reply_chain_completion_closes_job_from_skip_branch(self):
        ledger = {
            "job-1": {
                "assigned_to": "staff.one@sa.gov.au",
                "sami_id": "SAMI-ABC123",
                "conversation_id": "CONV-1",
                "ts": "2026-03-04T08:00:00",
            }
        }
        observed = self._run_single_message(
            "staff.one@sa.gov.au",
            "Re: [SAMI-ABC123] Finished",
            ledger=ledger,
            overrides={"enable_reply_chain_completion": True},
        )

        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        completed_rows = [row for row in observed["stats"] if row.get("event_type") == "COMPLETED"]
        self.assertEqual(len(completed_rows), 1)
        row = completed_rows[0]
        self.assertEqual(row["assigned_to"], "staff.one@sa.gov.au")
        self.assertEqual(row["sender"], "staff.one@sa.gov.au")
        self.assertEqual(row["action"], "COMPLETION_REPLY_CHAIN")
        self.assertEqual(row["msg_key"], "job-1")
        self.assertEqual(row["sami_id"], "SAMI-ABC123")
        self.assertEqual(observed["ledger"]["job-1"]["completion_source"], "reply_chain")
        self.assertEqual(observed["ledger"]["job-1"]["completed_by"], "staff.one@sa.gov.au")

    def test_keyword_completion_writer_remains_unchanged(self):
        ledger = {
            "job-1": {
                "assigned_to": "staff.one@sa.gov.au",
                "sami_id": "SAMI-ABC123",
                "conversation_id": "CONV-1",
                "ts": "2026-03-04T08:00:00",
            }
        }
        observed = self._run_single_message(
            "staff.one@sa.gov.au",
            "[COMPLETED] [SAMI-ABC123] Finished",
            ledger=ledger,
            overrides={"enable_reply_chain_completion": True},
        )

        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        self.assertEqual(len(observed["stats"]), 1)
        row = observed["stats"][0]
        self.assertEqual(row["action"], "COMPLETION_SUBJECT_KEYWORD")
        self.assertEqual(row["assigned_to"], "completed")
        self.assertEqual(row["event_type"], "")
        self.assertEqual(observed["ledger"]["job-1"]["completion_source"], "subject_keyword")

    def test_reply_chain_flag_off_preserves_skip_behavior(self):
        ledger = {
            "job-1": {
                "assigned_to": "staff.one@sa.gov.au",
                "sami_id": "SAMI-ABC123",
                "conversation_id": "CONV-1",
            }
        }
        observed = self._run_single_message(
            "staff.one@sa.gov.au",
            "Re: [SAMI-ABC123] Finished",
            ledger=ledger,
            overrides={},
        )

        self.assertEqual(observed["moves"], [])
        self.assertFalse(any(row.get("event_type") == "COMPLETED" for row in observed["stats"]))
        self.assertNotIn("completed_at", observed["ledger"]["job-1"])

    def test_system_notification_override_bypasses_reply_chain_logic(self):
        ledger = {
            "job-1": {
                "assigned_to": "staff.one@sa.gov.au",
                "sami_id": "SAMI-ABC123",
                "conversation_id": "CONV-1",
            }
        }
        overlay = copy.deepcopy(self.base_buckets)
        overlay["system_notification_senders"] = self.base_buckets["system_notification_senders"] + ["staff.one@sa.gov.au"]
        observed = self._run_single_message(
            "staff.one@sa.gov.au",
            "Routine system notification",
            ledger=ledger,
            overrides={"enable_reply_chain_completion": True},
            bucket_overlay=overlay,
        )

        self.assertEqual(observed["moves"], ["05_SYSTEM_NOTIFICATIONS"])
        self.assertFalse(any(row.get("action") == "COMPLETION_REPLY_CHAIN" for row in observed["stats"]))
        self.assertNotIn("completed_at", observed["ledger"]["job-1"])

    def test_non_reply_subject_does_not_close_job(self):
        ledger = {
            "job-1": {
                "assigned_to": "staff.one@sa.gov.au",
                "sami_id": "SAMI-ABC123",
                "conversation_id": "CONV-1",
            }
        }
        observed = self._run_single_message(
            "staff.one@sa.gov.au",
            "[SAMI-ABC123] Finished",
            ledger=ledger,
            overrides={"enable_reply_chain_completion": True},
        )

        self.assertEqual(observed["moves"], [])
        self.assertFalse(any(row.get("event_type") == "COMPLETED" for row in observed["stats"]))
        self.assertNotIn("completed_at", observed["ledger"]["job-1"])


if __name__ == "__main__":
    unittest.main()
