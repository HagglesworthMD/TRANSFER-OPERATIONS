import copy
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
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

    def values(self):
        return [item.Address for item in self._items]


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
        if self._observed.get("send_error") is not None:
            raise self._observed["send_error"]
        self._observed["send_calls"] += 1
        self._observed["events"].append(f"send:{self.source_message.EntryID}")


class DummyStore:
    def __init__(self, display_name="UnitTest Mailbox", store_id="STORE-1"):
        self.DisplayName = display_name
        self.StoreID = store_id


class DummyItems:
    def __init__(self, items):
        self._items = list(items)

    @property
    def Count(self):
        return len(self._items)

    def Restrict(self, query):
        if query == "[UnRead] = True":
            return DummyItems([item for item in self._items if getattr(item, "UnRead", False)])
        return DummyItems(self._items)

    def Item(self, index):
        return self._items[index - 1]

    def __iter__(self):
        return iter(self._items)


class DummyFolder:
    def __init__(self, name, path=None, parent=None, store=None):
        self.Name = name
        self.Parent = parent
        self.Store = store if store is not None else (parent.Store if parent is not None else DummyStore())
        self.FolderPath = path or name
        self.Items = DummyItems([])
        self._observed = None


class DummyMessage:
    def __init__(
        self,
        observed,
        parent,
        sender,
        subject,
        body,
        *,
        entry_id,
        store_id=None,
        internet_message_id=None,
        conversation_id="CONV-1",
    ):
        self._observed = observed
        self.Parent = parent
        self.Subject = subject
        self.Body = body
        self.HTMLBody = body
        self.Importance = 1
        self.UnRead = True
        self.ReceivedTime = distributor.datetime(2026, 3, 4, 8, 0, 0)
        self.ConversationID = conversation_id
        self.EntryID = entry_id
        self.StoreID = store_id or parent.Store.StoreID
        self.InternetMessageID = internet_message_id or f"<{entry_id}@unit.test>"
        self.SenderEmailType = ""
        self.SenderEmailAddress = sender
        self.SenderName = sender
        self.MessageClass = "IPM.Note"
        self.moved_to = None
        self.forwards = []
        self.save_calls = 0
        self._observed["identity_map"][(self.EntryID, self.StoreID)] = self

    def Forward(self):
        self._observed["events"].append(f"forward:{self.EntryID}")
        forward = DummyForward(self._observed, self)
        self.forwards.append(forward)
        self._observed["forwards"].append(forward)
        return forward

    def Move(self, folder):
        self.moved_to = folder
        self._observed["moves"].append(folder.Name)
        self._observed["events"].append(f"move:{folder.Name}")
        if folder.Name == distributor.CONFIG["processed_folder"]:
            proc_index = self._observed["processed_move_count"] + 1
            self._observed["processed_move_count"] = proc_index
            archived = DummyMessage(
                self._observed,
                folder,
                self.SenderEmailAddress,
                self.Subject,
                self.Body,
                entry_id=f"PROC-{proc_index}",
                store_id=folder.Store.StoreID,
                internet_message_id=f"<proc-{proc_index}@unit.test>",
                conversation_id=self.ConversationID,
            )
            self._observed["archived_item"] = archived
            return archived
        self.Parent = folder
        self.StoreID = folder.Store.StoreID
        self._observed["identity_map"][(self.EntryID, self.StoreID)] = self
        return self

    def Save(self):
        self.save_calls += 1
        self._observed["events"].append(f"save:{self.EntryID}")


class DummyNamespace:
    def __init__(self, mailbox, observed):
        self.mailbox = mailbox
        self._observed = observed

    def GetDefaultFolder(self, _):
        return self.mailbox

    def GetItemFromID(self, entry_id, store_id=None):
        key = (entry_id, store_id or self.mailbox.Store.StoreID)
        item = self._observed["identity_map"].get(key)
        if item is None:
            raise KeyError(key)
        return item


class DummyOutlookApp:
    def __init__(self, namespace):
        self._namespace = namespace

    def GetNamespace(self, _):
        return self._namespace


class AssignmentArchiveRecoveryTests(unittest.TestCase):
    def _stats_recorder(self, observed):
        def _record(*args, **kwargs):
            record = {key: "" for key in STAT_KEYS}
            for key, value in zip(STAT_KEYS, args):
                record[key] = value
            record.update(kwargs)
            observed["stats"].append(record)
        return _record

    def _domain_policy(self):
        return {
            "internal_domains": ["sa.gov.au"],
            "vendor_domains": [],
            "transfer_domains": ["bensonradiology.com.au"],
            "system_notification_domains": [],
            "quarantine_domains": [],
            "always_hold_domains": [],
            "held_domains": [],
            "applications_direct_domains": [],
            "transfer_senders": [],
            "system_notification_senders": [],
            "quarantine_senders": [],
            "held_senders": [],
            "applications_direct_senders": [],
            "hib_noise": {},
        }

    def _hot_cfg(self):
        return {
            "staff": {
                "staff": ["brian.shaw@sa.gov.au"],
                "off_rotation": [],
                "leave": [],
            },
            "apps_team": {"recipients": ["apps.team@sa.gov.au"]},
            "manager_config": {"recipients": ["manager.review@sa.gov.au"]},
            "system_buckets": {
                "folders": {
                    "completed": "01_COMPLETED",
                    "non_actionable": "02_PROCESSED",
                    "quarantine": "03_QUARANTINE",
                    "hold": "04_HIB",
                    "system_notification": "05_SYSTEM_NOTIFICATIONS",
                }
            },
        }

    def _make_folders(self, observed):
        store = DummyStore()
        mailbox = DummyFolder("Mailbox", path="Mailbox", store=store)
        inbox = DummyFolder(distributor.CONFIG["inbox_folder"], path="Mailbox\\Inbox", parent=mailbox)
        processed = DummyFolder(distributor.CONFIG["processed_folder"], path="Mailbox\\02_PROCESSED", parent=mailbox)
        quarantine = DummyFolder("03_QUARANTINE", path="Mailbox\\03_QUARANTINE", parent=mailbox)
        hib = DummyFolder("04_HIB", path="Mailbox\\04_HIB", parent=mailbox)
        system_notification = DummyFolder("05_SYSTEM_NOTIFICATIONS", path="Mailbox\\05_SYSTEM_NOTIFICATIONS", parent=mailbox)
        completed = DummyFolder("01_COMPLETED", path="Mailbox\\01_COMPLETED", parent=mailbox)
        jira_follow_up = DummyFolder("06_JIRA_FOLLOW_UP", path="Mailbox\\06_JIRA_FOLLOW_UP", parent=mailbox)
        for folder in (mailbox, inbox, processed, quarantine, hib, system_notification, completed, jira_follow_up):
            folder._observed = observed
        return {
            "mailbox": mailbox,
            "inbox": inbox,
            "processed": processed,
            "quarantine": quarantine,
            "hib": hib,
            "system_notification": system_notification,
            "completed": completed,
            "jira_follow_up": jira_follow_up,
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
                distributor.JIRA_FOLLOW_UP_FOLDER_PATH: folders["jira_follow_up"],
            }
            return mapping.get(path), "unit_test"
        return _resolve_folder

    def _run_assignment(self, send_error=None):
        observed = {
            "stats": [],
            "forwards": [],
            "send_calls": 0,
            "moves": [],
            "events": [],
            "saved_ledgers": [],
            "identity_map": {},
            "processed_move_count": 0,
            "logs": [],
            "archived_item": None,
            "send_error": send_error,
            "get_next_staff_calls": 0,
        }
        folders = self._make_folders(observed)
        mailbox = folders["mailbox"]
        inbox = folders["inbox"]
        original = DummyMessage(
            observed,
            inbox,
            "someone@bensonradiology.com.au",
            "Image transfer request",
            "Original body",
            entry_id="INBOX-1",
        )
        inbox.Items = DummyItems([original])
        namespace = DummyNamespace(mailbox, observed)
        outlook = DummyOutlookApp(namespace)
        win32_stub = SimpleNamespace(client=SimpleNamespace(Dispatch=lambda _name: outlook))
        message_key, _identity = distributor.compute_message_identity(
            original,
            original.SenderEmailAddress,
            original.Subject,
            original.ReceivedTime.isoformat(),
        )
        expected_sami = distributor.compute_sami_id(original)

        def _save_processed_ledger(data):
            observed["saved_ledgers"].append(copy.deepcopy(data))
            return True

        def _log(msg, *_args, **_kwargs):
            observed["logs"].append(str(msg))

        def _get_next_staff():
            observed["get_next_staff_calls"] += 1
            return "brian.shaw@sa.gov.au"

        patches = [
            patch.object(distributor, "OUTLOOK_AVAILABLE", True),
            patch.object(distributor, "win32com", win32_stub, create=True),
            patch.object(distributor, "find_mailbox_root_robust", return_value=mailbox),
            patch.object(distributor, "resolve_folder", side_effect=self._resolve_folder_factory(folders)),
            patch.object(distributor, "get_or_create_subfolder", side_effect=lambda _parent, path: folders["quarantine"] if "03_QUARANTINE" in path else folders["hib"]),
            patch.object(distributor, "get_folder_path_safe", side_effect=lambda folder: folder.FolderPath if folder else ""),
            patch.object(distributor, "check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox")),
            patch.object(distributor, "load_settings_overrides", return_value={}),
            patch.object(distributor, "load_config_files_each_tick", return_value=(self._hot_cfg(), [])),
            patch.object(distributor, "load_domain_policy", return_value=(self._domain_policy(), True)),
            patch.object(distributor, "get_staff_list", return_value=["brian.shaw@sa.gov.au"]),
            patch.object(distributor, "ensure_processed_ledger_exists", return_value=True),
            patch.object(distributor, "load_processed_ledger", return_value={}),
            patch.object(distributor, "save_processed_ledger", side_effect=_save_processed_ledger),
            patch.object(distributor, "load_poison_counts", return_value={}),
            patch.object(distributor, "save_poison_counts", return_value=True),
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
            patch.object(distributor, "hib_contains_16111", return_value=False),
            patch.object(distributor, "is_jira_candidate", return_value=False),
            patch.object(distributor, "is_jira_comment_email", return_value=False),
            patch.object(distributor, "is_staff_completed_confirmation", return_value=False),
            patch.object(distributor, "is_jones_completion_notification", return_value=False),
            patch.object(distributor, "maybe_emit_heartbeat", return_value=None),
            patch.object(distributor, "log_safe_mode_status", return_value=None),
            patch.object(distributor, "log", side_effect=_log),
            patch.object(distributor, "determine_safe_mode", return_value=(False, "unit_test", False)),
        ]
        with ExitStack() as stack:
            for active in patches:
                stack.enter_context(active)
            distributor.process_inbox()
        return {
            "observed": observed,
            "message_key": message_key,
            "expected_sami": expected_sami,
            "namespace": namespace,
            "inbox": inbox,
            "original": original,
            "archived": observed["archived_item"],
            "ledger": observed["saved_ledgers"][-1] if observed["saved_ledgers"] else None,
        }

    def test_assignment_archives_original_after_successful_send_and_keeps_archive_untouched(self):
        result = self._run_assignment()
        observed = result["observed"]
        archived = result["archived"]
        forward = observed["forwards"][0]

        self.assertEqual(observed["moves"], [distributor.CONFIG["processed_folder"]])
        self.assertLess(observed["events"].index(f"forward:{result['original'].EntryID}"), observed["events"].index(f"send:{result['original'].EntryID}"))
        self.assertLess(observed["events"].index(f"send:{result['original'].EntryID}"), observed["events"].index(f"move:{distributor.CONFIG['processed_folder']}"))
        self.assertLess(observed["events"].index(f"move:{distributor.CONFIG['processed_folder']}"), observed["events"].index(f"save:{archived.EntryID}"))
        self.assertEqual(archived.Subject, "Image transfer request")
        self.assertEqual(archived.Body, "Original body")
        self.assertFalse(archived.UnRead)
        self.assertEqual(archived.save_calls, 1)
        self.assertIs(forward.source_message, result["original"])
        self.assertEqual(forward.Subject, f"[{result['expected_sami']}] Image transfer request")
        self.assertEqual(forward.Recipients.values(), ["brian.shaw@sa.gov.au"])

    def test_ledger_stores_processed_archive_identity_for_assignment(self):
        result = self._run_assignment()
        archived = result["archived"]
        original = result["original"]
        message_key = result["message_key"]
        saved_ledgers = result["observed"]["saved_ledgers"]

        self.assertEqual(len(saved_ledgers), 1)
        for ledger in saved_ledgers:
            entry = ledger[message_key]
            self.assertEqual(entry["entry_id"], archived.EntryID)
            self.assertEqual(entry["store_id"], archived.StoreID)
            self.assertNotEqual(entry["entry_id"], original.EntryID)

    def test_assignment_send_failure_leaves_original_in_inbox_without_processed_ledger_entry(self):
        result = self._run_assignment(send_error=RuntimeError("send failed"))
        observed = result["observed"]

        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], [])
        self.assertIsNone(result["archived"])
        self.assertIsNone(result["ledger"])
        self.assertIsNone(result["original"].moved_to)
        self.assertIs(result["original"].Parent, result["inbox"])

    def test_business_hours_skip_leaves_original_in_inbox_without_round_robin_or_ledger_entry(self):
        observed = {
            "stats": [],
            "forwards": [],
            "send_calls": 0,
            "moves": [],
            "events": [],
            "saved_ledgers": [],
            "identity_map": {},
            "processed_move_count": 0,
            "logs": [],
            "archived_item": None,
            "send_error": None,
            "get_next_staff_calls": 0,
        }
        folders = self._make_folders(observed)
        mailbox = folders["mailbox"]
        inbox = folders["inbox"]
        original = DummyMessage(
            observed,
            inbox,
            "someone@bensonradiology.com.au",
            "Image transfer request",
            "Original body",
            entry_id="INBOX-1",
        )
        inbox.Items = DummyItems([original])
        namespace = DummyNamespace(mailbox, observed)
        outlook = DummyOutlookApp(namespace)
        win32_stub = SimpleNamespace(client=SimpleNamespace(Dispatch=lambda _name: outlook))

        def _save_processed_ledger(data):
            observed["saved_ledgers"].append(copy.deepcopy(data))
            return True

        def _log(msg, *_args, **_kwargs):
            observed["logs"].append(str(msg))

        def _get_next_staff():
            observed["get_next_staff_calls"] += 1
            return "brian.shaw@sa.gov.au"

        patches = [
            patch.object(distributor, "OUTLOOK_AVAILABLE", True),
            patch.object(distributor, "win32com", win32_stub, create=True),
            patch.object(distributor, "find_mailbox_root_robust", return_value=mailbox),
            patch.object(distributor, "resolve_folder", side_effect=self._resolve_folder_factory(folders)),
            patch.object(distributor, "get_or_create_subfolder", side_effect=lambda _parent, path: folders["quarantine"] if "03_QUARANTINE" in path else folders["hib"]),
            patch.object(distributor, "get_folder_path_safe", side_effect=lambda folder: folder.FolderPath if folder else ""),
            patch.object(distributor, "check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox")),
            patch.object(distributor, "load_settings_overrides", return_value={}),
            patch.object(distributor, "load_config_files_each_tick", return_value=(self._hot_cfg(), [])),
            patch.object(distributor, "load_domain_policy", return_value=(self._domain_policy(), True)),
            patch.object(distributor, "get_staff_list", return_value=["brian.shaw@sa.gov.au"]),
            patch.object(distributor, "ensure_processed_ledger_exists", return_value=True),
            patch.object(distributor, "load_processed_ledger", return_value={}),
            patch.object(distributor, "save_processed_ledger", side_effect=_save_processed_ledger),
            patch.object(distributor, "load_poison_counts", return_value={}),
            patch.object(distributor, "save_poison_counts", return_value=True),
            patch.object(distributor, "append_stats", side_effect=self._stats_recorder(observed)),
            patch.object(distributor, "get_next_staff", side_effect=_get_next_staff),
            patch.object(
                distributor,
                "_get_normal_assignment_business_context",
                return_value=(datetime(2026, 3, 9, 10, 0), False, "public_holiday"),
            ),
            patch.object(distributor, "inject_completion_hotlink", return_value=False),
            patch.object(distributor, "send_manager_hold_notification", return_value=True),
            patch.object(distributor, "detect_risk", return_value=("normal", None)),
            patch.object(distributor, "is_hib_notification", return_value=False),
            patch.object(distributor, "hib_contains_16110", return_value=False),
            patch.object(distributor, "hib_contains_16111", return_value=False),
            patch.object(distributor, "is_jira_candidate", return_value=False),
            patch.object(distributor, "is_jira_comment_email", return_value=False),
            patch.object(distributor, "is_staff_completed_confirmation", return_value=False),
            patch.object(distributor, "is_jones_completion_notification", return_value=False),
            patch.object(distributor, "maybe_emit_heartbeat", return_value=None),
            patch.object(distributor, "log_safe_mode_status", return_value=None),
            patch.object(distributor, "log", side_effect=_log),
            patch.object(distributor, "determine_safe_mode", return_value=(False, "unit_test", False)),
        ]
        with ExitStack() as stack:
            for active in patches:
                stack.enter_context(active)
            distributor.process_inbox()

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], [])
        self.assertEqual(observed["saved_ledgers"], [])
        self.assertTrue(original.UnRead)
        self.assertIsNone(original.moved_to)
        self.assertIs(original.Parent, inbox)
        self.assertTrue(any("BUSINESS_HOURS_SKIP" in msg and "reason=public_holiday" in msg for msg in observed["logs"]))

    def test_resolve_mailitem_from_ledger_entry_returns_processed_archive_item(self):
        result = self._run_assignment()
        entry = result["ledger"][result["message_key"]]

        resolved = distributor._resolve_mailitem_from_ledger_entry(result["namespace"], entry)

        self.assertIs(resolved, result["archived"])

    @patch("distributor.log")
    @patch("distributor.atomic_write_json", return_value=True)
    @patch("distributor.save_processed_ledger")
    @patch("distributor.append_stats")
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_manual_stale_release_uses_processed_archive_identity(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        _mock_store_guard,
        mock_stats,
        mock_save_ledger,
        _mock_atomic_write,
        _mock_log,
    ):
        result = self._run_assignment()
        message_key = result["message_key"]
        archived = result["archived"]
        processed = result["observed"]["archived_item"].Parent
        ledger = copy.deepcopy(result["ledger"])
        mock_safe_load.return_value = {
            f"msg:{message_key}": {
                "request_id": "manual-stale-1",
                "request_key": f"msg:{message_key}",
                "msg_key": message_key,
                "sami_id": result["expected_sami"],
                "reason": "",
                "requested_by": "dashboard_admin",
                "requested_ts": "2026-03-06T10:00:00+00:00",
            }
        }
        mock_runtime.return_value = (result["namespace"], processed, "")
        mock_load_ledger.return_value = ledger
        mock_save_ledger.return_value = True

        distributor.process_manual_stale_requests()

        saved_ledger = mock_save_ledger.call_args[0][0]
        self.assertIsNone(archived.moved_to)
        self.assertFalse(archived.UnRead)
        self.assertEqual(saved_ledger[message_key]["assigned_to"], "brian.shaw@sa.gov.au")
        self.assertEqual(saved_ledger[message_key]["stale_last_owner"], "brian.shaw@sa.gov.au")
        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE")

    @patch("distributor.log")
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list", return_value=["brian.shaw@sa.gov.au"])
    @patch("distributor._resolve_stale_reloop_runtime")
    def test_stale_reloop_uses_processed_archive_identity(
        self,
        mock_runtime,
        _mock_staff,
        mock_load_ledger,
        mock_save_ledger,
        mock_stats,
        _mock_store_guard,
        _mock_log,
    ):
        result = self._run_assignment()
        message_key = result["message_key"]
        archived = result["archived"]
        processed = result["observed"]["archived_item"].Parent
        ledger = copy.deepcopy(result["ledger"])
        ledger[message_key]["ts"] = (datetime.now() - timedelta(hours=13)).isoformat()
        mock_runtime.return_value = (result["namespace"], processed, "")
        mock_load_ledger.return_value = ledger
        mock_save_ledger.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save_ledger.call_args[0][0]
        self.assertIsNone(archived.moved_to)
        self.assertFalse(archived.UnRead)
        self.assertEqual(saved_ledger[message_key]["assigned_to"], "brian.shaw@sa.gov.au")
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")


if __name__ == "__main__":
    unittest.main()
