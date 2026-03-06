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

    @property
    def Count(self):
        return len(self._items)

    def Item(self, index):
        return self._items[index - 1]

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
        self.sent = False

    def Send(self):
        self.sent = True
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
        return DummyItems(self._items)

    def Item(self, index):
        return self._items[index - 1]

    def __iter__(self):
        return iter(self._items)


class DummyMessage:
    def __init__(self, sender, subject, parent, body="Body"):
        self.Subject = subject
        self.Body = body
        self.HTMLBody = body
        self.Importance = 1
        self.UnRead = True
        self.ReceivedTime = distributor.datetime(2026, 3, 4, 8, 0, 0)
        self.ConversationID = "CONV-1"
        self.EntryID = "ENTRY-1"
        self.StoreID = parent.Store.StoreID
        self.InternetMessageID = "<unit@test>"
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


class BucketBehaviourContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path("system_buckets.json")
        data = json.loads(path.read_text(encoding="utf-8"))
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

    def _build_hot_cfg(self, buckets, *, apps_team_recipients=None):
        if apps_team_recipients is None:
            apps_team_recipients = ["apps.team@sa.gov.au"]
        return {
            "staff": {
                "staff": ["staff.one@sa.gov.au"],
                "off_rotation": [],
                "leave": [],
            },
            "apps_team": {"recipients": list(apps_team_recipients)},
            "manager_config": {"recipients": ["manager.review@sa.gov.au"]},
            "system_buckets": buckets,
        }

    def _make_folders(self, observed, include_system_notification=True):
        store = DummyStore()
        mailbox = DummyFolder("Mailbox", path="Mailbox", store=store)
        inbox = DummyFolder("Transfer Bot Test Received", path="Mailbox\\Transfer Bot Test Received", parent=mailbox)
        processed = DummyFolder("Transfer Bot Test", path="Mailbox\\Transfer Bot Test", parent=mailbox)
        quarantine = DummyFolder("03_QUARANTINE", path="Mailbox\\Inbox\\03_QUARANTINE", parent=mailbox)
        hib = DummyFolder("04_HIB", path="Mailbox\\Inbox\\04_HIB", parent=mailbox)
        system_notification = DummyFolder(
            "05_SYSTEM_NOTIFICATIONS",
            path="Mailbox\\Inbox\\05_SYSTEM_NOTIFICATIONS",
            parent=mailbox,
        ) if include_system_notification else None
        for folder in (mailbox, inbox, processed, quarantine, hib, system_notification):
            if folder is not None:
                folder._observed = observed
        return {
            "mailbox": mailbox,
            "inbox": inbox,
            "processed": processed,
            "quarantine": quarantine,
            "hib": hib,
            "system_notification": system_notification,
        }

    def _resolve_folder_factory(self, folders):
        def _resolve_folder(_root, path):
            mapping = {
                distributor.CONFIG["inbox_folder"]: folders["inbox"],
                distributor.CONFIG["processed_folder"]: folders["processed"],
                "Inbox/03_QUARANTINE": folders["quarantine"],
                "Inbox/04_HIB": folders["hib"],
                "Inbox/05_SYSTEM_NOTIFICATIONS": folders["system_notification"],
                distributor.JIRA_FOLLOW_UP_FOLDER_PATH: folders["processed"],
            }
            folder = mapping.get(path)
            return folder, "unit_test"

        return _resolve_folder

    def _run_single_message(
        self,
        sender,
        subject,
        *,
        bucket_overlay=None,
        include_system_notification=True,
        apps_team_recipients=None,
        hib_notification=False,
        hib_contains_16110=False,
        hib_contains_16111=False,
    ):
        observed = {
            "stats": [],
            "forwards": [],
            "send_calls": 0,
            "moves": [],
            "saved_ledgers": [],
            "get_next_staff_calls": 0,
            "hotlink_calls": 0,
            "manager_hold_notifications": 0,
        }
        buckets = copy.deepcopy(self.base_buckets)
        if bucket_overlay:
            for key, value in bucket_overlay.items():
                buckets[key] = value
        hot_cfg = self._build_hot_cfg(buckets, apps_team_recipients=apps_team_recipients)
        folders = self._make_folders(observed, include_system_notification=include_system_notification)
        mailbox = folders["mailbox"]
        inbox = folders["inbox"]
        message = DummyMessage(sender, subject, inbox)
        inbox.Items = DummyItems([message])
        namespace = DummyNamespace(mailbox)
        outlook = DummyOutlookApp(namespace)
        win32_stub = SimpleNamespace(client=SimpleNamespace(Dispatch=lambda _name: outlook))
        ledger = {}

        def _save_processed_ledger(data):
            observed["saved_ledgers"].append(copy.deepcopy(data))
            return True

        def _get_next_staff():
            observed["get_next_staff_calls"] += 1
            return "staff.one@sa.gov.au"

        def _inject_completion_hotlink(*args, **kwargs):
            observed["hotlink_calls"] += 1
            mode_out = args[4] if len(args) > 4 else None
            if isinstance(mode_out, list):
                mode_out.append("HTML")
            return True

        def _send_manager_hold_notification(*args, **kwargs):
            observed["manager_hold_notifications"] += 1
            return True

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
            patch.object(distributor, "load_processed_ledger", return_value=ledger),
            patch.object(distributor, "save_processed_ledger", side_effect=_save_processed_ledger),
            patch.object(distributor, "append_stats", side_effect=self._stats_recorder(observed)),
            patch.object(distributor, "get_next_staff", side_effect=_get_next_staff),
            patch.object(distributor, "inject_completion_hotlink", side_effect=_inject_completion_hotlink),
            patch.object(distributor, "send_manager_hold_notification", side_effect=_send_manager_hold_notification),
            patch.object(distributor, "detect_risk", return_value=("normal", None)),
            patch.object(distributor, "is_hib_notification", return_value=hib_notification),
            patch.object(distributor, "hib_contains_16110", return_value=hib_contains_16110),
            patch.object(distributor, "hib_contains_16111", return_value=hib_contains_16111),
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
        return observed

    def test_current_system_buckets_json_parses(self):
        self.assertIn("health.digitalhealthsaservicedesk@sa.gov.au", self.base_buckets["system_notification_senders"])
        self.assertIn("gwemail@chib.had.sa.gov.au", self.base_buckets["applications_direct_senders"])

    def test_external_image_request_round_robins_and_creates_hotlink(self):
        observed = self._run_single_message("someone@bensonradiology.com.au", "Image transfer request")

        self.assertEqual(observed["get_next_staff_calls"], 1)
        self.assertEqual(observed["hotlink_calls"], 1)
        self.assertEqual(observed["send_calls"], 1)
        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        row = next(record for record in observed["stats"] if record.get("event_type") == "ASSIGNED")
        self.assertEqual(row["domain_bucket"], "external_image_request")
        self.assertEqual(row["assigned_to"], "staff.one@sa.gov.au")
        self.assertTrue(row["action"].startswith("IMAGE_REQUEST_EXTERNAL"))
        self.assertTrue(row["sami_id"])

    def test_system_notification_sender_override_silent_moves_without_assignment(self):
        observed = self._run_single_message(
            "health.digitalhealthsaservicedesk@sa.gov.au",
            "Digital Health notification",
        )

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], ["05_SYSTEM_NOTIFICATIONS"])
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))
        row = observed["stats"][-1]
        self.assertEqual(row["domain_bucket"], "system_notification")
        self.assertEqual(row["assigned_to"], "system_notification")
        self.assertTrue(row["action"].startswith("SYSTEM_NOTIFICATION"))

    def test_system_notification_falls_back_to_processed_when_folder_missing(self):
        observed = self._run_single_message(
            "health.digitalhealthsaservicedesk@sa.gov.au",
            "Digital Health notification",
            include_system_notification=False,
        )

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))

    def test_applications_direct_sender_override_forwards_without_assignment(self):
        observed = self._run_single_message("gwemail@chib.had.sa.gov.au", "Applications direct item")

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 1)
        self.assertEqual(observed["moves"], ["04_HIB"])
        self.assertEqual(len(observed["forwards"]), 1)
        self.assertEqual(observed["forwards"][0].Recipients.values(), ["apps.team@sa.gov.au"])
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))
        row = observed["stats"][-1]
        self.assertEqual(row["domain_bucket"], "applications_direct")
        self.assertEqual(row["assigned_to"], "applications_direct")
        self.assertTrue(row["action"].startswith("APPS_FORWARD_ONLY"))

    def test_applications_direct_non_16111_beats_generic_hib_and_forwards_all_apps_recipients(self):
        recipients = [
            "apps@sa.gov.au",
            "kate.cook@sa.gov.au",
            "tony.penna@sa.gov.au",
            "brian.shaw@sa.gov.au",
        ]
        observed = self._run_single_message(
            "pas.health@sa.gov.au",
            "Applications direct item",
            apps_team_recipients=recipients,
            hib_notification=True,
            hib_contains_16111=False,
        )

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 1)
        self.assertEqual(observed["moves"], ["04_HIB"])
        self.assertEqual(len(observed["forwards"]), 1)
        self.assertEqual(observed["forwards"][0].Recipients.values(), recipients)
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))
        row = observed["stats"][-1]
        self.assertEqual(row["domain_bucket"], "applications_direct")
        self.assertEqual(row["assigned_to"], "applications_direct")
        self.assertTrue(row["action"].startswith("APPS_FORWARD_ONLY"))

    def test_applications_direct_16111_still_routes_hib_only(self):
        observed = self._run_single_message(
            "pas.health@sa.gov.au",
            "ERROR: 16111 HIB alert",
            hib_notification=True,
            hib_contains_16111=True,
        )

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], ["04_HIB"])
        self.assertEqual(len(observed["forwards"]), 0)
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))
        row = observed["stats"][-1]
        self.assertEqual(row["domain_bucket"], "hib")
        self.assertEqual(row["assigned_to"], "hib")
        self.assertEqual(row["action"], "ROUTE_HIB")

    def test_internal_non_staff_routes_manager_review_without_assignment(self):
        observed = self._run_single_message("nonstaff@sa.gov.au", "Internal non-staff request")

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 1)
        self.assertEqual(observed["moves"], ["Transfer Bot Test"])
        self.assertEqual(len(observed["forwards"]), 1)
        self.assertEqual(observed["forwards"][0].Recipients.values(), ["manager.review@sa.gov.au"])
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))
        row = observed["stats"][-1]
        self.assertEqual(row["assigned_to"], "manager_review")
        self.assertEqual(row["action"], "INTERNAL_NON_STAFF")

    def test_unknown_external_routes_hold_unknown_domain(self):
        observed = self._run_single_message("unknown@randomdomain.com", "Unknown external request")

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["manager_hold_notifications"], 1)
        self.assertEqual(observed["moves"], ["03_QUARANTINE"])
        row = observed["stats"][-1]
        self.assertEqual(row["assigned_to"], "hold")
        self.assertEqual(row["domain_bucket"], "unknown")
        self.assertEqual(row["action"], "HOLD_UNKNOWN_DOMAIN")

    def test_quarantine_sender_short_circuits_before_assignment_and_forwarding(self):
        overlay = copy.deepcopy(self.base_buckets)
        overlay["quarantine_senders"] = self.base_buckets["quarantine_senders"] + ["blocked@example.com"]
        observed = self._run_single_message(
            "blocked@example.com",
            "Blocked sender",
            bucket_overlay=overlay,
        )

        self.assertEqual(observed["get_next_staff_calls"], 0)
        self.assertEqual(observed["hotlink_calls"], 0)
        self.assertEqual(observed["send_calls"], 0)
        self.assertEqual(observed["moves"], ["03_QUARANTINE"])
        self.assertEqual(observed["forwards"], [])
        self.assertFalse(any(record.get("event_type") == "ASSIGNED" for record in observed["stats"]))

    def test_zedtechnologies_collision_prefers_system_notification(self):
        bucket, match_level = distributor.classify_sender(
            "someone@zedtechnologies.com.au",
            "zedtechnologies.com.au",
            self.base_buckets,
        )
        self.assertEqual((bucket, match_level), ("system_notification", "domain"))


if __name__ == "__main__":
    unittest.main()
