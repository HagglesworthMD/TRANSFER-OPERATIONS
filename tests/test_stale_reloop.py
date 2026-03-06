import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import distributor


class _DummyInbox:
    Name = "Inbox"


class _DummyMailItem:
    def __init__(self):
        self.UnRead = False
        self.moved_to = None

    def Move(self, folder):
        self.moved_to = folder
        return self


class StaleReloopTests(unittest.TestCase):
    def _base_ledger_entry(self, hours_ago=13, **overrides):
        entry = {
            "assigned_to": "alice@test.com",
            "risk": "normal",
            "sami_id": "SAMI-100",
            "ts": (datetime.now() - timedelta(hours=hours_ago)).isoformat(),
            "entry_id": "ENTRY-1",
            "store_id": "STORE-1",
        }
        entry.update(overrides)
        return entry

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_clock_uses_latest_touch(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        now = datetime.now()
        mock_known_staff.return_value = {"alice@test.com", "bob@test.com"}
        mock_staff.return_value = ["alice@test.com", "bob@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                hours_ago=20,
                stale_last_reloop_at=(now - timedelta(hours=1)).isoformat(),
            )
        }
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()

    @patch("distributor.log")
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_item_reloops_to_inbox_and_updates_ledger(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_store_guard, mock_log
    ):
        mock_known_staff.return_value = {"z@test.com", "a@test.com", "m@test.com"}
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        inbox = _DummyInbox()
        dummy_item = _DummyMailItem()
        mock_runtime.return_value = (object(), inbox, "")
        original_ts = (datetime.now() - timedelta(hours=13)).isoformat()
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                ts=original_ts,
                assigned_to="m@test.com",
                sami_id="SAMI-101",
            )
        }
        mock_resolve_item.return_value = dummy_item
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(dummy_item.moved_to, inbox)
        self.assertTrue(dummy_item.UnRead)
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "")
        self.assertEqual(saved_ledger["k1"]["ts"], original_ts)
        self.assertIn("stale_last_reloop_at", saved_ledger["k1"])
        self.assertEqual(saved_ledger["k1"]["stale_reloop_count"], 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "unassigned")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_stale_reloop_uses_known_staff_when_assignee_is_off_rotation(
        self, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_store_guard, mock_log
    ):
        mock_staff.return_value = ["brian.shaw@sa.gov.au"]
        inbox = _DummyInbox()
        dummy_item = _DummyMailItem()
        original_ts = (datetime.now() - timedelta(hours=13)).isoformat()
        mock_runtime.return_value = (object(), inbox, "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="hannah.cutting@sa.gov.au",
                ts=original_ts,
                sami_id="SAMI-103",
            )
        }
        mock_resolve_item.return_value = dummy_item
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(dummy_item.moved_to, inbox)
        self.assertTrue(dummy_item.UnRead)
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "")
        self.assertEqual(saved_ledger["k1"]["ts"], original_ts)
        self.assertIn("stale_last_reloop_at", saved_ledger["k1"])
        self.assertEqual(saved_ledger["k1"]["stale_reloop_count"], 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "unassigned")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_stale_reloop_skips_excluded_non_assignee_staff(
        self, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        mock_staff.return_value = ["brian.shaw@sa.gov.au"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="kate.cook@sa.gov.au",
                ts=(datetime.now() - timedelta(hours=13)).isoformat(),
                sami_id="SAMI-104",
            )
        }

        distributor.process_stale_assignment_reloop()

        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_jira_followup_entries_are_skipped_from_stale_reloop(
        self, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        mock_staff.return_value = ["alice@test.com", "bob@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        old_ts = (datetime.now() - timedelta(hours=13)).isoformat()
        mock_load.return_value = {
            "k1::JIRA_FOLLOWUP": self._base_ledger_entry(
                assigned_to="alice@test.com",
                ts=old_ts,
            ),
            "k2": self._base_ledger_entry(
                assigned_to="bob@test.com",
                ts=old_ts,
                route="JIRA_FOLLOWUP",
            ),
        }

        distributor.process_stale_assignment_reloop()

        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_item_not_found_records_backoff_state(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        mock_known_staff.return_value = {"alice@test.com"}
        mock_staff.return_value = ["alice@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="alice@test.com",
                ts=(datetime.now() - timedelta(hours=13)).isoformat(),
            )
        }
        mock_resolve_item.return_value = None
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(saved_ledger["k1"]["stale_item_not_found_count"], 1)
        self.assertIn("stale_item_not_found_at", saved_ledger["k1"])
        mock_stats.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_ITEM_NOT_FOUND key=k1", log_messages)
        self.assertIn("STALE_RELOOP_DONE scanned=1 stale_candidates=1 changed=True", log_messages)

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_recent_item_not_found_is_skipped_until_backoff_expires(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        mock_known_staff.return_value = {"alice@test.com"}
        mock_staff.return_value = ["alice@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="alice@test.com",
                ts=(datetime.now() - timedelta(hours=13)).isoformat(),
                stale_item_not_found_at=(datetime.now() - timedelta(minutes=10)).isoformat(),
                stale_item_not_found_count=1,
            )
        }

        distributor.process_stale_assignment_reloop()

        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_DONE scanned=1 stale_candidates=0 changed=False", log_messages)

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_item_not_found_retries_after_backoff_expires(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        mock_known_staff.return_value = {"alice@test.com"}
        mock_staff.return_value = ["alice@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="alice@test.com",
                ts=(datetime.now() - timedelta(hours=13)).isoformat(),
                stale_item_not_found_at=(datetime.now() - timedelta(hours=2)).isoformat(),
                stale_item_not_found_count=1,
            )
        }
        mock_resolve_item.return_value = None
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(saved_ledger["k1"]["stale_item_not_found_count"], 2)
        mock_stats.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_ITEM_NOT_FOUND key=k1", log_messages)
        self.assertIn("STALE_RELOOP_DONE scanned=1 stale_candidates=1 changed=True", log_messages)

    @patch("distributor.log")
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_max_reloops_escalates_to_manager_review(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, mock_log
    ):
        now = datetime.now()
        mock_known_staff.return_value = {"alice@test.com", "bob@test.com"}
        mock_staff.return_value = ["alice@test.com", "bob@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="alice@test.com",
                risk="normal",
                sami_id="SAMI-102",
                stale_reloop_count=3,
                ts=(now - timedelta(hours=13)).isoformat(),
            )
        }
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        mock_resolve_item.assert_not_called()
        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "manager_review")
        self.assertEqual(mock_stats.call_args[1]["action"], "STALE_RELOOP_MAXED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "manager_review")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_processed_ledger_saved_once_per_pass(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_store_guard, mock_log
    ):
        now = datetime.now()
        mock_known_staff.return_value = {"alice@test.com", "bob@test.com", "carol@test.com"}
        mock_staff.return_value = ["alice@test.com", "bob@test.com", "carol@test.com"]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load.return_value = {
            "k1": self._base_ledger_entry(
                assigned_to="alice@test.com",
                sami_id="SAMI-201",
                ts=(now - timedelta(hours=13)).isoformat(),
            ),
            "k2": self._base_ledger_entry(
                assigned_to="bob@test.com",
                sami_id="SAMI-202",
                ts=(now - timedelta(hours=14)).isoformat(),
                entry_id="ENTRY-2",
            ),
        }
        mock_resolve_item.side_effect = [_DummyMailItem(), _DummyMailItem()]
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        self.assertEqual(mock_stats.call_count, 2)
        mock_save.assert_called_once()


class RunJobStaleIntegrationTests(unittest.TestCase):
    @patch("distributor.log")
    @patch("distributor.process_reassign_queue")
    @patch("distributor.process_stale_assignment_reloop")
    @patch("distributor.append_stats")
    @patch("distributor.process_inbox")
    def test_run_job_skips_stale_when_mailbox_unavailable(
        self, mock_inbox, mock_stats, mock_stale, mock_reassign, mock_log
    ):
        mock_stale.return_value = False

        distributor.run_job()

        mock_stale.assert_called_once()
        mock_inbox.assert_called_once()
        mock_reassign.assert_called_once()
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_SKIP_MAILBOX_UNAVAILABLE")
        self.assertEqual(mock_stats.call_args[1]["action"], "STALE_SKIP_MAILBOX_UNAVAILABLE")

    @patch("distributor.log")
    @patch("distributor.process_reassign_queue")
    @patch("distributor.process_stale_assignment_reloop")
    @patch("distributor.append_stats")
    @patch("distributor.process_inbox")
    def test_run_job_runs_stale_when_mailbox_available(
        self, mock_inbox, mock_stats, mock_stale, mock_reassign, mock_log
    ):
        mock_stale.return_value = True

        distributor.run_job()

        mock_stale.assert_called_once()
        mock_inbox.assert_called_once()
        mock_reassign.assert_called_once()
        mock_stats.assert_not_called()


if __name__ == "__main__":
    unittest.main()
