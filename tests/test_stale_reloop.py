import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import distributor


class _DummyProcessed:
    Name = "Processed"


class _DummyMailItem:
    def __init__(self):
        self.UnRead = False
        self.moved_to = None

    def Move(self, folder):
        self.moved_to = folder
        return self


class StaleReloopTests(unittest.TestCase):
    def test_normal_assignment_business_context_allows_weekday_in_window(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 3, 10, 9, 0))
        self.assertTrue(allowed)
        self.assertIsNone(reason)

    def test_normal_assignment_business_context_blocks_before_open(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 3, 10, 8, 29))
        self.assertFalse(allowed)
        self.assertEqual(reason, "outside_hours")

    def test_normal_assignment_business_context_blocks_at_close(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 3, 10, 17, 0))
        self.assertFalse(allowed)
        self.assertEqual(reason, "outside_hours")

    def test_normal_assignment_business_context_blocks_weekend(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 3, 14, 10, 0))
        self.assertFalse(allowed)
        self.assertEqual(reason, "outside_hours")

    def test_normal_assignment_business_context_blocks_march_holiday(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 3, 9, 10, 0))
        self.assertFalse(allowed)
        self.assertEqual(reason, "public_holiday")

    def test_normal_assignment_business_context_blocks_october_holiday(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 10, 5, 10, 0))
        self.assertFalse(allowed)
        self.assertEqual(reason, "public_holiday")

    def test_normal_assignment_business_context_allows_christmas_eve_daytime(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 12, 24, 10, 0))
        self.assertTrue(allowed)
        self.assertIsNone(reason)

    def test_normal_assignment_business_context_allows_new_years_eve_daytime(self):
        _now_local, allowed, reason = distributor._get_normal_assignment_business_context(datetime(2026, 12, 31, 10, 0))
        self.assertTrue(allowed)
        self.assertIsNone(reason)

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
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 11, 10, 0), True, None))
    @patch("distributor._get_next_stale_staff_excluding_owner", return_value="a@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_item_reassigns_in_place_and_updates_ledger(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_next_staff, _mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"z@test.com", "a@test.com", "m@test.com"}
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        processed = _DummyProcessed()
        dummy_item = _DummyMailItem()
        mock_runtime.return_value = (object(), processed, "")
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
        self.assertIsNone(dummy_item.moved_to)
        self.assertFalse(dummy_item.UnRead)
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "a@test.com")
        self.assertEqual(saved_ledger["k1"]["ts"], original_ts)
        self.assertIn("stale_last_reloop_at", saved_ledger["k1"])
        self.assertEqual(saved_ledger["k1"]["stale_reloop_count"], 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "a@test.com")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 10, 18, 30), False, "outside_business_hours"))
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_reloop_deferred_after_hours_leaves_state_unchanged(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"z@test.com", "a@test.com", "m@test.com"}
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
        original_entry = self._base_ledger_entry(
            assigned_to="m@test.com",
            sami_id="SAMI-301",
        )
        mock_load.return_value = {"k1": dict(original_entry)}

        distributor.process_stale_assignment_reloop()

        self.assertEqual(mock_load.return_value["k1"], original_entry)
        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_DEFERRED reason=outside_business_hours key=k1", log_messages)

    @patch("distributor.log")
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 14, 10, 0), False, "weekend"))
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_reloop_deferred_on_weekend_leaves_state_unchanged(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"z@test.com", "a@test.com", "m@test.com"}
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
        original_entry = self._base_ledger_entry(
            assigned_to="m@test.com",
            sami_id="SAMI-302",
        )
        mock_load.return_value = {"k1": dict(original_entry)}

        distributor.process_stale_assignment_reloop()

        self.assertEqual(mock_load.return_value["k1"], original_entry)
        mock_resolve_item.assert_not_called()
        mock_stats.assert_not_called()
        mock_save.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_DEFERRED reason=weekend key=k1", log_messages)

    @patch("distributor.log")
    @patch("distributor._get_stale_reloop_business_context")
    @patch("distributor._get_next_stale_staff_excluding_owner", return_value="a@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_stale_reloop_deferred_after_hours_then_reloops_next_business_tick(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_next_staff, mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"z@test.com", "a@test.com", "m@test.com"}
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
        dummy_item = _DummyMailItem()
        original_ts = (datetime.now() - timedelta(hours=13)).isoformat()
        ledger = {
            "k1": self._base_ledger_entry(
                ts=original_ts,
                assigned_to="m@test.com",
                sami_id="SAMI-303",
            )
        }
        mock_load.return_value = ledger
        mock_business_hours.side_effect = [
            (datetime(2026, 3, 13, 18, 30), False, "outside_business_hours"),
            (datetime(2026, 3, 16, 9, 0), True, None),
        ]
        mock_resolve_item.return_value = dummy_item
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        self.assertEqual(ledger["k1"]["assigned_to"], "m@test.com")
        self.assertEqual(ledger["k1"]["ts"], original_ts)
        self.assertNotIn("stale_last_reloop_at", ledger["k1"])
        mock_resolve_item.assert_not_called()
        mock_save.assert_not_called()

        distributor.process_stale_assignment_reloop()

        self.assertEqual(ledger["k1"]["assigned_to"], "a@test.com")
        self.assertEqual(ledger["k1"]["ts"], original_ts)
        self.assertIn("stale_last_reloop_at", ledger["k1"])
        self.assertEqual(ledger["k1"]["stale_reloop_count"], 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "a@test.com")
        self.assertEqual(mock_resolve_item.call_count, 1)
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 11, 10, 0), True, None))
    @patch("distributor._get_next_stale_staff_excluding_owner", return_value="brian.shaw@sa.gov.au")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_stale_reloop_uses_known_staff_when_assignee_is_off_rotation(
        self, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_next_staff, _mock_business_hours, mock_log
    ):
        mock_staff.return_value = ["brian.shaw@sa.gov.au"]
        processed = _DummyProcessed()
        dummy_item = _DummyMailItem()
        original_ts = (datetime.now() - timedelta(hours=13)).isoformat()
        mock_runtime.return_value = (object(), processed, "")
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
        self.assertIsNone(dummy_item.moved_to)
        self.assertFalse(dummy_item.UnRead)
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "brian.shaw@sa.gov.au")
        self.assertEqual(saved_ledger["k1"]["ts"], original_ts)
        self.assertIn("stale_last_reloop_at", saved_ledger["k1"])
        self.assertEqual(saved_ledger["k1"]["stale_reloop_count"], 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "brian.shaw@sa.gov.au")
        mock_save.assert_called_once()

    @patch("distributor.save_roster_state")
    @patch("distributor.get_roster_state", return_value={"current_index": 0, "total_processed": 0})
    @patch("distributor.get_staff_list", return_value=["alice@test.com", "bob@test.com"])
    def test_stale_owner_skip_helper_skips_current_owner_when_alternative_exists(
        self, _mock_staff, mock_state, mock_save
    ):
        picked = distributor._get_next_stale_staff_excluding_owner("alice@test.com")

        self.assertEqual(picked, "bob@test.com")
        mock_save.assert_called_once_with({"current_index": 2, "total_processed": 1})

    @patch("distributor.save_roster_state")
    @patch("distributor.get_roster_state", return_value={"current_index": 0, "total_processed": 0})
    @patch("distributor.get_staff_list", return_value=["alice@test.com"])
    def test_stale_owner_skip_helper_allows_same_owner_when_only_staff_available(
        self, _mock_staff, mock_state, mock_save
    ):
        picked = distributor._get_next_stale_staff_excluding_owner("alice@test.com")

        self.assertEqual(picked, "alice@test.com")
        mock_save.assert_called_once_with({"current_index": 1, "total_processed": 1})

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
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 11, 10, 0), True, None))
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_item_not_found_records_backoff_state(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"alice@test.com"}
        mock_staff.return_value = ["alice@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 11, 10, 0), True, None))
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_item_not_found_retries_after_backoff_expires(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_business_hours, mock_log
    ):
        mock_known_staff.return_value = {"alice@test.com"}
        mock_staff.return_value = ["alice@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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
    @patch("distributor._get_stale_reloop_business_context", return_value=(datetime(2026, 3, 11, 10, 0), True, None))
    @patch("distributor.get_next_staff", return_value="a@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    @patch("distributor._get_known_staff_for_stale_reloop")
    def test_processed_ledger_saved_once_per_pass(
        self, mock_known_staff, mock_staff, mock_load, mock_save, mock_stats, mock_runtime, mock_resolve_item, _mock_forward, _mock_next_staff, _mock_business_hours, mock_log
    ):
        now = datetime.now()
        mock_known_staff.return_value = {"alice@test.com", "bob@test.com", "carol@test.com"}
        mock_staff.return_value = ["alice@test.com", "bob@test.com", "carol@test.com"]
        mock_runtime.return_value = (object(), _DummyProcessed(), "")
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


class _DummyRecipients:
    def Add(self, _value):
        return self

    def ResolveAll(self):
        return True


class _DummyForward:
    def __init__(self):
        self.Recipients = _DummyRecipients()
        self.Body = "Original"
        self.Subject = ""
        self.SentOnBehalfOfName = None

    def Send(self):
        self.sent = True


class _DummyHotlinkMessage:
    Subject = "Image transfer request"
    SenderEmailAddress = "requester@example.com"
    EntryID = "ENTRY-1"
    ConversationID = "CONV-1"

    def Forward(self):
        self.forward = _DummyForward()
        return self.forward


class SafeModeTests(unittest.TestCase):
    def test_live_sami_production_target_is_not_treated_as_test_folder(self):
        with patch.dict("os.environ", {"TRANSFER_BOT_LIVE": "true"}, clear=False):
            is_safe, reason, override_active = distributor.determine_safe_mode(
                "Inbox",
                "Health:SAMISupportTeam",
                "Inbox/02_PROCESSED",
            )

        self.assertFalse(is_safe)
        self.assertEqual(reason, "live_mode_armed")
        self.assertFalse(override_active)

    def test_test_folder_is_not_suppressed_anymore(self):
        with patch.dict("os.environ", {"TRANSFER_BOT_LIVE": "true", "TRANSFER_BOT_ALLOW_TEST_FOLDER": ""}, clear=False):
            is_safe, reason, override_active = distributor.determine_safe_mode(
                "Transfer Bot Test Received",
                "Brian.Shaw@sa.gov.au",
                "Transfer Bot Test",
            )

        self.assertFalse(is_safe)
        self.assertEqual(reason, "live_mode_armed")
        self.assertFalse(override_active)


class StaleReassignHelperTests(unittest.TestCase):
    @patch("distributor.log")
    @patch("distributor.is_safe_mode", return_value=(False, ""))
    @patch("distributor.inject_completion_hotlink", side_effect=RuntimeError("boom"))
    @patch("distributor.check_msg_mailbox_store", return_value=(True, "UnitTest Mailbox"))
    def test_hotlink_failure_does_not_block_stale_reassign_send(
        self, _mock_store, _mock_hotlink, _mock_safe_mode, mock_log
    ):
        msg = _DummyHotlinkMessage()
        entry = {"sami_id": "SAMI-123456"}

        ok = distributor._forward_stale_reassign_in_place(
            msg,
            entry,
            "staff.one@sa.gov.au",
            ["staff.one@sa.gov.au"],
            "",
        )

        self.assertTrue(ok)
        self.assertTrue(getattr(msg.forward, "sent", False))
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertTrue(any("COMPLETION_HOTLINK_FAIL context=stale_reassign" in message for message in log_messages))


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

