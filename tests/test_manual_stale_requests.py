import unittest
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


class ManualStaleRequestTests(unittest.TestCase):
    def _queue_entry(self, **overrides):
        entry = {
            "request_id": "manual-stale-1",
            "request_key": "msg:store:abc|entry:a1",
            "msg_key": "store:abc|entry:a1",
            "sami_id": "SAMI-AAA111",
            "reason": "",
            "requested_by": "dashboard_admin",
            "requested_ts": "2026-03-06T10:00:00+00:00",
        }
        entry.update(overrides)
        return entry

    def _ledger_entry(self, **overrides):
        entry = {
            "assigned_to": "hannah.cutting@sa.gov.au",
            "risk": "normal",
            "sami_id": "SAMI-AAA111",
            "ts": "2026-03-06T08:00:00",
            "entry_id": "ENTRY-1",
            "store_id": "STORE-1",
        }
        entry.update(overrides)
        return entry

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.append_stats")
    @patch("distributor.get_next_staff", return_value="alex@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_manual_stale_release_reassigns_in_place_and_consumes_request(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_resolve_item,
        _mock_forward,
        _mock_next_staff,
        mock_stats,
        mock_save_ledger,
        mock_atomic_write,
        mock_log,
    ):
        processed = _DummyInbox()
        item = _DummyMailItem()
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (object(), processed, "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_resolve_item.return_value = item
        mock_save_ledger.return_value = True
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()

        saved_ledger = mock_save_ledger.call_args[0][0]
        self.assertFalse(item.UnRead)
        self.assertIsNone(item.moved_to)
        self.assertEqual(saved_ledger["store:abc|entry:a1"]["assigned_to"], "alex@test.com")
        self.assertEqual(saved_ledger["store:abc|entry:a1"]["stale_last_owner"], "hannah.cutting@sa.gov.au")
        self.assertIn("stale_last_reloop_at", saved_ledger["store:abc|entry:a1"])
        self.assertNotIn("stale_reloop_count", saved_ledger["store:abc|entry:a1"])
        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "assigned")
        self.assertEqual(mock_atomic_write.call_args[0][0], distributor.MANUAL_STALE_REQUESTS_PATH)
        self.assertEqual(mock_atomic_write.call_args[0][1], {})
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("MANUAL_STALE_RELEASE_OK request_id=manual-stale-1 key=store:abc|entry:a1 source=processed action=stale_reassign_in_place old_owner=hannah.cutting@sa.gov.au new_owner=alex@test.com", log_messages)

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.append_stats")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_completed_item_is_skipped_and_request_consumed(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_stats,
        mock_save_ledger,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load_ledger.return_value = {
            "store:abc|entry:a1": self._ledger_entry(completed_at="2026-03-06T09:00:00")
        }
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()

        mock_save_ledger.assert_not_called()
        self.assertEqual(mock_atomic_write.call_args[0][1], {})
        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE_SKIPPED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "completed")
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("MANUAL_STALE_SKIP request_id=manual-stale-1 key=store:abc|entry:a1 reason=completed", log_messages)

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.append_stats")
    @patch("distributor.get_next_staff", return_value="alex@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_item_not_found_is_consumed_once_and_not_retried(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_resolve_item,
        _mock_forward,
        _mock_next_staff,
        mock_stats,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.side_effect = [
            {"msg:store:abc|entry:a1": self._queue_entry()},
            {},
        ]
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_resolve_item.return_value = None
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()
        distributor.process_manual_stale_requests()

        self.assertEqual(mock_resolve_item.call_count, 1)
        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE_SKIPPED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "item_not_found")
        self.assertEqual(mock_atomic_write.call_args_list[0][0][1], {})

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.append_stats")
    @patch("distributor.get_staff_list", return_value=[])
    @patch("distributor.load_processed_ledger")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_global_no_staff_available_consumes_requests_with_request_level_skip_audit(
        self,
        _mock_exists,
        mock_safe_load,
        mock_load_ledger,
        _mock_staff_list,
        mock_stats,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()

        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE_SKIPPED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "no_staff_available")
        self.assertEqual(mock_atomic_write.call_args[0][1], {})
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn(
            "MANUAL_STALE_SKIP request_id=manual-stale-1 key=store:abc|entry:a1 source=processed reason=no_staff_available",
            log_messages,
        )

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.append_stats")
    @patch("distributor.get_next_staff", return_value=None)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_no_staff_available_is_consumed_with_request_level_skip_audit(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_resolve_item,
        _mock_next_staff,
        mock_stats,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_resolve_item.return_value = _DummyMailItem()
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()

        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE_SKIPPED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "no_staff_available")
        self.assertEqual(mock_atomic_write.call_args[0][1], {})
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn(
            "MANUAL_STALE_SKIP request_id=manual-stale-1 key=store:abc|entry:a1 source=processed reason=no_staff_available",
            log_messages,
        )

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.append_stats")
    @patch("distributor.get_next_staff", return_value="alex@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=False)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_reassign_failure_is_consumed_with_request_level_skip_audit(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_resolve_item,
        _mock_forward,
        _mock_next_staff,
        mock_stats,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (object(), _DummyInbox(), "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_resolve_item.return_value = _DummyMailItem()
        mock_atomic_write.return_value = True

        distributor.process_manual_stale_requests()

        self.assertEqual(mock_stats.call_args[1]["event_type"], "MANUAL_STALE_RELEASE_SKIPPED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "reassign_failed")
        self.assertEqual(mock_atomic_write.call_args[0][1], {})
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn(
            "MANUAL_STALE_SKIP request_id=manual-stale-1 key=store:abc|entry:a1 source=processed reason=reassign_failed",
            log_messages,
        )

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.append_stats")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_mailbox_unavailable_leaves_request_pending(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_stats,
        mock_save_ledger,
        mock_atomic_write,
        mock_log,
    ):
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (None, None, "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}

        distributor.process_manual_stale_requests()

        mock_stats.assert_not_called()
        mock_save_ledger.assert_not_called()
        mock_atomic_write.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("MANUAL_STALE_SKIP reason=mailbox_unavailable pending=1", log_messages)

    @patch("distributor.log")
    @patch("distributor.atomic_write_json")
    @patch("distributor.save_processed_ledger", return_value=False)
    @patch("distributor.append_stats")
    @patch("distributor.get_next_staff", return_value="alex@test.com")
    @patch("distributor._forward_stale_reassign_in_place", return_value=True)
    @patch("distributor._resolve_mailitem_from_ledger_entry")
    @patch("distributor.load_processed_ledger")
    @patch("distributor._resolve_stale_reloop_runtime")
    @patch("distributor.safe_load_json")
    @patch("distributor.os.path.exists", return_value=True)
    def test_ledger_save_failure_preserves_queue_and_suppresses_success_audit(
        self,
        _mock_exists,
        mock_safe_load,
        mock_runtime,
        mock_load_ledger,
        mock_resolve_item,
        _mock_forward,
        _mock_next_staff,
        mock_stats,
        mock_save_ledger,
        mock_atomic_write,
        mock_log,
    ):
        processed = _DummyInbox()
        item = _DummyMailItem()
        mock_safe_load.return_value = {"msg:store:abc|entry:a1": self._queue_entry()}
        mock_runtime.return_value = (object(), processed, "")
        mock_load_ledger.return_value = {"store:abc|entry:a1": self._ledger_entry()}
        mock_resolve_item.return_value = item

        distributor.process_manual_stale_requests()

        self.assertFalse(item.UnRead)
        self.assertIsNone(item.moved_to)
        mock_save_ledger.assert_called_once()
        mock_atomic_write.assert_not_called()
        mock_stats.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STATE_WRITE_FAIL state=processed_ledger", log_messages)
        self.assertIn("MANUAL_STALE_ABORT reason=ledger_save_failed requested=1 released=1 skipped=0", log_messages)


if __name__ == "__main__":
    unittest.main()
