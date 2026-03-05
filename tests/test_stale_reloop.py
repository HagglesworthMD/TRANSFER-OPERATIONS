import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import distributor


class StaleReloopTests(unittest.TestCase):
    @patch("distributor.log")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_stale_clock_uses_latest_touch(self, mock_staff, mock_load, mock_save, mock_stats, mock_log):
        now = datetime.now()
        mock_staff.return_value = ["alice@test.com", "bob@test.com"]
        mock_load.return_value = {
            "k1": {
                "assigned_to": "alice@test.com",
                "risk": "normal",
                "sami_id": "SAMI-100",
                "ts": (now - timedelta(hours=20)).isoformat(),
                "stale_last_reloop_at": (now - timedelta(hours=1)).isoformat(),
            }
        }
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        mock_stats.assert_not_called()
        mock_save.assert_not_called()

    @patch("distributor.log")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_deterministic_next_uses_sorted_staff(self, mock_staff, mock_load, mock_save, mock_stats, mock_log):
        now = datetime.now()
        mock_staff.return_value = ["z@test.com", "a@test.com", "m@test.com"]
        mock_load.return_value = {
            "k1": {
                "assigned_to": "m@test.com",
                "risk": "normal",
                "sami_id": "SAMI-101",
                "ts": (now - timedelta(hours=13)).isoformat(),
            }
        }
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "z@test.com")
        self.assertEqual(mock_stats.call_args[1]["event_type"], "STALE_RELOOP")
        self.assertEqual(mock_stats.call_args[1]["assigned_to"], "z@test.com")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_max_reloops_escalates_to_manager_review(self, mock_staff, mock_load, mock_save, mock_stats, mock_log):
        now = datetime.now()
        mock_staff.return_value = ["alice@test.com", "bob@test.com"]
        mock_load.return_value = {
            "k1": {
                "assigned_to": "alice@test.com",
                "risk": "normal",
                "sami_id": "SAMI-102",
                "stale_reloop_count": 3,
                "ts": (now - timedelta(hours=13)).isoformat(),
            }
        }
        mock_save.return_value = True

        distributor.process_stale_assignment_reloop()

        saved_ledger = mock_save.call_args[0][0]
        self.assertEqual(saved_ledger["k1"]["assigned_to"], "manager_review")
        self.assertEqual(mock_stats.call_args[1]["action"], "STALE_RELOOP_MAXED")
        self.assertEqual(mock_stats.call_args[1]["status_after"], "manager_review")
        mock_save.assert_called_once()

    @patch("distributor.log")
    @patch("distributor.append_stats")
    @patch("distributor.save_processed_ledger")
    @patch("distributor.load_processed_ledger")
    @patch("distributor.get_staff_list")
    def test_processed_ledger_saved_once_per_pass(self, mock_staff, mock_load, mock_save, mock_stats, mock_log):
        now = datetime.now()
        mock_staff.return_value = ["alice@test.com", "bob@test.com", "carol@test.com"]
        mock_load.return_value = {
            "k1": {
                "assigned_to": "alice@test.com",
                "risk": "normal",
                "sami_id": "SAMI-201",
                "ts": (now - timedelta(hours=13)).isoformat(),
            },
            "k2": {
                "assigned_to": "bob@test.com",
                "risk": "normal",
                "sami_id": "SAMI-202",
                "ts": (now - timedelta(hours=14)).isoformat(),
            },
        }
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
        distributor._mailbox_resolution_ok_last_tick = False

        distributor.run_job()

        mock_stale.assert_not_called()
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
        distributor._mailbox_resolution_ok_last_tick = True

        distributor.run_job()

        mock_stale.assert_called_once()
        mock_reassign.assert_called_once()
        mock_stats.assert_not_called()


if __name__ == "__main__":
    unittest.main()
