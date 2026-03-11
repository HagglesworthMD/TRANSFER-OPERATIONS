import unittest
from unittest.mock import patch

import distributor


class RunJobAutoStaleGateTests(unittest.TestCase):
    @patch("distributor.log")
    @patch("distributor.load_settings_overrides", return_value={"auto_stale_reloop_enabled": False})
    @patch("distributor.process_manual_stale_requests")
    @patch("distributor.process_reassign_queue")
    @patch("distributor.process_stale_assignment_reloop")
    @patch("distributor.append_stats")
    @patch("distributor.process_inbox")
    def test_run_job_skips_automatic_stale_when_flag_disabled(
        self, mock_inbox, mock_stats, mock_stale, mock_reassign, mock_manual_stale, _mock_overrides, mock_log
    ):
        distributor.run_job()

        mock_stale.assert_not_called()
        mock_manual_stale.assert_called_once()
        mock_inbox.assert_called_once()
        mock_reassign.assert_called_once()
        mock_stats.assert_not_called()
        log_messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("STALE_RELOOP_AUTO_DISABLED", log_messages)

    @patch("distributor.log")
    @patch("distributor.load_settings_overrides", return_value={"auto_stale_reloop_enabled": True})
    @patch("distributor.process_manual_stale_requests")
    @patch("distributor.process_reassign_queue")
    @patch("distributor.process_stale_assignment_reloop")
    @patch("distributor.append_stats")
    @patch("distributor.process_inbox")
    def test_run_job_calls_automatic_stale_when_flag_enabled(
        self, mock_inbox, mock_stats, mock_stale, mock_reassign, mock_manual_stale, _mock_overrides, mock_log
    ):
        mock_stale.return_value = True

        distributor.run_job()

        mock_stale.assert_called_once()
        mock_manual_stale.assert_called_once()
        mock_inbox.assert_called_once()
        mock_reassign.assert_called_once()
        mock_stats.assert_not_called()


if __name__ == "__main__":
    unittest.main()