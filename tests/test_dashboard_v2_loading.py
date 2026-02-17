import unittest
import asyncio
from pathlib import Path
import shutil
import uuid
from unittest.mock import patch

from dashboard.backend import server


class DashboardV2LoadingTests(unittest.TestCase):
    def _make_temp_dir(self) -> Path:
        base = Path(f"tmp_dashboard_v2_{uuid.uuid4().hex}")
        base.mkdir(parents=True, exist_ok=False)
        self.addCleanup(lambda: shutil.rmtree(base, ignore_errors=True))
        return base

    def test_prefers_v2_when_present(self):
        base = self._make_temp_dir()
        legacy = base / "daily_stats.csv"
        v2 = base / "daily_stats_v2.csv"
        legacy.write_text("Date,Time\n", encoding="utf-8")
        v2.write_text("Date,Time,sami_id\n", encoding="utf-8")

        with patch.object(server.config, "DAILY_STATS_CSV", legacy), patch.object(
            server.config, "DAILY_STATS_V2_CSV", v2
        ):
            self.assertEqual(server._resolve_stats_csv_path(), v2)

    def test_falls_back_to_legacy_when_v2_missing(self):
        base = self._make_temp_dir()
        legacy = base / "daily_stats.csv"
        v2 = base / "daily_stats_v2.csv"
        legacy.write_text("Date,Time\n", encoding="utf-8")

        with patch.object(server.config, "DAILY_STATS_CSV", legacy), patch.object(
            server.config, "DAILY_STATS_V2_CSV", v2
        ):
            self.assertEqual(server._resolve_stats_csv_path(), legacy)

    def test_dashboard_does_not_emit_sami_mismatch_warning(self):
        rows = [
            {
                "Date": "2026-02-17",
                "Time": "10:00:00",
                "Subject": "Image transfer request",
                "event_type": "ASSIGNED",
                "assigned_to": "brian.shaw@sa.gov.au",
                "Sender": "requester@example.com",
                "sami_id": "SAMI-ABC123",
            },
            {
                "Date": "2026-02-17",
                "Time": "10:15:00",
                "Subject": "[COMPLETED] [SAMI-ABC123] done",
                "event_type": "COMPLETED",
                "Action": "STAFF_COMPLETED_CONFIRMATION",
                "assigned_to": "completed",
                "Sender": "brian.shaw@sa.gov.au",
                "sami_id": "SAMI-XYZ999",
            },
        ]

        with patch("dashboard.backend.server.load_csv", return_value=(rows, None)), patch(
            "dashboard.backend.server.load_json", return_value=(None, None)
        ), patch("dashboard.backend.server._load_staff_json", return_value=({"staff": []}, None)), patch(
            "dashboard.backend.server.load_reconciled_set", return_value=set()
        ):
            payload = asyncio.run(server.dashboard_endpoint(date_start="2026-02-17", date_end="2026-02-17"))

        self.assertNotIn("warning", payload)


if __name__ == "__main__":
    unittest.main()
