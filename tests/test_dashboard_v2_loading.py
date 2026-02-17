import unittest
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


if __name__ == "__main__":
    unittest.main()
