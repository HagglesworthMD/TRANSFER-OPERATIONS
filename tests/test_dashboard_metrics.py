import tempfile
import unittest
from datetime import datetime, timedelta

import pandas as pd

import dashboard_core


class DashboardMetricsTests(unittest.TestCase):
    def test_completions_today_counts_rows(self):
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        rows = [
            {'Date': today.strftime('%Y-%m-%d'), 'Assigned To': 'completed', 'Subject': 'x'},
            {'Date': today.strftime('%Y-%m-%d'), 'Assigned To': 'completed', 'Subject': 'y'},
            {'Date': today.strftime('%Y-%m-%d'), 'Assigned To': 'staff@example.com', 'Subject': 'z'},
            {'Date': yesterday.strftime('%Y-%m-%d'), 'Assigned To': 'completed', 'Subject': 'old'}
        ]
        df = pd.DataFrame(rows)
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            df.to_csv(tmp.name, index=False)
            loaded = pd.read_csv(tmp.name)
        count = dashboard_core.compute_completions_today(loaded, datetime.now())
        self.assertEqual(count, 2)


if __name__ == '__main__':
    unittest.main()
