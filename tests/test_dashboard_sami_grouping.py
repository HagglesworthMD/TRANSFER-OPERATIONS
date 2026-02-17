import unittest

from dashboard.backend.kpi_engine import compute_dashboard


class DashboardSamiGroupingTests(unittest.TestCase):
    DAY = "2026-02-17"

    def _row(
        self,
        *,
        date,
        time,
        subject,
        event_type,
        assigned_to="",
        sender="requester@example.com",
        msg_key="",
        sami_id="",
        action=None,
        risk_level="normal",
        domain_bucket="external_image_request",
        assigned_ts=None,
        completed_ts=None,
    ):
        if action is None:
            if event_type == "COMPLETED":
                action = "STAFF_COMPLETED_CONFIRMATION"
            else:
                action = "IMAGE_REQUEST_EXTERNAL"
        if assigned_ts is None and event_type == "ASSIGNED":
            assigned_ts = f"{date}T{time}"
        if completed_ts is None and event_type == "COMPLETED":
            completed_ts = f"{date}T{time}"

        return {
            "Date": date,
            "Time": time,
            "Subject": subject,
            "Assigned To": assigned_to,
            "Sender": sender,
            "Risk Level": risk_level,
            "Domain Bucket": domain_bucket,
            "Action": action,
            "event_type": event_type,
            "msg_key": msg_key,
            "sami_id": sami_id,
            "assigned_to": assigned_to,
            "assigned_ts": assigned_ts or "",
            "completed_ts": completed_ts or "",
            "duration_sec": "",
        }

    def _staff_map(self, payload):
        return {row["email"]: row for row in payload["staff_kpis"]}

    def test_assigned_and_completed_same_sami_id(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Job one",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="a1",
                sami_id="SAMI-AAA111",
            ),
            self._row(
                date=self.DAY,
                time="09:30:00",
                subject="[COMPLETED] Job one",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="bob.jones@example.com",
                msg_key="a2",
                sami_id="SAMI-AAA111",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        by_email = self._staff_map(payload)
        self.assertEqual(by_email["alice.smith@example.com"]["assigned"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["completed"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["active"], 0)
        self.assertEqual(by_email["alice.smith@example.com"]["median_min"], 30.0)
        self.assertNotIn("bob.jones@example.com", by_email)

    def test_assigned_without_completed_is_active(self):
        rows = [
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="Job two",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="b1",
                sami_id="SAMI-BBB222",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        by_email = self._staff_map(payload)
        self.assertEqual(by_email["alice.smith@example.com"]["assigned"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["completed"], 0)
        self.assertEqual(by_email["alice.smith@example.com"]["active"], 1)

    def test_duplicate_completed_rows_count_once(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Job three",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="c1",
                sami_id="SAMI-CCC333",
            ),
            self._row(
                date=self.DAY,
                time="09:40:00",
                subject="[COMPLETED] Job three",
                event_type="COMPLETED",
                assigned_to="alice.smith@example.com",
                sender="alice.smith@example.com",
                msg_key="c2",
                sami_id="SAMI-CCC333",
            ),
            self._row(
                date=self.DAY,
                time="10:10:00",
                subject="[COMPLETED] Job three duplicate",
                event_type="COMPLETED",
                assigned_to="alice.smith@example.com",
                sender="alice.smith@example.com",
                msg_key="c3",
                sami_id="SAMI-CCC333",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        by_email = self._staff_map(payload)
        self.assertEqual(by_email["alice.smith@example.com"]["assigned"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["completed"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["active"], 0)
        self.assertEqual(by_email["alice.smith@example.com"]["median_min"], 40.0)

    def test_blank_sami_id_rows_ignored(self):
        rows = [
            self._row(
                date=self.DAY,
                time="12:00:00",
                subject="Job four",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="d1",
                sami_id="",
            ),
            self._row(
                date=self.DAY,
                time="12:30:00",
                subject="[COMPLETED] Job four",
                event_type="COMPLETED",
                assigned_to="alice.smith@example.com",
                sender="alice.smith@example.com",
                msg_key="d2",
                sami_id="",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        by_email = self._staff_map(payload)
        self.assertNotIn("alice.smith@example.com", by_email)

    def test_staff_active_matches_summary_for_narrow_date_range(self):
        rows = [
            self._row(
                date="2026-02-16",
                time="09:00:00",
                subject="Prior-day open job",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="e1",
                sami_id="SAMI-EEE555",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        self.assertEqual(payload["summary"]["active_count"], 0)
        self.assertEqual(sum(r["active"] for r in payload["staff_kpis"]), 0)


if __name__ == "__main__":
    unittest.main()
