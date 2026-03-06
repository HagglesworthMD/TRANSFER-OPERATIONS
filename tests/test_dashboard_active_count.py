import unittest
from unittest.mock import patch

from dashboard.backend.kpi_engine import compute_dashboard, export_active_events


class DashboardActiveCountTests(unittest.TestCase):
    DAY = "2026-02-16"

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

    def test_summary_active_matches_export_active_count(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="[SAMI-AAA111] Open ticket",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="a1",
            ),
            self._row(
                date=self.DAY,
                time="09:10:00",
                subject="[SAMI-BBB222] Will be completed",
                event_type="ASSIGNED",
                assigned_to="bob.jones@example.com",
                msg_key="b1",
            ),
            self._row(
                date=self.DAY,
                time="10:30:00",
                subject="[COMPLETED] [SAMI-BBB222] Finished",
                event_type="COMPLETED",
                assigned_to="bob.jones@example.com",
                sender="bob.jones@example.com",
                msg_key="b1",
            ),
        ]
        rec_set = set()
        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=rec_set,
        )
        expected_count = len(
            export_active_events(rows, self.DAY, self.DAY, reconciled_set=rec_set)
        )
        self.assertEqual(payload["summary"]["active_count"], expected_count)

    def test_summary_active_ignores_unrelated_reconciled_history(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="[SAMI-CCC333] Open ticket",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="c1",
            ),
        ]
        rec_set = {"SAMI-NOT-IN-ACTIVE-LIST"}
        fake_state = {
            "reconciled": [{"staff_email": "alice.smith@example.com"} for _ in range(250)]
        }

        with patch("dashboard.backend.reconciliation.load_reconciled", return_value=fake_state):
            payload = compute_dashboard(
                rows,
                roster_state=None,
                settings=None,
                staff_list=["alice.smith@example.com"],
                hib_state=None,
                date_start=self.DAY,
                date_end=self.DAY,
                reconciled_set=rec_set,
            )

        expected_count = len(
            export_active_events(rows, self.DAY, self.DAY, reconciled_set=rec_set)
        )
        self.assertEqual(payload["summary"]["active_count"], expected_count)

    def test_staff_kpi_active_matches_summary_even_with_large_reconcile_history(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="[SAMI-GGG777] Alice open",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="g1",
                sami_id="SAMI-GGG777",
            ),
            self._row(
                date=self.DAY,
                time="09:05:00",
                subject="[SAMI-HHH888] Bob open",
                event_type="ASSIGNED",
                assigned_to="bob.jones@example.com",
                msg_key="h1",
                sami_id="SAMI-HHH888",
            ),
        ]
        rec_set = {"SAMI-NOT-IN-ACTIVE-LIST"}
        fake_state = {
            "reconciled": (
                [{"staff_email": "alice.smith@example.com"} for _ in range(500)]
                + [{"staff_email": "bob.jones@example.com"} for _ in range(500)]
            )
        }
        ledger = {
            "g1": {"sami_id": "SAMI-GGG777", "assigned_to": "alice.smith@example.com"},
            "h1": {"sami_id": "SAMI-HHH888", "assigned_to": "bob.jones@example.com"},
        }

        with patch("dashboard.backend.reconciliation.load_reconciled", return_value=fake_state), \
             patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            payload = compute_dashboard(
                rows,
                roster_state=None,
                settings=None,
                staff_list=["alice.smith@example.com", "bob.jones@example.com"],
                hib_state=None,
                date_start=self.DAY,
                date_end=self.DAY,
                reconciled_set=rec_set,
            )

        by_email = {row["email"]: row["active"] for row in payload["staff_kpis"]}
        self.assertEqual(payload["summary"]["active_count"], 2)
        self.assertEqual(sum(by_email.values()), payload["summary"]["active_count"])
        self.assertEqual(by_email.get("alice.smith@example.com"), 1)
        self.assertEqual(by_email.get("bob.jones@example.com"), 1)

    def test_summary_active_staff_filter_matches_export_count(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="[SAMI-DDD444] Alice open",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="d1",
            ),
            self._row(
                date=self.DAY,
                time="09:05:00",
                subject="[SAMI-EEE555] Bob open",
                event_type="ASSIGNED",
                assigned_to="bob.jones@example.com",
                msg_key="e1",
            ),
            self._row(
                date="2026-02-15",
                time="09:15:00",
                subject="[SAMI-FFF666] Previous day",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="f1",
            ),
        ]
        staff_filter = "Alice Smith"
        rec_set = set()
        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            staff_filter=staff_filter,
            reconciled_set=rec_set,
        )
        expected_count = len(
            export_active_events(
                rows,
                self.DAY,
                self.DAY,
                staff_name=staff_filter,
                reconciled_set=rec_set,
            )
        )
        self.assertEqual(payload["summary"]["active_count"], expected_count)

    def test_non_sami_assignments_stay_active_without_sami_id(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:a1",
            ),
            self._row(
                date=self.DAY,
                time="09:10:00",
                subject="request for priors",
                event_type="ASSIGNED",
                assigned_to="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:a8",
            ),
            self._row(
                date=self.DAY,
                time="09:20:00",
                subject="Image Release",
                event_type="ASSIGNED",
                assigned_to="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:ba",
            ),
            self._row(
                date=self.DAY,
                time="09:39:00",
                subject="[COMPLETED] [SAMI-C5D1A3] Image transfer request",
                event_type="COMPLETED",
                assigned_to="brian.shaw@sa.gov.au",
                sender="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:a4",
            ),
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="[COMPLETED] [SAMI-CC567E] request for priors",
                event_type="COMPLETED",
                assigned_to="brian.shaw@sa.gov.au",
                sender="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:b0",
            ),
            self._row(
                date=self.DAY,
                time="10:53:00",
                subject="[COMPLETED] [SAMI-7F03FC] Image Release",
                event_type="COMPLETED",
                assigned_to="brian.shaw@sa.gov.au",
                sender="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:bd",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["brian.shaw@sa.gov.au"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )
        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())

        self.assertEqual(len(active_rows), 3)
        self.assertEqual(payload["summary"]["active_count"], 3)
        self.assertEqual(payload["staff_kpis"], [])

    def test_sami_id_groups_assignment_and_completion_when_msg_key_differs(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Open request one",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="legacy-a1",
                sami_id="SAMI-AB12CD",
            ),
            self._row(
                date=self.DAY,
                time="09:25:00",
                subject="[COMPLETED] Closed request one",
                event_type="COMPLETED",
                assigned_to="alice.smith@example.com",
                sender="alice.smith@example.com",
                msg_key="legacy-c9",
                sami_id="SAMI-AB12CD",
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
        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())

        self.assertEqual(len(active_rows), 0)
        self.assertEqual(payload["summary"]["active_count"], 0)
        staff_row = payload["staff_kpis"][0]
        self.assertEqual(staff_row["assigned"], 1)
        self.assertEqual(staff_row["completed"], 1)
        self.assertEqual(staff_row["active"], 0)

    def test_blank_sami_id_does_not_close_via_legacy_fallback(self):
        rows = [
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="legacy-a1",
                sami_id="",
            ),
            self._row(
                date=self.DAY,
                time="10:25:00",
                subject="[COMPLETED] Image transfer request",
                event_type="COMPLETED",
                assigned_to="alice.smith@example.com",
                sender="alice.smith@example.com",
                msg_key="legacy-c9",
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
        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())

        self.assertEqual(len(active_rows), 1)
        self.assertEqual(payload["summary"]["active_count"], 1)

    def test_assigned_sami_id_is_exposed_in_active_and_activity_feed(self):
        rows = [
            self._row(
                date=self.DAY,
                time="11:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="legacy-z1",
                sami_id="SAMI-XYZ123",
            ),
        ]
        ledger = {
            "legacy-z1": {"sami_id": "SAMI-XYZ123", "assigned_to": "alice.smith@example.com"},
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        self.assertEqual(len(active_rows), 1)
        self.assertEqual(active_rows[0]["SAMI Ref"], "SAMI-XYZ123")

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
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
        self.assertEqual(payload["activity_feed"][0]["sami_ref"], "SAMI-XYZ123")

    def test_completed_subject_sami_does_not_close_when_completed_sami_id_mismatches(self):
        rows = [
            self._row(
                date=self.DAY,
                time="12:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="brian.shaw@sa.gov.au",
                msg_key="legacy-b1",
                sami_id="SAMI-49764E",
            ),
            self._row(
                date=self.DAY,
                time="12:45:00",
                subject="[COMPLETED] [SAMI-49764E] Image transfer request",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="brian.shaw@sa.gov.au",
                msg_key="legacy-c1",
                sami_id="SAMI-2CFB3C",
            ),
        ]
        ledger = {
            "legacy-b1": {"sami_id": "SAMI-49764E", "assigned_to": "brian.shaw@sa.gov.au"},
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        self.assertEqual(len(active_rows), 1)

    def test_stale_reloop_updates_active_owner(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="john.drousas@sa.gov.au",
                msg_key="stale-1",
                sami_id="SAMI-B2F9FB",
            ),
            self._row(
                date=self.DAY,
                time="11:43:55",
                subject="STALE_RELOOP key=stale-1",
                event_type="STALE_RELOOP",
                assigned_to="prav.mudaliar@sa.gov.au",
                sender="system",
                msg_key="stale-1",
                sami_id="SAMI-B2F9FB",
                action="STALE_RELOOP",
                assigned_ts=f"{self.DAY}T11:43:55",
            ),
        ]
        ledger = {
            "stale-1": {"sami_id": "SAMI-B2F9FB", "assigned_to": "prav.mudaliar@sa.gov.au"},
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        self.assertEqual(len(active_rows), 1)
        self.assertEqual(active_rows[0]["SAMI Ref"], "SAMI-B2F9FB")
        self.assertEqual(active_rows[0]["Staff Email"], "prav.mudaliar@sa.gov.au")

    def test_filter_jones_completion_closes_active_sami(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="john.drousas@sa.gov.au",
                msg_key="fjc-1",
                sami_id="SAMI-FJC001",
            ),
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="FILTER_JONES_COMPLETION key=fjc-1",
                event_type="FILTER_JONES_COMPLETION",
                assigned_to="system",
                sender="system",
                msg_key="fjc-1",
                sami_id="SAMI-FJC001",
                action="FILTER_JONES_COMPLETION",
            ),
        ]

        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        self.assertEqual(len(active_rows), 0)

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["john.drousas@sa.gov.au"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )
        self.assertEqual(payload["summary"]["active_count"], 0)

    def test_ledger_closed_assignment_is_removed_from_active_rows(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="john.drousas@sa.gov.au",
                msg_key="ledger-1",
                sami_id="SAMI-LEDGER1",
            ),
        ]
        ledger = {
            "ledger-key-1": {
                "sami_id": "SAMI-LEDGER1",
                "assigned_to": "",
            }
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
            self.assertEqual(len(active_rows), 0)

    def test_active_assignment_before_date_start_is_excluded(self):
        rows = [
            self._row(
                date="2026-02-28",
                time="09:00:00",
                subject="Old open job",
                event_type="ASSIGNED",
                assigned_to="john.drousas@sa.gov.au",
                msg_key="old-1",
                sami_id="SAMI-OLD001",
            ),
        ]
        ledger = {
            "old-1": {"sami_id": "SAMI-OLD001", "assigned_to": "john.drousas@sa.gov.au"},
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            active_rows = export_active_events(rows, "2026-03-05", "2026-03-05", reconciled_set=set())
            self.assertEqual(len(active_rows), 0)

    def test_stale_reloop_without_sami_is_excluded_from_active(self):
        rows = [
            self._row(
                date=self.DAY,
                time="11:43:55",
                subject="STALE_RELOOP key=store:0000000038A1BB100...",
                event_type="STALE_RELOOP",
                assigned_to="hannah.cutting@sa.gov.au",
                sender="system",
                msg_key="store:abc|entry:xyz",
                sami_id="",
                action="STALE_RELOOP",
                assigned_ts=f"{self.DAY}T11:43:55",
            ),
        ]

        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        self.assertEqual(len(active_rows), 0)

    def test_manual_stale_release_closes_non_sami_active_row_by_identity(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="brian.shaw@sa.gov.au",
                msg_key="store:abc|entry:a1",
            ),
            self._row(
                date=self.DAY,
                time="09:30:00",
                subject="MANUAL_STALE_RELEASE key=store:abc|entry:a1",
                event_type="MANUAL_STALE_RELEASE",
                assigned_to="unassigned",
                sender="dashboard_admin",
                msg_key="store:abc|entry:a1",
                action="MANUAL_STALE_RELEASE",
                assigned_ts=f"{self.DAY}T09:30:00",
            ),
        ]

        active_rows = export_active_events(rows, self.DAY, self.DAY, reconciled_set=set())
        staff_filtered = export_active_events(
            rows,
            self.DAY,
            self.DAY,
            staff_name="Brian Shaw",
            reconciled_set=set(),
        )
        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["brian.shaw@sa.gov.au"],
            hib_state=None,
            date_start=self.DAY,
            date_end=self.DAY,
            reconciled_set=set(),
        )

        self.assertEqual(len(active_rows), 0)
        self.assertEqual(len(staff_filtered), 0)
        self.assertEqual(payload["summary"]["active_count"], 0)

    def test_staff_filter_applies_after_ledger_owner_override(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Image transfer request",
                event_type="ASSIGNED",
                assigned_to="john.drousas@sa.gov.au",
                msg_key="ledger-owner-1",
                sami_id="SAMI-LEDGEROVR1",
            ),
        ]
        ledger = {
            "ledger-owner-1": {"sami_id": "SAMI-LEDGEROVR1", "assigned_to": "prav.mudaliar@sa.gov.au"},
        }

        with patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            john_rows = export_active_events(
                rows,
                self.DAY,
                self.DAY,
                staff_name="John Drousas",
                reconciled_set=set(),
            )
            prav_rows = export_active_events(
                rows,
                self.DAY,
                self.DAY,
                staff_name="Prav Mudaliar",
                reconciled_set=set(),
            )

        self.assertEqual(len(john_rows), 0)
        self.assertEqual(len(prav_rows), 1)
        self.assertEqual(prav_rows[0]["Staff Email"], "prav.mudaliar@sa.gov.au")

if __name__ == "__main__":
    unittest.main()
