import unittest
from unittest.mock import patch

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
        ledger = {
            "b1": {"sami_id": "SAMI-BBB222", "assigned_to": "alice.smith@example.com"},
        }

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
        ledger = {
            "e1": {"sami_id": "SAMI-EEE555", "assigned_to": "alice.smith@example.com"},
        }

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

        self.assertEqual(payload["summary"]["active_count"], 1)
        self.assertEqual(sum(r["active"] for r in payload["staff_kpis"]), 1)



    def test_summary_flags_unmatched_completion_jobs(self):
        rows = [
            self._row(
                date=self.DAY,
                time="14:00:00",
                subject="[COMPLETED] orphan completion",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="alice.smith@example.com",
                msg_key="u1",
                sami_id="SAMI-UNMATCH1",
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

        self.assertEqual(payload["summary"]["completions_today"], 0)
        self.assertEqual(payload["summary"]["completions_matched"], 0)
        self.assertEqual(payload["summary"]["completions_unmatched"], 1)

    def test_processed_counts_unique_sami_assignments(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Duplicate assignment A",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="p1",
                sami_id="SAMI-PROC01",
            ),
            self._row(
                date=self.DAY,
                time="09:05:00",
                subject="Duplicate assignment B",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="p2",
                sami_id="SAMI-PROC01",
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

        self.assertEqual(payload["summary"]["processed_today"], 1)
        self.assertEqual(payload["summary"]["completions_today"], 0)

    def test_reassigned_job_follows_new_owner_in_staff_kpis(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Job reassign",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="r1",
                sami_id="SAMI-REASN1",
            ),
            self._row(
                date="2026-02-18",
                time="10:00:00",
                subject="REASSIGN: SAMI-REASN1 alice.smith@example.com -> bob.jones@example.com",
                event_type="REASSIGN_MANUAL",
                assigned_to="bob.jones@example.com",
                sender="dashboard_admin",
                msg_key="",
                sami_id="SAMI-REASN1",
                action="JIRA_FOLLOWUP",
                assigned_ts="2026-02-18T10:00:00",
            ),
            self._row(
                date="2026-02-18",
                time="11:00:00",
                subject="[COMPLETED] Job reassign",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="bob.jones@example.com",
                msg_key="r2",
                sami_id="SAMI-REASN1",
                completed_ts="2026-02-18T11:00:00",
            ),
        ]

        payload = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start=self.DAY,
            date_end="2026-02-18",
            reconciled_set=set(),
        )

        by_email = self._staff_map(payload)
        self.assertNotIn("alice.smith@example.com", by_email)
        self.assertEqual(by_email["bob.jones@example.com"]["assigned"], 1)
        self.assertEqual(by_email["bob.jones@example.com"]["assigned_in_range"], 1)
        self.assertEqual(by_email["bob.jones@example.com"]["completed"], 1)
        self.assertEqual(by_email["bob.jones@example.com"]["active"], 0)


    def test_non_jira_followup_is_not_counted_as_jira_followups(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Direct assignment",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="dir-1",
                sami_id="SAMI-DIRECT1",
            ),
            self._row(
                date=self.DAY,
                time="09:45:00",
                subject="[COMPLETED] Direct assignment",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="alice.smith@example.com",
                msg_key="dir-2",
                sami_id="SAMI-DIRECT1",
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
        self.assertEqual(by_email["alice.smith@example.com"]["jira_followups"], 0)


    def test_jira_followup_followup_counts_on_original_owner(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Original assignment",
                event_type="ASSIGNED",
                action="IMAGE_REQUEST_EXTERNAL/DOMAIN",
                assigned_to="alice.smith@example.com",
                sender="requester@example.com",
                msg_key="jira-1",
                sami_id="SAMI-JIRA1",
                assigned_ts=f"{self.DAY}T09:00:00",
            ),
            self._row(
                date=self.DAY,
                time="09:30:00",
                subject="Request ID: ITSD-1 | [COMPLETED] [SAMI-JIRA1] Jira follow-up assignment",
                event_type="JIRA_FOLLOWUP_ASSIGNED",
                assigned_to="alice.smith@example.com",
                sender="jira@example.com",
                msg_key="jira-2",
                sami_id="",
                action="JIRA_FOLLOWUP",
                assigned_ts=f"{self.DAY}T09:30:00",
            ),
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="[COMPLETED] Jira follow-up assignment",
                event_type="COMPLETED",
                assigned_to="completed",
                sender="bob.jones@example.com",
                msg_key="jira-3",
                sami_id="SAMI-JIRA1",
                completed_ts=f"{self.DAY}T10:00:00",
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
        self.assertEqual(by_email["alice.smith@example.com"]["jira_followups"], 1)


    def test_jira_followup_followup_respects_selected_date_range(self):
        rows = [
            self._row(
                date="2026-02-18",
                time="09:00:00",
                subject="Original assignment",
                event_type="ASSIGNED",
                action="IMAGE_REQUEST_EXTERNAL/DOMAIN",
                assigned_to="alice.smith@example.com",
                sender="requester@example.com",
                msg_key="jira-range-1",
                sami_id="SAMI-JIRARANGE1",
                assigned_ts="2026-02-18T09:00:00",
            ),
            self._row(
                date="2026-02-18",
                time="09:30:00",
                subject="Jira follow-up reassigned",
                event_type="JIRA_FOLLOWUP_ASSIGNED",
                assigned_to="bob.jones@example.com",
                sender="dashboard_admin",
                msg_key="jira-range-2",
                sami_id="SAMI-JIRARANGE1",
                action="JIRA_FOLLOWUP",
                assigned_ts="2026-02-18T09:30:00",
            ),
        ]

        same_day = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start="2026-02-18",
            date_end="2026-02-18",
            reconciled_set=set(),
        )
        next_day = compute_dashboard(
            rows,
            roster_state=None,
            settings=None,
            staff_list=["alice.smith@example.com", "bob.jones@example.com"],
            hib_state=None,
            date_start="2026-02-19",
            date_end="2026-02-19",
            reconciled_set=set(),
        )

        self.assertEqual(self._staff_map(same_day)["alice.smith@example.com"]["jira_followups"], 1)
        self.assertNotIn("alice.smith@example.com", self._staff_map(next_day))


    def test_activity_feed_can_filter_jira_followup_followups_for_original_owner(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Original assignment",
                event_type="ASSIGNED",
                action="IMAGE_REQUEST_EXTERNAL/DOMAIN",
                assigned_to="alice.smith@example.com",
                sender="requester@example.com",
                msg_key="jira-af-1",
                sami_id="SAMI-JIRAFILT1",
                assigned_ts=f"{self.DAY}T09:00:00",
            ),
            self._row(
                date=self.DAY,
                time="09:30:00",
                subject="Request ID: ITSD-2 | [COMPLETED] [SAMI-JIRAFILT1] Jira follow-up assignment",
                event_type="JIRA_FOLLOWUP_ASSIGNED",
                assigned_to="bob.jones@example.com",
                sender="jira@example.com",
                msg_key="jira-af-2",
                sami_id="",
                action="JIRA_FOLLOWUP",
                assigned_ts=f"{self.DAY}T09:30:00",
            ),
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="Plain assignment",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                sender="requester@example.com",
                msg_key="plain-1",
                sami_id="SAMI-PLAIN1",
                assigned_ts=f"{self.DAY}T10:00:00",
            ),
            self._row(
                date=self.DAY,
                time="10:15:00",
                subject="Plain reassigned",
                event_type="REASSIGN_MANUAL",
                assigned_to="bob.jones@example.com",
                sender="dashboard_admin",
                msg_key="plain-2",
                sami_id="SAMI-PLAIN1",
                action="REASSIGN",
                assigned_ts=f"{self.DAY}T10:15:00",
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
            activity_mode="jira_followups",
            activity_staff="Alice Smith",
        )

        self.assertEqual(len(payload["activity_feed"]), 1)
        self.assertEqual(payload["activity_feed"][0]["type"], "JIRA_FOLLOWUP_ASSIGNED")
        self.assertEqual(payload["activity_feed"][0]["sami_ref"], "SAMI-JIRAFILT1")
        self.assertEqual(payload["activity_feed"][0]["sender"], "Jira")


    def test_reconciled_active_item_counts_as_completed_in_range(self):
        rows = [
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="Open but reconciled",
                event_type="ASSIGNED",
                assigned_to="alice.smith@example.com",
                msg_key="rec-1",
                sami_id="SAMI-REC001",
            ),
        ]

        fake_state = {
            "version": 1,
            "reconciled": [
                {
                    "identity": "SAMI-REC001",
                    "staff_email": "alice.smith@example.com",
                    "reason": "balanced",
                    "ts": f"{self.DAY}T12:00:00+00:00",
                    "sami_ref": "SAMI-REC001",
                }
            ],
        }
        ledger = {
            "rec-1": {"sami_id": "SAMI-REC001", "assigned_to": "alice.smith@example.com"},
        }
        with patch("dashboard.backend.reconciliation.load_reconciled", return_value=fake_state), \
             patch("dashboard.backend.data_reader.load_json", return_value=(ledger, None)):
            payload = compute_dashboard(
                rows,
                roster_state=None,
                settings=None,
                staff_list=["alice.smith@example.com"],
                hib_state=None,
                date_start=self.DAY,
                date_end=self.DAY,
                reconciled_set={"SAMI-REC001"},
            )

        by_email = self._staff_map(payload)
        self.assertEqual(payload["summary"]["active_count"], 0)
        self.assertEqual(payload["summary"]["completions_today"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["completed"], 1)
        self.assertEqual(by_email["alice.smith@example.com"]["active"], 0)


if __name__ == "__main__":
    unittest.main()
