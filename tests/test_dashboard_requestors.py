import asyncio
import unittest
from unittest.mock import patch

from dashboard.backend import server
from dashboard.backend.kpi_engine import (
    _normalize_requestor_email,
    _requestor_domain,
    _requestor_identity,
    export_requestor_stats,
)


class DashboardRequestorExportTests(unittest.TestCase):
    DAY = "2026-03-10"

    def _row(
        self,
        *,
        date,
        time,
        subject,
        event_type,
        assigned_to="alice.smith@example.com",
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
            action = "STAFF_COMPLETED_CONFIRMATION" if event_type == "COMPLETED" else "IMAGE_REQUEST_EXTERNAL"
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

    def test_requestor_identity_helpers_normalize_email_domain_and_unknown(self):
        self.assertEqual(_normalize_requestor_email(" Specific.Sender@Example.com "), "specific.sender@example.com")
        self.assertEqual(_requestor_domain("specific.sender@example.com"), "example.com")
        self.assertEqual(_requestor_identity("sender.one@jonesradiology.com.au")["requestor_group"], "Jones")
        self.assertEqual(_requestor_identity("worker@sa.gov.au")["requestor_group"], "Internal")
        self.assertEqual(_requestor_identity("")["requestor_key"], "unknown")

    def test_export_requestor_stats_builds_canonical_job_rows_and_group_rollups(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Jones urgent request",
                event_type="ASSIGNED",
                sender="sender.one@jonesradiology.com.au",
                sami_id="SAMI-REQ001",
                risk_level="urgent",
            ),
            self._row(
                date=self.DAY,
                time="09:30:00",
                subject="[COMPLETED] Jones urgent request",
                event_type="COMPLETED",
                sender="alice.smith@example.com",
                assigned_to="completed",
                sami_id="SAMI-REQ001",
            ),
            self._row(
                date=self.DAY,
                time="10:00:00",
                subject="Jones critical request",
                event_type="ASSIGNED",
                sender="sender.two@jonesradiology.com.au",
                sami_id="SAMI-REQ002",
                risk_level="critical",
            ),
            self._row(
                date=self.DAY,
                time="11:00:00",
                subject="[COMPLETED] Jones critical request",
                event_type="COMPLETED",
                sender="alice.smith@example.com",
                assigned_to="completed",
                sami_id="SAMI-REQ002",
            ),
            self._row(
                date=self.DAY,
                time="12:00:00",
                subject="RadSA open request",
                event_type="ASSIGNED",
                sender="staff.one@radiologysa.com.au",
                sami_id="SAMI-REQ003",
                assigned_to="bob.jones@example.com",
            ),
        ]

        export_data = export_requestor_stats(rows, self.DAY, self.DAY)

        summary = {row["metric"]: row["value"] for row in export_data["summary_rows"]}
        self.assertEqual(summary["total_requestors"], 3)
        self.assertEqual(summary["total_jobs"], 3)
        self.assertEqual(summary["total_open_jobs"], 1)
        self.assertEqual(summary["total_completed_jobs"], 2)
        self.assertEqual(summary["total_urgent"], 1)
        self.assertEqual(summary["total_critical"], 1)

        by_key = {row["requestor_key"]: row for row in export_data["requestor_rows"]}
        self.assertEqual(by_key["sender.one@jonesradiology.com.au"]["median_turnaround_sec"], 1800.0)
        self.assertEqual(by_key["sender.two@jonesradiology.com.au"]["critical_count"], 1)
        self.assertEqual(by_key["staff.one@radiologysa.com.au"]["requestor_group"], "RadSA")
        self.assertEqual(by_key["staff.one@radiologysa.com.au"]["open_jobs"], 1)
        self.assertEqual(by_key["sender.one@jonesradiology.com.au"]["first_seen"], "2026-03-10 09:00")
        self.assertEqual(by_key["sender.one@jonesradiology.com.au"]["last_seen"], "2026-03-10 09:00")

        groups = {row["requestor_group"]: row for row in export_data["requestor_groups"]}
        self.assertEqual(groups["Jones"]["requestor_count"], 2)
        self.assertEqual(groups["Jones"]["total_jobs"], 2)
        self.assertEqual(groups["Jones"]["median_turnaround_sec"], 2700.0)

        daily = {row["date"]: row for row in export_data["daily_rows"]}
        self.assertEqual(daily[self.DAY]["total_jobs"], 3)
        self.assertEqual(daily[self.DAY]["completed_jobs"], 2)
        self.assertEqual(daily[self.DAY]["open_jobs"], 1)

        monthly = {row["month"]: row for row in export_data["monthly_rows"]}
        self.assertEqual(monthly["2026-03"]["total_jobs"], 3)
        self.assertEqual(monthly["2026-03"]["direction"], "flat")

        hourly = {row["hour"]: row for row in export_data["hourly_rows"]}
        self.assertEqual(hourly["09:00"]["load_band"], "peak")
        self.assertEqual(hourly["09:00"]["total_jobs"], 1)
        self.assertEqual(hourly["12:00"]["open_jobs"], 1)

    def test_export_endpoint_returns_excel_workbook(self):
        rows = [
            self._row(
                date=self.DAY,
                time="09:00:00",
                subject="Request",
                event_type="ASSIGNED",
                sender="sender.one@jonesradiology.com.au",
                sami_id="SAMI-REQ001",
            ),
        ]

        async def collect(response):
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            return body

        with patch("dashboard.backend.server.load_csv", return_value=(rows, None)):
            response = asyncio.run(
                server.requestor_stats_export(date_start=self.DAY, date_end=self.DAY)
            )
            body = asyncio.run(collect(response))

        self.assertEqual(response.media_type, "application/vnd.ms-excel")
        self.assertIn("requestor_stats_2026-03-10_2026-03-10.xls", response.headers["Content-Disposition"])
        self.assertIn(b"Requestor Stats", body)
        self.assertIn(b"Requestor Groups", body)
        self.assertIn(b"Daily Trends", body)
        self.assertIn(b"Monthly Trends", body)
        self.assertIn(b"Hourly Activity", body)
        self.assertNotIn(b"RadSA Domains", body)
        self.assertNotIn(b"jobs_graph", body)
        self.assertNotIn(b"completed_graph", body)

    def test_empty_export_still_returns_headers_only_workbook(self):
        async def collect(response):
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            return body

        with patch("dashboard.backend.server.load_csv", return_value=([], None)):
            response = asyncio.run(
                server.requestor_stats_export(date_start=self.DAY, date_end=self.DAY)
            )
            body = asyncio.run(collect(response))

        self.assertEqual(response.media_type, "application/vnd.ms-excel")
        self.assertIn(b"Requestor Stats", body)
        self.assertIn(b"requestor_key", body)
        self.assertIn(b"jobs_change_count", body)
        self.assertIn(b"jobs_change_pct", body)
        self.assertNotIn(b"completed_graph", body)

    def test_export_summary_uses_dashboard_summary_values(self):
        async def collect(response):
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            return body

        fake_export = {
            "summary_rows": [
                {"metric": "total_jobs", "value": 999},
                {"metric": "total_open_jobs", "value": 999},
                {"metric": "total_completed_jobs", "value": 999},
            ],
            "requestor_rows": [],
            "requestor_groups": [],
            "radsa_domain_rows": [],
            "daily_rows": [],
            "monthly_rows": [],
            "hourly_rows": [],
        }

        with patch("dashboard.backend.server.load_csv", return_value=([], None)), \
             patch("dashboard.backend.server.load_reconciled_set", return_value=set()), \
             patch("dashboard.backend.server.export_requestor_stats", return_value=fake_export), \
             patch("dashboard.backend.server.compute_dashboard", return_value={"summary": {"processed_in_range": 666, "active_count": 777, "completions_today": 555}}):
            response = asyncio.run(
                server.requestor_stats_export(date_start=self.DAY, date_end=self.DAY)
            )
            body = asyncio.run(collect(response))

        self.assertIn(b"total_jobs", body)
        self.assertIn(b">666<", body)
        self.assertIn(b"total_open_jobs", body)
        self.assertIn(b">777<", body)
        self.assertIn(b"total_completed_jobs", body)
        self.assertIn(b">555<", body)


if __name__ == "__main__":
    unittest.main()
