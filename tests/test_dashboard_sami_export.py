import unittest

from dashboard.backend import server


class DashboardSamiExportTests(unittest.TestCase):
    def test_normalize_sami_lookup_accepts_common_formats(self):
        self.assertEqual(server._normalize_sami_lookup("sami-abc123"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup("[SAMI-abc123]"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup("abc123"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup(""), "")

    def test_filter_rows_by_sami_ref_uses_sami_id_only(self):
        rows = [
            {
                "Date": "2026-02-17",
                "Time": "09:00:00",
                "event_type": "ASSIGNED",
                "Subject": "Image transfer request",
                "sami_id": "SAMI-ABC123",
            },
            {
                "Date": "2026-02-17",
                "Time": "09:14:00",
                "event_type": "COMPLETED",
                "Subject": "[COMPLETED] [SAMI-ABC123] done",
                "sami_id": "SAMI-ABC123",
            },
            {
                "Date": "2026-02-17",
                "Time": "10:00:00",
                "event_type": "ASSIGNED",
                "Subject": "[SAMI-ABC123] another request",
                "sami_id": "",
            },
        ]

        matched = server._filter_rows_by_sami_ref(rows, "sami-abc123")
        self.assertEqual(len(matched), 2)
        self.assertEqual([r.get("event_type") for r in matched], ["ASSIGNED", "COMPLETED"])

    def test_build_sami_audit_csv_includes_lifecycle_and_follow_up(self):
        rows = [
            {
                "Date": "2026-02-17",
                "Time": "09:00:00",
                "event_type": "ASSIGNED",
                "Action": "IMAGE_REQUEST_EXTERNAL",
                "Subject": "Image transfer request",
                "assigned_to": "alice.smith@example.com",
                "Sender": "requester@example.com",
                "msg_key": "m1",
                "assigned_ts": "2026-02-17T09:00:00",
                "sami_id": "SAMI-ABC123",
            },
            {
                "Date": "2026-02-17",
                "Time": "09:10:00",
                "event_type": "FOLLOW_UP",
                "Action": "FOLLOW_UP_NOTE",
                "Subject": "Re: Image transfer request",
                "assigned_to": "alice.smith@example.com",
                "Sender": "alice.smith@example.com",
                "msg_key": "m1-fu",
                "sami_id": "SAMI-ABC123",
            },
            {
                "Date": "2026-02-17",
                "Time": "09:45:00",
                "event_type": "COMPLETED",
                "Action": "STAFF_COMPLETED_CONFIRMATION",
                "Subject": "[COMPLETED] done",
                "assigned_to": "alice.smith@example.com",
                "Sender": "alice.smith@example.com",
                "msg_key": "m1-c",
                "completed_ts": "2026-02-17T09:45:00",
                "sami_id": "SAMI-ABC123",
            },
        ]

        csv_rows, fieldnames = server._build_sami_audit_csv(rows, "sami-abc123")

        self.assertEqual(len(csv_rows), 3)
        self.assertEqual(fieldnames[0], "SAMI Ref")
        self.assertEqual(fieldnames[1], "Status")
        self.assertEqual(fieldnames[2], "Open")
        self.assertEqual(fieldnames[3], "Completed")
        self.assertIn("Assigned to Completed", fieldnames)
        self.assertIn("Follow Up Count", fieldnames)
        self.assertEqual(csv_rows[0]["SAMI Ref"], "SAMI-ABC123")
        self.assertEqual(csv_rows[0]["Assigned At"], "2026-02-17T09:00:00")
        self.assertEqual(csv_rows[0]["Completed At"], "2026-02-17T09:45:00")
        self.assertEqual(csv_rows[0]["Assigned to Completed"], "45 min")
        self.assertEqual(csv_rows[0]["Status"], "CLOSED")
        self.assertEqual(csv_rows[0]["Assigned Count"], 1)
        self.assertEqual(csv_rows[0]["Completed Count"], 1)
        self.assertEqual(csv_rows[0]["Follow Up Count"], 1)
        self.assertEqual(csv_rows[1]["Follow Up"], "yes")

    def test_format_elapsed_for_audit_prefers_hours_after_sixty_minutes(self):
        self.assertEqual(server._format_elapsed_for_audit(45 * 60), "45 min")
        self.assertEqual(server._format_elapsed_for_audit(90 * 60), "1.50 hrs")

    def test_build_sami_audit_workbook_contains_excel_sheets(self):
        rows = [{"SAMI Ref": "SAMI-ABC123", "Status": "OPEN", "Open": "yes", "Completed": "no", "Follow Up Count": 1, "Assigned At": "2026-02-17T09:00:00", "Completed At": "", "Assigned to Completed": "", "Assigned Count": 1, "Completed Count": 0, "Total Events": 1, "Event #": 1, "Event Time": "2026-02-17T09:00:00", "Event Type": "ASSIGNED", "Action": "IMAGE_REQUEST_EXTERNAL", "Follow Up": "no", "Staff Email": "alice@example.com", "Subject": "Image transfer request"}]
        fieldnames = list(rows[0].keys())
        workbook = server._build_sami_audit_workbook("SAMI-ABC123", rows, fieldnames)
        self.assertIn(b"SAMI Audit Summary", workbook)
        self.assertIn(b"Event Timeline", workbook)

    def test_count_completed_sami_mismatches(self):
        rows = [
            {
                "event_type": "COMPLETED",
                "Action": "STAFF_COMPLETED_CONFIRMATION",
                "Subject": "[COMPLETED] [SAMI-AAA111] done",
                "sami_id": "SAMI-BBB222",
            },
            {
                "event_type": "COMPLETED",
                "Action": "STAFF_COMPLETED_CONFIRMATION",
                "Subject": "[COMPLETED] [SAMI-CCC333] done",
                "sami_id": "SAMI-CCC333",
            },
        ]
        self.assertEqual(server._count_completed_sami_mismatches(rows), 1)


if __name__ == "__main__":
    unittest.main()

