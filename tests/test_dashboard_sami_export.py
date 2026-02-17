import unittest

from dashboard.backend import server


class DashboardSamiExportTests(unittest.TestCase):
    def test_normalize_sami_lookup_accepts_common_formats(self):
        self.assertEqual(server._normalize_sami_lookup("sami-abc123"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup("[SAMI-abc123]"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup("abc123"), "SAMI-ABC123")
        self.assertEqual(server._normalize_sami_lookup(""), "")

    def test_filter_rows_by_sami_ref_matches_sami_id_and_subject_fallback(self):
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
                "sami_id": "",
            },
            {
                "Date": "2026-02-17",
                "Time": "10:00:00",
                "event_type": "ASSIGNED",
                "Subject": "[SAMI-ZZZ999] another request",
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
        self.assertEqual(fieldnames[0], "Audit SAMI Ref")
        self.assertEqual(fieldnames[1], "Manager Assigned TS")
        self.assertEqual(fieldnames[2], "Manager Completed TS")
        self.assertEqual(fieldnames[3], "Manager Assigned -> Completed (mins)")
        self.assertIn("Audit Lifecycle Status", fieldnames)
        self.assertIn("Audit Follow Up Count", fieldnames)
        self.assertEqual(csv_rows[0]["Audit SAMI Ref"], "SAMI-ABC123")
        self.assertEqual(csv_rows[0]["Manager Assigned TS"], "2026-02-17T09:00:00")
        self.assertEqual(csv_rows[0]["Manager Completed TS"], "2026-02-17T09:45:00")
        self.assertEqual(csv_rows[0]["Manager Assigned -> Completed (mins)"], "45.00")
        self.assertEqual(csv_rows[0]["Audit Lifecycle Status"], "CLOSED")
        self.assertEqual(csv_rows[0]["Audit Assigned Count"], 1)
        self.assertEqual(csv_rows[0]["Audit Completed Count"], 1)
        self.assertEqual(csv_rows[0]["Audit Follow Up Count"], 1)
        self.assertEqual(csv_rows[1]["Audit Follow Up"], "yes")


if __name__ == "__main__":
    unittest.main()
