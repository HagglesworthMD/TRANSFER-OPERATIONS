import unittest
from unittest.mock import patch

from dashboard.backend import config, server


class _FakeRequest:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


class ManualStaleApiTests(unittest.IsolatedAsyncioTestCase):
    @patch("dashboard.backend.server._atomic_write_json")
    @patch("dashboard.backend.server._safe_load_json_direct")
    async def test_manual_stale_request_write_is_atomic_and_valid(self, mock_load, mock_write):
        mock_load.return_value = ({}, None)
        mock_write.return_value = (True, None)

        response = await server.manual_stale(
            _FakeRequest({
                "msg_key": "Store:ABC|Entry:123",
                "sami_id": "SAMI-ab12cd",
                "reason": "Owner unavailable",
                "requested_by": "operator@example.com",
            })
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "queued")
        write_path, payload = mock_write.call_args[0]
        self.assertEqual(write_path, config.MANUAL_STALE_REQUESTS_JSON)
        self.assertIsInstance(payload, dict)
        self.assertEqual(list(payload.keys()), ["msg:store:abc|entry:123"])
        entry = payload["msg:store:abc|entry:123"]
        self.assertEqual(entry["request_key"], "msg:store:abc|entry:123")
        self.assertEqual(entry["msg_key"], "Store:ABC|Entry:123")
        self.assertEqual(entry["sami_id"], "SAMI-AB12CD")
        self.assertEqual(entry["reason"], "Owner unavailable")
        self.assertEqual(entry["requested_by"], "operator@example.com")
        self.assertTrue(entry["requested_ts"])
        self.assertTrue(entry["request_id"].startswith("manual-stale-"))

    @patch("dashboard.backend.server._atomic_write_json")
    @patch("dashboard.backend.server._safe_load_json_direct")
    async def test_duplicate_request_key_overwrites_existing_request(self, mock_load, mock_write):
        mock_load.return_value = ({
            "msg:store:abc|entry:123": {
                "request_id": "old-request",
                "request_key": "msg:store:abc|entry:123",
                "msg_key": "store:abc|entry:123",
                "sami_id": "",
                "reason": "old reason",
                "requested_by": "old_user",
                "requested_ts": "2026-03-06T10:00:00+00:00",
            }
        }, None)
        mock_write.return_value = (True, None)

        response = await server.manual_stale(
            _FakeRequest({
                "msg_key": "store:abc|entry:123",
                "reason": "new reason",
                "requested_by": "dashboard_admin",
            })
        )

        self.assertEqual(response["request_key"], "msg:store:abc|entry:123")
        payload = mock_write.call_args[0][1]
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload["msg:store:abc|entry:123"]["reason"], "new reason")
        self.assertEqual(payload["msg:store:abc|entry:123"]["requested_by"], "dashboard_admin")
        self.assertNotEqual(payload["msg:store:abc|entry:123"]["request_id"], "old-request")


if __name__ == "__main__":
    unittest.main()
