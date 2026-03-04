import asyncio
import copy
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from dashboard.backend import server


class DashboardBucketDedupTests(unittest.TestCase):
    def _payload(self):
        return {
            'transfer_domains': ['bensonradiology.com.au'],
            'system_notification_domains': ['service-now.com'],
            'quarantine_domains': [],
            'held_domains': [],
            'applications_direct_domains': ['carestream.com'],
            'transfer_senders': [],
            'system_notification_senders': ['jira@example.com'],
            'quarantine_senders': [],
            'held_senders': [],
            'applications_direct_senders': ['apps@example.com'],
            'folders': {
                'completed': '01_COMPLETED',
                'non_actionable': '02_PROCESSED',
                'quarantine': '03_QUARANTINE',
                'hold': '04_HIB',
                'system_notification': '05_SYSTEM_NOTIFICATIONS',
            },
        }

    def test_add_sender_rejects_duplicate_in_other_bucket(self):
        payload = self._payload()
        with patch('dashboard.backend.server._build_domain_policy_payload', return_value=copy.deepcopy(payload)), patch(
            'dashboard.backend.server._save_domain_policy_payload'
        ) as save_mock:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(server.add_sender('applications_direct', server.SenderRequest(sender='jira@example.com')))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, 'jira@example.com already in system_notification')
        save_mock.assert_not_called()

    def test_add_domain_rejects_duplicate_in_other_bucket(self):
        payload = self._payload()
        with patch('dashboard.backend.server._build_domain_policy_payload', return_value=copy.deepcopy(payload)), patch(
            'dashboard.backend.server._save_domain_policy_payload'
        ) as save_mock:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(server.add_domain('applications_direct', server.DomainRequest(domain='service-now.com')))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, 'service-now.com already in system_notification')
        save_mock.assert_not_called()

    def test_add_sender_still_allows_unique_value(self):
        payload = self._payload()
        saved = {}

        def _save(obj):
            saved.update(obj)
            return True, None

        with patch('dashboard.backend.server._build_domain_policy_payload', side_effect=[copy.deepcopy(payload), copy.deepcopy({**payload, 'applications_direct_senders': ['apps@example.com', 'unique@example.com']})]), patch(
            'dashboard.backend.server._save_domain_policy_payload', side_effect=_save
        ):
            result = asyncio.run(server.add_sender('applications_direct', server.SenderRequest(sender='unique@example.com')))
        self.assertIn('unique@example.com', saved['applications_direct_senders'])
        self.assertIn('unique@example.com', result['senders'])


if __name__ == '__main__':
    unittest.main()
