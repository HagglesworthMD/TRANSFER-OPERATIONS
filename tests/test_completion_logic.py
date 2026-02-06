import unittest

import distributor


class CompletionLogicTests(unittest.TestCase):
    class _DummyMsg:
        def __init__(
            self,
            entry_id='',
            subject='',
            received_time=None,
            sender_email_address='',
            message_class='',
        ):
            self.EntryID = entry_id
            self.Subject = subject
            self.ReceivedTime = received_time
            self.SenderEmailAddress = sender_email_address
            self.MessageClass = message_class

    def test_is_completion_subject_case_insensitive(self):
        self.assertTrue(distributor.is_completion_subject('[COMPLETED] done'))
        self.assertTrue(distributor.is_completion_subject('prefix [completed] now'))
        self.assertFalse(distributor.is_completion_subject('no keyword here'))

    def test_completion_requires_staff_sender(self):
        staff_list = ['staff@example.com']
        sender_email = 'other@example.com'
        subject = '[COMPLETED] test'
        is_staff_sender = sender_email in staff_list
        keyword_hit = distributor.is_completion_subject(subject)
        self.assertFalse(is_staff_sender and keyword_hit)

    def test_completion_updates_ledger_with_conversation_id(self):
        staff_list = ['staff@example.com']
        sender_email = 'staff@example.com'
        subject = '[COMPLETED] finished'
        conversation_id = 'conv-123'
        processed_ledger = {
            'key-1': {
                'conversation_id': conversation_id,
                'assigned_to': 'someone'
            }
        }
        is_staff_sender = sender_email in staff_list
        keyword_hit = distributor.is_completion_subject(subject)
        if is_staff_sender and keyword_hit:
            match_key = distributor.find_ledger_key_by_conversation_id(processed_ledger, conversation_id)
            if match_key:
                entry = processed_ledger.get(match_key, {})
                entry['completed_at'] = '2026-01-01T00:00:00'
                entry['completed_by'] = sender_email
                entry['completion_source'] = 'subject_keyword'
                processed_ledger[match_key] = entry
        self.assertEqual(processed_ledger['key-1'].get('completion_source'), 'subject_keyword')

    def test_build_mailto_and_prepend_html(self):
        mailto = distributor.build_completion_mailto(
            'requester@example.com',
            distributor.SAMI_SHARED_INBOX,
            '[COMPLETED] Test Job'
        )
        self.assertIn('mailto:requester@example.com', mailto)
        self.assertIn('cc=health.samisupportteam@sa.gov.au', mailto)
        self.assertIn('subject=%5BCOMPLETED%5D%20Test%20Job', mailto)
        html = distributor.prepend_completion_hotlink_html('ORIGINAL', mailto)
        self.assertTrue(html.startswith('<p><b>'))
        self.assertIn('Mark job complete', html)
        self.assertIn('Click to notify requester (CC SAMI)', html)

    def test_build_mailto_omits_empty_cc(self):
        mailto_empty = distributor.build_completion_mailto(
            'a@b.com',
            '',
            'Subject'
        )
        self.assertIn('mailto:a@b.com', mailto_empty)
        self.assertNotIn('cc=', mailto_empty)
        mailto_none = distributor.build_completion_mailto(
            'a@b.com',
            None,
            'Subject'
        )
        self.assertIn('mailto:a@b.com', mailto_none)
        self.assertNotIn('cc=', mailto_none)

    def test_mailto_url_prefixes_completed_and_detector_checks_token(self):
        mailto_url = distributor.build_completion_mailto_url(
            'requester@example.com',
            distributor.SAMI_SHARED_INBOX,
            'Follow up',
            body=None,
        )
        self.assertIn('subject=%5BCOMPLETED%5D%20Follow%20up', mailto_url)
        self.assertTrue(distributor.is_completion_subject('[COMPLETED] done'))
        self.assertFalse(distributor.is_completion_subject('done'))

    def test_ensure_sami_id_in_subject_injects_once_and_is_stable(self):
        msg = self._DummyMsg(entry_id='ENTRY-12345', subject='Original Subject')
        first = distributor.ensure_sami_id_in_subject('Original Subject', msg)
        second = distributor.ensure_sami_id_in_subject('Original Subject', msg)
        self.assertTrue(first.startswith('[SAMI-'))
        self.assertEqual(first, second)
        self.assertEqual(distributor.ensure_sami_id_in_subject(first, msg), first)

    def test_ensure_sami_id_in_subject_already_tagged_unchanged(self):
        msg = self._DummyMsg(entry_id='ENTRY-ABCDE')
        tagged = '[SAMI-A1B2C3] Existing Subject'
        self.assertEqual(distributor.ensure_sami_id_in_subject(tagged, msg), tagged)

    def test_mailto_url_contains_completed_and_sami_when_subject_has_sami(self):
        msg = self._DummyMsg(entry_id='ENTRY-ABCDE', subject='Task X')
        subject_with_id = distributor.ensure_sami_id_in_subject('Task X', msg)
        mailto_url = distributor.build_completion_mailto_url(
            'requester@example.com',
            distributor.SAMI_SHARED_INBOX,
            subject_with_id,
            body=None,
        )
        self.assertIn('subject=%5BCOMPLETED%5D%20%5BSAMI-', mailto_url)


    def test_staff_completed_confirmation_positive(self):
        staff_set = {'staff@example.com'}
        self.assertTrue(distributor.is_staff_completed_confirmation('staff@example.com', '[COMPLETED] Test job', staff_set))
        self.assertTrue(distributor.is_staff_completed_confirmation('Staff@Example.com', 'RE: [completed] done', staff_set))

    def test_staff_completed_confirmation_negative_non_staff(self):
        staff_set = {'staff@example.com'}
        self.assertFalse(distributor.is_staff_completed_confirmation('outsider@other.com', '[COMPLETED] Test job', staff_set))
        self.assertFalse(distributor.is_staff_completed_confirmation('staff@example.com', 'no keyword', staff_set))
        self.assertFalse(distributor.is_staff_completed_confirmation('', '[COMPLETED] x', staff_set))
        self.assertFalse(distributor.is_staff_completed_confirmation(None, '[COMPLETED] x', staff_set))


if __name__ == '__main__':
    unittest.main()
