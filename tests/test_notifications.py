import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tender_notifications import TenderState, SMTPSettings, fingerprint, render_digest, send_digest


def row(**changes):
    return {'source': 'gem', 'tender_id': 'GEM/2026/B/1', 'title': 'GPU server',
            'description': 'H200 cluster', 'organization': 'Research Institute',
            'published_date': '01-Oct-2026', 'closing_date': '12-Oct-2026 11:00 AM',
            'matched_keywords': 'GPU; Server; H200', 'source_url': 'https://example.org/tender/1',
            'corrigendum': '', **changes}


class StateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        self.addCleanup(self.tmp.cleanup)

    def test_new_never_notified_and_persists_across_runs(self):
        with TenderState(self.db) as state:
            alerts = state.observe([row()], now='2026-10-09T01:30:00+00:00')
            self.assertEqual([r['status'] for r in alerts], ['NEW'])
        with TenderState(self.db) as state:
            self.assertEqual(state.observe([row()])[0]['status'], 'NEW')
            saved = state.connection.execute('SELECT * FROM tenders').fetchone()
            self.assertEqual(saved['first_seen'], '2026-10-09T01:30:00+00:00')
            self.assertIsNone(saved['notified_at'])
            self.assertNotEqual(saved['last_seen'], saved['first_seen'])

    def test_successful_send_suppresses_duplicates_after_reopen(self):
        with TenderState(self.db) as state:
            alerts = state.observe([row(), row()])
            self.assertEqual(len(alerts), 1)
            state.mark_notified(alerts)
        with TenderState(self.db) as state:
            self.assertEqual(state.observe([row()]), [])
            saved = state.connection.execute('SELECT * FROM tenders').fetchone()
            self.assertTrue(saved['notified_at'])
            self.assertEqual(saved['fingerprint'], saved['notified_fingerprint'])

    def test_meaningful_field_changes_are_updated(self):
        for field, changed in [('title', 'GPU server revised'), ('closing_date', '10-Oct-2026'),
                               ('organization', 'New Institute'), ('source_url', 'https://example.org/new'),
                               ('corrigendum', 'Corrigendum 2'), ('description', 'B300 cluster'),
                               ('opening_date', '14-Oct-2026')]:
            with self.subTest(field=field), TenderState(self.db) as state:
                base = row(tender_id=field)
                state.mark_notified(state.observe([base]))
                alerts = state.observe([{**base, field: changed}])
                self.assertEqual([r['status'] for r in alerts], ['UPDATED'])
                # Failed delivery must remain pending even after observing it.
                self.assertEqual(state.observe([{**base, field: changed}])[0]['status'], 'UPDATED')
                state.mark_notified(alerts)
                self.assertEqual(state.observe([{**base, field: changed}]), [])

    def test_keyword_changes_and_cosmetic_changes_do_not_send_updates(self):
        with TenderState(self.db) as state:
            state.mark_notified(state.observe([row()]))
            self.assertEqual(state.observe([row(title=' GPU   Server ', matched_keywords='GPU')]), [])
        self.assertEqual(fingerprint(row(closing_date='2026-10-12T05:30:00Z')),
                         fingerprint(row(closing_date='12-Oct-2026 11:00 AM')))

    def test_reverted_failed_update_is_not_sent(self):
        with TenderState(self.db) as state:
            state.mark_notified(state.observe([row()]))
            self.assertEqual(state.observe([row(corrigendum='Changed')])[0]['status'], 'UPDATED')
            self.assertEqual(state.observe([row()]), [])

    def test_source_and_id_form_identity(self):
        with TenderState(self.db) as state:
            alerts = state.observe([row(), row(source='cppp')])
            self.assertEqual(len(alerts), 2)
            with self.assertRaises(ValueError):
                state.observe([row(tender_id='')])


class EmailTests(unittest.TestCase):
    def test_rendering_includes_all_fields_escapes_html_and_orders_deadlines(self):
        alerts = [row(status='NEW', title='<GPU & Server>', tender_id='late',
                      closing_date='18-Oct-2026'),
                  row(status='UPDATED', tender_id='early', closing_date='10-Oct-2026 11:00 AM',
                      source='cppp', source_url='')]
        msg = render_digest(alerts, when=datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(str(msg['Subject']), 'Tender Radar — 1 New / 1 Updated — 10 Oct 2026')
        html = msg.get_body(preferencelist=('html',)).get_content()
        text = msg.get_body(preferencelist=('plain',)).get_content()
        self.assertIn('&lt;GPU &amp; Server&gt;', html)
        self.assertLess(html.index('early'), html.index('late'))
        for expected in ('NEW', 'UPDATED', 'cppp', 'Research Institute', '01-Oct-2026',
                         'GPU; Server; H200', 'https://eprocure.gov.in/cppp/latestactivetendersnew'):
            self.assertIn(expected, html)
            self.assertIn(expected, text)

    def test_unsafe_urls_are_never_rendered_as_links(self):
        msg = render_digest([row(status='NEW', source='unknown', source_url='javascript:alert(1)')])
        self.assertNotIn('javascript:', msg.get_body(preferencelist=('html',)).get_content())

    def test_missing_and_invalid_smtp_configuration(self):
        with self.assertRaisesRegex(ValueError, 'SMTP_HOST'):
            SMTPSettings.from_env({})
        valid = {'SMTP_HOST': 'smtp.example.org', 'EMAIL_FROM': 'radar@example.org',
                 'EMAIL_TO': 'one@example.org,two@example.org'}
        self.assertEqual(SMTPSettings.from_env(valid).recipients,
                         ['one@example.org', 'two@example.org'])
        for changes in ({'SMTP_PORT': 'bad'}, {'EMAIL_TO': 'broken'},
                        {'EMAIL_FROM': 'bad\nBcc: injected@example.org'},
                        {'SMTP_USER': 'user'}, {'SMTP_SECURITY': 'bad'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                SMTPSettings.from_env({**valid, **changes})

    def test_rejected_recipient_prevents_delivery_to_everyone(self):
        settings = SMTPSettings.from_env({'SMTP_HOST': 'smtp.example.org',
                                        'EMAIL_FROM': 'radar@example.org',
                                        'EMAIL_TO': 'one@example.org,two@example.org'})
        msg = render_digest([row(status='NEW')])
        with patch('tender_notifications.smtplib.SMTP') as factory:
            client = factory.return_value.__enter__.return_value
            client.mail.return_value = (250, b'OK')
            client.rcpt.side_effect = [(250, b'OK'), (550, b'rejected')]
            with self.assertRaises(Exception):
                send_digest(msg, settings)
            client.data.assert_not_called()

    def test_starttls_delivery_uses_all_recipients_and_authentication(self):
        settings = SMTPSettings.from_env({'SMTP_HOST': 'smtp.example.org',
                                        'SMTP_USER': 'user', 'SMTP_PASSWORD': 'secret',
                                        'EMAIL_FROM': 'radar@example.org',
                                        'EMAIL_TO': 'one@example.org,two@example.org'})
        msg = render_digest([row(status='NEW')])
        with patch('tender_notifications.smtplib.SMTP') as factory:
            client = factory.return_value.__enter__.return_value
            client.mail.return_value = (250, b'OK')
            client.rcpt.return_value = (250, b'OK')
            client.data.return_value = (250, b'accepted')
            send_digest(msg, settings)
            self.assertEqual(msg['To'], 'one@example.org, two@example.org')
            client.starttls.assert_called_once()
            client.login.assert_called_once_with('user', 'secret')
            self.assertEqual(client.rcpt.call_count, 2)
            client.data.assert_called_once()

class AdditionalDeliveryTests(unittest.TestCase):
    def test_unicode_digest_uses_ascii_safe_mime_transfer_encoding(self):
        msg = render_digest([row(status='NEW', organization='राष्ट्रीय संस्थान')])
        self.assertTrue(msg.as_bytes().isascii())

    def test_case_sensitive_source_path_change_is_meaningful(self):
        self.assertNotEqual(fingerprint(row(source_url='https://example.org/Tender/1')),
                            fingerprint(row(source_url='https://example.org/tender/1')))

    def test_smtp_cleanup_error_after_data_acceptance_does_not_request_retry(self):
        import smtplib
        settings = SMTPSettings.from_env({'SMTP_HOST': 'smtp.example.org',
                                         'EMAIL_FROM': 'radar@example.org', 'EMAIL_TO': 'one@example.org'})
        with patch('tender_notifications.smtplib.SMTP') as factory:
            client = factory.return_value.__enter__.return_value
            client.mail.return_value = (250, b'OK')
            client.rcpt.return_value = (250, b'OK')
            client.data.return_value = (250, b'Accepted')
            factory.return_value.__exit__.side_effect = smtplib.SMTPResponseException(500, b'QUIT failed')
            send_digest(render_digest([row(status='NEW')]), settings)
