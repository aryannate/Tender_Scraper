import asyncio
import csv
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import daily_job
import run_scraper
from pipeline import ResultSink, ScraperConfig
from keywords import KeywordMatcher
from tender_notifications import TenderState


def row(**changes):
    return {'source': 'cppp', 'tender_id': '2026_TEST_1', 'title': 'GPU Server',
            'description': 'H200', 'organization': 'Institute',
            'published_date': '01-Oct-2026', 'closing_date': '12-Oct-2026', **changes}


class DailyJobTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        self.args = daily_job.parse_args(['--out-dir', str(self.out), '--source', 'cppp'])

    async def source(self, session, config, sink, max_pages):
        await sink.emit(row())

    async def test_two_complete_runs_send_only_one_digest_and_persist_summary(self):
        env = {'SMTP_HOST': 'smtp.example.org', 'EMAIL_FROM': 'radar@example.org',
               'EMAIL_TO': 'one@example.org,two@example.org'}
        with patch.dict(os.environ, env, clear=True), patch('run_scraper.cppp_scraper.scrape_cppp', self.source), \
             patch('daily_job.send_digest') as send:
            self.assertEqual(await daily_job.execute(self.args), 0)
            self.assertEqual(await daily_job.execute(self.args), 0)
            self.assertEqual(send.call_count, 1)
        summary = json.loads((self.out / 'run_summary.json').read_text())
        self.assertEqual(summary['notifications_sent'], 0)
        self.assertEqual(summary['sources_attempted'], 1)
        self.assertEqual(summary['sources_successful'], 1)
        self.assertEqual(summary['tenders_scanned'], 1)
        self.assertEqual(summary['matching_tenders'], 1)
        self.assertEqual(summary['new_tenders'], 0)
        self.assertTrue((self.out / 'daily_job.log').is_file())

    async def test_missing_smtp_preserves_scrape_and_pending_state(self):
        with patch.dict(os.environ, {}, clear=True), patch('run_scraper.cppp_scraper.scrape_cppp', self.source):
            self.assertEqual(await daily_job.execute(self.args), 1)
        with (self.out / 'current_run.csv').open(encoding='utf-8-sig', newline='') as f:
            self.assertEqual(len(list(csv.DictReader(f))), 1)
        with TenderState(self.out / 'tender_state.sqlite3') as state:
            self.assertEqual(state.observe([row()])[0]['status'], 'NEW')
        self.assertIn('SMTP_HOST', (self.out / 'daily_job.log').read_text())

    async def test_failed_send_is_retried_as_new_and_success_then_suppresses(self):
        env = {'SMTP_HOST': 'smtp.example.org', 'EMAIL_FROM': 'radar@example.org', 'EMAIL_TO': 'one@example.org'}
        with patch.dict(os.environ, env, clear=True), patch('run_scraper.cppp_scraper.scrape_cppp', self.source), \
             patch('daily_job.send_digest', side_effect=[OSError('SMTP unavailable'), None]) as send:
            self.assertEqual(await daily_job.execute(self.args), 1)
            self.assertEqual(await daily_job.execute(self.args), 0)
            self.assertEqual(await daily_job.execute(self.args), 0)
            self.assertEqual(send.call_count, 2)

    async def test_dry_run_never_sends_or_marks_notified(self):
        self.args.dry_run = True
        with patch.dict(os.environ, {}, clear=True), patch('run_scraper.cppp_scraper.scrape_cppp', self.source), \
             patch('daily_job.send_digest') as send:
            self.assertEqual(await daily_job.execute(self.args), 0)
            send.assert_not_called()
        self.assertIn('GPU Server', (self.out / 'digest.html').read_text())
        with TenderState(self.out / 'tender_state.sqlite3') as state:
            self.assertEqual(state.observe([row()])[0]['status'], 'NEW')

    async def test_empty_results_send_nothing(self):
        async def empty(*args):
            return
        with patch.dict(os.environ, {}, clear=True), patch('run_scraper.cppp_scraper.scrape_cppp', empty), \
             patch('daily_job.send_digest') as send:
            self.assertEqual(await daily_job.execute(self.args), 0)
            send.assert_not_called()
        self.assertEqual(json.loads((self.out / 'run_summary.json').read_text())['matching_tenders'], 0)

    async def test_source_crash_does_not_kill_other_source_or_its_digest(self):
        async def broken(*args):
            raise RuntimeError('portal failed')
        async def good(session, config, sink, max_pages):
            await sink.emit(row(source='gail'))
        self.args.source = 'psus'
        env = {'SMTP_HOST': 'smtp.example.org', 'EMAIL_FROM': 'radar@example.org', 'EMAIL_TO': 'one@example.org'}
        with patch.dict(os.environ, env, clear=True), \
             patch.dict('run_scraper.psu_portals.PSU_PORTALS', {'broken': broken, 'good': good}, clear=True), \
             patch('daily_job.send_digest') as send:
            self.assertEqual(await daily_job.execute(self.args), 1)
            self.assertEqual(send.call_count, 1)
        summary = json.loads((self.out / 'run_summary.json').read_text())
        self.assertEqual(summary['sources_attempted'], 2)
        self.assertEqual(summary['sources_successful'], 1)
        self.assertEqual(summary['notifications_sent'], 1)
        self.assertEqual(summary['errors'], 1)

    async def test_handled_page_failure_is_counted_as_source_error(self):
        async def failed(session, config, sink, max_pages):
            await sink.emit_error('cppp', 'page:1', 'fetch failed')
        with patch('run_scraper.cppp_scraper.scrape_cppp', failed):
            self.assertEqual(await daily_job.execute(self.args), 1)
        summary = json.loads((self.out / 'run_summary.json').read_text())
        self.assertEqual(summary['sources_successful'], 0)
        self.assertEqual(summary['errors'], 1)

    async def test_historical_csv_rows_do_not_create_false_alerts_after_failed_scan(self):
        sink = ResultSink(ScraperConfig(out_dir=str(self.out)), KeywordMatcher(['GPU']))
        await sink.emit(row())
        await sink.finalize()
        async def failed(*args):
            raise RuntimeError('unavailable')
        with patch('run_scraper.cppp_scraper.scrape_cppp', failed), patch('daily_job.send_digest') as send:
            self.assertEqual(await daily_job.execute(self.args), 1)
            send.assert_not_called()
        with TenderState(self.out / 'tender_state.sqlite3') as state:
            self.assertEqual(state.connection.execute('SELECT COUNT(*) FROM tenders').fetchone()[0], 0)

    async def test_lock_prevents_overlapping_job(self):
        with daily_job.run_lock(self.out / 'tender_state.sqlite3'):
            with patch('daily_job.run_scraper.run') as scrape:
                self.assertEqual(await daily_job.execute(self.args), 0)
                scrape.assert_not_called()

    def test_daily_defaults_include_every_http_and_browser_source(self):
        self.assertEqual(daily_job.parse_args([]).source, 'everything')
