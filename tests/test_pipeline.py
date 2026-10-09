import asyncio
import csv
import tempfile
import unittest
from pathlib import Path

from keywords import KEYWORDS, KeywordMatcher, load_keywords
from pipeline import ResultSink, ScraperConfig, make_session


def tender(**changes):
    return {'source': 'gem', 'tender_id': 'GEM/2026/1', 'title': 'GPU server',
            'organization': 'Research Institute', 'published_date': '01-Oct-2026',
            'closing_date': '12-Oct-2026 11:00 AM', **changes}


class KeywordTests(unittest.TestCase):
    def test_target_keywords_and_boundaries(self):
        matcher = KeywordMatcher()
        for text, expected in [('AI server with H200 GPUs', 'H200'),
                               ('Spectrum-X 400GbE NVL72', 'Spectrum-X'),
                               ('Parallel file system with BeeGFS', 'BeeGFS'),
                               ('Data-Centre AI Workstations', 'Data Centre')]:
            with self.subTest(text=text):
                self.assertIn(expected, matcher.match(text))
        self.assertFalse(KeywordMatcher(['AI', 'GPU', 'H100']).matches('chair GPUSupport H1000'))
        self.assertIn('GPU', KeywordMatcher(['GPU']).match('GPUs'))

    def test_custom_list_is_trimmed_deduplicated_and_empty_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'keywords.txt'
            p.write_text('  # comment\n GPU \ngpu\nAI Server\n', encoding='utf-8')
            self.assertEqual(load_keywords(str(p)), ['GPU', 'AI Server'])
            p.write_text('# empty\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                load_keywords(str(p))


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_scan_updates_title_and_shortened_deadline_and_excludes_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ScraperConfig(out_dir=tmp)
            first = ResultSink(cfg, KeywordMatcher(['GPU']))
            await first.emit(tender())
            await first.emit(tender(tender_id='old-only'))
            await first.finalize()
            second = ResultSink(cfg, KeywordMatcher(['GPU']))
            await second.emit(tender(title='GPU server revised', closing_date='10-Oct-2026 11:00 AM'))
            await second.finalize()
            with open(cfg.output_path, encoding='utf-8-sig', newline='') as f:
                rows = {r['tender_id']: r for r in csv.DictReader(f)}
            self.assertEqual(rows['GEM/2026/1']['title'], 'GPU server revised')
            self.assertEqual(rows['GEM/2026/1']['closing_date'], '10-Oct-2026 11:00 AM')
            self.assertEqual([r['tender_id'] for r in second.current_rows], ['GEM/2026/1'])

    async def test_custom_keywords_apply_to_empanelments(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = ResultSink(ScraperConfig(out_dir=tmp), KeywordMatcher(['GPU']))
            await sink.emit(tender(title='Empanelment for software development'))
            self.assertEqual(sink._rows, {})

    async def test_scanned_counts_every_normalized_row_even_without_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = ResultSink(ScraperConfig(out_dir=tmp))
            await sink.emit(tender())
            await sink.emit(tender(tender_id='old', published_date='01-Jan-2025'))
            self.assertEqual(sink.scanned, 2)

    async def test_http_uses_proxy_environment_and_verifies_tls(self):
        async with make_session(ScraperConfig()) as session:
            self.assertTrue(session.trust_env)
            self.assertIsNot(session.connector._ssl, False)

    async def test_duplicate_variants_have_deterministic_selection_across_runs(self):
        from tender_notifications import TenderState
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ScraperConfig(out_dir=tmp)
            variants = [tender(description='GPU cluster', tender_type='Service'),
                        tender(description='H200 cluster', tender_type='Product')]
            with TenderState(Path(tmp) / 'state.sqlite3') as state:
                for index, observations in enumerate((variants, list(reversed(variants)))):
                    sink = ResultSink(cfg, KeywordMatcher(['GPU']))
                    for item in observations:
                        await sink.emit(item)
                    alerts = state.observe(sink.current_rows)
                    if index == 0:
                        state.mark_notified(alerts)
                    else:
                        self.assertEqual(alerts, [], 'Arrival order must not produce UPDATED')

    async def test_fetch_errors_do_not_log_response_cookie_values(self):
        import aiohttp
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from pipeline import RetryableError, fetch
        info = SimpleNamespace(real_url='https://example.org/list', headers={'Cookie': 'private-cookie'})
        error = aiohttp.ClientResponseError(info, (), status=403, message='Forbidden',
                                           headers={'Set-Cookie': 'private-cookie'})
        session = MagicMock()
        session.request.return_value.__aenter__.side_effect = error
        with self.assertRaises(RetryableError) as caught:
            await fetch(session, 'GET', 'https://example.org/list',
                        ScraperConfig(max_retries=0, rate_limit=0))
        self.assertNotIn('private-cookie', str(caught.exception))
        self.assertIn('403', str(caught.exception))
