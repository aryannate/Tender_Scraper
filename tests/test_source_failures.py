import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import ne_states
import playwright_states
import run_scraper
from keywords import KeywordMatcher
from pipeline import ResultSink, ScraperConfig


class SourceFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_playwright_pagination_timeout_marks_partial_source_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink = ResultSink(ScraperConfig(out_dir=tmp), KeywordMatcher(['GPU']))
            page = MagicMock()
            page.goto = AsyncMock()
            page.wait_for_timeout = AsyncMock()
            # One valid page, then an enabled Next control that fails to advance.
            page.evaluate = AsyncMock(return_value={
                'header': ['Department', 'Tender ID', 'Notice No', 'Category', 'Name of Work', 'Closing'],
                'rows': [['Institute', 'TS-1', 'REF', 'PRODUCTS', 'GPU server', '12-Oct-2026']]})
            next_control = MagicMock()
            next_control.click = AsyncMock(side_effect=TimeoutError('pagination timed out'))
            page.get_by_text.return_value.first = next_control
            page.get_by_text.side_effect = lambda text, **kw: (MagicMock(first=MagicMock(click=AsyncMock()))
                                                             if text != 'Next' else MagicMock(first=next_control))
            browser = MagicMock()
            browser.new_page = AsyncMock(return_value=page)
            browser.close = AsyncMock()
            with patch('playwright.async_api.async_playwright') as factory, \
                 patch('playwright_states._set_table_page_size', new=AsyncMock()), \
                 patch('playwright_states._next_enabled', new=AsyncMock(return_value=True)):
                factory.return_value.__aenter__.return_value.chromium.launch = AsyncMock(return_value=browser)
                await run_scraper._run_group(sink, 'playwright:telangana',
                    playwright_states.scrape_tsap(sink.config, sink, 'telangana'))
            self.assertEqual(sink.source_outcomes['playwright:telangana'], 'error')
            self.assertEqual(len(sink.current_rows), 1)  # Preserve the useful first page.
            self.assertIn('pagination', sink.new_errors[0]['reason'])

    async def test_next_control_inspection_failure_propagates_to_source(self):
        locator = MagicMock()
        locator.count = AsyncMock(side_effect=TimeoutError('button unavailable'))
        with self.assertRaises(TimeoutError):
            await playwright_states._next_enabled(locator)

    async def test_daman_server_error_after_first_page_is_not_end_of_list(self):
        html = '''<table><tr><th>Name</th><th>Title</th><th>Description</th><th>Start</th><th>End Date</th></tr>
        <tr><td>1</td><td>GPU Server</td><td>Cluster</td><td>01/10/2026</td><td>12/10/2026</td></tr>
        <tr><td>2</td><td>GPU Workstation</td><td>Compute</td><td>01/10/2026</td><td>12/10/2026</td></tr></table>'''
        with tempfile.TemporaryDirectory() as tmp:
            sink = ResultSink(ScraperConfig(out_dir=tmp), KeywordMatcher(['GPU']))
            with patch('ne_states.fetch', new=AsyncMock(side_effect=[html, RuntimeError('HTTP 500')])):
                await ne_states.scrape_daman_diu(None, sink.config, sink)
            self.assertTrue(sink.new_errors)
            self.assertEqual(len(sink.current_rows), 2)

    async def test_cppp_captcha_gate_is_logged_without_emitting_rows(self):
        import cppp_scraper
        with tempfile.TemporaryDirectory() as tmp:
            sink = ResultSink(ScraperConfig(out_dir=tmp), KeywordMatcher(['GPU']))
            with patch('cppp_scraper.fetch', new=AsyncMock(return_value='<input name="captchaText">GPU Server')):
                await cppp_scraper._scrape_page(None, sink.config, sink, 1)
            self.assertEqual(sink.current_rows, [])
            self.assertIn('captcha', sink.new_errors[0]['reason'])
