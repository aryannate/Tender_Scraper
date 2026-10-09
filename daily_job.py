#!/usr/bin/env python3
"""Run existing source scans once, then email only new/updated matching tenders."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

import run_scraper
from keywords import KeywordMatcher, load_keywords
from pipeline import FIELDS, _atomic_write_csv, _atomic_write_json, _write_and_replace_with_retry
from tender_notifications import SMTPSettings, TenderState, render_digest, send_digest

ROOT = Path(__file__).resolve().parent
LOGGER = logging.getLogger('tender_radar')


def parse_args(argv=None):
    parser = run_scraper.build_parser(include_inspection=False)
    parser.description = 'Full fresh scrape followed by one new/updated tender email digest'
    parser.set_defaults(source='everything', out_dir=os.environ.get('TENDER_OUTPUT_DIR') or
                        str(ROOT / 'outputs' / 'daily'),
                        keywords_file=os.environ.get('KEYWORDS_FILE'))
    parser.add_argument('--dry-run', action='store_true',
                        help='save digest preview and observations, but never send or mark notified')
    parser.add_argument('--state-db', default=os.environ.get('TENDER_STATE_DB'),
                        help='persistent SQLite file (default: <out-dir>/tender_state.sqlite3)')
    args = parser.parse_args(argv)
    if args.concurrency < 1 or args.rate_limit < 0 or args.checkpoint_every < 1:
        parser.error('concurrency/checkpoint-every must be positive; rate-limit must be nonnegative')
    if (args.max_pages is not None and args.max_pages < 1) or args.gem_pages_per_keyword < 1:
        parser.error('page limits must be positive')
    args.out_dir = str(Path(args.out_dir).expanduser().resolve())
    args.state_db = str(Path(args.state_db).expanduser().resolve() if args.state_db else
                        Path(args.out_dir) / 'tender_state.sqlite3')
    return args


class AlreadyRunning(Exception):
    pass


@contextlib.contextmanager
def run_lock(state_db):
    """Linux advisory lock lasts through scrape/send/commit and releases on exit."""
    import fcntl
    lock_path = Path(str(state_db) + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning('Another daily job is using this state database') from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def configure_logging(out_dir):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for handler in LOGGER.handlers[:]:
        LOGGER.removeHandler(handler)
        handler.close()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # Bind console before redirecting legacy print() output from connectors.
    console = logging.StreamHandler(sys.stdout)
    file = RotatingFileHandler(Path(out_dir) / 'daily_job.log', maxBytes=5_000_000,
                               backupCount=3, encoding='utf-8')
    for handler in (console, file):
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


class _LogStream:
    def __init__(self):
        self.buffer = ''

    def write(self, value):
        self.buffer += value
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            if line.strip():
                LOGGER.info('%s', line)
        return len(value)

    def flush(self):
        if self.buffer.strip():
            LOGGER.info('%s', self.buffer)
        self.buffer = ''


def _write_text(path: Path, text: str):
    _write_and_replace_with_retry(str(path), lambda tmp: Path(tmp).write_text(text, encoding='utf-8'))


async def execute(args) -> int:
    configure_logging(args.out_dir)
    try:
        with run_lock(args.state_db):
            return await _execute_locked(args)
    except AlreadyRunning as exc:
        LOGGER.info('%s; exiting without a second scan or email.', exc)
        return 0
    except Exception:
        LOGGER.exception('Daily job failed before completion; existing output/state retained.')
        return 1


async def _execute_locked(args) -> int:
    out = Path(args.out_dir)
    summary = {'started_at': datetime.now(timezone.utc).isoformat(),
               'sources_attempted': 0, 'sources_successful': 0, 'tenders_scanned': 0,
               'matching_tenders': 0, 'new_tenders': 0, 'updated_tenders': 0,
               'notifications_sent': 0, 'tenders_notified': 0, 'errors': 0,
               'dry_run': args.dry_run, 'sources': {}}
    LOGGER.info('Daily tender scan started; source=%s; dry_run=%s', args.source, args.dry_run)
    try:
        matcher = KeywordMatcher(load_keywords(args.keywords_file))
        scrape_args = argparse.Namespace(**vars(args))
        # daily --dry-run controls email; the old runner's flag inspects GePNIC.
        scrape_args.dry_run = False
        stream = _LogStream()
        try:
            with contextlib.redirect_stdout(stream):
                sink = await run_scraper.run(scrape_args)
        finally:
            stream.flush()
        summary['sources'] = sink.source_outcomes
        summary['sources_attempted'] = len(sink.source_outcomes)
        summary['sources_successful'] = sum(v == 'ok' for v in sink.source_outcomes.values())
        summary['tenders_scanned'] = sink.scanned
        summary['errors'] = len(sink.new_errors)
        if not sink.source_outcomes:
            raise ValueError('No sources were attempted; check --source and --state')
        for error in sink.new_errors:
            LOGGER.error('Source %s [%s]: %s', error['source'], error['tender_id'], error['reason'])
        # Strict current keyword list applies even if --no-filter fetched raw data.
        # Never read the accumulated CSV as today's observation set.
        matching = []
        for row in sink.current_rows:
            hits = matcher.match(row['title'], row['description'], row['organization'])
            if hits:
                matching.append({**row, 'matched_keywords': '; '.join(hits)})
        summary['matching_tenders'] = len(matching)
        _atomic_write_csv(str(out / 'current_run.csv'), FIELDS, matching)
        with TenderState(args.state_db) as state:
            pending = state.observe(matching)
            summary['new_tenders'] = sum(row['status'] == 'NEW' for row in pending)
            summary['updated_tenders'] = sum(row['status'] == 'UPDATED' for row in pending)
            if pending:
                message = render_digest(pending)
                _write_text(out / 'digest.html', message.get_body(preferencelist=('html',)).get_content())
                _write_text(out / 'digest.txt', message.get_body(preferencelist=('plain',)).get_content())
                if args.dry_run:
                    LOGGER.info('Digest preview saved; no email sent and no tenders marked notified.')
                else:
                    settings = SMTPSettings.from_env(os.environ)
                    # The process lock prevents competing jobs from sending the same pending rows.
                    send_digest(message, settings)
                    state.mark_notified(pending)
                    summary['notifications_sent'] = 1
                    summary['tenders_notified'] = len(pending)
                    LOGGER.info('One digest accepted by SMTP for %d recipient(s).', len(settings.recipients))
            else:
                for filename in ('digest.html', 'digest.txt'):
                    (out / filename).unlink(missing_ok=True)
                LOGGER.info('No new or updated matching tenders; no email sent.')
    except Exception as exc:
        summary['errors'] += 1
        LOGGER.error('Daily job error (%s): %s. Scraped output and pending notifications are retained.',
                     type(exc).__name__, exc)
    finally:
        summary['completed_at'] = datetime.now(timezone.utc).isoformat()
        _atomic_write_json(str(out / 'run_summary.json'), summary)
        LOGGER.info('Completion: sources attempted=%d; sources successful=%d; tenders scanned=%d; '
                    'matching tenders=%d; new tenders=%d; updated tenders=%d; '
                    'notifications sent=%d; tenders notified=%d; errors=%d',
                    *(summary[name] for name in ('sources_attempted', 'sources_successful',
                      'tenders_scanned', 'matching_tenders', 'new_tenders', 'updated_tenders',
                      'notifications_sent', 'tenders_notified', 'errors')))
    return 1 if summary['errors'] else 0


def main(argv=None):
    load_dotenv(ROOT / '.env', override=False)
    try:
        return asyncio.run(execute(parse_args(argv)))
    except KeyboardInterrupt:
        LOGGER.warning('Interrupted; no pending tenders were marked notified.')
        return 130


if __name__ == '__main__':
    sys.exit(main())
