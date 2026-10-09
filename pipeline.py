"""
Shared pipeline for the government-tender scraper.

Everything source-agnostic lives here so the GeM / CPPP / GePNIC modules only
have to worry about fetching + parsing their own pages and emitting normalized
tender dicts:

    - HTTP with retries (exponential backoff) + polite rate limiting
    - 60-worker concurrency (asyncio.Semaphore), shared across all sources
    - Every run is a full re-scan of every source, every listing page — no
      source is allowed to skip pages/tenders it thinks it already saw. The
      only "memory" across runs is the output CSV itself, which is loaded so
      already-collected rows aren't lost, then rewritten from scratch.
    - Live progress with rolling-average ETA
    - Deduplication on (source, tender_id), keeping the latest corrigendum
    - A separate errors_log.csv for anything that failed to parse
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import random
import re
import time
from datetime import datetime
from collections import deque
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Iterable

from keywords import EMPANELMENT_KEYWORDS, KeywordMatcher, is_empanelment

import aiohttp

# Normalized output schema. One row per tender, every source maps into this.
FIELDS = [
    "tender_id",
    "title",
    "description",
    "organization",
    "state",
    "published_date",
    "opening_date",  # bid-opening date where the listing exposes it; else blank
    "closing_date",
    "source",        # gem / cppp / <state name>
    "source_url",
    "corrigendum",   # marker used for keep-latest dedup; blank if none
    "matched_keywords",  # which target keywords this tender matched (if filtering)
    "is_empanelment",    # "1" if empanelment/EOI/rate-contract, else ""
    "tender_type",       # Service / Product / Works / Unidentified — extracted
                         # from the portal's own category data only, never
                         # inferred from title/description text
]

# Only tenders floated (published) on/after this date are kept. Rows whose
# published date is missing or unparseable are KEPT (we can't verify them;
# dropping would wipe whole sources that don't expose a published date).
FLOAT_FROM = datetime(2026, 1, 1)

ERROR_FIELDS = ["source", "tender_id", "reason"]
SOURCE_GROUP = ContextVar("source_group", default="")

# The only valid tender categories. A tender's category is EXTRACTED from the
# portal-supplied category value a scraper passes in `tender_type` (e.g. the
# TS/AP "Category" column, GeM's goods/services bid split). Unknown values are
# NOT mapped or guessed from listing text: normalize_category() only accepts a
# value whose tokens literally name one of the three categories; anything else
# is honestly "Unidentified".
_CATEGORY_TOKENS = {
    "service": "Service", "services": "Service",
    "product": "Product", "products": "Product",
    "work": "Works", "works": "Works",
}


def normalize_category(raw: Any) -> str:
    """Map a scraped category value to Service/Product/Works or Unidentified."""
    for tok in re.split(r"[^a-z]+", str(raw or "").lower()):
        if tok in _CATEGORY_TOKENS:
            return _CATEGORY_TOKENS[tok]
    return "Unidentified"


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class ScraperConfig:
    """Tunables shared by every source module."""
    concurrency: int = 60           # asyncio.Semaphore workers
    max_retries: int = 3            # retries per request on network error
    backoff_base: float = 0.5       # exponential backoff base (seconds)
    rate_limit: float = 0.25        # polite delay between requests (~0.2-0.3s)
    checkpoint_every: int = 20      # flush CSV every N rows
    request_timeout: float = 45.0
    out_dir: str = "outputs"
    output_csv: str = "tenders_gem_cppp.csv"
    errors_csv: str = "errors_log.csv"

    @property
    def output_path(self) -> str:
        return os.path.join(self.out_dir, self.output_csv)

    @property
    def errors_path(self) -> str:
        return os.path.join(self.out_dir, self.errors_csv)


class RetryableError(Exception):
    """Raised internally so callers can distinguish transient network failures."""


async def fetch(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    config: ScraperConfig,
    *,
    expect: str = "text",   # "text" or "json"
    **kwargs: Any,
) -> Any:
    """
    Fetch a URL with exponential-backoff retries and a polite delay.

    Raises RetryableError only after `max_retries` consecutive failures so the
    caller can log-and-skip that single unit of work without crashing the run.
    """
    last_exc: Exception | None = None
    for attempt in range(config.max_retries + 1):
        try:
            async with session.request(
                method, url,
                timeout=aiohttp.ClientTimeout(total=config.request_timeout),
                **kwargs,
            ) as resp:
                # 5xx / 429 are worth retrying; other 4xx are not.
                if resp.status in (429, 500, 502, 503, 504):
                    raise RetryableError(f"HTTP {resp.status} for {url}")
                resp.raise_for_status()
                if expect == "json":
                    # Some .gov.in endpoints send JSON as text/html.
                    text = await resp.text()
                    return json.loads(text)
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError, RetryableError,
                json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt < config.max_retries:
                # exponential backoff with jitter
                delay = config.backoff_base * (2 ** attempt) + random.uniform(0, 0.3)
                await asyncio.sleep(delay)
            else:
                raise RetryableError(f"{url} failed after "
                                     f"{config.max_retries} retries: "
                                     f"{type(exc).__name__}: {exc}") from last_exc
        finally:
            # Rate limit every attempt so retries also stay polite.
            await asyncio.sleep(config.rate_limit)


class Progress:
    """Live progress printer with rolling-average ETA (not single-sample)."""

    def __init__(self, window: int = 15):
        self._batch_durations: deque[tuple[int, float]] = deque(maxlen=window)
        self._total = 0
        self._done = 0
        self._last_tick = time.monotonic()

    def set_total(self, total: int) -> None:
        # Each source contributes its own estimate; totals accumulate so the
        # combined "X / Y" reflects all sources in one run.
        self._total += total

    def add_done(self, n: int) -> None:
        self._done += n

    def report(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_tick
        self._last_tick = now
        # record (items, seconds) for this batch
        self._batch_durations.append((max(self._done_delta(), 1), elapsed))
        # rolling average seconds-per-item over the window
        items = sum(i for i, _ in self._batch_durations)
        secs = sum(s for _, s in self._batch_durations)
        rate = (secs / items) if items else 0.0
        remaining = max(self._total - self._done, 0)
        eta = remaining * rate
        m, sdisp = divmod(int(eta), 60)
        total_disp = self._total if self._total else "?"
        print(f"  Processed {self._done} / {total_disp} tenders | "
              f"ETA: {m}m {sdisp}s", flush=True)

    _prev_done = 0

    def _done_delta(self) -> int:
        delta = self._done - self._prev_done
        self._prev_done = self._done
        return delta


class ResultSink:
    """
    Collects normalized tenders from every source, dedupes them, and handles
    progress / error logging. Source modules only call:
        sink.set_total(n)
        sink.emit(tender_dict)
        sink.emit_error(source, tender_id, reason)

    There is no checkpointing and no "already seen, skip it" logic. Every run
    re-scans every listing page of every source in full — the only thing
    carried over between runs is the existing output CSV, preloaded so a
    tender collected before isn't lost when the CSV is rewritten, but every
    source still walks its entire listing every time. This is deliberate:
    early-stopping on a "we've seen this before" signal is exactly what caused
    tenders to go missing previously (a portal's listing order or ID scheme
    doesn't have to match our assumptions), so no source is allowed to skip
    pages or bail out early because of prior-run state.
    """

    def __init__(self, config: ScraperConfig, matcher=None):
        self.config = config
        # Optional KeywordMatcher; when set, tenders that match no keyword
        # (and aren't a keyword-matching empanelment) are dropped.
        self.matcher = matcher
        # Empanelments must match the IT-domain empanelment keyword list.
        self._emp_matcher = matcher or KeywordMatcher(EMPANELMENT_KEYWORDS)
        self.filtered_out = 0
        self.scanned = 0
        os.makedirs(config.out_dir, exist_ok=True)
        self.progress = Progress()
        # dedup store keyed by (source, tender_id) -> row (only tenders that
        # PASSED the keyword filter, i.e. what ends up in the output CSV)
        self._rows: dict[tuple[str, str], dict[str, str]] = {}
        # Fresh observations only; historical CSV rows never become alerts.
        self._current_rows: dict[tuple[str, str], dict[str, str]] = {}
        self.new_errors: list[dict[str, str]] = []
        self.group_errors: dict[str, int] = {}
        self.source_outcomes: dict[str, str] = {}
        self._errors: list[dict[str, str]] = []
        self._since_flush = 0
        self._lock = asyncio.Lock()
        # Preload prior results so a tender collected in an earlier run isn't
        # lost when we atomically rewrite the CSV at the next flush.
        self._preload_existing()

    def _preload_existing(self) -> None:
        if os.path.exists(self.config.output_path):
            try:
                with open(self.config.output_path, encoding="utf-8-sig", newline="") as fh:
                    for row in csv.DictReader(fh):
                        key = (row.get("source", ""), row.get("tender_id", ""))
                        if key[1]:
                            self._rows[key] = {k: row.get(k, "") for k in FIELDS}
            except (OSError, csv.Error):
                pass
        if os.path.exists(self.config.errors_path):
            try:
                with open(self.config.errors_path, encoding="utf-8-sig", newline="") as fh:
                    self._errors = [dict(r) for r in csv.DictReader(fh)]
            except (OSError, csv.Error):
                pass
        if self._rows:
            print(f"  [preload] loaded {len(self._rows)} existing tenders "
                  f"from {self.config.output_path}")
            self._revalidate_preloaded()

    def _revalidate_preloaded(self) -> None:
        """
        Re-apply the CURRENT keyword/empanelment/category rules to every
        preloaded row and evict/re-tag anything that no longer qualifies.

        emit()'s own eviction (see below) only fires for a tender that gets
        RE-SCANNED this run — but a tender that has since closed or fallen off
        a search-driven source's result set (GeM) is never re-scanned at all,
        so without this pass a row that matched under an OLD keyword list
        could never be removed, even after the list changes and the run
        otherwise completes normally.
        """
        dropped = 0
        for key, row in list(self._rows.items()):
            row["tender_type"] = normalize_category(row.get("tender_type"))
            emp_hits = []
            if is_empanelment(row.get("title", ""), row.get("description", "")):
                emp_hits = self._emp_matcher.match(row.get("title", ""),
                                                    row.get("description", ""),
                                                    row.get("organization", ""))
            row["is_empanelment"] = "1" if emp_hits else ""
            if self.matcher is not None:
                hits = self.matcher.match(row.get("title", ""),
                                          row.get("description", ""),
                                          row.get("organization", ""))
                if not hits and not emp_hits:
                    del self._rows[key]
                    dropped += 1
                    continue
                row["matched_keywords"] = "; ".join(hits or emp_hits)
        if dropped:
            print(f"  [preload] re-validated against current rules: "
                  f"{dropped} row(s) no longer match and were dropped")

    # ---- results -------------------------------------------------------------
    def set_total(self, total: int) -> None:
        self.progress.set_total(total)

    @property
    def current_rows(self) -> list[dict[str, str]]:
        return list(self._current_rows.values())

    @staticmethod
    def _is_newer(new: dict[str, str], existing: dict[str, str]) -> bool:
        """Keep-latest rule for corrigendums / re-listings."""
        # Prefer corrigenda and later dates within this run. A total-order
        # tie break keeps concurrent keyword/page arrival order from generating
        # false UPDATED notifications when the same variants reappear tomorrow.
        def rank(row):
            def date_rank(name):
                value = row.get(name, "")
                dt = _parse_any_date(value)
                return (1, dt.isoformat()) if dt is not None else (0, value)
            content = json.dumps({name: row.get(name, "") for name in FIELDS
                                  if name not in ("matched_keywords", "is_empanelment")},
                                 sort_keys=True, ensure_ascii=False)
            return (bool(row.get("corrigendum")), date_rank("published_date"),
                    date_rank("closing_date"), content)
        return rank(new) > rank(existing)

    async def emit(self, tender: dict[str, Any]) -> None:
        row = {k: _clean(tender.get(k, "")) for k in FIELDS}
        key = (row["source"], row["tender_id"])
        self.scanned += 1
        # Empanelments are kept only when they match the IT-domain empanelment
        # keyword list (EMPANELMENT_KEYWORDS) — a drug-supply rate contract is
        # dropped, a software-development empanelment is kept. Flag them so the
        # dashboard's Empanelments tab can show them without polluting the
        # IT-only tender tab.
        emp_hits = []
        if is_empanelment(row["title"], row["description"]):
            emp_hits = self._emp_matcher.match(row["title"], row["description"],
                                               row["organization"])
        row["is_empanelment"] = "1" if emp_hits else ""
        # Category: extracted ONLY from the portal-supplied category value the
        # scraper passed (raw is fine — it's normalized here). Sources with no
        # category in their scraped details stay "Unidentified"; nothing is
        # inferred from title/description text.
        row["tender_type"] = normalize_category(tender.get("tender_type"))
        # Float-date cutoff: only tenders published on/after FLOAT_FROM. Rows
        # with no parseable published date are kept (can't verify).
        pub = _parse_any_date(row["published_date"])
        if pub is not None and pub < FLOAT_FROM:
            self.filtered_out += 1
            self.progress.add_done(1)
            return
        # Keyword filter (if enabled) — applied to EVERY source. Keep a tender
        # if it matches an IT/AI keyword OR is a keyword-matching empanelment.
        if self.matcher is not None:
            hits = self.matcher.match(row["title"], row["description"],
                                      row["organization"])
            if not hits and not emp_hits:
                self.filtered_out += 1
                self.progress.add_done(1)  # count scanned-but-dropped as progress
                # A tender re-scanned this run that no longer matches any
                # keyword must be EVICTED from a preloaded/previous-run row,
                # not just skipped — otherwise a keyword-list change (or a
                # portal edit that removes the matching text) can never
                # remove a row once it's in the CSV, since every run only
                # preloads-then-accumulates and nothing else deletes rows.
                async with self._lock:
                    self._rows.pop(key, None)
                    self._current_rows.pop(key, None)
                return
            row["matched_keywords"] = "; ".join(hits or emp_hits)
        async with self._lock:
            if not row["tender_id"]:
                error = {
                    "source": row["source"], "tender_id": "",
                    "reason": "empty tender_id after parse",
                }
                self._record_error(error)
                return
            # The first live observation supersedes yesterday's CSV even if
            # its deadline was shortened or its title changed without a date.
            existing = self._current_rows.get(key)
            if existing is None or self._is_newer(row, existing):
                self._rows[key] = row
            self._current_rows[key] = self._rows[key]
            self._since_flush += 1
            self.progress.add_done(1)
            if self._since_flush >= self.config.checkpoint_every:
                self._flush_locked()

    async def emit_error(self, source: str, tender_id: str, reason: str) -> None:
        async with self._lock:
            self._record_error({
                "source": source, "tender_id": str(tender_id or ""),
                "reason": reason,
            })

    def _record_error(self, error: dict[str, str]) -> None:
        self._errors.append(error)
        self.new_errors.append(error)
        group = SOURCE_GROUP.get() or error["source"]
        self.group_errors[group] = self.group_errors.get(group, 0) + 1

    # ---- flushing ------------------------------------------------------------
    def _flush_locked(self) -> None:
        """Assumes self._lock is held."""
        sorted_rows = sorted(self._rows.values(), key=_urgency_sort_key)
        _atomic_write_csv(self.config.output_path, FIELDS, sorted_rows)
        if self._errors:
            _atomic_write_csv(self.config.errors_path, ERROR_FIELDS, self._errors)
        self._since_flush = 0
        self.progress.report()

    async def flush(self) -> None:
        async with self._lock:
            self._flush_locked()

    async def finalize(self) -> None:
        async with self._lock:
            self._flush_locked()
        if self.matcher is not None:
            print(f"\nKeyword filter: {self.scanned} scanned, "
                  f"{self.filtered_out} dropped (no keyword match).")
        print(f"\nDone. {len(self._rows)} unique tenders -> "
              f"{self.config.output_path}")
        if self._errors:
            print(f"      {len(self._errors)} parse errors -> "
                  f"{self.config.errors_path}")


# ---- atomic file helpers ----------------------------------------------------
def _write_and_replace_with_retry(path: str, write_fn, attempts: int = 6) -> None:
    """
    Write to a fresh temp file and atomically replace `path`, retrying the
    WHOLE write+rename cycle on transient Windows/cloud-sync failures.

    Cloud-synced folders (OneDrive, etc.) can briefly hold an exclusive handle
    on a just-written file while indexing/uploading it (PermissionError,
    WinError 5) — or, worse, whisk the temp file away entirely before the
    rename runs (FileNotFoundError, WinError 2), because it treated a
    `*.tmp` file as transient junk. Once the temp file is gone, retrying
    os.replace() on that same path can never succeed — the fix is to
    re-create the temp file (a fresh, uniquely-named one each attempt, so a
    stale one from a failed attempt can't collide) and retry the full cycle.
    """
    last_exc: Exception | None = None
    for attempt in range(attempts):
        tmp = f"{path}.{os.getpid()}.{attempt}.tmp"
        try:
            write_fn(tmp)
            os.replace(tmp, path)
            return
        except (PermissionError, FileNotFoundError) as exc:
            last_exc = exc
            try:
                os.remove(tmp)
            except OSError:
                pass
            if attempt == attempts - 1:
                raise
            time.sleep(0.2 * (2 ** attempt) + random.uniform(0, 0.1))
    raise last_exc  # pragma: no cover - loop always returns or raises above


def _atomic_write_csv(path: str, fieldnames: list[str],
                      rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)  # materialize once; write_fn may run more than once

    def write_fn(tmp: str) -> None:
        with open(tmp, "w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    _write_and_replace_with_retry(path, write_fn)


def _atomic_write_json(path: str, data: Any) -> None:
    def write_fn(tmp: str) -> None:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)

    _write_and_replace_with_retry(path, write_fn)


# Date formats seen across ALL sources. Every scraper stores dates as the
# portal renders them; this parser normalizes for the float-date cutoff and
# the urgency sort. Add new formats here as new sources are wired in.
_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",       # GeM               2025-06-28T19:00:00Z
    "%d-%b-%Y %I:%M %p",        # CPPP, Haryana     08-Jul-2026 11:00 AM
    "%d/%m/%Y %H:%M:%S",        # Gujarat           10/07/2026 14:00:00
    "%d/%m/%Y %I:%M %p",        # TS/AP             27/07/2026 04:30 PM
    "%d-%m-%Y %H:%M",           # Coal India        22-07-2026 11:00
    "%d-%m-%Y %I:%M:%S %p",     # BHEL              15-07-2026 02:30:00 PM
    "%d/%m/%Y",                 # NTPC, Daman&Diu   25/07/2026
    "%d-%m-%Y",                 # Puducherry        30-06-2026
    "%d %b %Y",                 # Meghalaya         03 Jul 2026
    "%d - %b - %Y",             # aggregators       06 - Aug - 2026
    "%A, %B %d, %Y %I:%M:%S %p",  # GAIL            Sunday, July 5, 2026 4:30:00 PM
    "%A, %B %d, %Y",            # GAIL (date only)
    "%d-%b-%Y",                 # generic           08-Jul-2026
    "%Y-%m-%d %H:%M",           # Bihar             2026-07-08 15:00
    "%d-%m-%Y %I:%M %p",        # generic           08-07-2026 03:00 PM
]


def _parse_any_date(value: str) -> datetime | None:
    value = " ".join((value or "").split()).strip()
    if not value:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _urgency_sort_key(row: dict[str, str]) -> tuple:
    """
    Urgency ordering: tenders closest to their closing deadline first.
      1. Open tenders (closing date >= now), soonest deadline first.
      2. Rows with no parseable closing date (can't rank them).
      3. Already-closed tenders, most recently closed first.
    """
    dt = _parse_any_date(row.get("closing_date", ""))
    now = datetime.now()
    if dt is None:
        return (1, datetime.min)     # unknown deadline: between open and closed
    if dt >= now:
        return (0, dt)               # open: ascending (about-to-close on top)
    return (2, now - dt)             # closed: smallest gap = most recent first


def _clean(value: Any) -> str:
    """Normalize a scalar into a single clean CSV cell."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    return " ".join(str(value).split()).strip()


def make_session(config: ScraperConfig) -> aiohttp.ClientSession:
    """A shared session: capped connector + default headers."""
    connector = aiohttp.TCPConnector(limit=config.concurrency)
    return aiohttp.ClientSession(
        connector=connector,
        trust_env=True,
        headers={"User-Agent": DEFAULT_UA,
                 "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8"},
    )
