"""
Combined runner for the government-tender scraper.

Wires the GeM, CPPP, and (stubbed) state-GePNIC modules into one shared
pipeline that writes a single normalized CSV plus an errors log. Every run is
a full re-scan of every source, every listing page — nothing is skipped based
on prior-run state, so a repeat run can never silently miss a tender.

Examples
--------
    # Everything, full scan, default outputs/ dir
    python run_scraper.py --source all

    # Just GeM, first 5 pages (quick smoke test)
    python run_scraper.py --source gem --max-pages 5

    # Inspect a newly-registered state before enabling it
    python run_scraper.py --source state --state telangana --dry-run

Output defaults to tender_scraper/outputs/, anchored to this file's location
(not the current working directory) -- override with --out-dir.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

import cppp_scraper
import gem_scraper
import infralens_scraper
import ne_states
import psu_portals
import state_gepnic_scraper as gepnic
import state_portals
from keywords import KeywordMatcher, load_keywords
from pipeline import SOURCE_GROUP, ResultSink, ScraperConfig, make_session

# Anchor the default output dir to this file's location (not the process's
# CWD), so `outputs/` always lands inside tender_scraper/ regardless of where
# the script is invoked from.
DEFAULT_OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")


def build_config(args) -> ScraperConfig:
    return ScraperConfig(
        concurrency=args.concurrency,
        rate_limit=args.rate_limit,
        checkpoint_every=args.checkpoint_every,
        out_dir=args.out_dir,
    )


# At most this many concurrent headless Chromium instances for the Playwright
# portals — each is a full browser (hundreds of MB), so parallelism is capped
# far below the HTTP concurrency to stay memory-safe on a laptop.
PLAYWRIGHT_BROWSERS = 2


async def _run_group(sink: ResultSink, name: str, coro) -> None:
    """
    Await one source coroutine, containing any crash to that source. A source
    that dies (e.g. GeM's CSRF page changing shape) is logged to the errors
    CSV instead of aborting the whole run — with parallel sources this also
    guarantees one failure can't take down its siblings.
    """
    print(f"\n=== {name} ===")
    token = SOURCE_GROUP.set(name)
    try:
        await coro
    except Exception as exc:  # noqa: BLE001
        print(f"[{name}] SOURCE FAILED: {exc!r}")
        await sink.emit_error(name, "", f"source crashed: {exc!r}")
    finally:
        errors = sink.group_errors.get(name, 0)
        sink.source_outcomes[name] = "error" if errors else "ok"
        logging.getLogger("tender_radar").info("Source %s completed; errors=%d", name, errors)
        SOURCE_GROUP.reset(token)


async def run(args) -> ResultSink | None:
    config = build_config(args)

    # Keyword filtering is ON by default; --no-filter disables it.
    matcher = None
    if not args.no_filter:
        kws = load_keywords(args.keywords_file)
        matcher = KeywordMatcher(kws)
        print(f"Keyword filter ENABLED: {len(kws)} keywords "
              f"(matched_keywords column populated).")
    sink = ResultSink(config, matcher=matcher)

    async with make_session(config) as session:
        # --dry-run short-circuits everything else (state inspection only).
        if args.dry_run:
            if not args.state:
                print("--dry-run requires --state <name>")
                return
            await gepnic.dry_run_state(session, config, args.state)
            return

        want = args.source
        # Each group is an independent portal (distinct host), so groups can
        # run concurrently: the sink serializes all writes behind its lock,
        # dedup is keyed on (source, tender_id), and the shared session caps
        # total connections at config.concurrency — a host is never hit
        # HARDER than in a sequential run (sources now share the connection
        # pool each of them previously had to itself). Order of arrival
        # doesn't affect the output: every source still walks its full
        # listing, and the keep-latest dedup rule is date-based, not
        # arrival-order-based.
        groups: list[tuple[str, object]] = []

        if want in ("all", "everything", "gem"):
            if matcher is not None:
                groups.append(("GeM", gem_scraper.scrape_gem_keywords(
                    session, config, sink, matcher.keywords,
                    pages_per_keyword=args.gem_pages_per_keyword)))
            else:
                groups.append(("GeM", gem_scraper.scrape_gem(
                    session, config, sink, args.max_pages)))
        if want in ("all", "everything", "cppp"):
            groups.append(("CPPP", cppp_scraper.scrape_cppp(
                session, config, sink, args.max_pages)))
        if want in ("all", "everything", "states"):
            portals = ([args.state] if args.state
                       else list(state_portals.STATE_ASPNET_PORTALS))
            for key in portals:
                if key in state_portals.STATE_ASPNET_PORTALS:
                    groups.append((f"state:{key}", state_portals.scrape_state_portal(
                        session, config, sink, key, args.max_pages)))
            ne_keys = ([args.state] if args.state
                       else list(ne_states.NE_STATE_PORTALS))
            for key in ne_keys:
                if key in ne_states.NE_STATE_PORTALS:
                    groups.append((f"state:{key}", ne_states.NE_STATE_PORTALS[key](
                        session, config, sink, args.max_pages)))
        if want in ("all", "everything", "infralens"):
            kws = matcher.keywords if matcher is not None else load_keywords(None)
            groups.append(("Infralens", infralens_scraper.scrape_infralens(
                session, config, sink, kws, args.max_pages)))
        if want in ("all", "everything", "psus"):
            psus = ([args.state] if args.state
                    else list(psu_portals.PSU_PORTALS))
            for key in psus:
                if key in psu_portals.PSU_PORTALS:
                    groups.append((f"psu:{key}", psu_portals.PSU_PORTALS[key](
                        session, config, sink, args.max_pages)))
        if want in ("everything", "tsap", "playwright"):  # optional: needs Playwright
            import playwright_states
            if args.state:
                keys = [args.state]
            elif want == "tsap":
                keys = list(playwright_states.TSAP_PORTALS)
            else:
                keys = playwright_states.PLAYWRIGHT_PORTALS
            browser_sem = asyncio.Semaphore(PLAYWRIGHT_BROWSERS)

            async def _pw(key: str) -> None:
                async with browser_sem:
                    # These connectors previously printed a missing dependency
                    # then returned, which could look like successful coverage.
                    from playwright.async_api import async_playwright  # noqa: F401
                    await playwright_states.run_playwright_portal(
                        config, sink, key, args.max_pages)

            for key in keys:
                groups.append((f"playwright:{key}", _pw(key)))
        if want in ("gepnic",):  # NIC GePNIC states are CAPTCHA-gated; stub only
            states = [args.state] if args.state else list(gepnic.STATE_PORTALS)
            for st in states:
                groups.append((f"gepnic:{st}", gepnic.scrape_state(
                    session, config, sink, st, args.max_pages)))

        try:
            if args.sequential:
                for name, coro in groups:
                    await _run_group(sink, name, coro)
            else:
                await asyncio.gather(*(_run_group(sink, name, coro)
                                       for name, coro in groups))
        finally:
            await sink.finalize()
    return sink


def build_parser(*, include_inspection: bool = True):
    p = argparse.ArgumentParser(description="GeM + CPPP + state GePNIC tender scraper",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--source",
                   choices=["all", "everything", "gem", "cppp", "states",
                            "psus", "tsap", "playwright", "gepnic", "infralens"],
                   default="all",
                   help="which source(s): all (every HTTP source) | everything "
                        "(all + the Playwright portals, one process) | gem | "
                        "cppp | states (Gujarat/Haryana) | psus "
                        "(GAIL/CoalIndia/NTPC/BHEL) | tsap (Telangana/AP) | "
                        "playwright (TS/AP/BPCL/Bihar) | gepnic (stub) | "
                        "infralens (aggregator)")
    p.add_argument("--sequential", action="store_true",
                   help="run sources one after another (legacy behavior) "
                        "instead of concurrently")
    p.add_argument("--state",
                   help="single portal key for --source states (gujarat|haryana) "
                        "or state name for --source gepnic / --dry-run")
    if include_inspection:
        p.add_argument("--dry-run", action="store_true",
                       help="fetch one page of a state and print its structure")
    p.add_argument("--max-pages", type=int, default=None,
                   help="cap listing pages per source (CPPP full set ~3450 pages)")
    p.add_argument("--no-filter", action="store_true",
                   help="disable keyword filtering (scrape everything)")
    p.add_argument("--keywords-file",
                   help="file with one keyword per line (default: built-in AI/infrastructure list)")
    p.add_argument("--gem-pages-per-keyword", type=int, default=2000,
                   help="runaway safety backstop for GeM pages/keyword. Not a real "
                        "limit: we scan only OPEN bids, so keyword sets are small "
                        "and fully covered well under this.")
    p.add_argument("--concurrency", type=int, default=60)
    p.add_argument("--rate-limit", type=float, default=0.25,
                   help="polite delay between requests (seconds)")
    p.add_argument("--checkpoint-every", type=int, default=20)
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                   help="output directory; the default is anchored to the checkout")
    return p


def parse_args(argv=None):
    return build_parser().parse_args(argv)


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        print("\nInterrupted. Already-collected tenders were flushed to CSV; "
              "re-run to do a fresh full scan.")
