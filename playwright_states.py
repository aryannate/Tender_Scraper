"""
OPTIONAL module — TS & AP e-procurement via Playwright (headless browser).

Telangana and Andhra Pradesh run the APTS platform, whose full tender list is
loaded by JavaScript AND guarded against non-browser HTTP requests (a direct
POST returns "session terminated due to restricted/sensitive information"). So
plain aiohttp can't reach it. Playwright drives a real headless Chromium through
the actual user flow — land on the home page, click "More...", read the rendered
table, page via "Next" — which is a genuine browser session on a public listing
(no CAPTCHA, no login, nothing bypassed).

This is kept OUT of the core HTTP stack because it needs a heavy one-time setup:
    pip install playwright
    python -m playwright install chromium     # ~150 MB browser download

Run it explicitly:
    python run_scraper.py --source tsap                # both
    python run_scraper.py --source tsap --state telangana
    python run_scraper.py --source tsap --max-pages 10 # cap pages (each ~10 tenders)

Column layout (both portals share it):
  0 Department | 1 Tender ID | 2 Notice No | 3 Category | 4 Name of Work |
  5 Est. Value | 6 Published | 7 Bid Start | 8 Bid Closing | 9 Action(JS)
"""

from __future__ import annotations

import re

from pipeline import ResultSink, ScraperConfig

TSAP_PORTALS = {
    "telangana": {"source": "telangana", "state": "Telangana",
                  "home": "https://tender.telangana.gov.in/",
                  "more_text": "More..."},
    # AP shows an advertisement modal on load (closed via .sp-close-btn) and its
    # "More" is the #viewCurrentall element (clicked via JS to dodge overlays).
    "andhra_pradesh": {"source": "andhra_pradesh", "state": "Andhra Pradesh",
                       "home": "https://tender.apeprocurement.gov.in/",
                       "more_css": "#viewCurrentall"},
}

HARD_PAGE_CAP = 500


async def _next_enabled(locator) -> bool:
    if await locator.count() == 0:
        return False
    handle = await locator.first.element_handle()
    if handle is None:
        return False
    return await handle.evaluate("""el => {
      const node = el.closest('li, a, button, span') || el;
      const cls = `${node.className || ''} ${el.className || ''}`.toLowerCase();
      return !cls.includes('disabled') &&
             node.getAttribute('aria-disabled') !== 'true' &&
             !node.hasAttribute('disabled');
    }""")


# JS run in the page to extract the current table's header + rows. We map by
# HEADER NAME (not fixed index) because TS and AP differ: TS has 10 columns
# (incl. a Published + Bid-Start pair), AP has 9 (Start + Closing).
_EXTRACT_JS = """() => {
  const tbls = [...document.querySelectorAll('table')]
      .filter(t => t.rows.length > 1 && /Tender ID/i.test(t.innerText));
  if (!tbls.length) return null;
  const t = tbls[0];
  const header = [...t.rows[0].cells].map(c => c.innerText.trim());
  const rows = [...t.rows].slice(1).map(r => [...r.cells].map(c => c.innerText.trim()));
  return {header, rows};
}"""


def _colmap(header: list[str]) -> dict:
    def find(*keys):
        for i, h in enumerate(header):
            hl = h.lower()
            if any(k in hl for k in keys):
                return i
        return -1
    return {
        "dept": find("department"),
        "id": find("tender id"),
        "notice": find("notice"),
        "category": find("category"),
        "title": find("name of work", "name of"),
        "closing": find("closing"),
        "published": find("published", "start date"),
    }


def _row_to_tender(cells: list[str], cm: dict, cfg: dict) -> dict | None:
    def g(key: str) -> str:
        i = cm.get(key, -1)
        return cells[i].strip() if 0 <= i < len(cells) else ""
    tender_id, title = g("id"), g("title")
    if not tender_id or not title:
        return None
    return {
        "tender_id": tender_id,
        "title": title,
        "description": g("notice"),
        "organization": g("dept"),
        "state": cfg["state"],
        "published_date": g("published"),
        "closing_date": g("closing"),
        "source": cfg["source"],
        "source_url": "",                          # Action is a JS viewBtn(), no URL
        "corrigendum": "",
        # Raw "Category" column (the APTS portals use PRODUCTS/SERVICES/WORKS);
        # pipeline.normalize_category maps it, unknown values -> Unidentified.
        "tender_type": g("category"),
    }


async def _set_table_page_size(page, source: str, size: str = "100") -> None:
    """
    Best-effort: bump the DataTables page-size dropdown from 10 to `size` so
    we scrape fewer, bigger pages. Some portals (AP) keep this <select> in the
    DOM but visually hidden behind a custom-styled widget, which makes
    Playwright's select_option() wait out its full default timeout (60s) for
    "visible" before giving up — a bug-for-bug 60s stall on every AP run. A
    short explicit timeout here caps that stall at a few seconds instead.
    """
    try:
        page_size = page.locator("select[name='pagetable13_length']").first
        if await page_size.count() == 0:
            print(f"[{source}] page-size selector not found; using portal default.")
            return
        await page_size.select_option(size, timeout=5000)
        await page.wait_for_timeout(2500)
        print(f"[{source}] set listing page size to {size}.")
    except Exception as exc:  # noqa: BLE001
        print(f"[{source}] page-size selector not usable (using portal default): {exc.__class__.__name__}")


async def scrape_tsap(config: ScraperConfig, sink: ResultSink,
                      key: str, max_pages: int | None = None) -> None:
    cfg = TSAP_PORTALS.get(key)
    if not cfg:
        print(f"[{key}] not a TS/AP portal; skipping.")
        return
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[tsap] Playwright not installed. Run:\n"
              "    pip install playwright\n"
              "    python -m playwright install chromium")
        return

    print(f"[{cfg['source']}] launching headless browser ...")
    limit = max_pages or HARD_PAGE_CAP
    total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.set_default_timeout(60000)
        try:
            await page.goto(cfg["home"], wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)  # let any ad modal appear
            # Close the advertisement popup if present (AP shows one).
            try:
                await page.locator(".sp-close-btn").first.click(force=True, timeout=8000)
                await page.wait_for_timeout(1500)
            except Exception:  # noqa: BLE001
                pass  # no ad (e.g. TS) — fine
            # Reach the full list. AP uses #viewCurrentall (JS click dodges the
            # overlay); TS uses the "More..." text link.
            if cfg.get("more_css"):
                await page.evaluate(
                    f"() => {{ const m = document.querySelector('{cfg['more_css']}'); if (m) m.click(); }}")
            else:
                await page.get_by_text(cfg.get("more_text", "More..."), exact=False).first.click()
            await page.wait_for_timeout(5000)
            await _set_table_page_size(page, cfg["source"], "100")
        except Exception as exc:  # noqa: BLE001
            await sink.emit_error(cfg["source"], "nav", f"navigation failed: {exc!r}")
            await browser.close()
            return

        prev_sig = None
        pageno = 1
        while pageno <= limit:
            try:
                data = await page.evaluate(_EXTRACT_JS)
            except Exception as exc:  # noqa: BLE001
                await sink.emit_error(cfg["source"], f"page:{pageno}", f"extract failed: {exc!r}")
                break
            if not data or not data.get("rows"):
                break
            cm = _colmap(data["header"])
            tenders = [t for t in (_row_to_tender(r, cm, cfg) for r in data["rows"]) if t]
            sig = tuple(t["tender_id"] for t in tenders)
            if not tenders or sig == prev_sig:
                break
            prev_sig = sig
            sink.set_total(len(tenders))
            for t in tenders:
                await sink.emit(t)
            total += len(tenders)

            # Go to next page. The pager is a JS plugin whose "Next" is a plain
            # <a> (id like pagetable13_next); clicking its text advances it, and
            # on the last page the table stops changing so the sig-repeat check
            # above ends the loop.
            try:
                nxt = page.get_by_text("Next", exact=True).first
                if not await _next_enabled(nxt):
                    break
                await nxt.click()
                await page.wait_for_timeout(2500)
            except Exception as exc:  # noqa: BLE001
                await sink.emit_error(cfg["source"], f"page:{pageno}",
                                      f"pagination failed: {exc!r}")
                break
            pageno += 1

        await browser.close()
    await sink.flush()
    print(f"[{cfg['source']}] done — {total} tenders across {pageno} page(s).")


# --------------------------------------------------------------------------- BPCL
# BPCL's public dashboard is JS-rendered (CAPTCHA-free). Table:
#   S.No | CRFQ No | Tender Title [English] | Department | Due Date & Time | Detail
BPCL_URL = "https://bpcltenders.eproc.in/Product/publicDash"

_BPCL_EXTRACT = """() => {
  const t = [...document.querySelectorAll('table')]
      .find(t => /CRFQ No/i.test(t.innerText) && /Due Date/i.test(t.innerText));
  if (!t) return [];
  return [...t.rows].slice(1).map(r => [...r.cells].map(c => c.innerText.trim()));
}"""


async def scrape_bpcl(config: ScraperConfig, sink: ResultSink,
                      max_pages: int | None = None) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[bpcl] Playwright not installed (see playwright_states.py).")
        return
    print("[bpcl] launching headless browser ...")
    limit = max_pages or HARD_PAGE_CAP
    total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.set_default_timeout(60000)
        try:
            await page.goto(BPCL_URL, wait_until="networkidle")
            await page.wait_for_timeout(3500)
        except Exception as exc:  # noqa: BLE001
            await sink.emit_error("bpcl", "nav", f"navigation failed: {exc!r}")
            await browser.close()
            return
        prev_sig = None
        pageno = 1
        while pageno <= limit:
            try:
                rows = await page.evaluate(_BPCL_EXTRACT)
            except Exception as exc:  # noqa: BLE001
                await sink.emit_error("bpcl", f"page:{pageno}", f"extract failed: {exc!r}")
                break
            tenders = []
            for c in rows:
                if len(c) < 5 or not re.match(r"^\d+\.?$", c[0]):
                    continue
                crfq, title, dept, due = c[1], c[2], c[3], c[4]
                if not crfq or not title:
                    continue
                tenders.append({
                    "tender_id": crfq, "title": title, "description": "",
                    "organization": "BPCL - " + dept, "state": "",
                    "published_date": "", "closing_date": due,
                    "source": "bpcl", "source_url": "", "corrigendum": "",
                })
            sig = tuple(t["tender_id"] for t in tenders)
            if not tenders or sig == prev_sig:
                break
            prev_sig = sig
            sink.set_total(len(tenders))
            for t in tenders:
                await sink.emit(t)
            total += len(tenders)
            try:
                nxt = page.get_by_text("Next...", exact=False).first
                if not await _next_enabled(nxt):
                    break
                await nxt.click()
                await page.wait_for_timeout(2500)
            except Exception as exc:  # noqa: BLE001
                await sink.emit_error("bpcl", f"page:{pageno}",
                                      f"pagination failed: {exc!r}")
                break
            pageno += 1
        await browser.close()
    await sink.flush()
    print(f"[bpcl] done — {total} tenders across {pageno} page(s).")


# All Playwright-based portals: key -> coroutine(config, sink, max_pages).
# TS/AP share scrape_tsap (dispatched by key); BPCL has its own.
async def run_playwright_portal(config, sink, key, max_pages=None):
    import re as _re  # noqa: F401
    if key in TSAP_PORTALS:
        await scrape_tsap(config, sink, key, max_pages)
    elif key == "bpcl":
        await scrape_bpcl(config, sink, max_pages)
    elif key == "bihar":
        await scrape_bihar(config, sink, max_pages)
    else:
        print(f"[{key}] not a Playwright portal.")


# --------------------------------------------------------------------------- Bihar
# eproc2.bihar.gov.in is a JS app; its REST endpoint (rest/openarea/getTenderList)
# 500s outside a real browser session, so we read the rendered table instead.
# Columns: SL No | Tender/RFQ ID | Tender Description | Reference No | Department
#          | End Date Time | ...
BIHAR_URL = ("https://eproc2.bihar.gov.in/EPSV2Web/openarea/"
             "tenderListingPage.action#latestTenders")

_BIHAR_EXTRACT = """() => {
  const t = [...document.querySelectorAll('table')]
      .find(t => /Tender Description/i.test(t.innerText) && t.rows.length > 2);
  if (!t) return [];
  return [...t.rows].slice(1).map(r => [...r.cells].map(c => c.innerText.trim()));
}"""


async def scrape_bihar(config: ScraperConfig, sink: ResultSink,
                       max_pages: int | None = None) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[bihar] Playwright not installed (see module docstring).")
        return
    print("[bihar] launching headless browser ...")
    total = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.set_default_timeout(60000)
        try:
            await page.goto(BIHAR_URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(8000)   # let the Angular app fetch data
            rows = await page.evaluate(_BIHAR_EXTRACT)
        except Exception as exc:  # noqa: BLE001
            await sink.emit_error("bihar", "nav", f"failed: {exc!r}")
            await browser.close()
            return
        for c in rows:
            if len(c) < 6 or not c[1].strip():
                continue
            await sink.emit({
                "tender_id": c[1].strip(),
                "title": c[2].strip(),
                "description": c[3].strip(),      # reference no
                "organization": c[4].strip(),
                "state": "Bihar",
                "published_date": "",
                "closing_date": c[5].strip(),
                "source": "bihar",
                "source_url": "",                  # JS app, no deep link
                "corrigendum": "",
            })
            total += 1
        sink.set_total(total)
        await browser.close()
    await sink.flush()
    print(f"[bihar] done — {total} tenders.")

PLAYWRIGHT_PORTALS = ["telangana", "andhra_pradesh", "bpcl", "bihar"]
