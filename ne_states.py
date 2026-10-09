"""
Additional state / UT tender portals (batch added 2026-07-08).

Recon results for the 8 requested sites:
  * Meghalaya   meghalaya.gov.in/tenders          — plain table, no captcha. HTTP.
  * Daman & Diu ddd.gov.in/notice-category/tenders — WordPress table, /page/N. HTTP.
  * Puducherry  py.gov.in/tenders                 — Drupal views rows, ?page=N. HTTP.
                (the captcha on that page belongs to the feedback form, not the list)
  * Sikkim      sikkim.gov.in/tender              — listing rendered server-side
                (its reCAPTCHA v3 script is page-wide, not gating the list). Only
                the ~10 latest notices are exposed; incremental runs accumulate.
  * Manipur     manipurtenders.com                — PRIVATE AGGREGATOR. Public
                teaser cards only (sector / value / ref / closing date); real
                titles are login-walled, so keyword matching is weak here.
  * Tripura     tripuratenders.com                — same aggregator network.
  * Bihar       eproc2.bihar.gov.in               — JS app whose REST endpoint is
                session-guarded; scraped via Playwright (see playwright_states).
  * Mizoram     tender.mizoram.gov.in             — NIC GePNIC, CAPTCHA-gated
                listing. NOT scrapable under the no-captcha rule.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from pipeline import ResultSink, ScraperConfig, fetch

MAX_PAGES = 40  # all of these are small portals; hard backstop


def _find_table(soup, needle):
    for t in soup.find_all("table"):
        if re.search(needle, t.get_text(" ", strip=True), re.I) and len(t.find_all("tr")) > 2:
            return t
    return None


# --------------------------------------------------------------------------- Meghalaya
async def scrape_meghalaya(session, config: ScraperConfig, sink: ResultSink,
                           max_pages=None) -> None:
    base = "https://www.meghalaya.gov.in"
    print("[meghalaya] scraping ...")
    try:
        html = await fetch(session, "GET", f"{base}/tenders", config, expect="text")
    except Exception as exc:  # noqa: BLE001
        await sink.emit_error("meghalaya", "page", f"fetch failed: {exc!r}")
        return
    soup = BeautifulSoup(html, "html.parser")
    table = _find_table(soup, r"Last Date")
    total = 0
    if table:
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            a = tr.find("a", href=True)
            m = re.search(r"/content/(\d+)", a["href"]) if a else None
            await sink.emit({
                "tender_id": m.group(1) if m else tds[1].get_text(" ", strip=True)[:60],
                "title": tds[1].get_text(" ", strip=True),
                "description": "",
                "organization": tds[2].get_text(" ", strip=True),
                "state": "Meghalaya",
                "published_date": tds[3].get_text(" ", strip=True),
                "closing_date": tds[4].get_text(" ", strip=True),
                "source": "meghalaya",
                "source_url": (base + a["href"]) if a and a["href"].startswith("/") else (a["href"] if a else ""),
                "corrigendum": "",
            })
            total += 1
        sink.set_total(total)
    await sink.flush()
    print(f"[meghalaya] done — {total} tenders.")


# --------------------------------------------------------------------------- Daman & Diu (DNH&DD)
async def scrape_daman_diu(session, config: ScraperConfig, sink: ResultSink,
                           max_pages=None) -> None:
    base = "https://ddd.gov.in/notice-category/tenders/"
    print("[daman_diu] scraping ...")
    total = 0
    page = 1
    limit = max_pages or MAX_PAGES
    while page <= limit:
        url = base if page == 1 else f"{base}page/{page}/"
        try:
            html = await fetch(session, "GET", url, config, expect="text")
        except Exception as exc:  # noqa: BLE001
            # Only WordPress's terminal 404 is a normal end-of-list.
            if page == 1 or getattr(exc.__cause__, "status", None) != 404:
                await sink.emit_error("daman_diu", f"page:{page}", f"fetch failed: {exc!r}")
            break
        soup = BeautifulSoup(html, "html.parser")
        table = _find_table(soup, r"End Date")
        if not table:
            break
        got = 0
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            title = tds[1].get_text(" ", strip=True)
            a = tr.find("a", href=True)
            await sink.emit({
                "tender_id": f"ddd-{tds[3].get_text(strip=True)}-{title[:40]}",
                "title": title,
                "description": tds[2].get_text(" ", strip=True),
                "organization": "UT Administration of DNH & Daman and Diu",
                "state": "Daman & Diu / DNH",
                "published_date": tds[3].get_text(" ", strip=True),
                "closing_date": tds[4].get_text(" ", strip=True),
                "source": "daman_diu",
                "source_url": a["href"] if a else "",
                "corrigendum": "",
            })
            got += 1
        total += got
        sink.set_total(got)
        if not got:
            break
        page += 1
    await sink.flush()
    print(f"[daman_diu] done — {total} tenders.")


# --------------------------------------------------------------------------- Puducherry
async def scrape_puducherry(session, config: ScraperConfig, sink: ResultSink,
                            max_pages=None) -> None:
    base = "https://py.gov.in"
    print("[puducherry] scraping ...")
    total = 0
    page = 0                       # Drupal pager is 0-indexed (?page=1 is 2nd)
    limit = max_pages or MAX_PAGES
    prev_sig = None
    while page < limit:
        url = f"{base}/tenders" if page == 0 else f"{base}/tenders?page={page}"
        try:
            html = await fetch(session, "GET", url, config, expect="text")
        except Exception as exc:  # noqa: BLE001
            await sink.emit_error("puducherry", f"page:{page}", f"fetch failed: {exc!r}")
            break
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(".view-content .views-row") or soup.select(".view-content tr")
        tenders = []
        for r_ in rows:
            a = r_.find("a", href=True)
            if not a:
                continue
            title = a.get_text(" ", strip=True)
            text = r_.get_text(" ", strip=True)
            pub = re.search(r"Published Date:?\s*\|?\s*(\d{2}-\d{2}-\d{4})", text)
            close = re.search(r"(?:Closing|Last) Date:?\s*\|?\s*(\d{2}-\d{2}-\d{4})", text)
            href = a["href"]
            tenders.append({
                "tender_id": f"py-{href.rstrip('/').rsplit('/',1)[-1][:50]}",
                "title": title,
                "description": text[:250],
                "organization": "Government of Puducherry",
                "state": "Puducherry",
                "published_date": pub.group(1) if pub else "",
                "closing_date": close.group(1) if close else "",
                "source": "puducherry",
                "source_url": (base + href) if href.startswith("/") else href,
                "corrigendum": "",
            })
        sig = tuple(t["tender_id"] for t in tenders)
        if not tenders or sig == prev_sig:
            break
        prev_sig = sig
        sink.set_total(len(tenders))
        for t in tenders:
            await sink.emit(t)
        total += len(tenders)
        page += 1
    await sink.flush()
    print(f"[puducherry] done — {total} tenders.")


# --------------------------------------------------------------------------- Sikkim
async def scrape_sikkim(session, config: ScraperConfig, sink: ResultSink,
                        max_pages=None) -> None:
    base = "https://www.sikkim.gov.in"
    print("[sikkim] scraping (latest notices — portal exposes ~10 at a time) ...")
    try:
        html = await fetch(session, "GET", f"{base}/tender", config, expect="text")
    except Exception as exc:  # noqa: BLE001
        await sink.emit_error("sikkim", "page", f"fetch failed: {exc!r}")
        return
    soup = BeautifulSoup(html, "html.parser")
    total = 0
    for a in soup.find_all("a", href=re.compile(r"/tender/tender-info/\d+")):
        m = re.search(r"/tender-info/(\d+)", a["href"])
        title = a.get_text(" ", strip=True).removeprefix("New").strip()
        ctx = a.find_parent(["td", "li", "div"])
        ctx_text = ctx.get_text(" ", strip=True) if ctx else ""
        d = re.search(r"(\d{1,2}\s+\w{3}\s+\d{4})", ctx_text)
        await sink.emit({
            "tender_id": m.group(1),
            "title": title[:250],
            "description": "",
            "organization": "Government of Sikkim",
            "state": "Sikkim",
            "published_date": d.group(1) if d else "",
            "closing_date": "",
            "source": "sikkim",
            "source_url": base + a["href"].split("?")[0],
            "corrigendum": "",
        })
        total += 1
    sink.set_total(total)
    await sink.flush()
    print(f"[sikkim] done — {total} notices.")


# --------------------------------------------------------------------------- Manipur / Tripura (aggregator network)
async def _scrape_aggregator(session, config, sink, source, state, url):
    """
    manipurtenders.com / tripuratenders.com: private aggregator teaser cards.
    Public fields: sector-as-title, tender value, ref no, closing date. Real
    titles/descriptions are login-walled, so keyword matching is limited to the
    sector text — treat these as best-effort coverage, not complete.
    """
    print(f"[{source}] scraping aggregator teaser cards ...")
    try:
        html = await fetch(session, "GET", url, config, expect="text")
    except Exception as exc:  # noqa: BLE001
        await sink.emit_error(source, "page", f"fetch failed: {exc!r}")
        return
    soup = BeautifulSoup(html, "html.parser")
    total = 0
    for tb in soup.find_all("table"):
        text = tb.get_text(" ", strip=True)
        if "Ref.No" not in text or "Closing Date" not in text:
            continue
        # innermost card tables only (avoid nested duplicates)
        if tb.find("table") is not None:
            continue
        ref = re.search(r"Ref\.No\s*\|?\s*(\d+)", tb.get_text(" | ", strip=True))
        sector = re.search(r"Sector\s*\|\s*([^|]+)", tb.get_text(" | ", strip=True))
        value = re.search(r"Tender Value\s*\|\s*([^|]+)", tb.get_text(" | ", strip=True))
        close = re.search(r"Closing Date\s*\|\s*([^|]+)", tb.get_text(" | ", strip=True))
        if not ref:
            continue
        await sink.emit({
            "tender_id": ref.group(1),
            "title": (sector.group(1).strip() if sector else "")[:200],
            "description": f"Tender Value: {value.group(1).strip()}" if value else "",
            "organization": "",
            "state": state,
            "published_date": "",
            "closing_date": (close.group(1).strip() if close else ""),
            "source": source,
            "source_url": "",   # details are login-walled on the aggregator
            "corrigendum": "",
        })
        total += 1
    sink.set_total(total)
    await sink.flush()
    print(f"[{source}] done — {total} teaser cards.")


async def scrape_manipur(session, config, sink, max_pages=None):
    await _scrape_aggregator(session, config, sink, "manipur", "Manipur",
                             "https://www.manipurtenders.com/")


async def scrape_tripura(session, config, sink, max_pages=None):
    await _scrape_aggregator(session, config, sink, "tripura", "Tripura",
                             "https://www.tripuratenders.com/default.aspx")


NE_STATE_PORTALS = {
    "meghalaya": scrape_meghalaya,
    "daman_diu": scrape_daman_diu,
    "puducherry": scrape_puducherry,
    "sikkim": scrape_sikkim,
    "manipur": scrape_manipur,
    "tripura": scrape_tripura,
}
