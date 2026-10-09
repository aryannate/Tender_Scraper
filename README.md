# Government Tender Scraper (GeM + CPPP + State GePNIC)

Async scraper for public Indian government tender listings. No login, no CAPTCHA
solving — it only touches listing pages that are plainly viewable.

## Daily tender monitoring (Linux / cron)

`python daily_job.py` runs the existing scraper, checks local notification history,
and sends **one** email containing only **NEW** or meaningfully **UPDATED** matching
tenders. No changes means no email. It runs once and exits; cron starts it tomorrow.
The original `run_scraper.py` commands and dashboard remain available.

### Install and configure

Requires Python 3.10+ and a Linux machine with Internet access to the portals and
your SMTP server. Clone your fork, then run these commands inside its checkout:

```bash
cd /opt/Tender_Scraper                  # substitute your real checkout path
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install --with-deps chromium
# On a machine without package-install permission, ask its administrator to
# install Playwright's OS dependencies, then use: python -m playwright install chromium
cp .env.example .env                    # only if you do not already have .env
chmod 600 .env
```

Edit `.env` with your SMTP host, sender and recipients. Use `SMTP_PORT=587` with
`SMTP_SECURITY=starttls`, or `465` with `ssl`. Both modes verify TLS certificates.
Set `SMTP_USER` and `SMTP_PASSWORD` together for authenticated SMTP (an app password
may be required by your provider). Both can be blank for an unauthenticated relay.
`SMTP_SECURITY=none` is for a trusted local test relay only; credentials are never
sent without TLS. `EMAIL_TO` accepts comma-separated addresses. Real process
environment variables take precedence over `.env`, which is loaded beside
`daily_job.py` even when invoked from another directory. Never commit `.env`.
No SMTP password is stored in the state database or digest.

Chromium is needed for **Telangana, Andhra Pradesh, BPCL and Bihar**. Keep the
browser installation and cron under the same Unix user. If you override
`PLAYWRIGHT_BROWSERS_PATH`, use the same value while installing and running.
The daily default is `--source everything`: every existing HTTP and Playwright
source is attempted. Missing browsers and failed sources are logged; other
sources continue. `--source all` retains the old HTTP-only meaning.

### Keywords and manual runs

Edit `KEYWORDS` in `keywords.py`. The default list covers AI / GPU / HPC hardware,
networking, storage and cloud infrastructure, including H100/H200/B200/B300,
GB200/GB300, DGX/HGX/NVL72, InfiniBand, Slurm and the other target terms.
Alternatively, create `keywords.local.txt` with **one keyword per line**, then
set `KEYWORDS_FILE=keywords.local.txt` in `.env` or pass `--keywords-file`.
Blank lines and `#` comments are ignored, case-insensitive duplicates are removed,
and an empty custom list raises a clear error rather than silently restoring defaults.
Matching is case-insensitive, phrase-aware, word-bounded and accepts spaces/hyphens
and trailing plurals. Every kept row retains all `matched_keywords`.
The same custom list applies to empanelments too.

```bash
# Full scan, preview only: safe before SMTP is configured
.venv/bin/python daily_job.py --dry-run
# Inspect outputs/daily/digest.html or digest.txt if there are candidates.
# Preview runs save observations but NEVER mark them notified.

# One full daily run: sends only new or changed matching tenders
.venv/bin/python daily_job.py
# Run again: unchanged successfully notified tenders are suppressed
.venv/bin/python daily_job.py

# Bounded public-listing smoke test; no mail, separate output/state
.venv/bin/python daily_job.py --source cppp --max-pages 1 --dry-run --out-dir /tmp/tender-smoke
# GeM keyword mode has a separate per-keyword limit; --max-pages alone
# does not cap its keyword searches. Use a small custom keyword file for smoke tests.
.venv/bin/python daily_job.py --source gem --keywords-file keywords.local.txt --gem-pages-per-keyword 1 --dry-run --out-dir /tmp/tender-gem-smoke
```

Do not put page limits in the daily cron command: the production default does a
fresh full scan according to each connector's existing listing/search strategy.
**Notification state never skips pages or stops source crawling.** GeM's existing
ongoing-bid keyword searches, source safety caps, public aggregator limitations,
BHEL live-list stopping rule and the existing published-date cutoff are preserved.
The old CSV can still contain historical rows, but only tenders actually observed
in the **current** scan can generate this run's alerts. The packaged dashboard
sample CSV is not treated as freshly discovered data.

### State, output and failures

Default daily files live in `outputs/daily/`, separate from the packaged dashboard
CSV. Set `TENDER_OUTPUT_DIR` / `--out-dir` or `TENDER_STATE_DB` / `--state-db` to use
persistent absolute paths. Relative paths resolve against the command's working
directory; the cron example below sets that directory explicitly.

- `tenders_gem_cppp.csv`: existing accumulated normalized scraper output.
- `current_run.csv`: current matching observations only; atomically replaced each run.
- `tender_state.sqlite3`: source/ID identity, important fields, matched keywords,
  first/last seen timestamps, fingerprint, last notified fingerprint and timestamp.
- `digest.html`, `digest.txt`: latest candidate digest preview, also preserved when
  SMTP configuration or sending fails; removed when the current run has no candidates.
- `daily_job.log`: console output plus rotating file logging (5 MB, three backups).
- `errors_log.csv`: existing accumulated fetch/parse errors.
- `run_summary.json`: current run counts, source outcomes and UTC timestamps.

**NEW** means that `(source, tender_id)` has never been successfully notified.
**UPDATED** means its title, description, organisation, dates, URL, corrigendum,
state or exposed tender category differs from the last successfully emailed version.
Whitespace/case-only prose edits, equivalent date formatting, and keyword-list
changes alone do not trigger updates. URL path case remains significant.
Tenders from different sources remain separate even if their reference IDs match.
Sources that synthesize IDs from titles/URLs retain the original connector's
identity limitations; a changed synthetic ID can appear as NEW.

Emails use a compact single-column HTML layout plus plain text, list all requested
fields and matched keywords, and order upcoming closing dates first (unknown dates
next, already-closed entries last). Explicit UTC dates, including GeM's `Z` dates,
are interpreted correctly; portal-local dates and the subject's date use
Asia/Kolkata. Missing shareable detail links fall back to a portal listing where
you can search the reference ID; protected detail pages are never bypassed.

Scrape observations are committed before SMTP. Notifications are committed only
once the SMTP server accepts the digest. Missing/invalid SMTP configuration or a
failed send produces a nonzero exit code, preserves CSV/preview/state, and leaves
observed matching tenders pending for the next fresh scan. A pending tender is
retried only when rediscovered as matching in a later scan; notices that close or
disappear during a mail outage are not sent from historical state alone.
SMTP is not contacted
when there are no candidates. All recipients must be accepted before sending the
message body, so a rejected recipient prevents delivery to the others.
A process lock next to the SQLite file prevents overlapping daily jobs sharing
that state from sending duplicate digests. Use the **same state database** for cron
and manual production runs; deleting it resets notification history. Back it up.
Do not use an ephemeral filesystem for production state.

As with normal SMTP, a machine crash after the server accepts mail but before the
local notification commit, or an interrupted connection with an ambiguous DATA
response, can cause a retry. SMTP and SQLite cannot make that handoff transactional.
Successful completed runs suppress repeat alerts. SMTP acceptance is not a guarantee
of inbox delivery; your provider can subsequently bounce mail.

One failing source does not cancel the rest or prevent a digest from successful
sources. Completion logs report sources attempted/successful, rows scanned (including
keyword misses and repeated search hits), unique matching tenders, NEW/UPDATED counts,
digests sent, tenders notified and errors. A source is counted successful when its
connector completes without reported errors; that count alone does not establish
portal completeness or live coverage. Errors give exit code `1` even if other sources
sent their digest; no error or an overlapping job skipped by the lock gives `0`.
CAPTCHA/auth-gated sources remain skipped/logged under the original public-listing
rules. Never enable the GePNIC stub's CAPTCHA gate or add CAPTCHA solving.

### Schedule at 07:00 Asia/Kolkata

Install a **user crontab under the same user that installed Chromium** (`crontab -e`).
Substitute the absolute checkout path and use the virtualenv interpreter directly;
activation and a permanently running Python process are unnecessary. `.env` loads
automatically. Create `outputs/daily` first for the redirected log file.

On **Cronie or another cron that supports `CRON_TZ`**:

```cron
SHELL=/bin/sh
PATH=/usr/local/bin:/usr/bin:/bin
CRON_TZ=Asia/Kolkata
0 7 * * * cd /opt/Tender_Scraper && /opt/Tender_Scraper/.venv/bin/python daily_job.py >> /opt/Tender_Scraper/outputs/daily/cron.log 2>&1
```

**Ubuntu/Debian cron commonly does not support `CRON_TZ` for scheduling.** On a
host whose system timezone is UTC, use the equivalent **01:30 UTC** entry instead
(India has no daylight-saving time):

```cron
SHELL=/bin/sh
PATH=/usr/local/bin:/usr/bin:/bin
30 1 * * * cd /opt/Tender_Scraper && /opt/Tender_Scraper/.venv/bin/python daily_job.py >> /opt/Tender_Scraper/outputs/daily/cron.log 2>&1
```

Check the host's timezone with `timedatectl`. If it is already Asia/Kolkata,
ordinary cron uses `0 7 * * *`. Setting `TZ` in a job changes process time display,
not necessarily cron's scheduling timezone. Install **only one** of these entries.
The file logger rotates itself; rotate `cron.log` with the machine's logrotate
service if retaining cron's duplicate console capture. Scheduling is Linux/cron
primary; GitHub Actions is not required.

### Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip check
```

Tests cover keywords, fresh-vs-historical output, NEW/UPDATED classification,
persistence and duplicate suppression, retry after failed mail, HTML rendering,
SMTP validation and recipient rejection, source failure isolation and overlapping
runs. The end-to-end test uses the real CPPP parser with local HTTP and SMTP
servers, fetches every fixture page on all three runs, and verifies NEW → no mail
→ UPDATED. It sends no external email. Live portal checks are separate and bounded;
passing tests do not assert that every external source is currently reachable.

## Setup (fresh Windows machine)

Requires Python 3.10+ (tested on 3.12). In PowerShell or cmd:

```powershell
# 1. Get the code onto the new machine (copy the whole tender_scraper/ folder,
#    or clone/pull the repo it lives in).

# 2. From inside tender_scraper/, create and activate a virtual environment
cd path\to\tender_scraper
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run it
python run_scraper.py --source all
```

To deactivate the virtual environment later: `deactivate`. On the next session,
just re-run `venv\Scripts\activate` before using the scraper again — no need
to reinstall.

All output (`tenders_gem_cppp.csv`, `errors_log.csv`) is written to
`tender_scraper\outputs\` by default, regardless of which directory you run
the command from. Every run does a full re-scan of every source (no
checkpoint/skip state), so nothing carries over between runs except the CSV
itself.

## Reconnaissance summary

Before any scraping logic was written, both primary sources were inspected for
CAPTCHA gating and their real data-loading mechanism:

| Source | Listing viewable? | How data actually loads |
|--------|-------------------|--------------------------|
| **GeM** (`bidplus.gem.gov.in/all-bids`) | ✅ No CAPTCHA | XHR `POST /all-bids-data` returning **Solr-style JSON** (10 docs/page). We send `filter.bidStatusType="ongoing_bids"` so only **open** bids are returned (see "GeM completeness" below), then scan every page per keyword. |
| **CPPP** (`eprocure.gov.in`) | ✅ No CAPTCHA *via the Drupal frontend* | `GET /cppp/latestactivetendersnew/cpppdata?page=N` — server-rendered HTML table, 10 rows/page. Tender IDs are base64-encoded inside the `tendersfullview` detail link (segments split on `A13h1`). **The detail link itself is not a shareable URL** — see below. |
| **Gujarat (GIL)** (`gil.gujarat.gov.in/Tenders`) | ✅ No CAPTCHA | ASP.NET GridView; paginates via `__doPostBack` + `__VIEWSTATE` (full-form postback). Working `TenderDetails.aspx?TenID=` detail links. |
| **Haryana (Works)** (`works.haryana.gov.in/LiveTender.aspx`) | ✅ No CAPTCHA | ASP.NET GridView; captures estimated value + EMD. Detail is a postback button (no shareable URL). |
| **PSUs: GAIL** (`gailtenders.in`) | ✅ No CAPTCHA | Classic-ASP; `POST tender_report.asp val=4`, paginated via `gotopage`. ~114 tenders. |
| **PSUs: Coal India** (`coalindia.in/tenders`) | ✅ No CAPTCHA | One `<table>` per tender (label/value cards); ~250 tenders in one page, with value/EMD. |
| **PSUs: NTPC** (`ntpctender.ntpc.co.in/Index/Search`) | ✅ No CAPTCHA (list) | Single-page DataTable (~86); the CAPTCHA is on the search filter, not the listing. Detail links `/NITDetails/NITs/<id>`. |
| **PSUs: BHEL** (`tenders.bhel.com/tenders`) | ✅ No CAPTCHA | `?page=N` pagination; we stop at closed tenders (live-only), ~600 live. Real detail URLs. |
| **Other PSU portals** | ❌ CAPTCHA / GePNIC / SPA | ONGC, HPCL, SAIL, IOCL, BHEL-GePNIC, NALCO gate their listings; NMDC is a JS SPA. Most PSU/CPSE tenders are also in CPPP. |
| **Telangana** (APTS) | ✅ via Playwright | Full list is JS-loaded + bot-guarded against raw HTTP, so we drive headless Chromium: land → click "More..." → read the table → page via "Next". ~226 pages. See `playwright_states.py` (optional install). |
| **Andhra Pradesh** (APTS) | ✅ via Playwright | Same as TS, but first close the ad modal (`.sp-close-btn`) then JS-click `#viewCurrentall`. Header-based column mapping handles AP's 9-col layout vs TS's 10. |
| **Meghalaya / Daman&Diu / Puducherry / Sikkim** | ✅ HTTP (`ne_states.py`) | Plain server-rendered listings. Sikkim exposes only the ~10 latest notices (accumulates across refreshes). |
| **Manipur / Tripura** | ⚠️ aggregator teasers | manipurtenders.com / tripuratenders.com are PRIVATE aggregators; public cards show sector/value/ref/closing only (titles login-walled), so keyword matching is weak there. |
| **Bihar** (eproc2.bihar.gov.in) | ✅ via Playwright | JS app; REST endpoint is session-guarded, so we read the rendered latest-tenders table (~20/run, accumulates). |
| **Mizoram** | ❌ NIC GePNIC | CAPTCHA-gated listing — not scrapable under the no-captcha rule. |
| **Other NIC/GePNIC states + most PSU portals** | ❌ CAPTCHA-gated | All swept state portals (Odisha, MH, WB, TN, Kerala, UP, MP, …) and most PSU portals (ONGC, HPCL, SAIL, IOCL) gate their listings behind a CAPTCHA. Not scraped. Most PSU/CPSE tenders are covered via CPPP anyway. |

## Filters & columns (2026-07-08)

- **Merged tab**: GeM/CPPP + all state portals appear in one dashboard tab —
  "GEM,CPPP+State-wise Tender". PSUs and Empanelments stay separate.
- **Empanelments are IT-domain only**: an empanelment/EOI/rate-contract notice
  is kept only if it matches the same `KEYWORDS` list in `keywords.py` used
  for regular tenders (app, website, chatbot, software, AI/ML, OCR,
  surveillance, agents, etc.) — a drug-supply or freight-forwarder empanelment
  is dropped.
- **Float-date cutoff**: only tenders published on/after **2026-01-01**
  (`FLOAT_FROM` in pipeline.py). Rows whose source doesn't expose a published
  date are kept — they can't be verified either way.
- **`tender_type`**: the tender category — **Service / Product / Works** —
  extracted ONLY from the portal's own category data, never inferred from
  title/description text. GeM rows use the portal's structured goods/services
  bid split (`b_type` / `b_cat_id`); TS/AP use their listing's Category column
  (PRODUCTS/SERVICES/WORKS). Sources whose listings expose no category
  (CPPP, most states, PSUs) are honestly **"Unidentified"** — unknown values
  are never mapped or guessed.
- **Unknown floated date section**: rows whose source publishes no float date
  (Haryana, GAIL, NTPC, Bihar, TS/AP, aggregators) can't be verified against
  the 01/01/2026 cutoff. They are kept but shown in a clearly-marked section
  after all dated rows in every tab.
- **`opening_date`**: captured where the listing exposes it (CPPP, Gujarat,
  Coal India); blank elsewhere.
- **Urgency sort**: open tenders with the nearest closing date first, then
  unknown-deadline rows, then already-closed ones.

> **CPPP caveat:** the Tapestry pages at `/eprocure/app?page=FrontEndLatestActiveTenders`
> **are CAPTCHA-gated** ("Provide Captcha and click Search to list all active tenders").
> Those are deliberately *not* used. Only the CAPTCHA-free Drupal `/cppp/` frontend is scraped.

## Files

- `pipeline.py` — shared: async HTTP w/ retries + backoff, 60-worker semaphore,
  rolling-ETA progress, atomic CSV writes, dedup, error log. No checkpoint/resume:
  every run re-scans every source from scratch.
- `gem_scraper.py` — Module 1 (GeM), fully implemented (JSON).
- `cppp_scraper.py` — Module 2 (CPPP), fully implemented (HTML table).
- `state_gepnic_scraper.py` — Module 3, **STUB**: generic GePNIC parser +
  `STATE_PORTALS` registry + `add_state_portal(...)` + Telangana wiring + `--dry-run`.
- `run_scraper.py` — combined runner / CLI.
- `psu_portals.py` — PSU scrapers (GAIL, Coal India, NTPC, BHEL).
- `state_portals.py` — non-NIC state portals (Gujarat, Haryana).
- `playwright_states.py` — **optional** Telangana/AP scraper (headless browser).
- `dashboard.py` — local Flask web dashboard (4 tabs + refresh button).
- `requirements.txt` — third-party deps (`aiohttp`, `beautifulsoup4`, `flask`);
  everything else used is Python stdlib.

## Dashboard

A small local web UI over the scraped data:

```bash
pip install -r requirements.txt
python dashboard.py            # then open http://127.0.0.1:5000
```

Tabs:
- **GeM / CPPP** — central-portal tenders (`source` in gem/cppp).
- **State-wise** — state e-procurement tenders (Gujarat, Haryana).
- **PSUs** — dedicated PSU portals (GAIL, Coal India, NTPC, BHEL).
- **Empanelments** — cross-cut view of empanelment / EOI / rate-contract /
  enlistment notices detected across all sources.

**Keyword filtering applies to EVERY source.** Only tenders matching the target
IT/AI keyword list are kept and displayed — from GeM, CPPP, states, and PSUs
alike. Portals with no current IT tenders (e.g. GAIL, NTPC, Telangana civil
works) simply contribute 0 rows until such tenders appear; we still scrape them
so nothing is missed, but non-matching tenders are dropped rather than stored.

**Links per row:** every row is clickable. Sources with a stable detail URL
(GeM, Gujarat, Coal India, NTPC, BHEL) show a **direct** "open" link to the
tender. Sources whose detail is a JS postback or is session/WAF-guarded (CPPP,
Haryana, Telangana, AP, GAIL, BPCL) show a **"portal ↗"** link to that source's
live-tender listing, where you can search the ref number — because a direct link
there would hit an "Invalid Url"/error page.

The **Refresh** button runs a FULL re-scrape of every source in the background
and streams progress. It launches one `run_scraper.py --source everything`
child process, which scans all HTTP sources AND the Playwright sources
(Telangana, AP, BPCL, Bihar) concurrently — a single click refreshes
everything. Playwright must be installed for those four
(`python -m playwright install chromium`); if it isn't, they skip themselves
and the HTTP refresh still completes.

The **category dropdown** (Service / Product / Works / Unidentified / All)
filters the current tab's view by `tender_type`. "All" shows every category
including Unidentified; the other four options show only that one. A **5-minute
cooldown** is enforced — refreshing sooner returns how long to wait. Because
every source is fully re-scanned every time (no page/tender is skipped based
on prior-run state), a refresh can take up to the full estimated time shown in
the UI — this is deliberate so a refresh can never silently miss a tender
that's live on the portal.

## Usage

Independent sources (each a distinct portal/host) are scraped **concurrently**
by default: the shared sink serializes all writes behind an asyncio lock, dedup
is keyed on `(source, tender_id)`, and every source still walks its complete
listing — same output as a sequential run, just faster wall-clock. Pass
`--sequential` to run sources one after another (legacy behavior). Playwright
portals are capped at 2 concurrent browser instances to stay memory-safe.

```bash
# Everything (full scan every run), writes to ./outputs/
python run_scraper.py --source all

# All HTTP sources + Playwright portals in one process (what Refresh uses)
python run_scraper.py --source everything

# Quick smoke test: GeM, first 5 listing pages
python run_scraper.py --source gem --max-pages 5

# CPPP only
python run_scraper.py --source cppp

# State portals only (Gujarat + Haryana)
python run_scraper.py --source states
python run_scraper.py --source states --state gujarat   # single portal

# PSU portals (GAIL, Coal India, NTPC, BHEL)
python run_scraper.py --source psus

# Telangana / AP / BPCL via Playwright (OPTIONAL — needs the extra install below)
pip install playwright && python -m playwright install chromium
python run_scraper.py --source playwright                       # TS + AP + BPCL
python run_scraper.py --source tsap --state telangana           # single portal
python run_scraper.py --source tsap --state andhra_pradesh --max-pages 25

# Inspect a state's page structure BEFORE enabling it
python run_scraper.py --source gepnic --state telangana --dry-run
```

Key flags: `--max-pages N` (cap pages/source — GeM's full set is ~550k pages!),
`--concurrency` (default 60), `--rate-limit` (default 0.25s), `--out-dir`.

## Output

Written to `tender_scraper/outputs/` by default (anchored to this folder
regardless of your current working directory); override with `--out-dir`:

- `tenders_gem_cppp.csv` — one normalized row per tender, **sorted by
  closing_date descending** (latest/furthest-out deadline first; rows with a
  missing or unparseable closing date sort last):
  `tender_id, title, description, organization, state, published_date,
  closing_date, source, source_url, corrigendum, matched_keywords`
- `errors_log.csv` — anything that failed to fetch/parse, with the reason.

## Keyword filtering (default ON)

By default the scraper only keeps tenders matching an IT/software/AI keyword
list (`keywords.py`), tagging each kept row with which keyword(s) hit in
`matched_keywords`. Use `--no-filter` to disable and keep everything, or
`--keywords-file path.txt` for a custom list (one keyword per line).

GeM's server-side search is a **token-OR** match, not a phrase match — e.g.
searching "face recognition" returns paint and brooms because it matches
either word. So GeM keyword mode uses the server search only to shrink the
candidate set (`--gem-pages-per-keyword`, default 100 pages/keyword), then
applies a strict word-boundary/phrase-aware regex client-side to actually
decide what's kept.

## Every run is a full scan (no checkpoint/skip state)

Every source — GeM, CPPP, every state/UT portal, every PSU portal, and the
Playwright-driven ones (Telangana, AP, BPCL, Bihar) — walks its ENTIRE listing
on every run. Nothing is skipped because it was "already seen" in a prior run.
This is deliberate, and was a fix for a real bug: an earlier version tracked
every tender ID ever scanned (`seen_ids.json`) plus a per-source
`<source>_checkpoint.json` of "completed" listing pages, and CPPP used that to
stop paginating early once a batch of pages came back "all already known" —
assuming CPPP's listing was reliably sorted newest-first. That assumption
doesn't always hold, and the failure mode is silent: a tender sitting lower in
the list than expected, or a portal whose sort order shifts, would simply never
get (re-)fetched again. Since the whole point of this project is to never miss
a tender, that checkpoint/early-stop machinery has been removed entirely from
`pipeline.py`, `cppp_scraper.py`, and `state_gepnic_scraper.py`.

The only thing carried over between runs is the output CSV itself: it's
preloaded at startup so a tender collected in an earlier run isn't lost when
the file is atomically rewritten, but every source still re-fetches and
re-parses every page fresh, every time. The tradeoff is a slower refresh
(a full CPPP scan is several minutes, not ~10 seconds) in exchange for
"always right" over "always fast."

GeM keyword mode already did a fresh per-keyword search every run even before
this change — its ongoing-bid pages are NOT append-only like a typical
listing: bids open and close daily, so a page fully scraped last run can have
entirely different content today. Each keyword's ongoing set is small (the
`bidStatusType=ongoing_bids` filter keeps it to tens-to-hundreds of hits), so a
full fresh scan per keyword per run is cheap.

## GeM completeness (open bids, no cap)

GeM's search is relevance-ranked (not date-sorted) and returns the entire 5.5M
all-time archive by default, so an early cap silently missed open tenders. The
fix: the `all-bids-data` payload accepts a `filter` object we now send with
`bidStatusType="ongoing_bids"` — this restricts to currently-open bids only,
collapsing each keyword's result set from tens-of-thousands to a few hundred.
That lets us scan **every** page per keyword (no cap), so no open keyword tender
is missed. GeM's token-OR search is a superset of true phrase matches, and the
client-side `KeywordMatcher` narrows to real hits.

## `source_url` — valid for GeM, blank for CPPP

GeM's `source_url` (`/showbidDocument/<id>`) is a real, stable, cold-clickable
link — verified to serve the actual bid PDF with zero prior session state.

CPPP's `tendersfullview` link is **not** a shareable URL: the detail page only
renders if the same browser session first loaded the listing page (a
server-side anti-deep-linking check). Opened cold — e.g. pasted from this CSV
into a fresh browser — it returns "Invalid Url. Please Check" no matter how
the link was constructed; there's no query-based or ID-based lookup on CPPP
that works statelessly either. Since there's no way to produce a working
direct link, `source_url` is left blank for CPPP rows rather than handing out
a URL that will fail.

## Design notes

- **Concurrency unit** = one listing page (~10 tenders), fanned out across the
  60-worker semaphore. GeM/CPPP expose a total up front, so pages run concurrently.
- **Flushing**: every 20 emitted rows the full CSV is rewritten atomically
  (temp file → `os.replace`). The CSV is preloaded at startup so previously
  collected rows aren't lost when it's rewritten — but every source still
  re-scans every page from scratch (see "Every run is a full scan" above).
- **Dedup**: keyed on `(source, tender_id)`; the latest version wins
  (explicit corrigendum > later published/closing date).
- **Retries**: 3× exponential backoff + jitter on network/5xx/429; a persistently
  failing page is logged to `errors_log.csv` and skipped, never crashing the run.

## Adding a new state (Module 3)

1. Register it (or add an entry to `STATE_PORTALS`):
   ```python
   add_state_portal("kerala",
                    "https://etenders.kerala.gov.in",
                    "/nicgep/app?page=FrontEndLatestActiveTenders"
                    "&service=direct&session=T&sp=S{page}")
   ```
2. `python run_scraper.py --source gepnic --state kerala --dry-run` — eyeball the
   printed table/rows. If it shows a CAPTCHA, **stop** (leave it disabled).
3. If the generic parser fits, set `captcha_confirmed_free=True` for that state,
   then run without `--dry-run`.

The GePNIC pager fix (ported from the `find_company_names.py` pattern) stops
paging when the **Next** control's parent is disabled *or* the page's content
fingerprint repeats — GePNIC re-serves the last page instead of 404ing.
