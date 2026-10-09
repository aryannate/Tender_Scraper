"""Local notification history and SMTP digest for the existing scraper."""
from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import smtplib
import sqlite3
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email import policy
from email.message import EmailMessage
from email.utils import formatdate, getaddresses, make_msgid, parseaddr
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

from pipeline import FIELDS, _clean, _parse_any_date

IST = ZoneInfo('Asia/Kolkata')
# Derived keyword/category flags and observation times do not cause update mail.
IMPORTANT_FIELDS = [name for name in FIELDS
                    if name not in ('source', 'tender_id', 'matched_keywords', 'is_empanelment')]
DATE_FIELDS = {'published_date', 'closing_date', 'opening_date'}
SOURCE_LISTING_URLS = {
    'gem': 'https://bidplus.gem.gov.in/all-bids',
    'cppp': 'https://eprocure.gov.in/cppp/latestactivetendersnew',
    'gujarat': 'https://gil.gujarat.gov.in/Tenders',
    'haryana': 'https://works.haryana.gov.in/LiveTender.aspx',
    'gail': 'https://gailtenders.in/Gailtenders/Home.asp',
    'coalindia': 'https://www.coalindia.in/tenders/',
    'ntpc': 'https://ntpctender.ntpc.co.in/Index/Search',
    'bhel': 'https://tenders.bhel.com/tenders',
    'meghalaya': 'https://www.meghalaya.gov.in/tenders',
    'daman_diu': 'https://ddd.gov.in/notice-category/tenders/',
    'puducherry': 'https://py.gov.in/tenders',
    'sikkim': 'https://www.sikkim.gov.in/tender',
    'manipur': 'https://www.manipurtenders.com/',
    'tripura': 'https://www.tripuratenders.com/default.aspx',
    'infralens': 'https://tenders.infralens.in',
    'telangana': 'https://tender.telangana.gov.in/',
    'andhra_pradesh': 'https://tender.apeprocurement.gov.in/',
    'bpcl': 'https://bpcltenders.eproc.in/Product/publicDash',
    'bihar': 'https://eproc2.bihar.gov.in/EPSV2Web/openarea/tenderListingPage.action',
}


def parse_date(value: str) -> datetime | None:
    """Portal-local dates are IST; explicit ISO offsets (including GeM Z) win."""
    value = _clean(value)
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        dt = _parse_any_date(value)
    if dt is None:
        return None
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt


def fingerprint(row: dict) -> str:
    values = {}
    for name in IMPORTANT_FIELDS:
        value = _clean(row.get(name, ''))
        dt = parse_date(value) if name in DATE_FIELDS else None
        values[name] = (dt.astimezone(timezone.utc).isoformat() if dt else
                        value if name == 'source_url' else value.casefold())
    return hashlib.sha256(json.dumps(values, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class TenderState:
    """Seen fields and last *successfully notified* fingerprint are separate."""
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path), timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('''CREATE TABLE IF NOT EXISTS tenders (
            source TEXT NOT NULL, tender_id TEXT NOT NULL, title TEXT NOT NULL,
            organization TEXT NOT NULL, published_date TEXT NOT NULL,
            closing_date TEXT NOT NULL, matched_keywords TEXT NOT NULL,
            source_url TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
            notified_at TEXT, fingerprint TEXT NOT NULL, notified_fingerprint TEXT,
            data_json TEXT NOT NULL, PRIMARY KEY (source, tender_id))''')
        self.connection.commit()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()

    def observe(self, rows: list[dict], now: str | None = None) -> list[dict]:
        now = now or datetime.now(timezone.utc).isoformat()
        unique = {}
        for row in rows:
            row = {key: _clean(row.get(key, '')) for key in FIELDS}
            if not row['source'] or not row['tender_id']:
                raise ValueError('Notification state requires source and tender_id')
            unique[(row['source'], row['tender_id'])] = row
        pending = []
        with self.connection:
            for key, row in unique.items():
                saved = self.connection.execute(
                    'SELECT * FROM tenders WHERE source=? AND tender_id=?', key).fetchone()
                fp = fingerprint(row)
                status = ('NEW' if saved is None or saved['notified_at'] is None else
                          'UPDATED' if fp != saved['notified_fingerprint'] else '')
                self.connection.execute('''INSERT INTO tenders
                    (source,tender_id,title,organization,published_date,closing_date,
                     matched_keywords,source_url,first_seen,last_seen,fingerprint,data_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source,tender_id) DO UPDATE SET
                    title=excluded.title, organization=excluded.organization,
                    published_date=excluded.published_date, closing_date=excluded.closing_date,
                    matched_keywords=excluded.matched_keywords, source_url=excluded.source_url,
                    last_seen=excluded.last_seen, fingerprint=excluded.fingerprint,
                    data_json=excluded.data_json''',
                    tuple(row[name] for name in ('source', 'tender_id', 'title', 'organization',
                                                'published_date', 'closing_date',
                                                'matched_keywords', 'source_url')) +
                    (now, now, fp, json.dumps(row, ensure_ascii=False)))
                if status:
                    pending.append({**row, 'status': status})
        return pending

    def mark_notified(self, rows: list[dict]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            for row in rows:
                self.connection.execute('''UPDATE tenders SET notified_at=?, notified_fingerprint=?
                    WHERE source=? AND tender_id=?''',
                    (now, fingerprint(row), row['source'], row['tender_id']))


def _link(row: dict) -> tuple[str, str]:
    base = SOURCE_LISTING_URLS.get(row.get('source', ''), '')
    raw = row.get('source_url', '').strip()
    url = urljoin(base, raw) if raw else base
    parsed = urlsplit(url)
    if parsed.scheme not in ('https', 'http') or not parsed.hostname:
        return '', ''
    return url, 'Open tender' if raw else 'Open portal (search reference ID)'


def render_digest(rows: list[dict], when: datetime | None = None) -> EmailMessage:
    when = when or datetime.now(IST)
    if when.tzinfo is None:
        when = when.replace(tzinfo=IST)
    now = when.astimezone(IST)

    def urgency(row):
        dt = parse_date(row.get('closing_date', ''))
        if dt is None:
            return (1, 0, row['source'], row['tender_id'])
        return (0 if dt >= now else 2, dt.timestamp(), row['source'], row['tender_id'])

    rows = sorted(rows, key=urgency)
    new = sum(row['status'] == 'NEW' for row in rows)
    updated = sum(row['status'] == 'UPDATED' for row in rows)
    subject = f'Tender Radar — {new} New / {updated} Updated — {now:%d %b %Y}'
    cards, plain = [], [subject, 'Closest closing deadlines first. Portal dates as published.', '']
    for row in rows:
        def esc(key):
            return html.escape(row.get(key, '') or 'Not provided', quote=True)
        url, label = _link(row)
        link = (f'<a href="{html.escape(url, quote=True)}" style="color:#155e75;">'
                f'{label}</a>' if url else 'Source link not provided')
        corrigendum = (f'<br>Corrigendum: {esc("corrigendum")}' if row.get('corrigendum') else '')
        cards.append(f'''<tr><td style="padding:18px 16px;border-bottom:1px solid #dbe2ea;overflow-wrap:anywhere;word-break:break-word;">
            <div style="color:#155e75;font-size:13px;font-weight:bold;">{esc('status')} · {esc('source')}</div>
            <h2 style="font-size:18px;line-height:1.35;margin:6px 0;">{esc('title')}</h2>
            <div style="font-size:14px;line-height:1.6;">{esc('organization')}<br>
            Reference: {esc('tender_id')}<br>Published: {esc('published_date')}<br>
            <strong>Closing: {esc('closing_date')}</strong>{corrigendum}<br>
            Keywords: {esc('matched_keywords')}<br>{link}</div></td></tr>''')
        plain.extend([f"{row['status']} | {row['source']} | {row['title']}",
                      f"Organisation: {row.get('organization') or 'Not provided'}",
                      f"Reference: {row['tender_id']}",
                      f"Published: {row.get('published_date') or 'Not provided'}",
                      f"Closing: {row.get('closing_date') or 'Not provided'}",
                      f"Keywords: {row.get('matched_keywords', '')}"])
        if row.get('corrigendum'):
            plain.append(f"Corrigendum: {row['corrigendum']}")
        plain.extend([f'{label}: {url}' if url else 'Source link not provided', ''])
    body = f'''<!doctype html><html lang="en"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1"></head>
        <body style="margin:0;background:#f1f5f9;font-family:Arial,sans-serif;color:#172033;">
        <table role="presentation" style="width:100%;max-width:680px;margin:0 auto;background:white;border-collapse:collapse;">
        <tr><td style="padding:20px 16px;"><h1 style="font-size:22px;margin:0;">Tender Radar</h1>
        <p style="margin:8px 0 0;">{new} new · {updated} updated · {now:%d %b %Y}</p>
        <p style="font-size:12px;color:#475569;">Closest closing deadlines first. Portal dates as published.</p></td></tr>
        {''.join(cards)}</table></body></html>'''
    message = EmailMessage(policy=policy.SMTP)
    message['Subject'] = subject
    message['Date'] = formatdate(localtime=False)
    message['Message-ID'] = make_msgid()
    message.set_content('\n'.join(plain), cte='quoted-printable')
    message.add_alternative(body, subtype='html', cte='quoted-printable')
    return message


@dataclass
class SMTPSettings:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    sender: str
    recipients: list[str]
    security: str

    @classmethod
    def from_env(cls, env) -> 'SMTPSettings':
        required = [name for name in ('SMTP_HOST', 'EMAIL_FROM', 'EMAIL_TO') if not env.get(name, '').strip()]
        if required:
            raise ValueError('Missing notification configuration: ' + ', '.join(required))
        host = env['SMTP_HOST'].strip()
        if re.search(r'[\s/:]', host):
            raise ValueError('SMTP_HOST must be a hostname without scheme or port')
        try:
            port = int(env.get('SMTP_PORT', '587'))
        except ValueError:
            raise ValueError('SMTP_PORT must be an integer') from None
        if not 1 <= port <= 65535:
            raise ValueError('SMTP_PORT must be between 1 and 65535')
        security = env.get('SMTP_SECURITY', '').strip().lower() or ('ssl' if port == 465 else 'starttls')
        if security not in ('starttls', 'ssl', 'none'):
            raise ValueError('SMTP_SECURITY must be starttls, ssl, or none')
        user, password = env.get('SMTP_USER', ''), env.get('SMTP_PASSWORD', '')
        if bool(user) != bool(password):
            raise ValueError('SMTP_USER and SMTP_PASSWORD must both be set for authentication')
        if security == 'none' and user:
            raise ValueError('SMTP authentication requires TLS (starttls or ssl)')
        sender = env['EMAIL_FROM'].strip()
        recipients_text = env['EMAIL_TO'].strip()
        if '\n' in sender + recipients_text or '\r' in sender + recipients_text:
            raise ValueError('EMAIL_FROM and EMAIL_TO cannot contain newlines')
        addresses = getaddresses([recipients_text])
        recipients = list(dict.fromkeys(addr for _, addr in addresses))
        from_address = parseaddr(sender)[1]
        if not recipients or not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+', from_address) or any(
            not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+', addr) for addr in recipients
        ):
            raise ValueError('EMAIL_FROM and EMAIL_TO must contain valid email addresses (comma-separated recipients)')
        return cls(host, port, user, password, sender, recipients, security)


def send_digest(message: EmailMessage, settings: SMTPSettings) -> None:
    """Require every RCPT to be accepted before sending DATA; no partial digest."""
    message['From'] = settings.sender
    message['To'] = ', '.join(settings.recipients)
    context = ssl.create_default_context()
    connection = (smtplib.SMTP_SSL(settings.host, settings.port, timeout=60, context=context)
                  if settings.security == 'ssl' else
                  smtplib.SMTP(settings.host, settings.port, timeout=60))
    accepted = False
    try:
        with connection as smtp:
            smtp.ehlo_or_helo_if_needed()
            if settings.security == 'starttls':
                smtp.starttls(context=context)
                smtp.ehlo()
            if settings.user:
                smtp.login(settings.user, settings.password)
            sender = parseaddr(settings.sender)[1]
            code, response = smtp.mail(sender)
            if code != 250:
                raise smtplib.SMTPSenderRefused(code, response, sender)
            refused = {}
            for recipient in settings.recipients:
                code, response = smtp.rcpt(recipient)
                if code not in (250, 251):
                    refused[recipient] = (code, response)
            if refused:
                smtp.rset()
                raise smtplib.SMTPRecipientsRefused(refused)
            code, response = smtp.data(message.as_bytes())
            if code != 250:
                raise smtplib.SMTPDataError(code, response)
            accepted = True
    except (smtplib.SMTPException, OSError):
        if not accepted:
            raise
        logging.getLogger("tender_radar").warning("SMTP accepted the digest, but closing the session failed; retaining delivery success.")
