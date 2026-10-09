"""Exercise the real CPPP parser, HTTP pipeline, SQLite and SMTP on loopback."""
import base64
import json
import os
import socketserver
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp import web
from email.parser import BytesParser
from email import policy

import daily_job


class MailHandler(socketserver.StreamRequestHandler):
    def handle(self):
        def reply(line):
            self.wfile.write(line + b'\r\n')
            self.wfile.flush()
        reply(b'220 local test SMTP')
        while True:
            line = self.rfile.readline()
            if not line:
                return
            command = line.split(b' ', 1)[0].strip().upper()
            if command in (b'EHLO', b'HELO'):
                reply(b'250 localhost')
            elif command in (b'MAIL', b'RCPT', b'RSET'):
                reply(b'250 OK')
            elif command == b'DATA':
                reply(b'354 End with dot')
                data = []
                while True:
                    line = self.rfile.readline()
                    if not line or line == b'.\r\n':
                        break
                    data.append(line[1:] if line.startswith(b'..') else line)
                self.server.messages.append(BytesParser(policy=policy.default).parsebytes(b''.join(data)))
                reply(b'250 accepted')
            elif command == b'QUIT':
                reply(b'221 bye')
                return
            else:
                reply(b'500 unexpected command')


class EndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_scrape_mail_and_three_runs_new_unchanged_updated(self):
        visits = []
        closing = ['12-Oct-2026 11:00 AM']
        def encode(value):
            return base64.b64encode(value.encode()).decode()
        async def listing(request):
            visits.append(request.path_qs)
            page = int(request.query.get('page', '1'))
            title = 'H200 GPU Server <cluster>' if page == 1 else 'Office chairs'
            href = '/cppp/tendersfullview/xA13h1' + encode('REF-1') + 'A13h1' + encode(f'2026_FIXTURE_{page}')
            return web.Response(text=f'''<table><tr><th>Sl.No</th><th>Published</th>
                <th>Closing</th><th>Opening</th><th>Title/Ref.No./Tender Id</th>
                <th>Organisation</th><th>Corrigendum</th></tr>
                <tr><td>1</td><td>01-Oct-2026 11:00 AM</td><td>{closing[0]}</td>
                <td>13-Oct-2026 11:00 AM</td><td><a href="{href}">{title.replace('<', '&lt;').replace('>', '&gt;')} /REF-1/2026_FIXTURE_{page}</a></td>
                <td>राष्ट्रीय Research Institute</td><td>--</td></tr></table>
                <a href="?page=2">Last</a>''', content_type='text/html')
        app = web.Application()
        app.router.add_get('/first', listing)
        app.router.add_get('/data', listing)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), MailHandler)
        server.daemon_threads = True
        server.messages = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                args = daily_job.parse_args(['--source', 'cppp', '--rate-limit', '0', '--out-dir', tmp])
                env = {'SMTP_HOST': '127.0.0.1', 'SMTP_PORT': str(server.server_address[1]),
                       'SMTP_SECURITY': 'none', 'EMAIL_FROM': 'radar@example.org',
                       'EMAIL_TO': 'one@example.org,two@example.org', 'NO_PROXY': '127.0.0.1'}
                with patch.dict(os.environ, env, clear=True), \
                     patch('cppp_scraper.FIRST_PAGE', f'http://127.0.0.1:{port}/first'), \
                     patch('cppp_scraper.DATA_URL', f'http://127.0.0.1:{port}/data?page={{page}}'):
                    self.assertEqual(await daily_job.execute(args), 0)
                    self.assertEqual(len(server.messages), 1)
                    self.assertIn('1 New / 0 Updated', server.messages[0]['Subject'])
                    self.assertIn('राष्ट्रीय', server.messages[0].get_body(preferencelist=('html',)).get_content())
                    self.assertEqual(await daily_job.execute(args), 0)
                    self.assertEqual(len(server.messages), 1)
                    closing[0] = '11-Oct-2026 11:00 AM'
                    self.assertEqual(await daily_job.execute(args), 0)
                    self.assertEqual(len(server.messages), 2)
                    self.assertIn('0 New / 1 Updated', server.messages[1]['Subject'])
                self.assertEqual(visits.count('/data?page=1'), 3)
                self.assertEqual(visits.count('/data?page=2'), 3)
                summary = json.loads((Path(tmp) / 'run_summary.json').read_text())
                self.assertEqual(summary['tenders_scanned'], 2)
                self.assertEqual(summary['matching_tenders'], 1)
                self.assertEqual(summary['updated_tenders'], 1)
        finally:
            await runner.cleanup()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
