"""Offline checks: never connect to a real mail provider."""
import contextlib
import csv
from dataclasses import replace
from decimal import Decimal
from email import policy
from email.parser import BytesParser
import io
import json
import os
from pathlib import Path
import smtplib
import socketserver
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import pipeline


class SMTPReceiver(socketserver.StreamRequestHandler):
    """Minimal SMTP receiver used only for one local integration test."""

    def handle(self):
        self.connection.settimeout(5)
        self.wfile.write(b"220 localhost test receiver\r\n")
        while line := self.rfile.readline(65536):
            command = line.split(b" ", 1)[0].strip().upper()
            if command in {b"EHLO", b"HELO"}:
                self.wfile.write(b"250 localhost\r\n")
            elif command in {b"MAIL", b"RCPT", b"RSET"}:
                self.wfile.write(b"250 OK\r\n")
            elif command == b"DATA":
                self.wfile.write(b"354 End with a dot\r\n")
                chunks = []
                while True:
                    payload = self.rfile.readline(65536)
                    if not payload:
                        return
                    if payload == b".\r\n":
                        break
                    chunks.append(payload[1:] if payload.startswith(b"..") else payload)
                self.server.message = b"".join(chunks)
                self.wfile.write(b"250 Queued\r\n")
            elif command == b"QUIT":
                self.wfile.write(b"221 Bye\r\n")
                return
            else:
                self.wfile.write(b"502 Unsupported\r\n")


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.close_log_handlers)
        self.settings = pipeline.load_settings(pipeline.ROOT / "config.example.ini", send=False)

    @staticmethod
    def close_log_handlers():
        for handler in pipeline.LOG.handlers[:]:
            handler.close()
            pipeline.LOG.removeHandler(handler)

    def arguments(self):
        return ["--config", str(pipeline.ROOT / "config.example.ini"),
                "--output-dir", str(self.path / "output"), "--log-dir", str(self.path / "logs")]

    def write_config(self, port=465, security="ssl", username="", password=""):
        path = self.path / "config.ini"
        path.write_text(
            f"[smtp]\nhost=127.0.0.1\nport={port}\nsecurity={security}\n"
            f"username={username}\npassword={password}\ntimeout_seconds=5\n"
            "[mail]\nfrom=sender@localhost\nto=recipient@localhost\nsubject=Demo report\n",
            encoding="utf-8",
        )
        return path

    def make_message(self):
        report = self.path / "report.csv"
        rows, total = pipeline.build_report(pipeline.ROOT / "data/sample.csv", report)
        return pipeline.build_message(self.settings, report, "test-run", rows, total)

    def test_report_totals_and_mime_attachment(self):
        message = self.make_message()
        parsed = BytesParser(policy=policy.default).parsebytes(message.as_bytes())
        self.assertIn("Rows processed: 6", parsed.get_body().get_content())
        self.assertIn("Total amount: 10650.00", parsed.get_body().get_content())
        attachments = list(parsed.iter_attachments())
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].get_filename(), "report.csv")
        text = attachments[0].get_payload(decode=True).decode("utf-8-sig")
        rows = list(csv.DictReader(io.StringIO(text)))
        self.assertEqual([(r["category"], r["row_count"], r["total_amount"]) for r in rows],
                         [("Office", "2", "4200.00"), ("Online", "3", "3250.00"), ("Partner", "1", "3200.00")])

    def test_default_dry_run_does_not_open_smtp(self):
        with patch("pipeline.smtplib.SMTP") as smtp, patch("pipeline.smtplib.SMTP_SSL") as smtp_ssl:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(pipeline.main(self.arguments()), 0)
            smtp.assert_not_called()
            smtp_ssl.assert_not_called()
        result_path, = (self.path / "output").glob("*/result.json")
        result = json.loads(result_path.read_text())
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(Decimal(result["total_amount"]), Decimal("10650.00"))

    def test_invalid_input_prevents_submission(self):
        source = self.path / "bad.csv"
        source.write_text("date,category,amount\n2026-09-28,Online,NaN\n")
        config = self.write_config()
        args = self.arguments() + ["--config", str(config), "--input", str(source), "--send"]
        with patch("pipeline.send_message") as send, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(pipeline.main(args), 1)
            send.assert_not_called()

    def test_empty_and_malformed_csv_fail(self):
        invalid_inputs = [
            "date,category,amount\n",
            "date,category,amount\n2026-09-28,Online,1.234\n",
            "date,category,amount\nnot-a-date,Online,1.00\n",
            "date,category,amount\n2026-09-28,Online\n",
            "date,category,amount\n2026-09-28,Online,1.00,extra\n",
            "date,category,amount\n2026-09-28,,1.00\n",
        ]
        for text in invalid_inputs:
            with self.subTest(text=text):
                source = self.path / "bad.csv"
                source.write_text(text)
                with self.assertRaises(pipeline.PipelineError):
                    pipeline.build_report(source, self.path / "report.csv")

    def test_lock_prevents_overlap_and_releases(self):
        path = self.path / ".lock"
        with pipeline.run_lock(path):
            with self.assertRaises(pipeline.AlreadyRunning):
                with pipeline.run_lock(path):
                    self.fail("Second process must not acquire this lock")
        with pipeline.run_lock(path):
            pass

    def test_config_rejects_placeholders_for_send(self):
        with self.assertRaisesRegex(pipeline.PipelineError, "Replace example"):
            pipeline.load_settings(pipeline.ROOT / "config.example.ini", send=True)

    def test_password_override_and_literal_percent(self):
        path = self.write_config(username="demo", password="literal%password")
        self.assertEqual(pipeline.load_settings(path, send=True).password, "literal%password")
        with patch.dict(os.environ, {"SMTP_PASSWORD": "override"}):
            config = pipeline.load_settings(path, send=True)
            self.assertEqual(config.password, "override")
            self.assertNotIn("override", repr(config))

    def test_unencrypted_smtp_cannot_use_credentials_or_remote_host(self):
        path = self.write_config(security="none", username="demo", password="secret")
        with self.assertRaisesRegex(pipeline.PipelineError, "Unencrypted SMTP"):
            pipeline.load_settings(path, send=True)
        path = self.write_config(security="none")
        path.write_text(path.read_text().replace("127.0.0.1", "mail.some-domain.org"))
        with self.assertRaisesRegex(pipeline.PipelineError, "Unencrypted SMTP"):
            pipeline.load_settings(path, send=True)

    def test_starttls_happens_before_auth_and_send(self):
        settings = replace(self.settings, security="starttls", password="secret")
        connection = Mock()
        connection.send_message.return_value = {}
        with patch("pipeline.smtplib.SMTP", return_value=connection):
            pipeline.send_message(settings, self.make_message())
        self.assertEqual([call[0] for call in connection.mock_calls],
                         ["ehlo_or_helo_if_needed", "starttls", "ehlo", "login", "send_message", "quit"])

    def test_tls_failure_never_authenticates_or_sends(self):
        connection = Mock()
        connection.starttls.side_effect = smtplib.SMTPNotSupportedError("no TLS")
        with patch("pipeline.smtplib.SMTP", return_value=connection):
            with self.assertRaises(smtplib.SMTPNotSupportedError):
                pipeline.send_message(replace(self.settings, security="starttls", password="secret"), self.make_message())
        connection.login.assert_not_called()
        connection.send_message.assert_not_called()

    def test_ssl_and_partial_recipient_failure(self):
        connection = Mock()
        connection.send_message.return_value = {"recipient@example.com": (550, b"Rejected")}
        with patch("pipeline.smtplib.SMTP_SSL", return_value=connection) as smtp_ssl:
            with self.assertRaisesRegex(pipeline.PipelineError, "some recipients"):
                pipeline.send_message(self.settings, self.make_message())
        self.assertTrue(smtp_ssl.call_args.kwargs["context"].check_hostname)
        connection.starttls.assert_not_called()

    def test_smtp_errors_do_not_expose_password(self):
        config = self.write_config()
        args = self.arguments() + ["--config", str(config), "--send"]
        error_output = io.StringIO()
        with patch("pipeline.send_message", side_effect=smtplib.SMTPException("TOP_SECRET_PASSWORD")):
            with contextlib.redirect_stderr(error_output):
                self.assertEqual(pipeline.main(args), 1)
        self.assertNotIn("TOP_SECRET_PASSWORD", error_output.getvalue())
        self.assertNotIn("TOP_SECRET_PASSWORD", (self.path / "logs/pipeline.log").read_text())
        self.assertEqual(list((self.path / "output").glob("*/result.json")), [])

    def test_end_to_end_submission_to_local_smtp(self):
        with socketserver.ThreadingTCPServer(("127.0.0.1", 0), SMTPReceiver) as server:
            server.daemon_threads = True
            server.message = None
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            thread.start()
            try:
                config = self.write_config(port=server.server_address[1], security="none")
                args = self.arguments() + ["--config", str(config), "--send"]
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(pipeline.main(args), 0)
                received = BytesParser(policy=policy.default).parsebytes(server.message)
                self.assertEqual(str(received["To"]), "recipient@localhost")
                self.assertEqual(len(list(received.iter_attachments())), 1)
                result_path, = (self.path / "output").glob("*/result.json")
                self.assertEqual(json.loads(result_path.read_text())["status"], "smtp_accepted")
            finally:
                server.shutdown()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
