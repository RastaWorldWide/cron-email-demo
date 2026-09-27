#!/usr/bin/env python3
"""Small cron -> CSV -> EmailMessage -> SMTP pipeline; Python 3.10+, stdlib only."""
from __future__ import annotations

import argparse
import configparser
import csv
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email import policy
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import smtplib
import ssl
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("cron_email_demo")


class PipelineError(Exception):
    """A safe, actionable error message, containing no SMTP credentials."""


class AlreadyRunning(PipelineError):
    pass


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    security: str
    username: str
    password: str = field(repr=False)
    timeout: int
    sender: str
    recipients: tuple[str, ...]
    subject: str


def valid_address(value: str) -> str:
    try:
        if not value or "\r" in value or "\n" in value:
            raise ValueError
        value.encode("ascii")
        address = Address(addr_spec=value)
        if not address.username or not address.domain:
            raise ValueError
    except (ValueError, UnicodeError) as exc:
        raise PipelineError("mail.from/to must contain plain ASCII email addresses.") from exc
    return value


def load_settings(path: Path, send: bool) -> Settings:
    parser = configparser.ConfigParser(interpolation=None)
    try:
        with path.open(encoding="utf-8") as source:
            parser.read_file(source)
        sender = valid_address(parser.get("mail", "from").strip())
        recipients = tuple(valid_address(item.strip()) for item in parser.get("mail", "to").split(","))
        settings = Settings(
            host=parser.get("smtp", "host").strip(),
            port=parser.getint("smtp", "port"),
            security=parser.get("smtp", "security").strip().lower(),
            username=parser.get("smtp", "username", fallback="").strip(),
            password=os.environ.get("SMTP_PASSWORD", parser.get("smtp", "password", fallback="")),
            timeout=parser.getint("smtp", "timeout_seconds", fallback=30),
            sender=sender,
            recipients=recipients,
            subject=parser.get("mail", "subject", fallback="Demo report"),
        )
    except (configparser.Error, ValueError, OSError) as exc:
        raise PipelineError("Cannot read configuration. Check the INI file and numeric SMTP values.") from exc
    if not settings.host or not 1 <= settings.port <= 65535 or not 1 <= settings.timeout <= 300:
        raise PipelineError("Check SMTP host, port (1..65535), and timeout (1..300 seconds).")
    if settings.security not in {"ssl", "starttls", "none"}:
        raise PipelineError("smtp.security must be ssl, starttls, or none.")
    if settings.security == "none" and (
        settings.host not in {"127.0.0.1", "::1"} or settings.username or settings.password
    ):
        raise PipelineError("Unencrypted SMTP is allowed only on loopback, without credentials.")
    if not settings.subject.strip() or "\r" in settings.subject or "\n" in settings.subject:
        raise PipelineError("mail.subject must be a non-empty single line.")
    if send:
        domains = [settings.host.lower()] + [x.rsplit("@", 1)[1].lower() for x in (sender, *recipients)]
        if any(d in {"example.com", "example.org", "example.net"} or d.endswith((".example.com", ".invalid", ".test")) for d in domains):
            raise PipelineError("Replace example SMTP host and email addresses before --send.")
        if bool(settings.username) != bool(settings.password):
            raise PipelineError("Set both SMTP username and password, or leave both empty for an authorized relay.")
    return settings


@contextmanager
def run_lock(path: Path):
    # flock locks are released by the OS even if this process crashes.
    try:
        import fcntl
    except ImportError as exc:
        raise PipelineError("Run on Linux, macOS, or inside WSL; locking requires fcntl.") from exc
    with path.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AlreadyRunning("Another run holds the output-directory lock; skipped.") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_report(source: Path, destination: Path) -> tuple[int, Decimal]:
    """Aggregate all input rows by category, with exact decimal arithmetic."""
    totals: dict[str, Decimal] = defaultdict(Decimal)
    counts: dict[str, int] = defaultdict(int)
    row_count = 0
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["date", "category", "amount"]:
            raise PipelineError("CSV header must be exactly: date,category,amount.")
        for line, row in enumerate(reader, start=2):
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError
                date.fromisoformat(row["date"])
                category = row["category"].strip()
                amount = Decimal(row["amount"])
                if not category or not amount.is_finite() or abs(amount) > Decimal("1000000000000"):
                    raise ValueError
                if amount != amount.quantize(Decimal("0.01")):
                    raise ValueError
            except (ValueError, InvalidOperation) as exc:
                raise PipelineError(f"Invalid CSV row {line}: check date, category and amount (max 2 decimals).") from exc
            totals[category] += amount
            counts[category] += 1
            row_count += 1
    if not row_count:
        raise PipelineError("The CSV is empty; no report will be sent.")
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["category", "row_count", "total_amount"])
        for category in sorted(totals):
            # Neutralize spreadsheet formulas if users later supply their own categories.
            safe_category = "'" + category if category.startswith(("=", "+", "-", "@")) else category
            writer.writerow([safe_category, counts[category], f"{totals[category]:.2f}"])
    return row_count, sum(totals.values(), Decimal("0"))


def build_message(settings: Settings, report: Path, run_id: str, rows: int, total: Decimal) -> EmailMessage:
    msg = EmailMessage(policy=policy.SMTP)
    msg["From"] = settings.sender
    msg["To"] = ", ".join(settings.recipients)
    msg["Subject"] = settings.subject
    msg["Date"] = format_datetime(datetime.now(timezone.utc))
    msg["Message-ID"] = make_msgid(domain=settings.sender.rsplit("@", 1)[1])
    msg.set_content(
        "The demo pipeline has processed the input CSV.\n\n"
        f"Run: {run_id}\nRows processed: {rows}\nTotal amount: {total:.2f}\n\n"
        "See the attached report.csv for the breakdown by category.\n"
        "This is a demo report; the bundled input contains synthetic data.\n"
    )
    msg.add_attachment(report.read_bytes(), maintype="text", subtype="csv", filename="report.csv")
    return msg


def send_message(settings: Settings, msg: EmailMessage) -> None:
    context = ssl.create_default_context()
    if settings.security == "ssl":
        connection = smtplib.SMTP_SSL(settings.host, settings.port, timeout=settings.timeout, context=context)
    else:
        connection = smtplib.SMTP(settings.host, settings.port, timeout=settings.timeout)
    try:
        connection.ehlo_or_helo_if_needed()
        if settings.security == "starttls":
            connection.starttls(context=context)
            connection.ehlo()
        if settings.username:
            connection.login(settings.username, settings.password)
        refused = connection.send_message(msg, from_addr=settings.sender, to_addrs=list(settings.recipients))
        if refused:
            # SMTP may accept some recipients and reject others. Never silently report success.
            raise PipelineError("SMTP rejected some recipients; others may have been accepted. Check before retrying.")
    finally:
        # A failed QUIT must not turn a successful DATA response into a send failure.
        try:
            connection.quit()
        except (OSError, smtplib.SMTPException):
            connection.close()


def configure_logging(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(directory / "pipeline.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    for old in LOG.handlers[:]:
        LOG.removeHandler(old)
        old.close()
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)
    LOG.propagate = False


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Build CSV and .eml without SMTP (default).")
    mode.add_argument("--send", action="store_true", help="Send via the configured SMTP server.")
    parser.add_argument("--config", type=Path, help="INI file; default: config.ini, or example for dry-run.")
    parser.add_argument("--input", type=Path, default=ROOT / "data/sample.csv")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--log-dir", type=Path, default=ROOT / "logs")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    os.umask(0o077)
    try:
        configure_logging(args.log_dir)
        config_path = args.config or ROOT / "config.ini"
        if args.config is None and not config_path.exists() and not args.send:
            config_path = ROOT / "config.example.ini"
        settings = load_settings(config_path, args.send)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        with run_lock(args.output_dir / ".run.lock"):
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
            run_dir = args.output_dir / run_id
            run_dir.mkdir()
            LOG.info("run=%s started mode=%s", run_id, "send" if args.send else "dry-run")
            rows, total = build_report(args.input, run_dir / "report.csv")
            msg = build_message(settings, run_dir / "report.csv", run_id, rows, total)
            (run_dir / "email.eml").write_bytes(msg.as_bytes())
            if args.send:
                send_message(settings, msg)
            outcome = "smtp_accepted" if args.send else "dry_run"
            (run_dir / "result.json").write_text(json.dumps({
                "run_id": run_id, "status": outcome, "rows": rows, "total_amount": str(total),
                "message_id": str(msg["Message-ID"]),
            }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            LOG.info("run=%s status=%s rows=%s total=%s", run_id, outcome, rows, total)
            print(f"OK: {outcome}; artifacts: {run_dir.resolve()}")
            return 0
    except AlreadyRunning as exc:
        LOG.warning("%s", exc)
        print(str(exc), file=sys.stderr)
        return 0
    except Exception as exc:
        # Never log SMTP responses or arbitrary exception text: they may contain secrets.
        detail = str(exc) if isinstance(exc, PipelineError) else type(exc).__name__
        LOG.error("Pipeline failed: %s", detail)
        print(f"ERROR: {detail}. See logs/pipeline.log; no automatic retry.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
