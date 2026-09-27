# Cron + Email Demo Pipeline

A small, working alternative for a simple scheduled reporting task: **cron starts a Python script, the script builds a CSV report, and SMTP delivers an email with the report attached.**

This is a standalone learning project, not an Airflow replacement. It uses only the Python standard library: `csv`, `decimal`, `email`, `smtplib`, and other built-in modules. No pip packages or external data services are required.

## What happens on each run

1. Read `data/sample.csv` and validate its rows.
2. Group **all input rows** by category, calculating row counts and total amounts.
3. Save `report.csv` and assemble an email using `EmailMessage`.
4. Save a preview as `email.eml`.
5. With `--send`, submit the message to your SMTP server using `smtplib`.
6. Write `result.json` and a rotating log.

The bundled data is synthetic: **6 rows, total amount 10650.00**. The report is rebuilt from the entire CSV on every run; this demo does not filter by the current date.

## Requirements

- Python **3.10 or later**.
- Linux or macOS. On Windows, run inside **WSL**.
- A running cron service for scheduled execution.
- For real email delivery: an SMTP server, its connection settings, and authorization to use the sender address. The computer/server must be running when the cron job is due.

GitHub stores the project. Uploading it to GitHub does **not** install cron or run the pipeline on your computer.

## 1. Run a safe local test

Open a terminal in the project directory:

```bash
python3 -m venv .venv
.venv/bin/python pipeline.py --dry-run
```

No credentials, network connection, or configuration changes are needed. If `config.ini` does not exist, dry-run uses `config.example.ini`. Running without a mode flag also defaults to dry-run.

Each run creates a new directory:

```text
output/<UTC-timestamp>-<run-id>/
  report.csv
  email.eml
  result.json
```

Open `email.eml` in a desktop mail client to inspect the message and attachment. The file is only a preview; creating it does not send a message.

Expected `report.csv` contents:

| category | row_count | total_amount |
| --- | ---: | ---: |
| Office | 2 | 4200.00 |
| Online | 3 | 3250.00 |
| Partner | 1 | 3200.00 |

The CSV includes a UTF-8 BOM for convenient opening in spreadsheet applications. If your spreadsheet uses a different delimiter, import the file and select comma-separated UTF-8.

## 2. Configure real email delivery

Create a local configuration file without overwriting an existing one:

```bash
test -f config.ini || cp config.example.ini config.ini
chmod 600 config.ini
```

Edit `config.ini`:

```ini
[smtp]
host = smtp.your-provider.com
port = 465
security = ssl
username = your-mailbox@your-domain.com
password = YOUR_APP_PASSWORD
timeout_seconds = 30

[mail]
from = your-mailbox@your-domain.com
to = your-test-mailbox@your-domain.com
subject = Daily demo report
```

Replace every placeholder. Start with a mailbox you control as the recipient. For multiple recipients, use comma-separated plain email addresses.

- `ssl`: TLS from the start of the connection, commonly port 465.
- `starttls`: upgrade the SMTP connection to TLS before authentication, commonly port 587.
- `none`: only for an unauthenticated local SMTP test server at `127.0.0.1` or `::1`.

Use the exact settings provided by your mail administrator/provider. Certificate verification stays enabled. If your organization uses an SMTP relay without login, leave both `username` and `password` empty and use the approved TLS settings. OAuth-only providers need an additional authentication implementation; this demo supports SMTP username/password or an authorized unauthenticated relay.

The `SMTP_PASSWORD` environment variable overrides the password in the INI file. If you use it, make sure it is also available to the scheduled process: cron does not automatically inherit variables exported in your interactive terminal. For this demo, a local `config.ini` with permissions `600` is the simplest setup.

Values in the INI file are literal: do not quote the password. Do not commit `config.ini`, `.env`, credentials, generated messages, or real datasets. The project ignores the local config, logs, and output directory; never put real credentials into `config.example.ini`.

Run one manual delivery test after configuring SMTP:

```bash
.venv/bin/python pipeline.py --send
```

`smtp_accepted` means the SMTP server accepted the message for all listed recipients. It does **not** guarantee inbox delivery; check the mailbox, spam folder, and any server-side delivery reports.

## 3. Add a cron schedule

First verify the wrapper manually:

```bash
/bin/sh scripts/run.sh --dry-run
```

Get the absolute project path with `pwd`, then open your user crontab:

```bash
crontab -e
```

**Add** a line, preserving your existing jobs. Do not run `crontab crontab.example`: that would replace your whole user crontab with the example.

Test every five minutes, without sending:

```cron
*/5 * * * * /bin/sh /absolute/path/cron-email-demo/scripts/run.sh --dry-run
```

After the manual SMTP test succeeds, replace the test line with a daily job:

```cron
0 9 * * * /bin/sh /absolute/path/cron-email-demo/scripts/run.sh --send
```

The five schedule fields are **minute, hour, day of month, month, day of week**. The example runs at 09:00 in the cron daemon's timezone. For 09:00 Moscow on a server whose cron uses UTC, use `0 6 * * *`. Confirm the host/cron timezone before scheduling. `CRON_TZ` support varies between cron implementations; the examples do not assume it exists.

Replace the example path with your real path. Quote the path if it contains spaces. Avoid `%` in the path because cron treats it specially. Keep only one active example schedule unless you intentionally want both.

Check the saved schedule and logs:

```bash
crontab -l
tail -n 30 logs/pipeline.log
```

The wrapper changes into the project directory and invokes its virtual environment using an absolute path. It does not depend on an interactive shell or an activated environment. If cron itself fails before Python starts, inspect your operating system's cron logs.

## Custom input and paths

```bash
.venv/bin/python pipeline.py --dry-run --input /absolute/path/input.csv
.venv/bin/python pipeline.py --send --config /absolute/path/config.ini
```

The input header must be exactly `date,category,amount`. Dates use ISO format (`YYYY-MM-DD`), categories must be non-empty, and amounts must be finite numbers with at most two decimal places. Negative amounts are allowed for adjustments; absolute amounts above one trillion are rejected. Invalid or empty input fails before SMTP is called.

`--output-dir` and `--log-dir` are also available. Default paths are relative to the project, even when the script starts from another directory. Explicit relative CLI paths are relative to the invoking process's current directory.

To adapt the demo to your real job, replace `build_report()` in `pipeline.py` with your SQL/API/data-processing logic and keep the scheduling and email parts.

## Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Tests cover report totals, message attachments, dry-run behavior, invalid input, lock contention, TLS/authentication ordering, SMTP failures, and SMTP submission to a temporary **loopback-only test server**. They do not send messages to real mailboxes or need credentials. Real-provider connectivity must be checked separately after local configuration.

## Behavior and limits

- Default mode is dry-run; `--send` is always explicit.
- A file lock prevents overlapping runs that use the same output directory on the same host. It is not a distributed lock and should be used on a local filesystem.
- A completed run returns exit code `0`. A failed run returns `1`. An overlapping run is skipped with a warning and returns `0`.
- Logs rotate at approximately 1 MB, with three backups. They do not contain credentials, message bodies, or raw SMTP server responses.
- No automatic SMTP retries: if the connection drops after sending, delivery may be ambiguous. Check the recipient/server before rerunning. A partial recipient rejection is reported as a failure, even though other recipients may have received the message.
- Every successful invocation with `--send` submits a new email. The demo has no persistent queue, backfill, daily deduplication, or exactly-once guarantee.
- `result.json` is written only after a successful dry-run or SMTP acceptance. A later disk error can prevent that file from being written even if the server accepted the email. Use the logs and mailbox to investigate; do not infer non-delivery from a missing result file alone.
- Old output directories are retained for inspection. Remove them periodically; this demo does not delete reports automatically.
- Cron does not automatically replay a job missed while the host was off. Multi-host coordination, complex dependencies, retries, alerting, and a monitoring UI need additional orchestration.

## Project files

| File | Purpose |
| --- | --- |
| `pipeline.py` | CSV processing, email creation, SMTP delivery, locking and logs |
| `config.example.ini` | Safe configuration template |
| `data/sample.csv` | Synthetic demo input |
| `scripts/run.sh` | Cron-friendly launcher |
| `crontab.example` | Commented schedule examples |
| `tests/test_pipeline.py` | Automated checks, including a local SMTP receiver |

## Reference documentation

- [Python email examples](https://docs.python.org/3/library/email.examples.html)
- [Python smtplib](https://docs.python.org/3/library/smtplib.html)
- [crontab manual](https://man7.org/linux/man-pages/man5/crontab.5.html)
