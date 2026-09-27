#!/bin/sh
# The working directory and interpreter are explicit because cron has a small PATH.
set -eu
umask 077
PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_DIR"
if [ ! -x "$PROJECT_DIR/.venv/bin/python" ]; then
    echo "Create the environment first: python3 -m venv .venv" >&2
    exit 1
fi
exec "$PROJECT_DIR/.venv/bin/python" "$PROJECT_DIR/pipeline.py" "$@"

