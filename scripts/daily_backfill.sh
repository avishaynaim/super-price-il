#!/usr/bin/env bash
# Daily ingest: pulls the last 24h of PriceFull + Stores across all live chains
# and prunes price_observations + raw/ older than 7 days. Idempotent.
#
# Invoke from cron or the accompanying systemd timer. Logs go to stdout so
# the timer's journal captures them.
#
# Env:
#   REPO_ROOT — project root (defaults to the directory containing this script's parent).
#   PYTHON    — python interpreter (defaults to `python3`).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
PYTHON="${PYTHON:-python3}"

cd "$REPO_ROOT"

echo "[$(date -u +%FT%TZ)] super-price-il daily backfill starting"
"$PYTHON" -m src.cli.backfill --chain all --days 1 --retain 7 --kinds PriceFull,Stores,StoresFull
echo "[$(date -u +%FT%TZ)] super-price-il daily backfill done"
