#!/usr/bin/env bash
# Weekly-cadence ingest with on-demand catch-up:
#   * iterate every active chain
#   * skip those that completed a successful scrape within the last 7 days
#   * scrape the rest immediately
#   * prune price_observations + raw/ to the configured retention window
# Idempotent. Safe to run on any schedule (hourly cron / systemd timer / etc.) —
# chains scraped recently are no-ops, so frequent invocations cost almost nothing.
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
# --retain 0   ⇒ read from data/settings.json (retention_days)
# --skip-recent 7 ⇒ weekly cadence per chain — anything scraped within 7 days
#                   is a no-op; anything older runs immediately.
"$PYTHON" -m src.cli.backfill \
    --chain all --days 1 --retain 0 --skip-recent 7 \
    --kinds PriceFull,Stores,StoresFull
echo "[$(date -u +%FT%TZ)] super-price-il daily backfill done"
