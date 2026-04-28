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
# --kinds PriceFull,PromoFull — prices + promotions daily.
# --refresh-stores-days 7    — automatically adds Stores/StoresFull for chains
#                              whose store files are older than 7 days.
# Cache tables (chain_stats_cache, store_prices_cache) are refreshed per-chain
# automatically inside backfill.py after each scrape run.
"$PYTHON" -m src.cli.backfill \
    --chain all --days 1 \
    --kinds PriceFull,PromoFull \
    --refresh-stores-days 7
echo "[$(date -u +%FT%TZ)] super-price-il daily backfill done"
