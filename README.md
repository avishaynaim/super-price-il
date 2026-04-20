# super-price-il

Israeli supermarket price intelligence. Scrapes the per-chain price-transparency portals mandated by the 2015 Price Transparency Law, normalizes everything into SQLite, and exposes an AI-assisted web UI plus a receipt-to-savings pipeline.

## Status

Early scaffold. See `docs/ARCHITECTURE.md`.

## Layout

```
src/
  scraper/        chain-specific downloaders + orchestrator
    chains/       one module per chain
    registry.py   portal URLs, auth profiles
    base.py       BaseChainScraper
  parser/         XML → normalized rows (PriceFull, PromoFull, Stores)
  db/             schema.sql + migrations + connection helper
  api/            FastAPI endpoints
  cli/            operator commands (backfill, run-chain, stats)
data/
  raw/<chain>/<YYYY-MM-DD>/    original gz files (audit trail)
  prices.db                    SQLite normalized store
web/              React/Vite frontend (later)
scripts/          one-off ops scripts
```

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m src.db.migrate                          # create prices.db
python -m src.cli.backfill --chain all --days 1   # daily scrape (auto-prunes >7d)
python -m src.cli.prune --retain 7                # manual prune
python -m src.api.main                            # FastAPI on :8000
```

## Retention

Daily scrape of last 24h per chain, rolling 7-day retention on `price_observations`
and `data/raw/`. `current_prices` (latest value per store×product) is preserved.
Run `src/cli/backfill.py` on a daily cron — it self-prunes.

## Chains

Top 3 first (Shufersal, Rami Levi, Victory); roadmap covers the rest of the ~10 chains from `gov.il/he/pages/cpfta_prices_regulations`.
