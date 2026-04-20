# Architecture

## Goals

1. Daily, reliable scrape of every Israeli supermarket chain under the Price Transparency Law.
2. Normalized local store (SQLite) queryable across chains by product / store / area / time.
3. Web UI for search, filter, trends, and AI-assisted natural-language queries.
4. Receipt ingest (photo OCR + digital) → match to catalog → "you'd save ₪X at chain Y within your area".

## Data flow

```
gov.il hub ──▶ per-chain portal ──▶ .gz XML (Prices/PriceFull/Promo/PromoFull/Stores)
                                        │
                                        ▼
                                  data/raw/<chain>/<date>/   (audit copy, never mutated)
                                        │
                                        ▼
                                 src/parser (lxml, streaming)
                                        │
                                        ▼
                                 SQLite (src/db/schema.sql)
                                        │
                  ┌─────────────────────┼───────────────────────┐
                  ▼                     ▼                       ▼
             FastAPI /api         DuckDB views            receipt matcher
                  │                     │                       │
                  └────────────────── React UI ─────────────────┘
```

## Why SQLite first

- Entire normalized dataset for ~10 chains × ~1M prices/day fits comfortably (WAL mode, indexed).
- DuckDB can query the same file for ad-hoc trend aggregations without a second store.
- Single-file backup, portable, zero ops. Postgres migration path preserved (standard SQL only).

## Scrape cadence

- Chains push `PriceFull` once/day, `Price` (deltas) hourly, `PromoFull`/`Promo` similar, `Stores` weekly.
- Default: PriceFull + PromoFull + Stores daily. Delta files only if we need intraday.

## Chain auth shapes

| chain         | portal                          | auth                                 |
|---------------|---------------------------------|--------------------------------------|
| Shufersal     | prices.shufersal.co.il          | none (public listing)                |
| Rami Levi     | url.publishedprices.co.il       | user=RamiLevi, blank pw              |
| Victory       | laibcatalog.co.il               | user=victory, blank pw (form POST)   |
| Yohananof     | url.publishedprices.co.il       | user=yohananof                       |
| Mega          | mega.co.il/mega-cs              | none                                 |
| Osher Ad      | osherad.binaprojects.com        | none                                 |
| King Store    | kingstore.binaprojects.com      | none                                 |
| Hazi Hinam    | shop.hazi-hinam.co.il/...       | none                                 |
| Keshet        | publishprice.mehadrin.co.il/... | none                                 |
| Tiv Taam      | url.retail.publishedprices.co.il| user=TivTaam                         |

(Schemas drift per chain; parser normalizes.)

## Schema highlights

- `chains`, `stores`, `products` keyed by natural IDs (chain code, store code, barcode).
- `price_observations` is append-only with `fetched_at` — this is what powers trends.
- `current_prices` is a materialized view for fast UI lookups.
- `scrape_runs` tracks per-chain health.
