-- super-price-il schema. SQLite dialect; stays portable to Postgres.

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS chains (
    id            INTEGER PRIMARY KEY,
    code          TEXT NOT NULL UNIQUE,         -- 'shufersal', 'rami_levi', ...
    name_he       TEXT NOT NULL,
    name_en       TEXT,
    portal_url    TEXT NOT NULL,
    auth_profile  TEXT,                         -- registry key (not a secret)
    active        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS stores (
    id            INTEGER PRIMARY KEY,
    chain_id      INTEGER NOT NULL REFERENCES chains(id),
    store_code    TEXT NOT NULL,                -- chain-internal code
    sub_chain_id  TEXT,
    name          TEXT,
    address       TEXT,
    city          TEXT,
    zip_code      TEXT,
    store_type    TEXT,
    latitude      REAL,
    longitude     REAL,
    last_seen_at  TEXT,
    UNIQUE (chain_id, store_code)
);
CREATE INDEX IF NOT EXISTS idx_stores_city ON stores(city);

CREATE TABLE IF NOT EXISTS products (
    id            INTEGER PRIMARY KEY,
    barcode       TEXT NOT NULL UNIQUE,         -- ItemCode
    name          TEXT,                         -- canonical name (first seen; refined)
    manufacturer  TEXT,
    country       TEXT,
    unit_qty      REAL,
    unit_type     TEXT,                         -- 'kg','ml','pcs'
    is_weighted   INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT,
    last_seen_at  TEXT
);

-- Per-chain product aliases: chains disagree on names/packaging text.
CREATE TABLE IF NOT EXISTS product_aliases (
    product_id    INTEGER NOT NULL REFERENCES products(id),
    chain_id      INTEGER NOT NULL REFERENCES chains(id),
    chain_item_id TEXT,
    name          TEXT,
    PRIMARY KEY (product_id, chain_id)
);

-- Append-only time series. This is the source of truth for trends.
CREATE TABLE IF NOT EXISTS price_observations (
    id            INTEGER PRIMARY KEY,
    store_id      INTEGER NOT NULL REFERENCES stores(id),
    product_id    INTEGER NOT NULL REFERENCES products(id),
    price         REAL NOT NULL,
    unit_price    REAL,                         -- price per unit_type
    price_update  TEXT,                         -- chain's PriceUpdateDate
    fetched_at    TEXT NOT NULL,
    source_file   TEXT                          -- path of gz for audit
);
CREATE INDEX IF NOT EXISTS idx_obs_product_time ON price_observations(product_id, fetched_at);
CREATE INDEX IF NOT EXISTS idx_obs_store_time   ON price_observations(store_id, fetched_at);

-- Fast lookup: most-recent price per (store, product).
CREATE TABLE IF NOT EXISTS current_prices (
    store_id      INTEGER NOT NULL REFERENCES stores(id),
    product_id    INTEGER NOT NULL REFERENCES products(id),
    price         REAL NOT NULL,
    unit_price    REAL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (store_id, product_id)
);

CREATE TABLE IF NOT EXISTS promotions (
    id            INTEGER PRIMARY KEY,
    chain_id      INTEGER NOT NULL REFERENCES chains(id),
    store_id      INTEGER REFERENCES stores(id),
    promo_code    TEXT NOT NULL,
    description   TEXT,
    starts_at     TEXT,
    ends_at       TEXT,
    reward_type   TEXT,
    min_qty       REAL,
    discount_price REAL,
    discount_rate  REAL,
    fetched_at    TEXT NOT NULL,
    UNIQUE (chain_id, promo_code, starts_at)
);

CREATE TABLE IF NOT EXISTS promotion_items (
    promotion_id  INTEGER NOT NULL REFERENCES promotions(id) ON DELETE CASCADE,
    product_id    INTEGER NOT NULL REFERENCES products(id),
    PRIMARY KEY (promotion_id, product_id)
);

-- Receipts (feature later; schema defined now to avoid churn).
CREATE TABLE IF NOT EXISTS receipts (
    id            INTEGER PRIMARY KEY,
    user_ref      TEXT,                         -- local-only identifier
    source_type   TEXT NOT NULL,                -- 'photo' | 'digital'
    file_path     TEXT,
    chain_id      INTEGER REFERENCES chains(id),
    store_id      INTEGER REFERENCES stores(id),
    purchased_at  TEXT,
    total         REAL,
    uploaded_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipt_items (
    id            INTEGER PRIMARY KEY,
    receipt_id    INTEGER NOT NULL REFERENCES receipts(id) ON DELETE CASCADE,
    product_id    INTEGER REFERENCES products(id),
    raw_name      TEXT,
    raw_barcode   TEXT,
    quantity      REAL,
    unit_price    REAL,
    line_total    REAL,
    match_confidence REAL
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id            INTEGER PRIMARY KEY,
    chain_id      INTEGER NOT NULL REFERENCES chains(id),
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL,                -- 'running','ok','error','partial'
    files_ok      INTEGER NOT NULL DEFAULT 0,
    files_failed  INTEGER NOT NULL DEFAULT 0,
    rows_written  INTEGER NOT NULL DEFAULT 0,
    error_msg     TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_chain_time ON scrape_runs(chain_id, started_at);
