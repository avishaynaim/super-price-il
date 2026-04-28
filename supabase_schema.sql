-- =============================================================
-- super-price-il  —  Supabase schema + stored procedures
-- Run once in the Supabase SQL Editor:
--   https://axdluubyohjrfjqxgpft.supabase.co/project/default/sql
-- =============================================================

-- ── tables ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS chains (
    id         SERIAL PRIMARY KEY,
    code       TEXT NOT NULL UNIQUE,
    name_he    TEXT NOT NULL,
    name_en    TEXT,
    portal_url TEXT NOT NULL,
    active     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS stores (
    id           SERIAL PRIMARY KEY,
    chain_id     INTEGER NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    store_code   TEXT NOT NULL,
    sub_chain_id TEXT,
    name         TEXT,
    address      TEXT,
    city         TEXT,
    zip_code     TEXT,
    store_type   TEXT,
    UNIQUE (chain_id, store_code)
);
CREATE INDEX IF NOT EXISTS idx_stores_chain ON stores(chain_id);
CREATE INDEX IF NOT EXISTS idx_stores_city  ON stores(city);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    barcode     TEXT NOT NULL UNIQUE,
    name        TEXT,
    manufacturer TEXT,
    unit_qty    REAL,
    unit_type   TEXT,
    is_weighted BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);

-- chain_id is denormalised here for fast chain-level DELETE (1-day retention).
CREATE TABLE IF NOT EXISTS current_prices (
    chain_id   INTEGER NOT NULL REFERENCES chains(id)   ON DELETE CASCADE,
    store_id   INTEGER NOT NULL REFERENCES stores(id)   ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price      REAL NOT NULL,
    unit_price REAL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_cp_chain   ON current_prices(chain_id);
CREATE INDEX IF NOT EXISTS idx_cp_product ON current_prices(product_id);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id           SERIAL PRIMARY KEY,
    chain_id     INTEGER NOT NULL REFERENCES chains(id),
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'running',
    files_ok     INTEGER NOT NULL DEFAULT 0,
    files_failed INTEGER NOT NULL DEFAULT 0,
    files_total  INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    error_msg    TEXT,
    progress_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_runs_chain ON scrape_runs(chain_id, started_at DESC);

CREATE TABLE IF NOT EXISTS receipts (
    id           SERIAL PRIMARY KEY,
    user_ref     TEXT,
    source_type  TEXT NOT NULL,
    file_path    TEXT,
    chain_id     INTEGER REFERENCES chains(id),
    store_id     INTEGER REFERENCES stores(id),
    purchased_at TIMESTAMPTZ,
    total        REAL,
    uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS receipt_items (
    id               SERIAL PRIMARY KEY,
    receipt_id       INTEGER NOT NULL REFERENCES receipts(id) ON DELETE CASCADE,
    product_id       INTEGER REFERENCES products(id),
    raw_name         TEXT,
    raw_barcode      TEXT,
    quantity         REAL,
    unit_price       REAL,
    line_total       REAL,
    match_confidence REAL
);
CREATE INDEX IF NOT EXISTS idx_ri_receipt ON receipt_items(receipt_id);

CREATE TABLE IF NOT EXISTS promotions (
    id             SERIAL PRIMARY KEY,
    chain_id       INTEGER NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    store_id       INTEGER REFERENCES stores(id) ON DELETE SET NULL,
    promo_code     TEXT NOT NULL,
    description    TEXT,
    starts_at      TIMESTAMPTZ,
    ends_at        TIMESTAMPTZ,
    reward_type    TEXT,
    min_qty        REAL,
    discount_price REAL,
    discount_rate  REAL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, store_id, promo_code)
);
CREATE INDEX IF NOT EXISTS idx_promo_chain ON promotions(chain_id);
CREATE INDEX IF NOT EXISTS idx_promo_store ON promotions(store_id);
CREATE INDEX IF NOT EXISTS idx_promo_end   ON promotions(ends_at);

CREATE TABLE IF NOT EXISTS promotion_items (
    promotion_id INTEGER NOT NULL REFERENCES promotions(id) ON DELETE CASCADE,
    barcode      TEXT    NOT NULL,
    product_id   INTEGER REFERENCES products(id) ON DELETE SET NULL,
    PRIMARY KEY (promotion_id, barcode)
);
CREATE INDEX IF NOT EXISTS idx_pi_barcode ON promotion_items(barcode);

-- ── disable RLS so the anon key can INSERT ────────────────────

ALTER TABLE chains           DISABLE ROW LEVEL SECURITY;
ALTER TABLE stores           DISABLE ROW LEVEL SECURITY;
ALTER TABLE products         DISABLE ROW LEVEL SECURITY;
ALTER TABLE current_prices   DISABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_runs      DISABLE ROW LEVEL SECURITY;
ALTER TABLE receipts         DISABLE ROW LEVEL SECURITY;
ALTER TABLE receipt_items    DISABLE ROW LEVEL SECURITY;
ALTER TABLE promotions       DISABLE ROW LEVEL SECURITY;
ALTER TABLE promotion_items  DISABLE ROW LEVEL SECURITY;

-- ── stored procedures ─────────────────────────────────────────

-- Product search with aggregates
CREATE OR REPLACE FUNCTION search_products(
    q             TEXT,
    chain_codes   TEXT[]    DEFAULT NULL,
    city_spellings TEXT[]   DEFAULT NULL,
    limit_n       INTEGER   DEFAULT 50
)
RETURNS TABLE (
    barcode            TEXT,
    name               TEXT,
    manufacturer       TEXT,
    min_price          REAL,
    max_price          REAL,
    chains_with_price  BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT
        p.barcode,
        p.name,
        p.manufacturer,
        MIN(cp.price)::REAL            AS min_price,
        MAX(cp.price)::REAL            AS max_price,
        COUNT(DISTINCT cp.chain_id)    AS chains_with_price
    FROM products p
    JOIN current_prices cp ON cp.product_id = p.id
    JOIN stores s          ON s.id = cp.store_id
    WHERE
        CASE WHEN q ~ '^\d{6,}$' THEN p.barcode = q
             ELSE p.name ILIKE '%' || q || '%'
        END
        AND (chain_codes    IS NULL OR cp.chain_id IN (SELECT id FROM chains WHERE code = ANY(chain_codes)))
        AND (city_spellings IS NULL OR s.city = ANY(city_spellings))
    GROUP BY p.id, p.barcode, p.name, p.manufacturer
    ORDER BY chains_with_price DESC, min_price ASC
    LIMIT limit_n;
$$;

-- Per-product all-store price list
CREATE OR REPLACE FUNCTION get_product_prices(
    p_barcode      TEXT,
    chain_codes    TEXT[]  DEFAULT NULL,
    city_spellings TEXT[]  DEFAULT NULL
)
RETURNS TABLE (
    chain_code    TEXT,
    chain_name_he TEXT,
    store_id      INTEGER,
    store_name    TEXT,
    store_city    TEXT,
    price         REAL,
    updated_at    TEXT
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT
        ch.code          AS chain_code,
        ch.name_he       AS chain_name_he,
        s.id             AS store_id,
        s.name           AS store_name,
        s.city           AS store_city,
        cp.price         AS price,
        cp.updated_at::TEXT AS updated_at
    FROM products p
    JOIN current_prices cp ON cp.product_id = p.id
    JOIN stores s          ON s.id = cp.store_id
    JOIN chains ch         ON ch.id = cp.chain_id
    WHERE p.barcode = p_barcode
      AND (chain_codes    IS NULL OR ch.code  = ANY(chain_codes))
      AND (city_spellings IS NULL OR s.city   = ANY(city_spellings))
    ORDER BY cp.price ASC;
$$;

-- Chain-level price comparison (cheapest per chain)
CREATE OR REPLACE FUNCTION compare_product(
    p_barcode      TEXT,
    chain_codes    TEXT[]  DEFAULT NULL,
    city_spellings TEXT[]  DEFAULT NULL
)
RETURNS TABLE (
    chain_code    TEXT,
    chain_name_he TEXT,
    min_price     REAL,
    stores_with   BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT
        ch.code          AS chain_code,
        ch.name_he       AS chain_name_he,
        MIN(cp.price)::REAL AS min_price,
        COUNT(DISTINCT s.id) AS stores_with
    FROM products p
    JOIN current_prices cp ON cp.product_id = p.id
    JOIN stores s          ON s.id = cp.store_id
    JOIN chains ch         ON ch.id = cp.chain_id
    WHERE p.barcode = p_barcode
      AND (chain_codes    IS NULL OR ch.code = ANY(chain_codes))
      AND (city_spellings IS NULL OR s.city  = ANY(city_spellings))
    GROUP BY ch.id, ch.code, ch.name_he
    ORDER BY min_price ASC;
$$;

-- Dashboard: per-chain coverage + last scrape run
CREATE OR REPLACE FUNCTION chain_coverage_stats()
RETURNS TABLE (
    code                TEXT,
    name_he             TEXT,
    name_en             TEXT,
    stores              BIGINT,
    products_priced     BIGINT,
    current_prices_count BIGINT,
    last_status         TEXT,
    last_started_at     TIMESTAMPTZ,
    last_finished_at    TIMESTAMPTZ,
    last_files_ok       INTEGER,
    last_rows_written   INTEGER
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    WITH last_run AS (
        SELECT DISTINCT ON (chain_id)
            chain_id, status, started_at, finished_at, files_ok, rows_written
        FROM scrape_runs
        ORDER BY chain_id, started_at DESC
    )
    SELECT
        ch.code,
        ch.name_he,
        ch.name_en,
        COUNT(DISTINCT s.id)         AS stores,
        COUNT(DISTINCT cp.product_id) AS products_priced,
        COUNT(cp.*)                  AS current_prices_count,
        lr.status                    AS last_status,
        lr.started_at                AS last_started_at,
        lr.finished_at               AS last_finished_at,
        lr.files_ok                  AS last_files_ok,
        lr.rows_written              AS last_rows_written
    FROM chains ch
    LEFT JOIN stores s         ON s.chain_id  = ch.id
    LEFT JOIN current_prices cp ON cp.chain_id = ch.id
    LEFT JOIN last_run lr       ON lr.chain_id = ch.id
    WHERE ch.active = TRUE
    GROUP BY ch.id, ch.code, ch.name_he, ch.name_en,
             lr.status, lr.started_at, lr.finished_at, lr.files_ok, lr.rows_written
    ORDER BY ch.name_he;
$$;

-- City list with store counts (avoids per-row REST limit)
CREATE OR REPLACE FUNCTION list_cities()
RETURNS TABLE(name_he TEXT, stores BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT city AS name_he, COUNT(*) AS stores
    FROM stores
    WHERE city IS NOT NULL
      AND city != ''
      AND city !~ '^\d+$'
    GROUP BY city
    ORDER BY city;
$$;

-- Top price spread: products with biggest min/max gap across chains
CREATE OR REPLACE FUNCTION top_price_spread(
    city_spellings TEXT[]  DEFAULT NULL,
    chain_codes    TEXT[]  DEFAULT NULL,
    limit_n        INTEGER DEFAULT 20
)
RETURNS TABLE (
    barcode       TEXT,
    name          TEXT,
    min_price     REAL,
    max_price     REAL,
    spread        REAL,
    chains_count  BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT
        p.barcode,
        p.name,
        MIN(cp.price)::REAL                     AS min_price,
        MAX(cp.price)::REAL                     AS max_price,
        (MAX(cp.price) - MIN(cp.price))::REAL   AS spread,
        COUNT(DISTINCT cp.chain_id)             AS chains_count
    FROM products p
    JOIN current_prices cp ON cp.product_id = p.id
    JOIN stores s          ON s.id = cp.store_id
    WHERE LENGTH(p.barcode) >= 8
      AND p.barcode ~ '^\d+$'
      AND (chain_codes    IS NULL OR cp.chain_id IN (SELECT id FROM chains WHERE code = ANY(chain_codes)))
      AND (city_spellings IS NULL OR s.city = ANY(city_spellings))
    GROUP BY p.id, p.barcode, p.name
    HAVING COUNT(DISTINCT cp.chain_id) >= 2
    ORDER BY spread DESC
    LIMIT limit_n;
$$;
