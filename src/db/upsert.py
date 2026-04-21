from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from ..parser.pricefull import PriceRow
from ..parser.promofull import PromoRow
from ..parser.stores import StoreRow


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def chain_id_for_code(conn: sqlite3.Connection, code: str) -> int:
    row = conn.execute("SELECT id FROM chains WHERE code = ?", (code,)).fetchone()
    if row is None:
        raise RuntimeError(f"chain not seeded: {code}")
    return row[0]


def upsert_store(conn: sqlite3.Connection, chain_id: int, s: StoreRow) -> int:
    conn.execute(
        """
        INSERT INTO stores(chain_id, store_code, sub_chain_id, name, address, city, zip_code, store_type, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chain_id, store_code) DO UPDATE SET
            sub_chain_id = excluded.sub_chain_id,
            name = COALESCE(excluded.name, stores.name),
            address = COALESCE(excluded.address, stores.address),
            city = COALESCE(excluded.city, stores.city),
            zip_code = COALESCE(excluded.zip_code, stores.zip_code),
            store_type = COALESCE(excluded.store_type, stores.store_type),
            last_seen_at = excluded.last_seen_at
        """,
        (chain_id, s.store_code, s.sub_chain_id, s.name, s.address, s.city, s.zip_code, s.store_type, now_iso()),
    )
    row = conn.execute(
        "SELECT id FROM stores WHERE chain_id = ? AND store_code = ?",
        (chain_id, s.store_code),
    ).fetchone()
    return row[0]


def get_or_create_store_by_code(conn: sqlite3.Connection, chain_id: int, store_code: str) -> int:
    row = conn.execute(
        "SELECT id FROM stores WHERE chain_id = ? AND store_code = ?",
        (chain_id, store_code),
    ).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT INTO stores(chain_id, store_code, last_seen_at) VALUES (?, ?, ?)",
        (chain_id, store_code, now_iso()),
    )
    return conn.execute(
        "SELECT id FROM stores WHERE chain_id = ? AND store_code = ?",
        (chain_id, store_code),
    ).fetchone()[0]


def upsert_product(conn: sqlite3.Connection, row: PriceRow) -> int:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO products(barcode, name, manufacturer, country, unit_qty, unit_type, is_weighted, first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(barcode) DO UPDATE SET
            name = COALESCE(products.name, excluded.name),
            manufacturer = COALESCE(products.manufacturer, excluded.manufacturer),
            country = COALESCE(products.country, excluded.country),
            unit_qty = COALESCE(products.unit_qty, excluded.unit_qty),
            unit_type = COALESCE(products.unit_type, excluded.unit_type),
            is_weighted = excluded.is_weighted,
            last_seen_at = excluded.last_seen_at
        """,
        (
            row.barcode, row.name, row.manufacturer, row.country,
            row.unit_qty, row.unit_type, int(row.is_weighted), ts, ts,
        ),
    )
    rid = conn.execute("SELECT id FROM products WHERE barcode = ?", (row.barcode,)).fetchone()[0]
    return rid


def upsert_promotion(
    conn: sqlite3.Connection,
    chain_id: int,
    store_id: int | None,
    p: PromoRow,
) -> int:
    """Insert or update a promotion and replace its item list. Returns promotion id."""
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO promotions(
            chain_id, store_id, promo_code, description, starts_at, ends_at,
            reward_type, min_qty, discount_price, discount_rate, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chain_id, promo_code, starts_at) DO UPDATE SET
            store_id       = excluded.store_id,
            description    = excluded.description,
            ends_at        = excluded.ends_at,
            reward_type    = excluded.reward_type,
            min_qty        = excluded.min_qty,
            discount_price = excluded.discount_price,
            discount_rate  = excluded.discount_rate,
            fetched_at     = excluded.fetched_at
        """,
        (chain_id, store_id, p.promo_code, p.description, p.starts_at, p.ends_at,
         p.reward_type, p.min_qty, p.discount_price, p.discount_rate, ts),
    )
    pid = conn.execute(
        "SELECT id FROM promotions WHERE chain_id = ? AND promo_code = ? AND COALESCE(starts_at,'') = COALESCE(?, '')",
        (chain_id, p.promo_code, p.starts_at),
    ).fetchone()[0]

    if p.item_barcodes:
        conn.execute("DELETE FROM promotion_items WHERE promotion_id = ?", (pid,))
        for bc in p.item_barcodes:
            pr = conn.execute("SELECT id FROM products WHERE barcode = ?", (bc,)).fetchone()
            if not pr:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO promotion_items(promotion_id, product_id) VALUES (?, ?)",
                (pid, pr[0]),
            )
    return pid


def insert_promotions(
    conn: sqlite3.Connection,
    chain_id: int,
    store_id: int | None,
    rows: Iterable[PromoRow],
) -> int:
    count = 0
    for p in rows:
        upsert_promotion(conn, chain_id, store_id, p)
        count += 1
    return count


def insert_observations(
    conn: sqlite3.Connection,
    store_id: int,
    rows: Iterable[PriceRow],
    source_file: str,
) -> int:
    count = 0
    ts = now_iso()
    for r in rows:
        product_id = upsert_product(conn, r)
        conn.execute(
            """
            INSERT INTO price_observations(store_id, product_id, price, unit_price, price_update, fetched_at, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (store_id, product_id, r.price, r.unit_price, r.price_update, ts, source_file),
        )
        conn.execute(
            """
            INSERT INTO current_prices(store_id, product_id, price, unit_price, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(store_id, product_id) DO UPDATE SET
                price = excluded.price,
                unit_price = excluded.unit_price,
                updated_at = excluded.updated_at
            """,
            (store_id, product_id, r.price, r.unit_price, ts),
        )
        count += 1
    return count
