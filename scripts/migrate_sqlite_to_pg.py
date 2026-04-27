#!/usr/bin/env python3
"""One-shot migration: copy all data from data/prices.db (SQLite) → Supabase (PostgreSQL).

Run from the project root:
    python3 scripts/migrate_sqlite_to_pg.py

Tables migrated in dependency order:
    chains → stores → products → current_prices → scrape_runs

existing Supabase rows are upserted (not duplicated).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

SQLITE_PATH = PROJECT / "data" / "prices.db"

from src.db.pg import connect as pg_connect  # noqa: E402


def _sqlite_rows(sq: sqlite3.Connection, sql: str) -> list[dict]:
    sq.row_factory = sqlite3.Row
    return [dict(r) for r in sq.execute(sql).fetchall()]


def migrate() -> None:
    sq = sqlite3.connect(str(SQLITE_PATH))
    sq.row_factory = sqlite3.Row
    pg = pg_connect()
    pg.autocommit = False

    try:
        print("── chains ──")
        rows = [dict(r) for r in sq.execute(
            "SELECT code, name_he, name_en, portal_url, active FROM chains"
        ).fetchall()]
        with pg.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO chains (code, name_he, name_en, portal_url, active)
                   VALUES %s
                   ON CONFLICT (code) DO UPDATE SET
                       name_he    = EXCLUDED.name_he,
                       name_en    = EXCLUDED.name_en,
                       portal_url = EXCLUDED.portal_url,
                       active     = EXCLUDED.active""",
                [(r["code"], r["name_he"], r["name_en"], r["portal_url"], bool(r["active"]))
                 for r in rows],
            )
        pg.commit()
        print(f"  {len(rows)} chains upserted")

        # build chain code→id map
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, code FROM chains")
            chain_id_map: dict[str, int] = {r["code"]: r["id"] for r in cur.fetchall()}

        # sqlite chain id→code
        sq_chain_id_map: dict[int, str] = {
            r["id"]: r["code"] for r in sq.execute("SELECT id, code FROM chains").fetchall()
        }

        print("── stores ──")
        store_rows = [dict(r) for r in sq.execute(
            "SELECT chain_id, store_code, sub_chain_id, name, address, city, zip_code, store_type "
            "FROM stores"
        ).fetchall()]
        BATCH = 2000
        stored = 0
        for i in range(0, len(store_rows), BATCH):
            batch = store_rows[i:i + BATCH]
            valid = [(chain_id_map[sq_chain_id_map[r["chain_id"]]], r["store_code"],
                      r["sub_chain_id"], r["name"], r["address"], r["city"],
                      r["zip_code"], r["store_type"])
                     for r in batch if sq_chain_id_map.get(r["chain_id"]) in chain_id_map]
            if not valid:
                continue
            with pg.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO stores (chain_id, store_code, sub_chain_id, name, address, city, zip_code, store_type)
                       VALUES %s
                       ON CONFLICT (chain_id, store_code) DO UPDATE SET
                           name    = COALESCE(EXCLUDED.name, stores.name),
                           address = COALESCE(EXCLUDED.address, stores.address),
                           city    = COALESCE(EXCLUDED.city, stores.city)""",
                    valid,
                    page_size=500,
                )
            pg.commit()
            stored += len(valid)
            print(f"  {stored}/{len(store_rows)} stores", end="\r")
        print(f"\n  {stored} stores upserted")

        # build store (chain_id_pg, store_code) → pg store id
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, chain_id, store_code FROM stores")
            store_id_map: dict[tuple[int, str], int] = {
                (r["chain_id"], r["store_code"]): r["id"] for r in cur.fetchall()
            }

        # sqlite store id → (pg_chain_id, store_code)
        sq_store_map: dict[int, tuple[int, str]] = {}
        for r in sq.execute("SELECT id, chain_id, store_code FROM stores").fetchall():
            code = sq_chain_id_map.get(r["chain_id"])
            if code and code in chain_id_map:
                sq_store_map[r["id"]] = (chain_id_map[code], r["store_code"])

        print("── products ──")
        total_products = sq.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        offset = 0
        product_id_map: dict[str, int] = {}
        while offset < total_products:
            batch = [dict(r) for r in sq.execute(
                "SELECT barcode, name, manufacturer, unit_qty, unit_type, is_weighted "
                "FROM products LIMIT 5000 OFFSET ?", (offset,)
            ).fetchall()]
            if not batch:
                break
            with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO products (barcode, name, manufacturer, unit_qty, unit_type, is_weighted)
                       VALUES %s
                       ON CONFLICT (barcode) DO UPDATE SET
                           name         = COALESCE(products.name, EXCLUDED.name),
                           manufacturer = COALESCE(products.manufacturer, EXCLUDED.manufacturer)
                       RETURNING id, barcode""",
                    [(r["barcode"], r["name"], r["manufacturer"],
                      r["unit_qty"], r["unit_type"], bool(r["is_weighted"]))
                     for r in batch],
                    page_size=1000,
                )
                for row in cur.fetchall():
                    product_id_map[row["barcode"]] = row["id"]
            pg.commit()
            offset += len(batch)
            print(f"  {offset}/{total_products} products", end="\r")

        # Fetch IDs for any barcodes still missing (existing rows, RETURNING skips them)
        missing = [r["barcode"] for r in sq.execute("SELECT barcode FROM products").fetchall()
                   if r["barcode"] not in product_id_map]
        if missing:
            for i in range(0, len(missing), 5000):
                chunk = missing[i:i + 5000]
                with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT id, barcode FROM products WHERE barcode = ANY(%s)", (chunk,))
                    for row in cur.fetchall():
                        product_id_map[row["barcode"]] = row["id"]

        print(f"\n  {len(product_id_map)} products mapped")

        # Pre-build sqlite product id → pg product id (avoid per-row queries in the cp loop)
        print("  building product id map from sqlite...", end="\r")
        sq_prod_to_pg: dict[int, int] = {}
        for r in sq.execute("SELECT id, barcode FROM products").fetchall():
            pg_id = product_id_map.get(r["barcode"])
            if pg_id:
                sq_prod_to_pg[r["id"]] = pg_id
        print(f"  {len(sq_prod_to_pg)} sqlite product ids mapped to pg ids")

        # Pre-build sqlite store id → (pg_chain_id, pg_store_id)
        sq_store_pg_id: dict[int, tuple[int, int]] = {}
        for sq_sid, (pg_cid, sc) in sq_store_map.items():
            pg_sid = store_id_map.get((pg_cid, sc))
            if pg_sid:
                sq_store_pg_id[sq_sid] = (pg_cid, pg_sid)

        print("── current_prices ──")
        total_cp = sq.execute("SELECT COUNT(*) FROM current_prices").fetchone()[0]
        offset = 0
        cp_done = 0
        while offset < total_cp:
            batch = [dict(r) for r in sq.execute(
                "SELECT store_id, product_id, price, unit_price, updated_at "
                "FROM current_prices LIMIT 10000 OFFSET ?", (offset,)
            ).fetchall()]
            if not batch:
                break

            valid_cp = []
            for r in batch:
                store_pg = sq_store_pg_id.get(r["store_id"])
                if not store_pg:
                    continue
                pg_chain_id, pg_store_id = store_pg
                pg_prod_id = sq_prod_to_pg.get(r["product_id"])
                if not pg_prod_id:
                    continue
                valid_cp.append((pg_chain_id, pg_store_id, pg_prod_id,
                                  r["price"], r["unit_price"], r["updated_at"]))

            if valid_cp:
                with pg.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO current_prices (chain_id, store_id, product_id, price, unit_price, updated_at)
                           VALUES %s
                           ON CONFLICT (store_id, product_id) DO UPDATE SET
                               price      = EXCLUDED.price,
                               unit_price = EXCLUDED.unit_price,
                               updated_at = EXCLUDED.updated_at""",
                        valid_cp,
                        page_size=2000,
                    )
                pg.commit()
                cp_done += len(valid_cp)

            offset += len(batch)
            print(f"  {offset}/{total_cp} current_prices rows processed ({cp_done} upserted)", end="\r")

        print(f"\n  {cp_done} current_prices upserted")
        print("Migration complete.")

    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()
        sq.close()


if __name__ == "__main__":
    migrate()
