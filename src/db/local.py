"""Local SQLite spillover store.

Used when Supabase Postgres is unavailable (over-quota / paused / recovery).
Mirrors a subset of supa.py's write API. Data written here is re-pushed to
Supabase later by `python -m src.cli.sync_local`.

Schema is the same schema.sql but PRAGMA foreign_keys=OFF (we batch-write
without ordering guarantees).
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..parser.pricefull import PriceRow

LOCAL_DB = Path(__file__).resolve().parents[2] / "data" / "local_prices.db"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_db():
    LOCAL_DB.parent.mkdir(parents=True, exist_ok=True)
    fresh = (not LOCAL_DB.exists()) or LOCAL_DB.stat().st_size == 0
    # timeout=30: wait up to 30s for a write lock instead of failing immediately.
    # busy_timeout via PRAGMA so readers don't block behind scraper writes.
    conn = sqlite3.connect(LOCAL_DB, timeout=30)
    conn.executescript("PRAGMA foreign_keys=OFF; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000;")
    if fresh:
        schema_sql = (Path(__file__).parent / "schema.sql").read_text()
        # Strip Postgres-only bits (FK ON, journal mode handled above)
        conn.executescript(schema_sql)
        # Track which scrape rows came from the spillover (so we know what
        # to push when supa is back).
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS spilled (
                id INTEGER PRIMARY KEY,
                kind TEXT NOT NULL,           -- 'observation','store','promo','run'
                created_at TEXT NOT NULL,
                synced_at TEXT,
                ref_json TEXT NOT NULL        -- JSON payload for sync
            );
            CREATE INDEX IF NOT EXISTS idx_spilled_unsynced ON spilled(synced_at) WHERE synced_at IS NULL;
        """)
    conn.commit()
    return conn


@contextmanager
def cursor():
    conn = _ensure_db()
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ---------- chain / store helpers ----------

def chain_id_for_code(code: str) -> int:
    """Get or create a chain_id locally (matches supa's by-code semantics)."""
    with cursor() as conn:
        cur = conn.execute("SELECT id FROM chains WHERE code=?", (code,))
        r = cur.fetchone()
        if r:
            return r["id"]
        cur = conn.execute(
            "INSERT INTO chains(code, name_he, portal_url) VALUES(?,?,?)",
            (code, code, ""),
        )
        return cur.lastrowid


def _numeric_store_code_fallback(conn, chain_id: int, store_code: str):
    """For numeric codes, find an existing store whose integer value matches.
    Handles zero-padding mismatches: '002' == '0002' == '2'.
    Returns the Row or None.
    """
    if not store_code.isdigit():
        return None
    return conn.execute(
        """SELECT id FROM stores
           WHERE chain_id=?
             AND store_code GLOB '[0-9]*'
             AND CAST(store_code AS INTEGER)=CAST(? AS INTEGER)""",
        (chain_id, store_code),
    ).fetchone()


def get_or_create_store_by_code(chain_id: int, store_code: str) -> int:
    with cursor() as conn:
        r = conn.execute(
            "SELECT id FROM stores WHERE chain_id=? AND store_code=?",
            (chain_id, store_code),
        ).fetchone()
        if r:
            return r["id"]
        # Zero-padding fallback: price files use '0062', Stores XML uses '062'.
        r = _numeric_store_code_fallback(conn, chain_id, store_code)
        if r:
            return r["id"]
        cur = conn.execute(
            "INSERT INTO stores(chain_id, store_code, last_seen_at) VALUES(?,?,?)",
            (chain_id, store_code, _now_iso()),
        )
        return cur.lastrowid


def upsert_store(chain_id: int, sr: Any) -> int:
    """sr is a StoreRow with attributes: store_code, sub_chain_id, name, address, city, zip_code, store_type."""
    with cursor() as conn:
        r = conn.execute(
            "SELECT id FROM stores WHERE chain_id=? AND store_code=?",
            (chain_id, sr.store_code),
        ).fetchone()
        if not r:
            r = _numeric_store_code_fallback(conn, chain_id, sr.store_code)
        if r:
            conn.execute(
                """UPDATE stores SET name=?, address=?, city=?, zip_code=?,
                   store_type=?, sub_chain_id=?, last_seen_at=? WHERE id=?""",
                (sr.name, sr.address, sr.city, sr.zip_code, sr.store_type,
                 getattr(sr, "sub_chain_id", None), _now_iso(), r["id"]),
            )
            return r["id"]
        cur = conn.execute(
            """INSERT INTO stores(chain_id, store_code, sub_chain_id, name, address,
                                  city, zip_code, store_type, last_seen_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (chain_id, sr.store_code, getattr(sr, "sub_chain_id", None),
             sr.name, sr.address, sr.city, sr.zip_code, sr.store_type, _now_iso()),
        )
        return cur.lastrowid


# ---------- price observations ----------

def insert_observations(
    chain_id: int,
    store_id: int,
    rows: Iterable[PriceRow],
    source_file: str,
    product_cache: dict[str, int],
) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0
    ts = _now_iso()
    written = 0
    with cursor() as conn:
        # Upsert products (barcode unique). Build product_cache.
        for r in rows_list:
            if r.barcode in product_cache:
                continue
            cur = conn.execute("SELECT id FROM products WHERE barcode=?", (r.barcode,))
            row = cur.fetchone()
            if row:
                product_cache[r.barcode] = row["id"]
            else:
                cur = conn.execute(
                    """INSERT INTO products(barcode, name, manufacturer, country,
                                            unit_qty, unit_type, is_weighted,
                                            first_seen_at, last_seen_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (r.barcode, r.name, r.manufacturer, r.country,
                     r.unit_qty, r.unit_type, 1 if r.is_weighted else 0, ts, ts),
                )
                product_cache[r.barcode] = cur.lastrowid

        # Upsert current_prices
        for r in rows_list:
            pid = product_cache.get(r.barcode)
            if not pid:
                continue
            conn.execute(
                """INSERT INTO current_prices(store_id, product_id, price, unit_price, updated_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(store_id, product_id) DO UPDATE SET
                     price=excluded.price, unit_price=excluded.unit_price,
                     updated_at=excluded.updated_at""",
                (store_id, pid, r.price, r.unit_price, ts),
            )
            written += 1
    return written


def delete_chain_current_prices(chain_id: int) -> None:
    with cursor() as conn:
        conn.execute(
            """DELETE FROM current_prices
               WHERE store_id IN (SELECT id FROM stores WHERE chain_id=?)""",
            (chain_id,),
        )


def delete_stale_chain_prices(chain_id: int, run_start: str) -> None:
    """Remove current_prices rows not updated within a 14-day retention window.

    Using a fixed retention window (not run_start) prevents wiping prices for
    stores whose portal files weren't available in THIS run but were downloaded
    recently. A store not seen in 14 days is genuinely stale.
    """
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat(timespec="seconds")
    with cursor() as conn:
        conn.execute(
            """DELETE FROM current_prices
               WHERE store_id IN (SELECT id FROM stores WHERE chain_id=?)
               AND updated_at < ?""",
            (chain_id, cutoff),
        )


# ---------- scrape_runs (no-op stubs; we don't need run tracking locally) ----------

def scrape_run_start(chain_id: int) -> int:
    with cursor() as conn:
        cur = conn.execute(
            "INSERT INTO scrape_runs(chain_id, started_at, status) VALUES(?,?,?)",
            (chain_id, _now_iso(), "running"),
        )
        return cur.lastrowid


def scrape_run_update(run_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [run_id]
    with cursor() as conn:
        conn.execute(f"UPDATE scrape_runs SET {sets} WHERE id=?", vals)


def scrape_run_finish(
    run_id: int, status: str, files_ok: int, files_failed: int,
    rows_written: int, error_msg: str | None = None,
) -> None:
    with cursor() as conn:
        conn.execute(
            """UPDATE scrape_runs SET status=?, finished_at=?, files_ok=?,
               files_failed=?, rows_written=?, error_msg=? WHERE id=?""",
            (status, _now_iso(), files_ok, files_failed, rows_written, error_msg, run_id),
        )


def upsert_promotions(chain_id: int, store_id: int | None, promos: list, product_cache: dict) -> int:
    # Promotions schema is more complex; for spillover we just count.
    # The XML files remain in raw/ if needed (until parsed).
    return 0


def refresh_caches(chain_id: int | None = None) -> None:
    pass  # local doesn't need cache tables


def seed_chains(specs: list[Any]) -> None:
    """Idempotent insert of chain registry into local DB."""
    with cursor() as conn:
        for s in specs:
            conn.execute(
                """INSERT INTO chains(code, name_he, name_en, portal_url, auth_profile, active)
                   VALUES(?,?,?,?,?,1)
                   ON CONFLICT(code) DO UPDATE SET
                     name_he=excluded.name_he, name_en=excluded.name_en,
                     portal_url=excluded.portal_url, auth_profile=excluded.auth_profile""",
                (s.code, s.name_he, s.name_en, s.portal_url, s.auth_kind),
            )


def lookup_barcodes(barcodes: list[str]) -> dict[str, dict]:
    """Return {barcode: {barcode, name, manufacturer}} for each barcode found in local products."""
    if not barcodes:
        return {}
    placeholders = ",".join("?" * len(barcodes))
    with cursor() as conn:
        rows = conn.execute(
            f"SELECT barcode, name, manufacturer FROM products WHERE barcode IN ({placeholders})",
            barcodes,
        ).fetchall()
    return {r["barcode"]: {"barcode": r["barcode"], "name": r["name"], "manufacturer": r["manufacturer"]} for r in rows}
