"""All database write operations — psycopg2 direct PostgreSQL backend.

Replaces the supabase-py REST approach with a direct connection for
much faster bulk inserts (execute_values vs HTTP round-trips).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable

import psycopg2
import psycopg2.extras

from .pg import connect, cursor
from ..parser.pricefull import PriceRow
from ..parser.stores import StoreRow


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- supabase-py shim: sb() still used by geo.py for simple selects ----------

class _SbShim:
    """Thin wrapper so geo.py / stats.py can call sb().table(…) patterns."""
    def table(self, name: str) -> "_TableQuery":
        return _TableQuery(name)

    def rpc(self, fn: str, params: dict | None = None) -> "_RpcQuery":
        return _RpcQuery(fn, params or {})


class _TableQuery:
    def __init__(self, table: str):
        self._table = table
        self._selects: list[str] = ["*"]
        self._filters: list[tuple] = []
        self._order_col: str | None = None
        self._order_desc: bool = False
        self._limit_n: int | None = None
        self._count_mode: str | None = None

    def select(self, cols: str, count: str | None = None) -> "_TableQuery":
        self._selects = [cols]
        self._count_mode = count
        return self

    def eq(self, col: str, val: Any) -> "_TableQuery":
        self._filters.append(("eq", col, val))
        return self

    def in_(self, col: str, vals: list) -> "_TableQuery":
        self._filters.append(("in", col, vals))
        return self

    def not_(self) -> "_TableQuery":  # placeholder: .not_.is_("col","null")
        return _NotQuery(self)

    def is_(self, col: str, val: str) -> "_TableQuery":
        if val == "null":
            self._filters.append(("notnull", col, None))
        return self

    def order(self, col: str, desc: bool = False) -> "_TableQuery":
        self._order_col = col
        self._order_desc = desc
        return self

    def limit(self, n: int) -> "_TableQuery":
        self._limit_n = n
        return self

    def maybe_single(self) -> "_TableQuery":
        self._limit_n = 1
        return self

    def single(self) -> "_TableQuery":
        self._limit_n = 1
        return self

    def execute(self) -> "_Result":
        parts = ["SELECT"]
        # count mode
        col_expr = self._selects[0] if self._selects else "*"
        # strip embedded table refs like chains!chain_id(code,name_he)
        col_expr = col_expr.replace("not_", "")
        parts.append(col_expr)
        parts.append(f"FROM {self._table}")

        where_parts, params = [], []
        for f in self._filters:
            if f[0] == "eq":
                where_parts.append(f"{f[1]} = %s")
                params.append(f[2])
            elif f[0] == "in":
                where_parts.append(f"{f[1]} = ANY(%s)")
                params.append(f[2])
            elif f[0] == "notnull":
                where_parts.append(f"{f[1]} IS NOT NULL")
        if where_parts:
            parts.append("WHERE " + " AND ".join(where_parts))
        if self._order_col:
            d = "DESC" if self._order_desc else "ASC"
            parts.append(f"ORDER BY {self._order_col} {d}")
        if self._limit_n:
            parts.append(f"LIMIT {self._limit_n}")

        sql = " ".join(parts)
        with cursor() as cur:
            if self._count_mode == "exact":
                cur.execute(f"SELECT COUNT(*) AS n FROM ({sql}) sub", params)
                n = cur.fetchone()["n"]
                return _Result([], n)
            cur.execute(sql, params)
            rows = cur.fetchall()
            data = [dict(r) for r in rows]
        result = _Result(data)
        if self._limit_n == 1:
            result.data = data[0] if data else None
        return result


class _NotQuery(_TableQuery):
    """Proxy that lets .not_.is_("col","null") work on the parent."""
    def __init__(self, parent: _TableQuery):
        super().__init__(parent._table)
        self.__dict__.update(parent.__dict__)

    def is_(self, col: str, val: str) -> "_TableQuery":
        if val == "null":
            self._filters.append(("notnull", col, None))
        return self


class _RpcQuery:
    def __init__(self, fn: str, params: dict):
        self._fn = fn
        self._params = params

    def execute(self) -> "_Result":
        # Build positional args from params dict to match function signature.
        # Functions are called via SELECT * FROM fn(arg1=>%s, arg2=>%s, ...)
        named = ", ".join(f"{k} => %s" for k in self._params)
        sql = f"SELECT * FROM {self._fn}({named})"
        vals = list(self._params.values())
        with cursor() as cur:
            cur.execute(sql, vals)
            rows = cur.fetchall()
        return _Result([dict(r) for r in rows])


class _Result:
    def __init__(self, data: list | dict | None, count: int | None = None):
        self.data = data
        self.count = count


_shim = _SbShim()


def sb() -> _SbShim:
    """Return the shim so existing code calling sb().table(…) still works."""
    return _shim


# ---------- chains ----------

def seed_chains(specs: list[Any]) -> None:
    rows = [
        (c.code, c.name_he, c.name_en, c.portal_url, True)
        for c in specs
    ]
    conn = connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO chains (code, name_he, name_en, portal_url, active)
                VALUES %s
                ON CONFLICT (code) DO UPDATE SET
                    name_he    = EXCLUDED.name_he,
                    name_en    = EXCLUDED.name_en,
                    portal_url = EXCLUDED.portal_url
                """,
                rows,
            )
    finally:
        conn.close()


def chain_id_for_code(code: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT id FROM chains WHERE code = %s", (code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"chain not seeded: {code}")
        return row["id"]


def get_all_chain_ids() -> dict[str, int]:
    with cursor() as cur:
        cur.execute("SELECT id, code FROM chains")
        return {r["code"]: r["id"] for r in cur.fetchall()}


# ---------- stores ----------

def upsert_store(chain_id: int, s: StoreRow) -> int:
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO stores (chain_id, store_code, sub_chain_id, name, address, city, zip_code, store_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chain_id, store_code) DO UPDATE SET
                sub_chain_id = COALESCE(EXCLUDED.sub_chain_id, stores.sub_chain_id),
                name         = COALESCE(EXCLUDED.name, stores.name),
                address      = COALESCE(EXCLUDED.address, stores.address),
                city         = COALESCE(EXCLUDED.city, stores.city),
                zip_code     = COALESCE(EXCLUDED.zip_code, stores.zip_code),
                store_type   = COALESCE(EXCLUDED.store_type, stores.store_type)
            RETURNING id
            """,
            (chain_id, s.store_code, s.sub_chain_id, s.name, s.address, s.city, s.zip_code, s.store_type),
        )
        return cur.fetchone()["id"]


def get_or_create_store_by_code(chain_id: int, store_code: str) -> int:
    with cursor() as cur:
        cur.execute(
            "SELECT id FROM stores WHERE chain_id = %s AND store_code = %s",
            (chain_id, store_code),
        )
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO stores (chain_id, store_code) VALUES (%s, %s) RETURNING id",
            (chain_id, store_code),
        )
        return cur.fetchone()["id"]


# ---------- products + prices ----------

def _upsert_products_batch(rows: list[PriceRow], cache: dict[str, int], conn: Any) -> None:
    """Upsert products not yet in cache; update cache with returned IDs."""
    new_rows = [
        (r.barcode, r.name, r.manufacturer, r.unit_qty, r.unit_type, bool(r.is_weighted))
        for r in rows
        if r.barcode not in cache
    ]
    if not new_rows:
        return
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO products (barcode, name, manufacturer, unit_qty, unit_type, is_weighted)
            VALUES %s
            ON CONFLICT (barcode) DO UPDATE SET
                name         = COALESCE(products.name, EXCLUDED.name),
                manufacturer = COALESCE(products.manufacturer, EXCLUDED.manufacturer),
                unit_qty     = COALESCE(products.unit_qty, EXCLUDED.unit_qty),
                unit_type    = COALESCE(products.unit_type, EXCLUDED.unit_type)
            RETURNING id, barcode
            """,
            new_rows,
            page_size=1000,
        )
        for row in cur.fetchall():
            cache[row["barcode"]] = row["id"]

    # Fetch IDs for any barcode still missing (existing row, no change → RETURNING skips it)
    missing = [r.barcode for r in rows if r.barcode not in cache]
    if missing:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, barcode FROM products WHERE barcode = ANY(%s)", (missing,))
            for row in cur.fetchall():
                cache[row["barcode"]] = row["id"]


def insert_observations(
    chain_id: int,
    store_id: int,
    rows: Iterable[PriceRow],
    source_file: str,
    product_cache: dict[str, int],
    conn: Any = None,
) -> int:
    """Bulk upsert products + current_prices for one store. Reuses conn for speed."""
    row_list = list(rows)
    if not row_list:
        return 0

    own_conn = conn is None
    if own_conn:
        conn = connect()
        conn.autocommit = True
    try:
        _upsert_products_batch(row_list, product_cache, conn)

        ts = now_iso()
        cp_rows = [
            (chain_id, store_id, product_cache[r.barcode], r.price, r.unit_price, ts)
            for r in row_list
            if r.barcode in product_cache
        ]
        if cp_rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO current_prices (chain_id, store_id, product_id, price, unit_price, updated_at)
                    VALUES %s
                    ON CONFLICT (store_id, product_id) DO UPDATE SET
                        price      = EXCLUDED.price,
                        unit_price = EXCLUDED.unit_price,
                        updated_at = EXCLUDED.updated_at
                    """,
                    cp_rows,
                    page_size=1000,
                )
        return len(cp_rows)
    finally:
        if own_conn:
            conn.close()


def delete_chain_current_prices(chain_id: int) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM current_prices WHERE chain_id = %s", (chain_id,))


# ---------- scrape_runs ----------

def scrape_run_start(chain_id: int) -> int:
    ts = now_iso()
    with cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (chain_id, started_at, status, files_total, progress_at) "
            "VALUES (%s, %s, 'running', 0, %s) RETURNING id",
            (chain_id, ts, ts),
        )
        return cur.fetchone()["id"]


def scrape_run_update(run_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    sets = ", ".join(f"{k} = %s" for k in kwargs)
    vals = list(kwargs.values()) + [now_iso(), run_id]
    with cursor() as cur:
        cur.execute(f"UPDATE scrape_runs SET {sets}, progress_at = %s WHERE id = %s", vals)


def scrape_run_finish(
    run_id: int, status: str, files_ok: int, files_failed: int,
    rows_written: int, error_msg: str | None = None,
) -> None:
    ts = now_iso()
    with cursor() as cur:
        cur.execute(
            "UPDATE scrape_runs SET status=%s, finished_at=%s, files_ok=%s, "
            "files_failed=%s, rows_written=%s, error_msg=%s, progress_at=%s WHERE id=%s",
            (status, ts, files_ok, files_failed, rows_written, error_msg, ts, run_id),
        )
