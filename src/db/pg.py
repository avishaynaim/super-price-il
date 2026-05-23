"""DB connection layer.

Default: psycopg2 connection pool to Supabase Postgres.
With SUPER_PRICE_LOCAL_ONLY=1: a SQLite cursor that translates %s→? so
existing supa.py shim code (which builds Postgres SQL) just works against
data/local_prices.db.
"""
from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras
import psycopg2.pool

_DSN = {
    "host": os.environ.get("PG_HOST", "db.axdluubyohjrfjqxgpft.supabase.co"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "dbname": os.environ.get("PG_DB", "postgres"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASS", "mosheSemet123@"),
    "connect_timeout": 15,
    "options": "-c statement_timeout=60000 -c default_transaction_read_only=off",
}

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _local_only() -> bool:
    return os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1"


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=8, **_DSN)
    return _pool


def connect():
    """Return a raw connection (caller must close)."""
    if _local_only():
        from .local import LOCAL_DB
        c = sqlite3.connect(LOCAL_DB)
        c.row_factory = sqlite3.Row
        return c
    return psycopg2.connect(**_DSN)


class _SqliteCursorShim:
    """Wraps a sqlite3 cursor so it answers like psycopg2.extras.RealDictCursor.
    - Translates %s placeholders to ? at execute time.
    - Rewrites Postgres-isms: 'col = ANY(%s)' -> 'col IN (?,?,...)'.
    - Returns rows as dicts.
    Any RPC-style query (SELECT * FROM fn_name(...)) is detected and
    short-circuited to an empty result so callers fail soft.
    """
    _ANY_RE = re.compile(r"(\b\w+(?:\.\w+)?)\s*=\s*ANY\(\s*%s\s*\)")

    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur
        self._empty = False

    def execute(self, sql: str, params=None):
        self._empty = False
        # Detect supa.sb().rpc() pattern: SELECT * FROM <fn>(args)
        # Heuristic: function name (not a known table) followed by '('
        if re.search(r"FROM\s+(search_products|get_product_prices|compare_product|trends_for_product|retailers_status)\s*\(", sql, re.I):
            self._empty = True
            return
        params = list(params) if params else []
        # Expand ANY(%s) → IN (?,?,...) using next param (a list)
        out = []
        new_params = []
        i = 0
        for m in self._ANY_RE.finditer(sql):
            # add prefix from i to start
            out.append(sql[i:m.start()])
            # consume the corresponding %s param (which is a list)
            # find param index up to here
            prefix_count = sql[:m.start()].count("%s")
            list_param = params[prefix_count]
            placeholders = ",".join(["?"] * len(list_param))
            out.append(f"{m.group(1)} IN ({placeholders})")
            i = m.end()
            new_params.append(list_param)
        out.append(sql[i:])
        if new_params:
            # Rebuild full param list, expanding lists
            rebuilt = []
            sep = sql.count("%s")
            j = 0
            list_idx = 0
            for k in range(sep):
                if list_idx < len(new_params) and params[k] is new_params[list_idx]:
                    rebuilt.extend(new_params[list_idx])
                    list_idx += 1
                else:
                    rebuilt.append(params[k])
            params = rebuilt
            sql = "".join(out)
        # Translate %s -> ?
        sql = sql.replace("%s", "?")
        self._cur.execute(sql, params)

    def executemany(self, sql, seq):
        sql = sql.replace("%s", "?")
        self._cur.executemany(sql, seq)

    def fetchone(self):
        if self._empty:
            return None
        r = self._cur.fetchone()
        return dict(r) if r else None

    def fetchall(self):
        if self._empty:
            return []
        return [dict(r) for r in self._cur.fetchall()]

    def __iter__(self):
        if self._empty:
            return iter([])
        for r in self._cur:
            yield dict(r)

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


@contextmanager
def cursor() -> Iterator[psycopg2.extras.RealDictCursor]:
    """Context manager: yields a dict-cursor. SQLite-backed if LOCAL_ONLY=1."""
    if _local_only():
        from .local import LOCAL_DB
        conn = sqlite3.connect(LOCAL_DB)
        conn.row_factory = sqlite3.Row
        try:
            yield _SqliteCursorShim(conn.cursor())
            conn.commit()
        finally:
            conn.close()
        return
    conn = _get_pool().getconn()
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        _get_pool().putconn(conn)
