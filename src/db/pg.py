"""psycopg2 connection pool for the Supabase PostgreSQL backend."""
from __future__ import annotations

import os
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
    "options": "-c statement_timeout=60000",  # 60 s query cap
}

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=8, **_DSN)
    return _pool


def connect() -> psycopg2.extensions.connection:
    """Return a raw connection (caller must close/return to pool)."""
    return psycopg2.connect(**_DSN)


@contextmanager
def cursor() -> Iterator[psycopg2.extras.RealDictCursor]:
    """Context manager: borrow a pooled connection, yield a RealDictCursor."""
    conn = _get_pool().getconn()
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        _get_pool().putconn(conn)
