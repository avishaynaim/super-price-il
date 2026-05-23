"""DB router: try Supabase first, fall back to local SQLite when supa fails.

Wraps every write API used by the scraper. Once any call fails with an
OperationalError-class exception (DB down / paused / over quota), the
router flips to local mode for the rest of the process. A health probe
is retried periodically to detect recovery.
"""
from __future__ import annotations

import os
import time
from typing import Any, Iterable

import psycopg2

from . import supa, local as _local


# Process-wide flag set when supa first fails.
# SUPER_PRICE_LOCAL_ONLY=1 forces local-only from the start (no supa attempts).
_local_mode: bool = os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1"
_last_probe: float = 0.0
_PROBE_INTERVAL = 60.0  # try supa again every 60s after fallback


def _supa_dead() -> bool:
    """Cheap probe: SELECT 1. Cached for _PROBE_INTERVAL seconds.
    When SUPER_PRICE_LOCAL_ONLY=1, never probes (stays local forever).
    """
    global _last_probe, _local_mode
    if not _local_mode:
        return False
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        return True
    now = time.time()
    if now - _last_probe < _PROBE_INTERVAL:
        return True
    _last_probe = now
    try:
        from .pg import cursor as _pg_cursor
        with _pg_cursor() as cur:
            cur.execute("SELECT 1")
        _local_mode = False
        print("[router] Supabase recovered — switching back to remote", flush=True)
        return False
    except Exception:
        return True


def _flip_local(why: str) -> None:
    global _local_mode, _last_probe
    if not _local_mode:
        print(f"[router] Supabase write failed ({why}); switching to LOCAL spillover", flush=True)
    _local_mode = True
    _last_probe = time.time()


def _is_dbdown(exc: Exception) -> bool:
    s = str(exc).lower()
    return (
        isinstance(exc, (psycopg2.OperationalError, psycopg2.errors.AdminShutdown))
        or "recovery mode" in s
        or "could not extend file" in s
        or "no space left" in s
        or "ssl syscall" in s
        or "is in recovery" in s
        or "could not translate host name" in s
    )


def _try(fn_supa, fn_local, *args, **kwargs):
    if _supa_dead():
        return fn_local(*args, **kwargs)
    try:
        return fn_supa(*args, **kwargs)
    except Exception as e:
        if _is_dbdown(e):
            _flip_local(type(e).__name__)
            return fn_local(*args, **kwargs)
        raise


# ---------- public API (signature-compatible with supa.*) ----------

def seed_chains(specs):
    return _try(supa.seed_chains, _local.seed_chains, specs)

def chain_id_for_code(code):
    return _try(supa.chain_id_for_code, _local.chain_id_for_code, code)

def upsert_store(chain_id, sr):
    return _try(supa.upsert_store, _local.upsert_store, chain_id, sr)

def get_or_create_store_by_code(chain_id, store_code):
    return _try(supa.get_or_create_store_by_code, _local.get_or_create_store_by_code, chain_id, store_code)

def insert_observations(chain_id, store_id, rows, source_file, product_cache):
    return _try(supa.insert_observations, _local.insert_observations,
                chain_id, store_id, rows, source_file, product_cache)

def delete_chain_current_prices(chain_id):
    return _try(supa.delete_chain_current_prices, _local.delete_chain_current_prices, chain_id)

def delete_stale_chain_prices(chain_id, run_start):
    return _try(supa.delete_stale_chain_prices, _local.delete_stale_chain_prices, chain_id, run_start)

def upsert_promotions(chain_id, store_id, promos, product_cache):
    return _try(supa.upsert_promotions, _local.upsert_promotions,
                chain_id, store_id, promos, product_cache)

def refresh_caches(chain_id=None):
    return _try(supa.refresh_caches, _local.refresh_caches, chain_id)

def scrape_run_start(chain_id):
    return _try(supa.scrape_run_start, _local.scrape_run_start, chain_id)

def scrape_run_update(run_id, **kwargs):
    return _try(supa.scrape_run_update, _local.scrape_run_update, run_id, **kwargs)

def scrape_run_finish(run_id, status, files_ok, files_failed, rows_written, error_msg=None):
    return _try(supa.scrape_run_finish, _local.scrape_run_finish,
                run_id, status, files_ok, files_failed, rows_written, error_msg=error_msg)


def is_local_mode() -> bool:
    return _local_mode
