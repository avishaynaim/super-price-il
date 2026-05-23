"""Dashboard stats — Supabase backend.

  GET /api/stats/chains       — per-chain coverage + last scrape
  GET /api/stats/scrape-runs  — recent scrape runs
  GET /api/stats/top-spread   — biggest price spread across chains
  GET /api/stats/cities       — stores per city
"""
from __future__ import annotations

from fastapi import APIRouter, Query

from ..db import supa
from .geo import compute_city_spellings

stats_router = APIRouter(prefix="/stats", tags=["stats"])


@stats_router.get("/chains")
def chain_coverage(
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
):
    import os
    from ..db.pg import cursor as _cursor
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        with _cursor() as cur:
            cur.execute("""
                WITH last_run AS (
                    SELECT chain_id, status, started_at, finished_at,
                           files_ok, files_failed, rows_written
                    FROM scrape_runs
                    WHERE id IN (SELECT MAX(id) FROM scrape_runs GROUP BY chain_id)
                )
                SELECT
                    ch.code, ch.name_he, ch.name_en,
                    COUNT(DISTINCT s.id)          AS stores,
                    COUNT(DISTINCT cp.product_id) AS products_priced,
                    COUNT(cp.product_id)          AS current_prices,
                    lr.status                     AS last_status,
                    lr.started_at                 AS last_started_at,
                    lr.finished_at                AS last_finished_at,
                    lr.files_ok                   AS last_files_ok,
                    lr.files_failed               AS last_files_failed,
                    lr.rows_written               AS last_rows_written
                FROM chains ch
                LEFT JOIN last_run lr       ON lr.chain_id = ch.id
                LEFT JOIN stores s          ON s.chain_id  = ch.id
                LEFT JOIN current_prices cp ON cp.store_id = s.id
                WHERE ch.active = 1
                GROUP BY ch.id, ch.code, ch.name_he, ch.name_en,
                         lr.status, lr.started_at, lr.finished_at,
                         lr.files_ok, lr.files_failed, lr.rows_written
                ORDER BY ch.name_he
            """)
            return [dict(r) for r in cur.fetchall()]
    with _cursor() as cur:
        cur.execute("""
            WITH last_run AS (
                SELECT DISTINCT ON (chain_id)
                    chain_id, status, started_at, finished_at,
                    files_ok, files_failed, rows_written
                FROM scrape_runs ORDER BY chain_id, started_at DESC
            ),
            s_counts AS (
                SELECT chain_id, COUNT(*) AS total FROM stores GROUP BY chain_id
            )
            SELECT
                ch.code, ch.name_he, ch.name_en,
                COALESCE(s.total, 0)           AS stores,
                COALESCE(cs.uniq_prods, 0)     AS products_priced,
                COALESCE(cs.total_prices, 0)   AS current_prices,
                lr.status                       AS last_status,
                lr.started_at                   AS last_started_at,
                lr.finished_at                  AS last_finished_at,
                lr.files_ok                     AS last_files_ok,
                lr.files_failed                 AS last_files_failed,
                lr.rows_written                 AS last_rows_written
            FROM chains ch
            LEFT JOIN last_run lr          ON lr.chain_id = ch.id
            LEFT JOIN s_counts s           ON s.chain_id  = ch.id
            LEFT JOIN chain_stats_cache cs ON cs.chain_id = ch.id
            WHERE ch.active = TRUE
            ORDER BY ch.name_he
        """)
        return [dict(r) for r in cur.fetchall()]


@stats_router.get("/scrape-runs")
def scrape_runs(limit: int = Query(50, le=200)):
    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute(
            """
            SELECT sr.id, sr.chain_id, sr.started_at, sr.finished_at, sr.status,
                   sr.files_total, sr.files_ok, sr.files_failed, sr.rows_written,
                   sr.error_msg, sr.progress_at,
                   ch.code AS chain_code, ch.name_he AS chain_name_he
            FROM scrape_runs sr
            JOIN chains ch ON ch.id = sr.chain_id
            ORDER BY sr.started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


@stats_router.get("/top-spread")
def top_spread(
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
    limit: int = Query(20, le=100),
    min_chains: int = Query(2, ge=1, le=50),
    min_price: float = Query(1.0, ge=0),
):
    import os
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    chain_codes: list[str] | None = None
    if chains:
        chain_codes = [c.strip() for c in chains.split(",") if c.strip()] or None
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        from ..db.pg import cursor as _cursor
        extra_where = ""
        params: list = []
        if chain_codes:
            ph = ",".join(["?" for _ in chain_codes])
            extra_where += f" AND ch.code IN ({ph})"
            params.extend(chain_codes)
        if city_spellings is not None:
            if not city_spellings:
                return []
            ph = ",".join(["?" for _ in city_spellings])
            extra_where += f" AND s.city IN ({ph})"
            params.extend(city_spellings)
        params.extend([min_price, min_chains, limit])
        with _cursor() as cur:
            cur.execute(f"""
                SELECT p.barcode, p.name, p.manufacturer,
                       COUNT(DISTINCT s.chain_id)  AS chains_with_price,
                       MIN(cp.price)               AS min_price,
                       MAX(cp.price)               AS max_price,
                       ROUND(
                           (CAST(MAX(cp.price) AS REAL) - MIN(cp.price))
                           / MIN(cp.price) * 100, 1
                       )                           AS spread_pct
                FROM products p
                JOIN current_prices cp ON cp.product_id = p.id
                JOIN stores s ON s.id = cp.store_id
                JOIN chains ch ON ch.id = s.chain_id{extra_where}
                GROUP BY p.id, p.barcode, p.name, p.manufacturer
                HAVING MIN(cp.price) >= ? AND COUNT(DISTINCT s.chain_id) >= ?
                ORDER BY spread_pct DESC
                LIMIT ?
            """, params)
            return [dict(r) for r in cur.fetchall()]
    res = supa.sb().rpc("top_price_spread", {
        "city_spellings": city_spellings,
        "chain_codes": chain_codes,
        "limit_n": limit,
    }).execute()
    return res.data or []


@stats_router.get("/recent-promotions")
def recent_promotions(limit: int = Query(20, le=100)):
    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute(
            """
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   p.description, p.discount_price, p.discount_rate,
                   p.starts_at, p.ends_at, p.reward_type,
                   COUNT(pi.barcode) AS items
            FROM promotions p
            JOIN chains ch ON ch.id = p.chain_id
            LEFT JOIN promotion_items pi ON pi.promotion_id = p.id
            WHERE p.ends_at IS NULL OR p.ends_at >= NOW()
            GROUP BY p.id, ch.code, ch.name_he
            ORDER BY p.updated_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


@stats_router.get("/cities")
def cities_stats():
    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute("""
            SELECT city,
                   COUNT(*)                AS stores,
                   COUNT(DISTINCT chain_id) AS chains
            FROM stores
            WHERE city IS NOT NULL
              AND city <> ''
              AND city !~ '^[0-9]+$'
            GROUP BY city
            ORDER BY stores DESC
            LIMIT 100
        """)
        return [dict(r) for r in cur.fetchall()]


@stats_router.get("/promo-counts")
def promo_counts():
    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute(
            """
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   COUNT(DISTINCT p.id) AS active_promos,
                   COUNT(pi.barcode)    AS items
            FROM chains ch
            LEFT JOIN promotions p ON p.chain_id = ch.id
                AND (p.ends_at IS NULL OR p.ends_at >= NOW())
            LEFT JOIN promotion_items pi ON pi.promotion_id = p.id
            WHERE ch.active = TRUE
            GROUP BY ch.id, ch.code, ch.name_he
            ORDER BY active_promos DESC
            """
        )
        return [dict(r) for r in cur.fetchall()]


_retailers_cache: list = []
_retailers_cache_ts: float = 0.0
_RETAILERS_TTL = 15.0  # seconds


def _retailers_status_local():
    """Local SQLite version of retailers-status. Builds per-chain row from
    raw tables — no stored functions, no DISTINCT ON."""
    import os, sqlite3
    from datetime import datetime, timezone, timedelta
    from ..db.local import LOCAL_DB
    if not LOCAL_DB.exists():
        return []
    c = sqlite3.connect(LOCAL_DB, timeout=3)
    c.execute("PRAGMA busy_timeout=3000")
    c.row_factory = sqlite3.Row
    rows = []
    chains = c.execute("SELECT id, code, name_he, name_en FROM chains WHERE active=1 ORDER BY name_he").fetchall()
    for ch in chains:
        cid = ch["id"]
        st = dict(c.execute("""
            SELECT count(*) AS total,
                   sum(CASE WHEN EXISTS(SELECT 1 FROM current_prices cp WHERE cp.store_id=s.id) THEN 1 ELSE 0 END) AS has_data
            FROM stores s WHERE s.chain_id=?
        """, (cid,)).fetchone())
        cp_count = c.execute("""
            SELECT count(*) FROM current_prices cp
            JOIN stores s ON s.id=cp.store_id WHERE s.chain_id=?
        """, (cid,)).fetchone()[0]
        prod_priced = c.execute("""
            SELECT count(DISTINCT cp.product_id) FROM current_prices cp
            JOIN stores s ON s.id=cp.store_id WHERE s.chain_id=?
        """, (cid,)).fetchone()[0]
        last_run = c.execute("""
            SELECT started_at, finished_at, status, files_ok, files_total,
                   rows_written, progress_at
            FROM scrape_runs WHERE chain_id=? ORDER BY id DESC LIMIT 1
        """, (cid,)).fetchone()
        last_scrape_at = last_run["finished_at"] if last_run and last_run["finished_at"] else None
        running_now = bool(last_run and last_run["status"] == "running")
        days_stale = None
        if last_scrape_at:
            try:
                d = datetime.fromisoformat(last_scrape_at.replace("Z", "+00:00"))
                if d.tzinfo is None:
                    d = d.replace(tzinfo=timezone.utc)
                days_stale = (datetime.now(timezone.utc) - d).days
            except Exception:
                pass
        # Status / health buckets (matches the supabase RPC's logic)
        if last_scrape_at is None:
            status = "never"
        elif days_stale == 0:
            status = "fresh"
        elif days_stale and days_stale < 7:
            status = "stale"
        else:
            status = "gone"
        if cp_count == 0:
            health = "dead"
        elif st["has_data"] and st["total"] and st["has_data"] / st["total"] >= 0.8:
            health = "ok"
        elif st["has_data"] and st["has_data"] / max(st["total"], 1) >= 0.3:
            health = "partial"
        else:
            health = "thin"
        rows.append({
            "code": ch["code"],
            "name_he": ch["name_he"],
            "name_en": ch["name_en"],
            "status": status,
            "health": health,
            "stores_total": st["total"] or 0,
            "stores_with_data": st["has_data"] or 0,
            "stores_missing": (st["total"] or 0) - (st["has_data"] or 0),
            "products_priced": prod_priced,
            "products_observed": prod_priced,  # local doesn't track unpriced observations
            "products_missing": 0,
            "current_prices": cp_count,
            "last_scrape_at": last_scrape_at,
            "days_stale": days_stale,
            "running_now": running_now,
            "_run": dict(last_run) if last_run else None,
        })
    c.close()
    return rows


@stats_router.get("/retailers-status")
def retailers_status():
    import os, time
    from datetime import datetime, timezone, timedelta
    global _retailers_cache, _retailers_cache_ts

    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        # Always serve from in-memory cache — never block a request on DB.
        # The background refresher (main.py startup) keeps the cache warm.
        if _retailers_cache:
            return _retailers_cache
        # Cold start: cache not yet populated — try once with a short timeout
        try:
            rows = _retailers_status_local()
            _retailers_cache = rows
            _retailers_cache_ts = time.monotonic()
        except Exception:
            return []  # graceful empty rather than 500
        running = {r["code"]: r["_run"] for r in rows if r.get("_run") and r.get("running_now")}
    else:
        from ..db.pg import cursor as _cursor
        with _cursor() as cur:
            cur.execute("SELECT * FROM retailers_status()")
            rows = [dict(r) for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT ON (sr.chain_id)
                    ch.code,
                    sr.files_ok, sr.files_failed, sr.files_total, sr.rows_written,
                    sr.started_at, sr.progress_at
                FROM scrape_runs sr
                JOIN chains ch ON ch.id = sr.chain_id
                WHERE sr.status = 'running'
                ORDER BY sr.chain_id, sr.started_at DESC
            """)
            running = {r["code"]: dict(r) for r in cur.fetchall()}

    now = datetime.now(timezone.utc)
    # next 04:15 UTC daily window
    target = now.replace(hour=4, minute=15, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    next_run_secs = int((target - now).total_seconds())

    for r in rows:
        code = r["code"]
        if r.get("running_now") and code in running:
            run = running[code]
            total = run["files_total"] or 0
            done = run["files_ok"] or 0
            rows_written = run["rows_written"] or 0
            pct = round(done / total * 100, 1) if total > 0 else 0
            ref = run["progress_at"] or run["started_at"]
            if isinstance(ref, str):
                try:
                    ref = datetime.fromisoformat(ref.replace("Z", "+00:00"))
                    if ref.tzinfo is None:
                        ref = ref.replace(tzinfo=timezone.utc)
                except Exception:
                    ref = None
            elapsed = (now - ref).total_seconds() if ref else 0
            rate = done / elapsed if elapsed > 0 and done > 0 else 0
            remaining = int((total - done) / rate) if rate > 0 and total > done else None
            r["progress"] = {
                "pct": pct,
                "files_done": done,
                "files_total": total,
                "rate_files_per_sec": round(rate, 3),
                "rows_written": rows_written,
            }
            r["eta_seconds"] = remaining
            r["next_run_eta_seconds"] = None
        else:
            r["progress"] = None
            r["eta_seconds"] = None
            r["next_run_eta_seconds"] = next_run_secs

    return rows


@stats_router.get("/chain-stores/{chain_code}")
def chain_stores(chain_code: str):
    import os, sqlite3
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        from ..db.local import LOCAL_DB
        c = sqlite3.connect(LOCAL_DB); c.row_factory = sqlite3.Row
        ch = c.execute("SELECT id, name_he, name_en, portal_url FROM chains WHERE code=?", (chain_code,)).fetchone()
        if not ch:
            c.close()
            return {"chain": None, "totals": {}, "stores": []}
        ch = dict(ch)
        rows = c.execute("""
            SELECT s.store_code, s.name, s.city, s.address,
                   COUNT(cp.product_id) AS prices,
                   MAX(cp.updated_at)   AS last_priced
            FROM stores s
            LEFT JOIN current_prices cp ON cp.store_id = s.id
            WHERE s.chain_id = ?
            GROUP BY s.id, s.store_code, s.name, s.city, s.address
            ORDER BY (CASE WHEN s.city IS NULL THEN 1 ELSE 0 END), s.city,
                     (CASE WHEN s.name IS NULL THEN 1 ELSE 0 END), s.name
        """, (ch["id"],)).fetchall()
        stores = [dict(r) for r in rows]
        c.close()
        totals = {
            "total":          len(stores),
            "with_prices":    sum(1 for s in stores if (s["prices"] or 0) > 0),
            "missing_prices": sum(1 for s in stores if (s["prices"] or 0) == 0),
            "with_city":      sum(1 for s in stores if s["city"]),
        }
        return {"chain": ch, "totals": totals, "stores": stores}

    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute(
            "SELECT id, name_he, name_en, portal_url FROM chains WHERE code=%s",
            (chain_code,),
        )
        ch = cur.fetchone()
        if not ch:
            return {"chain": None, "totals": {}, "stores": []}
        chain_id = ch["id"]
        cur.execute(
            """
            SELECT s.store_code, s.name, s.city, s.address,
                   COUNT(cp.product_id)  AS prices,
                   MAX(cp.updated_at)::TEXT AS last_priced
            FROM stores s
            LEFT JOIN current_prices cp ON cp.store_id = s.id
            WHERE s.chain_id = %s
            GROUP BY s.id, s.store_code, s.name, s.city, s.address
            ORDER BY s.city NULLS LAST, s.name NULLS LAST
            """,
            (chain_id,),
        )
        stores = [dict(r) for r in cur.fetchall()]
    totals = {
        "total":          len(stores),
        "with_prices":    sum(1 for s in stores if (s["prices"] or 0) > 0),
        "missing_prices": sum(1 for s in stores if (s["prices"] or 0) == 0),
        "with_city":      sum(1 for s in stores if s["city"]),
    }
    return {"chain": dict(ch), "totals": totals, "stores": stores}
