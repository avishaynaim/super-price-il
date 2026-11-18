"""super-price-il FastAPI — Supabase backend.

Endpoints:
  GET  /api/health
  GET  /api/chains
  GET  /api/stores?chain=&city=
  GET  /api/search?q=&chain=&city=&limit=
  GET  /api/products/{barcode}
  GET  /api/compare/{barcode}
  POST /api/nl-filter
"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ..db import supa
from .geo import geo_router, compute_city_spellings, city_aliases_for_filter
from .live_ocr import live_ocr_router
from .nl import nl_router
from .receipts import receipts_router
from .stats import stats_router

log = logging.getLogger(__name__)

APP_VERSION = "0.4"

# Supabase is not available in local/offline mode — always use local SQLite.
os.environ.setdefault("SUPER_PRICE_LOCAL_ONLY", "1")

app = FastAPI(
    title="super-price-il",
    version=APP_VERSION,
    description="Israeli supermarket price intelligence — Supabase backend",
)

app.add_middleware(
    CORSMiddleware,
    # The bundled UI is served same-origin; only explicitly listed origins may
    # call the API cross-origin. Override with CORS_ALLOW_ORIGINS (comma-separated).
    allow_origins=[
        o.strip()
        for o in os.environ.get(
            "CORS_ALLOW_ORIGINS",
            "http://localhost:8000,http://127.0.0.1:8000",
        ).split(",")
        if o.strip()
    ],
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# Force browsers to always fetch fresh JS/CSS (no stale cache served to clients).
from starlette.types import ASGIApp, Receive, Scope, Send

class NoCacheStaticMiddleware:
    """Add Cache-Control: no-store to static assets and the root page.
    Uses raw ASGI (not BaseHTTPMiddleware) so WebSocket connections are
    passed through untouched — BaseHTTPMiddleware breaks WS upgrades.
    """
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path = scope.get("path", "")
            if path == "/" or path.endswith((".js", ".css", ".html", ".svg")):
                async def send_with_header(message):
                    if message["type"] == "http.response.start":
                        headers = dict(message.get("headers", []))
                        headers[b"cache-control"] = b"no-store"
                        message = {**message, "headers": list(headers.items())}
                    await send(message)
                await self.app(scope, receive, send_with_header)
                return
        # WebSocket and non-static HTTP: pass through unchanged
        await self.app(scope, receive, send)

app.add_middleware(NoCacheStaticMiddleware)


@app.on_event("startup")
async def _start_price_index():
    """Kick off background preload of the in-memory barcode→price index.
    Server starts immediately; first lookups use live SQL (52ms), subsequent
    ones hit the RAM cache (<1ms). Full preload completes in ~30s."""
    from ..db.price_index import price_index
    price_index.preload_background()


@app.on_event("startup")
async def _start_ocr_daemon():
    """Start the persistent Claude OCR daemon.
    Amortizes the 46s CLI startup: subsequent requests take ~15-20s instead of 60s."""
    from ..receipts.claude_ocr_daemon import ocr_daemon
    ocr_daemon.start()


@app.on_event("startup")
async def _start_cache_refresher():
    """Background thread that refreshes slow caches every 10s.
    Requests NEVER block on DB — they always read from the in-memory cache."""
    import threading, time as _time
    import src.api.stats as _stats

    def _refresh_loop():
        while True:
            # health cache
            try:
                result = _local_health()
                # _local_health already updates its own globals
            except Exception:
                log.exception("cache-refresher: health refresh failed")
            # retailers-status cache
            try:
                rows = _stats._retailers_status_local()
                _stats._retailers_cache = rows
                _stats._retailers_cache_ts = _time.monotonic()
            except Exception:
                log.exception("cache-refresher: retailers-status refresh failed")
            _time.sleep(10)

    t = threading.Thread(target=_refresh_loop, daemon=True, name="cache-refresher")
    t.start()

app.include_router(nl_router, prefix="/api")
app.include_router(receipts_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
app.include_router(geo_router, prefix="/api")
app.include_router(live_ocr_router, prefix="/api")


@app.get("/api/lookup/{barcode}")
async def lookup_barcode(barcode: str) -> dict:
    """Instant barcode price lookup — uses in-memory cache, <100ms.
    Used by the live ZXing scanner so no WebSocket or OCR is needed.
    """
    from ..db.price_index import price_index
    try:
        info = price_index.lookup(barcode, raise_errors=True)
    except Exception:
        raise HTTPException(503, "price lookup temporarily unavailable")
    if not info:
        raise HTTPException(404, f"barcode {barcode!r} not found")
    cheapest = info.get("cheapest")
    priciest = info.get("priciest")
    return {
        "barcode":        barcode,
        "name":           info.get("name"),
        "manufacturer":   info.get("manufacturer"),
        "min_price":      cheapest["min_price"] if cheapest else None,
        "max_price":      priciest["max_price"] if priciest else None,
        "cheapest_chain": cheapest["chain_name_he"] if cheapest else None,
        "priciest_chain": priciest["chain_name_he"] if priciest else None,
        "chains":         info.get("chains", [])[:10],  # top 10 by price
    }


# ---------- models ----------

class ChainOut(BaseModel):
    code: str
    name_he: str
    name_en: str | None
    portal_url: str
    active: bool


class StoreOut(BaseModel):
    id: int
    chain_code: str
    chain_name_he: str
    store_code: str
    name: str | None
    city: str | None
    address: str | None


class PriceRow(BaseModel):
    chain_code: str
    chain_name_he: str
    store_id: int
    store_name: str | None
    store_city: str | None
    price: float
    updated_at: str


class ProductOut(BaseModel):
    barcode: str
    name: str | None
    manufacturer: str | None
    unit_qty: float | None
    unit_type: str | None
    prices: list[PriceRow]


class SearchHit(BaseModel):
    barcode: str
    name: str | None
    manufacturer: str | None
    min_price: float
    max_price: float
    chains_with_price: int


# ---------- endpoints ----------

_health_cache: dict = {}
_health_cache_ts: float = 0.0
_HEALTH_TTL = 30.0  # seconds


def _local_health():
    """Local SQLite health — cached for 30s so heavy scraping doesn't block the UI."""
    import sqlite3, shutil, time
    from pathlib import Path
    from src.db.local import LOCAL_DB

    global _health_cache, _health_cache_ts
    now = time.monotonic()
    if _health_cache and now - _health_cache_ts < _HEALTH_TTL:
        return _health_cache

    if not LOCAL_DB.exists():
        return {"status": "local-empty", "chains_active": 0, "stores": 0, "products": 0, "current_prices": 0, "version": APP_VERSION}

    try:
        c = sqlite3.connect(LOCAL_DB, timeout=3)
        c.execute("PRAGMA busy_timeout=3000")
        n = lambda q: c.execute(q).fetchone()[0]

        db_bytes = LOCAL_DB.stat().st_size
        raw_root = Path(__file__).resolve().parents[2] / "data" / "raw"
        raw_bytes = sum(
            f.stat().st_size for f in raw_root.rglob("*") if f.is_file()
        ) if raw_root.exists() else 0
        try:
            free_bytes = shutil.disk_usage(LOCAL_DB.parent).free
        except Exception:
            free_bytes = None

        # Use quick_stats cache table if available (avoids full COUNT on 10M rows)
        try:
            row = c.execute("SELECT * FROM quick_stats LIMIT 1").fetchone()
            cols = [d[0] for d in c.description]
            qs = dict(zip(cols, row)) if row else {}
        except Exception:
            qs = {}

        out = {
            "status": "local-only",
            "version": APP_VERSION,
            "chains_active": qs.get("chains_active") or n("SELECT count(*) FROM chains WHERE active=1"),
            "stores":        qs.get("stores")        or n("SELECT count(*) FROM stores"),
            "products":      qs.get("products")      or n("SELECT count(*) FROM products"),
            "current_prices":qs.get("current_prices")or n("SELECT count(*) FROM current_prices"),
            "db_bytes": db_bytes,
            "raw_bytes": raw_bytes,
            "free_bytes": free_bytes,
        }
        c.close()
        _health_cache = out
        _health_cache_ts = now
        return out
    except Exception:
        log.exception("local health check failed")
        # DB busy/failing — return last cached value (marked degraded) or minimal response
        if _health_cache:
            return {**_health_cache, "status": "degraded"}
        return {"status": "degraded", "version": APP_VERSION,
                "chains_active": 0, "stores": 0, "products": 0, "current_prices": 0}


@app.get("/api/health")
def health():
    # In local-only mode, always use the local helper (it includes disk stats).
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        return _local_health()
    try:
        sb = supa.sb()
        chains_n  = (sb.table("chains").select("id", count="exact").eq("active", True).execute()).count or 0
        stores_n  = (sb.table("stores").select("id", count="exact").execute()).count or 0
        products_n = (sb.table("products").select("id", count="exact").execute()).count or 0
        prices_n  = (sb.table("current_prices").select("store_id", count="exact").execute()).count or 0
        return {
            "status": "ok",
            "version": APP_VERSION,
            "chains_active": chains_n,
            "stores": stores_n,
            "products": products_n,
            "current_prices": prices_n,
        }
    except Exception:
        log.exception("Supabase health check failed; falling back to local")
        return _local_health()


@app.get("/api/chains", response_model=list[ChainOut])
def chains():
    try:
        res = supa.sb().table("chains").select("code,name_he,name_en,portal_url,active").order("name_he").execute()
        return [ChainOut(**r) for r in (res.data or [])]
    except Exception:
        import sqlite3
        from src.db.local import LOCAL_DB
        c = sqlite3.connect(LOCAL_DB)
        c.row_factory = sqlite3.Row
        rows = c.execute("SELECT code,name_he,name_en,portal_url,active FROM chains ORDER BY name_he").fetchall()
        c.close()
        return [ChainOut(code=r["code"], name_he=r["name_he"], name_en=r["name_en"],
                         portal_url=r["portal_url"], active=bool(r["active"])) for r in rows]


@app.get("/api/stores", response_model=list[StoreOut])
def stores(
    chain: str | None = Query(None),
    chains: str | None = Query(None, description="csv of chain codes"),
    city: str | None = Query(None),
    lat: float | None = Query(None),
    lng: float | None = Query(None),
    radius_km: float | None = Query(None, ge=0, le=500),
    limit: int = Query(500, le=5000),
):
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        import sqlite3
        from src.db.local import LOCAL_DB

        l_where: list[str] = []
        l_args: list = []
        if chain:
            l_where.append("ch.code = ?")
            l_args.append(chain)
        elif chains:
            l_codes = [c.strip() for c in chains.split(",") if c.strip()]
            if l_codes:
                l_where.append(f"ch.code IN ({','.join(['?'] * len(l_codes))})")
                l_args += l_codes
        l_cities = compute_city_spellings(city, lat, lng, radius_km)
        if l_cities is not None:
            if not l_cities:
                return []
            l_where.append(f"s.city IN ({','.join(['?'] * len(l_cities))})")
            l_args += l_cities
        l_sql = f"""
            SELECT s.id, s.store_code, s.name, s.city, s.address,
                   ch.code AS chain_code, ch.name_he AS chain_name_he
            FROM stores s
            JOIN chains ch ON ch.id = s.chain_id
            {("WHERE " + " AND ".join(l_where)) if l_where else ""}
            ORDER BY s.city
            LIMIT ?
        """
        l_args.append(limit)
        conn = sqlite3.connect(LOCAL_DB)
        conn.row_factory = sqlite3.Row
        try:
            l_rows = [dict(r) for r in conn.execute(l_sql, l_args).fetchall()]
        finally:
            conn.close()
        return [StoreOut(
            id=r["id"],
            chain_code=r["chain_code"],
            chain_name_he=r["chain_name_he"],
            store_code=r["store_code"],
            name=r.get("name"),
            city=r.get("city"),
            address=r.get("address"),
        ) for r in l_rows]

    from ..db.pg import cursor as _cursor

    where_parts: list[str] = []
    params: list = []

    if chain:
        where_parts.append("ch.code = %s")
        params.append(chain)
    elif chains:
        codes = [c.strip() for c in chains.split(",") if c.strip()]
        if codes:
            where_parts.append("ch.code = ANY(%s)")
            params.append(codes)

    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if city_spellings is not None:
        if not city_spellings:
            return []
        where_parts.append("s.city = ANY(%s)")
        params.append(city_spellings)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    params.append(limit)

    sql = f"""
        SELECT s.id, s.store_code, s.name, s.city, s.address,
               ch.code AS chain_code, ch.name_he AS chain_name_he
        FROM stores s
        JOIN chains ch ON ch.id = s.chain_id
        {where_sql}
        ORDER BY s.city
        LIMIT %s
    """
    with _cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [StoreOut(
        id=r["id"],
        chain_code=r["chain_code"],
        chain_name_he=r["chain_name_he"],
        store_code=r["store_code"],
        name=r.get("name"),
        city=r.get("city"),
        address=r.get("address"),
    ) for r in rows]


def _local_filters(where: list, args: list, chain_codes, city_spellings) -> None:
    """Append the shared chain/city IN-clauses (SQLite) to where/args."""
    if chain_codes:
        where.append(f"ch.code IN ({','.join(['?'] * len(chain_codes))})")
        args += chain_codes
    if city_spellings:
        where.append(f"s.city IN ({','.join(['?'] * len(city_spellings))})")
        args += city_spellings


def _local_search(q: str, chain_codes, city_spellings, limit):
    from src.db.local import cursor as _local_cursor
    where = ["(p.name LIKE ? OR p.barcode = ?)"]
    args  = [f"%{q}%", q]
    _local_filters(where, args, chain_codes, city_spellings)
    sql = f"""
      SELECT p.barcode, p.name, p.manufacturer,
             min(cp.price) AS min_price, max(cp.price) AS max_price,
             count(DISTINCT ch.id) AS chains_with_price
      FROM products p
      JOIN current_prices cp ON cp.product_id=p.id
      JOIN stores s ON s.id=cp.store_id
      JOIN chains ch ON ch.id=s.chain_id
      WHERE {' AND '.join(where)}
      GROUP BY p.id
      ORDER BY count(*) DESC
      LIMIT ?"""
    args.append(limit)
    with _local_cursor() as c:
        return [dict(r) for r in c.execute(sql, args).fetchall()]


@app.get("/api/search", response_model=list[SearchHit])
def search(
    q: str = Query(..., min_length=1),
    chain: str | None = None,
    chains: str | None = Query(None),
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    limit: int = Query(50, le=500),
):
    chain_codes = _resolve_chain_codes(chain, chains)
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        return [SearchHit(**r) for r in _local_search(q, chain_codes, city_spellings, limit)]
    res = supa.sb().rpc("search_products", {
        "q": q,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
        "limit_n": limit,
    }).execute()
    return [SearchHit(**r) for r in (res.data or [])]


def _local_product_prices(barcode, chain_codes, city_spellings):
    from src.db.local import cursor as _local_cursor
    where = ["p.barcode = ?"]; args = [barcode]
    _local_filters(where, args, chain_codes, city_spellings)
    sql = f"""
      SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
             s.id AS store_id, s.name AS store_name, s.city AS store_city,
             cp.price, cp.updated_at
      FROM products p
      JOIN current_prices cp ON cp.product_id=p.id
      JOIN stores s ON s.id=cp.store_id
      JOIN chains ch ON ch.id=s.chain_id
      WHERE {' AND '.join(where)}
      ORDER BY cp.price ASC"""
    with _local_cursor() as c:
        return [dict(r) for r in c.execute(sql, args).fetchall()]


@app.get("/api/products/{barcode}", response_model=ProductOut)
def product(
    barcode: str,
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
):
    chain_codes = _resolve_chain_codes(None, chains)
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        from src.db.local import cursor as _local_cursor
        with _local_cursor() as c:
            prod = c.execute("SELECT barcode,name,manufacturer,unit_qty,unit_type FROM products WHERE barcode=?", (barcode,)).fetchone()
        if not prod: raise HTTPException(404, "product not found")
        prices = _local_product_prices(barcode, chain_codes, city_spellings)
        return ProductOut(
            barcode=prod["barcode"], name=prod["name"], manufacturer=prod["manufacturer"],
            unit_qty=prod["unit_qty"], unit_type=prod["unit_type"],
            prices=[PriceRow(**r) for r in prices],
        )
    prod_res = supa.sb().table("products").select("barcode,name,manufacturer,unit_qty,unit_type").eq("barcode", barcode).maybe_single().execute()
    if not prod_res.data:
        raise HTTPException(404, "product not found")
    prod = prod_res.data
    price_res = supa.sb().rpc("get_product_prices", {
        "p_barcode": barcode,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
    }).execute()
    return ProductOut(
        barcode=prod["barcode"], name=prod.get("name"), manufacturer=prod.get("manufacturer"),
        unit_qty=prod.get("unit_qty"), unit_type=prod.get("unit_type"),
        prices=[PriceRow(**r) for r in (price_res.data or [])],
    )


@app.get("/api/compare/{barcode}")
def compare(
    barcode: str,
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
):
    chain_codes = _resolve_chain_codes(None, chains)
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        from src.db.local import cursor as _local_cursor
        where = ["p.barcode = ?"]; args = [barcode]
        _local_filters(where, args, chain_codes, city_spellings)
        sql = f"""
          SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                 min(cp.price) AS min_price, max(cp.price) AS max_price,
                 avg(cp.price) AS avg_price, count(*) AS store_count
          FROM products p
          JOIN current_prices cp ON cp.product_id=p.id
          JOIN stores s ON s.id=cp.store_id
          JOIN chains ch ON ch.id=s.chain_id
          WHERE {' AND '.join(where)}
          GROUP BY ch.id
          ORDER BY min_price ASC"""
        with _local_cursor() as c:
            return [dict(r) for r in c.execute(sql, args).fetchall()]
    res = supa.sb().rpc("compare_product", {
        "p_barcode": barcode,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
    }).execute()
    return res.data or []


@app.get("/api/trends/{barcode}")
def trends(barcode: str, days: int = Query(7, ge=1, le=90)):
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        import sqlite3
        from src.db.local import LOCAL_DB
        c = sqlite3.connect(LOCAL_DB); c.row_factory = sqlite3.Row
        rows = c.execute("""
            SELECT po.price, po.fetched_at AS observed_at,
                   ch.code AS chain_code, ch.name_he AS chain_name_he
            FROM price_observations po
            JOIN products p ON p.id = po.product_id
            JOIN stores s ON s.id = po.store_id
            JOIN chains ch ON ch.id = s.chain_id
            WHERE p.barcode = ?
              AND po.fetched_at >= datetime('now', ? || ' days')
            ORDER BY po.fetched_at ASC
        """, (barcode, f"-{days}")).fetchall()
        c.close()
        return [dict(r) for r in rows]
    from ..db.pg import cursor as _cursor
    with _cursor() as cur:
        cur.execute("""
            SELECT po.price, po.observed_at::TEXT AS observed_at,
                   ch.code AS chain_code, ch.name_he AS chain_name_he
            FROM price_observations po
            JOIN products p ON p.id = po.product_id
            JOIN stores s ON s.id = po.store_id
            JOIN chains ch ON ch.id = s.chain_id
            WHERE p.barcode = %s
              AND po.observed_at >= NOW() - (%s || ' days')::interval
            ORDER BY po.observed_at ASC
        """, (barcode, str(days)))
        return [dict(r) for r in cur.fetchall()]


@app.get("/api/promotions/{barcode}")
def promotions(barcode: str, chain: str | None = None, chains: str | None = None,
               city: str | None = None, lat: float | None = None,
               lng: float | None = None, radius_km: float | None = Query(None)):
    from .geo import compute_city_spellings
    chain_codes = _resolve_chain_codes(chain, chains)
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if os.environ.get("SUPER_PRICE_LOCAL_ONLY", "").strip() == "1":
        # Local DB doesn't track promotions yet — return empty list.
        return []
    from ..db.pg import cursor as _cursor
    chain_ids: list[int] | None = None
    if chain_codes:
        with _cursor() as cur:
            cur.execute("SELECT id FROM chains WHERE code = ANY(%s)", (chain_codes,))
            chain_ids = [r["id"] for r in cur.fetchall()]
    return supa.get_promotions_for_barcode(barcode, chain_ids, city_spellings)


# ---------- helpers ----------

def _resolve_chain_codes(chain: str | None, chains: str | None) -> list[str] | None:
    if chain:
        return [chain]
    if chains:
        codes = [c.strip() for c in chains.split(",") if c.strip()]
        return codes if codes else None
    return None


# ---------- settings (stub — always 1-day) ----------

from pydantic import BaseModel as _BM  # noqa: E402

class AppSettings(_BM):
    retention_days: int


@app.get("/api/app-settings", response_model=AppSettings)
def get_app_settings():
    return AppSettings(retention_days=1)


@app.put("/api/app-settings", response_model=AppSettings)
def update_app_settings(settings: AppSettings):
    return AppSettings(retention_days=1)


# ---------- static web UI ----------
_WEB_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "web")
_DIST = os.path.join(_WEB_ROOT, "dist")
_STATIC = _DIST if os.path.isdir(_DIST) and os.listdir(_DIST) else _WEB_ROOT
if os.path.isdir(_STATIC) and os.path.isfile(os.path.join(_STATIC, "index.html")):
    from starlette.staticfiles import StaticFiles as _SF
    from starlette.types import ASGIApp, Receive, Scope, Send

    class _WSafeStaticFiles(_SF):
        """StaticFiles that ignores WebSocket connections (lets them pass through)."""
        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] == "websocket":
                return
            await super().__call__(scope, receive, send)

    app.mount("/", _WSafeStaticFiles(directory=_STATIC, html=True), name="web")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)
