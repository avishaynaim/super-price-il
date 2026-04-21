"""super-price-il FastAPI.

Endpoints:
  GET  /api/chains                           — active chains
  GET  /api/stores?chain=&city=              — stores, optional filter
  GET  /api/search?q=&chain=&city=&limit=    — product name/barcode search (cheapest first)
  GET  /api/products/{barcode}               — product + latest prices by chain
  GET  /api/trends/{barcode}?days=           — historical price series
  GET  /api/compare/{barcode}?city=          — chain-by-chain comparison table
  POST /api/nl-filter                        — Claude NL → structured filter (see nl.py)
  GET  /api/health                           — DB stats

No auth; this is a local research tool.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ..db.connection import connect
from .nl import nl_router
from .receipts import receipts_router

app = FastAPI(
    title="super-price-il",
    version="0.1",
    description="Israeli supermarket price intelligence",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nl_router, prefix="/api")
app.include_router(receipts_router, prefix="/api")


@contextmanager
def db() -> Iterator:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


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
    has_promo: int = 0


class TrendPoint(BaseModel):
    chain_code: str
    chain_name_he: str
    store_id: int
    fetched_at: str
    price: float


# ---------- endpoints ----------

@app.get("/api/health")
def health():
    with db() as c:
        stats = {
            "chains_active":      c.execute("SELECT COUNT(*) FROM chains WHERE active=1").fetchone()[0],
            "stores":             c.execute("SELECT COUNT(*) FROM stores").fetchone()[0],
            "products":           c.execute("SELECT COUNT(*) FROM products").fetchone()[0],
            "price_observations": c.execute("SELECT COUNT(*) FROM price_observations").fetchone()[0],
            "current_prices":     c.execute("SELECT COUNT(*) FROM current_prices").fetchone()[0],
        }
    return {"status": "ok", **stats}


@app.get("/api/chains", response_model=list[ChainOut])
def chains():
    with db() as c:
        rows = c.execute(
            "SELECT code, name_he, name_en, portal_url, active FROM chains ORDER BY name_he"
        ).fetchall()
    return [
        ChainOut(code=r["code"], name_he=r["name_he"], name_en=r["name_en"],
                 portal_url=r["portal_url"], active=bool(r["active"]))
        for r in rows
    ]


@app.get("/api/stores", response_model=list[StoreOut])
def stores(
    chain: str | None = Query(None, description="chain code"),
    city: str | None = Query(None),
    limit: int = Query(500, le=5000),
):
    sql = (
        "SELECT s.id, ch.code AS chain_code, ch.name_he AS chain_name_he, "
        "       s.store_code, s.name, s.city, s.address "
        "FROM stores s JOIN chains ch ON ch.id = s.chain_id "
        "WHERE 1=1"
    )
    params: list = []
    if chain:
        sql += " AND ch.code = ?"
        params.append(chain)
    if city:
        sql += " AND s.city LIKE ?"
        params.append(f"%{city}%")
    sql += " ORDER BY ch.name_he, s.city, s.name LIMIT ?"
    params.append(limit)
    with db() as c:
        rows = c.execute(sql, params).fetchall()
    return [StoreOut(**dict(r)) for r in rows]


@app.get("/api/search", response_model=list[SearchHit])
def search(
    q: str = Query(..., min_length=1),
    chain: str | None = None,
    city: str | None = None,
    limit: int = Query(50, le=500),
):
    """Search by product name (fuzzy) or exact barcode.
    Returns distinct products ranked by chains-with-price, showing min/max price."""
    is_bc = q.isdigit() and len(q) >= 6
    where = "p.barcode = ?" if is_bc else "p.name LIKE ?"
    params: list = [q if is_bc else f"%{q}%"]
    sql = f"""
        SELECT p.barcode, p.name, p.manufacturer,
               MIN(cp.price) AS min_price,
               MAX(cp.price) AS max_price,
               COUNT(DISTINCT ch.id) AS chains_with_price,
               EXISTS(SELECT 1 FROM promotion_items pi WHERE pi.product_id = p.id) AS has_promo
          FROM products p
          JOIN current_prices cp ON cp.product_id = p.id
          JOIN stores s          ON s.id = cp.store_id
          JOIN chains ch         ON ch.id = s.chain_id
         WHERE {where}
    """
    if chain:
        sql += " AND ch.code = ?"
        params.append(chain)
    if city:
        sql += " AND s.city LIKE ?"
        params.append(f"%{city}%")
    sql += " GROUP BY p.id ORDER BY chains_with_price DESC, min_price ASC LIMIT ?"
    params.append(limit)
    with db() as c:
        rows = c.execute(sql, params).fetchall()
    return [SearchHit(**dict(r)) for r in rows]


@app.get("/api/products/{barcode}", response_model=ProductOut)
def product(barcode: str):
    with db() as c:
        prod = c.execute(
            "SELECT id, barcode, name, manufacturer, unit_qty, unit_type "
            "FROM products WHERE barcode = ?",
            (barcode,),
        ).fetchone()
        if not prod:
            raise HTTPException(404, "product not found")
        prices = c.execute(
            """
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   s.id AS store_id, s.name AS store_name, s.city AS store_city,
                   cp.price, cp.updated_at
              FROM current_prices cp
              JOIN stores s  ON s.id = cp.store_id
              JOIN chains ch ON ch.id = s.chain_id
             WHERE cp.product_id = ?
             ORDER BY cp.price ASC
            """,
            (prod["id"],),
        ).fetchall()
    return ProductOut(
        barcode=prod["barcode"], name=prod["name"], manufacturer=prod["manufacturer"],
        unit_qty=prod["unit_qty"], unit_type=prod["unit_type"],
        prices=[PriceRow(**dict(r)) for r in prices],
    )


@app.get("/api/trends/{barcode}", response_model=list[TrendPoint])
def trends(barcode: str, days: int = Query(7, ge=1, le=90)):
    with db() as c:
        rows = c.execute(
            """
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   s.id AS store_id, po.fetched_at, po.price
              FROM price_observations po
              JOIN products p ON p.id = po.product_id
              JOIN stores s   ON s.id = po.store_id
              JOIN chains ch  ON ch.id = s.chain_id
             WHERE p.barcode = ?
               AND po.fetched_at >= datetime('now', ?)
             ORDER BY po.fetched_at ASC
            """,
            (barcode, f"-{days} days"),
        ).fetchall()
    return [TrendPoint(**dict(r)) for r in rows]


@app.get("/api/promotions/{barcode}")
def promotions(barcode: str, chain: str | None = None):
    """Active promotions touching a given barcode (chain-filterable)."""
    sql = """
        SELECT pr.id, ch.code AS chain_code, ch.name_he AS chain_name_he,
               pr.promo_code, pr.description,
               pr.starts_at, pr.ends_at,
               pr.reward_type, pr.min_qty,
               pr.discount_price, pr.discount_rate,
               pr.fetched_at
          FROM promotion_items pi
          JOIN promotions pr ON pr.id = pi.promotion_id
          JOIN products  p  ON p.id  = pi.product_id
          JOIN chains    ch ON ch.id = pr.chain_id
         WHERE p.barcode = ?
    """
    params: list = [barcode]
    if chain:
        sql += " AND ch.code = ?"
        params.append(chain)
    sql += " ORDER BY pr.starts_at DESC"
    with db() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/compare/{barcode}")
def compare(barcode: str, city: str | None = None):
    """Min price per chain — the headline 'which chain is cheapest for X' view."""
    sql = """
        SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
               MIN(cp.price) AS min_price,
               COUNT(DISTINCT s.id) AS stores_with
          FROM current_prices cp
          JOIN products p ON p.id = cp.product_id
          JOIN stores s   ON s.id = cp.store_id
          JOIN chains ch  ON ch.id = s.chain_id
         WHERE p.barcode = ?
    """
    params: list = [barcode]
    if city:
        sql += " AND s.city LIKE ?"
        params.append(f"%{city}%")
    sql += " GROUP BY ch.id ORDER BY min_price ASC"
    with db() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# serve web UI: prefer web/dist (when someone later adds a build step),
# fall back to web/ (vanilla static HTML/JS/CSS).
import os
_WEB_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "web")
_DIST = os.path.join(_WEB_ROOT, "dist")
_STATIC = _DIST if os.path.isdir(_DIST) and os.listdir(_DIST) else _WEB_ROOT
if os.path.isdir(_STATIC) and os.path.isfile(os.path.join(_STATIC, "index.html")):
    app.mount("/", StaticFiles(directory=_STATIC, html=True), name="web")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)
