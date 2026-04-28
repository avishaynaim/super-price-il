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

app = FastAPI(
    title="super-price-il",
    version="0.2",
    description="Israeli supermarket price intelligence — Supabase backend",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nl_router, prefix="/api")
app.include_router(receipts_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
app.include_router(geo_router, prefix="/api")
app.include_router(live_ocr_router, prefix="/api")


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

@app.get("/api/health")
def health():
    sb = supa.sb()
    chains_n  = (sb.table("chains").select("id", count="exact").eq("active", True).execute()).count or 0
    stores_n  = (sb.table("stores").select("id", count="exact").execute()).count or 0
    products_n = (sb.table("products").select("id", count="exact").execute()).count or 0
    prices_n  = (sb.table("current_prices").select("store_id", count="exact").execute()).count or 0
    return {
        "status": "ok",
        "chains_active": chains_n,
        "stores": stores_n,
        "products": products_n,
        "current_prices": prices_n,
    }


@app.get("/api/chains", response_model=list[ChainOut])
def chains():
    res = supa.sb().table("chains").select("code,name_he,name_en,portal_url,active").order("name_he").execute()
    return [ChainOut(**r) for r in (res.data or [])]


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
    q = supa.sb().table("stores").select("id,store_code,name,city,address,chains!chain_id(code,name_he)")
    if chain:
        # filter via subquery — fetch chain id first
        chain_res = supa.sb().table("chains").select("id").eq("code", chain).single().execute()
        if chain_res.data:
            q = q.eq("chain_id", chain_res.data["id"])
    elif chains:
        codes = [c.strip() for c in chains.split(",") if c.strip()]
        if codes:
            chain_res = supa.sb().table("chains").select("id").in_("code", codes).execute()
            ids = [r["id"] for r in (chain_res.data or [])]
            if ids:
                q = q.in_("chain_id", ids)

    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    if city_spellings is not None:
        if not city_spellings:
            return []
        q = q.in_("city", city_spellings)

    res = q.order("city").limit(limit).execute()
    out = []
    for r in (res.data or []):
        ch = r.get("chains") or {}
        out.append(StoreOut(
            id=r["id"],
            chain_code=ch.get("code", ""),
            chain_name_he=ch.get("name_he", ""),
            store_code=r["store_code"],
            name=r.get("name"),
            city=r.get("city"),
            address=r.get("address"),
        ))
    return out


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
    res = supa.sb().rpc("search_products", {
        "q": q,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
        "limit_n": limit,
    }).execute()
    return [SearchHit(**r) for r in (res.data or [])]


@app.get("/api/products/{barcode}", response_model=ProductOut)
def product(
    barcode: str,
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
):
    prod_res = supa.sb().table("products").select("barcode,name,manufacturer,unit_qty,unit_type").eq("barcode", barcode).maybe_single().execute()
    if not prod_res.data:
        raise HTTPException(404, "product not found")
    prod = prod_res.data

    chain_codes = _resolve_chain_codes(None, chains)
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)

    price_res = supa.sb().rpc("get_product_prices", {
        "p_barcode": barcode,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
    }).execute()

    return ProductOut(
        barcode=prod["barcode"],
        name=prod.get("name"),
        manufacturer=prod.get("manufacturer"),
        unit_qty=prod.get("unit_qty"),
        unit_type=prod.get("unit_type"),
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
    res = supa.sb().rpc("compare_product", {
        "p_barcode": barcode,
        "chain_codes": chain_codes,
        "city_spellings": city_spellings,
    }).execute()
    return res.data or []


@app.get("/api/trends/{barcode}")
def trends(barcode: str, days: int = Query(7, ge=1, le=90)):
    # No price_observations with 1-day retention — return empty.
    return []


@app.get("/api/promotions/{barcode}")
def promotions(barcode: str, chain: str | None = None, chains: str | None = None,
               city: str | None = None, lat: float | None = None,
               lng: float | None = None, radius_km: float | None = Query(None)):
    from .geo import compute_city_spellings
    from ..db.pg import cursor as _cursor
    chain_codes = _resolve_chain_codes(chain, chains)
    chain_ids: list[int] | None = None
    if chain_codes:
        with _cursor() as cur:
            cur.execute("SELECT id FROM chains WHERE code = ANY(%s)", (chain_codes,))
            chain_ids = [r["id"] for r in cur.fetchall()]
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
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
    app.mount("/", StaticFiles(directory=_STATIC, html=True), name="web")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)
