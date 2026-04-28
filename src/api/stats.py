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
    res = supa.sb().rpc("chain_coverage_stats").execute()
    return res.data or []


@stats_router.get("/scrape-runs")
def scrape_runs(limit: int = Query(50, le=200)):
    res = (
        supa.sb()
        .table("scrape_runs")
        .select("*,chains!chain_id(code,name_he)")
        .order("started_at", desc=True)
        .limit(limit)
        .execute()
    )
    out = []
    for r in (res.data or []):
        ch = r.pop("chains", {}) or {}
        out.append({**r, "chain_code": ch.get("code"), "chain_name_he": ch.get("name_he")})
    return out


@stats_router.get("/top-spread")
def top_spread(
    city: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float | None = Query(None, ge=0, le=500),
    chains: str | None = None,
    limit: int = Query(20, le=100),
):
    city_spellings = compute_city_spellings(city, lat, lng, radius_km)
    chain_codes: list[str] | None = None
    if chains:
        chain_codes = [c.strip() for c in chains.split(",") if c.strip()] or None
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
    res = supa.sb().table("stores").select("city").not_.is_("city", "null").execute()
    counts: dict[str, int] = {}
    for r in (res.data or []):
        raw = (r.get("city") or "").strip()
        if raw:
            counts[raw] = counts.get(raw, 0) + 1
    return [{"city": k, "stores": v} for k, v in sorted(counts.items(), key=lambda x: -x[1])]


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


@stats_router.get("/retailers-status")
def retailers_status():
    res = supa.sb().rpc("retailers_status").execute()
    return res.data or []


@stats_router.get("/chain-stores/{chain_code}")
def chain_stores(chain_code: str):
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
