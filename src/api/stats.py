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
    return []


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
    return []


@stats_router.get("/retailers-status")
def retailers_status():
    res = supa.sb().rpc("retailers_status").execute()
    return res.data or []
