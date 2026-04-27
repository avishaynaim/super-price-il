"""Geography endpoints + helpers.

Israeli cities are loaded from data/il_cities.json. The stores table has no
lat/lng, so "radius" filtering works city-to-city: we find all cities whose
centroid falls within N km of the reference point, then constrain stores to
that set.

  GET /api/cities          — distinct cities in stores, merged with coord data
  GET /api/nearest-city    — reverse-geocode lat/lng → nearest city (for 'use my location')
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from math import asin, cos, radians, sin, sqrt
from typing import Iterable

from fastapi import APIRouter, HTTPException, Query

from ..db import supa

geo_router = APIRouter(tags=["geo"])

_DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "il_cities.json")


@lru_cache(maxsize=1)
def _load_cities() -> list[dict]:
    with open(_DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _alias_to_canonical() -> dict[str, str]:
    """Map every known spelling (canonical + aliases) → canonical name."""
    m: dict[str, str] = {}
    for c in _load_cities():
        name = c["name_he"]
        m[name] = name
        for a in c.get("aliases", []):
            m[a] = name
    return m


def canonicalize(city: str | None) -> str | None:
    """Normalize a DB city string to its canonical name if known; else return as-is."""
    if not city:
        return None
    s = city.strip()
    if not s:
        return None
    return _alias_to_canonical().get(s, s)


@lru_cache(maxsize=1)
def _coords_by_canonical() -> dict[str, tuple[float, float]]:
    return {c["name_he"]: (float(c["lat"]), float(c["lng"])) for c in _load_cities()}


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    return 2 * r * asin(sqrt(a))


def cities_within_radius(lat: float, lng: float, radius_km: float) -> list[str]:
    """Canonical city names whose centroid is within radius_km of (lat,lng)."""
    out = []
    for c in _load_cities():
        d = haversine_km(lat, lng, c["lat"], c["lng"])
        if d <= radius_km:
            out.append(c["name_he"])
    return out


def city_aliases(canonical: str) -> list[str]:
    """All spellings (canonical + aliases) for SQL IN clause matching against stores.city."""
    for c in _load_cities():
        if c["name_he"] == canonical:
            return [c["name_he"], *c.get("aliases", [])]
    return [canonical]


def expand_cities_for_sql(canonical_names: Iterable[str]) -> list[str]:
    """Given canonical city names, return every spelling that should match a DB row."""
    seen: set[str] = set()
    out: list[str] = []
    for n in canonical_names:
        for spelling in city_aliases(n):
            if spelling not in seen:
                seen.add(spelling)
                out.append(spelling)
    return out


def norm_city(s: str | None) -> str:
    """Collapse whitespace + drop hyphens so 'תל אביב - יפו' == 'תל אביב-יפו' == 'תל אביב יפו'."""
    if not s:
        return ""
    return "".join(ch for ch in s.strip() if ch not in (" ", "-", "\t"))


def chain_filter_sql(chains: str | None, alias: str = "ch") -> tuple[str, list]:
    """Build ` AND {alias}.code IN (...)` from a comma-separated chain-codes string.
    Returns ('', []) when no chains are given. Used to scope dashboard/search queries
    to the user's preferred retailers."""
    if not chains:
        return "", []
    codes = [c for c in (x.strip() for x in chains.split(",")) if c]
    if not codes:
        return "", []
    placeholders = ",".join("?" for _ in codes)
    return f" AND {alias}.code IN ({placeholders})", codes


def chain_scope_sql(
    city: str | None,
    lat: float | None,
    lng: float | None,
    radius_km: float | None,
    chain_alias: str = "ch",
) -> tuple[str, list]:
    """Chain-level scope predicate. Returns a WHERE fragment that restricts
    `{chain_alias}.id` to chains that have at least one *store* in the
    requested city/radius. Use this — instead of `city_filter_sql` on a store
    alias — for queries that compare/aggregate prices at the chain level
    (compare, top-spread, basket alternatives), so prices attached to
    "logical" master-store rows (Tiv Taam 000, Super Pharm 006/036+, etc.)
    still surface when the *physical* stores of the chain are nearby.

    Returns ('', []) when no scope is set.
    """
    inner_sql, inner_params = city_filter_sql(city, lat, lng, radius_km, alias="s2")
    if not inner_sql:
        return "", []
    return (
        f" AND {chain_alias}.id IN ("
        f"SELECT DISTINCT s2.chain_id FROM stores s2 WHERE 1=1 {inner_sql})",
        inner_params,
    )


def compute_city_spellings(
    city: str | None,
    lat: float | None,
    lng: float | None,
    radius_km: float | None,
) -> list[str] | None:
    """Return the list of city spellings to pass to Supabase .in_() / RPC array params.

    Returns:
      None           — no location filter
      []             — filter active but no matching cities (zero-result sentinel)
      [str, ...]     — city name spellings to match against stores.city
    """
    if lat is not None and lng is not None and radius_km and radius_km > 0:
        canonical = cities_within_radius(lat, lng, radius_km)
        if not canonical:
            return []
        return expand_cities_for_sql(canonical)
    if city:
        return city_aliases_for_filter(city)
    return None


def city_aliases_for_filter(city: str) -> list[str]:
    """All spellings for a city string, trying canonical lookup first."""
    canonical = canonicalize(city)
    if canonical:
        return city_aliases(canonical)
    return [city]


def city_filter_sql(
    city: str | None,
    lat: float | None,
    lng: float | None,
    radius_km: float | None,
    alias: str = "s",
) -> tuple[str, list]:
    """Legacy SQLite helper — kept for any remaining SQLite paths."""
    col = f"{alias}.city"
    if lat is not None and lng is not None and radius_km and radius_km > 0:
        canonical = cities_within_radius(lat, lng, radius_km)
        if not canonical:
            return " AND 1=0", []
        spellings = expand_cities_for_sql(canonical)
        normed = list({norm_city(s) for s in spellings if norm_city(s)})
        placeholders = ",".join("?" for _ in normed)
        return (
            f" AND REPLACE(REPLACE(TRIM({col}), ' ', ''), '-', '') IN ({placeholders})",
            normed,
        )
    if city:
        return (
            f" AND REPLACE(REPLACE(TRIM({col}), ' ', ''), '-', '') LIKE ?",
            [f"%{norm_city(city)}%"],
        )
    return "", []


@geo_router.get("/cities")
def list_cities(with_stores_only: bool = Query(True, description="only cities that actually have stores")):
    """Full city picker. Merges the bundled coord data with DB store counts.

    When with_stores_only=True (default), returns only canonical cities that have
    at least one store in the DB (sum across aliases). When False, includes every
    city in the bundle — useful if we later add stores.
    """
    coords = _coords_by_canonical()
    alias_map = _alias_to_canonical()

    # Pull distinct cities from Supabase
    res = supa.sb().table("stores").select("city").not_.is_("city", "null").execute()
    counts: dict[str, int] = {}
    for r in (res.data or []):
        raw = (r.get("city") or "").strip()
        if not raw:
            continue
        canonical = alias_map.get(raw, raw)
        counts[canonical] = counts.get(canonical, 0) + 1

    # assemble result
    out = []
    seen = set()
    for c in _load_cities():
        name = c["name_he"]
        seen.add(name)
        stores = counts.get(name, 0)
        if with_stores_only and stores == 0:
            continue
        out.append({
            "name_he": name,
            "lat": float(c["lat"]),
            "lng": float(c["lng"]),
            "stores": stores,
        })

    # cities present in DB but not in the bundle — include without coords
    for name, n in counts.items():
        if name not in seen:
            out.append({"name_he": name, "lat": None, "lng": None, "stores": n})

    out.sort(key=lambda x: (-x["stores"], x["name_he"]))
    return out


@geo_router.get("/nearest-city")
def nearest_city(lat: float = Query(...), lng: float = Query(...)):
    """For 'use my location': browser hands us coords, we return the nearest known city."""
    cities = _load_cities()
    if not cities:
        raise HTTPException(500, "city data unavailable")
    best = min(cities, key=lambda c: haversine_km(lat, lng, c["lat"], c["lng"]))
    d = haversine_km(lat, lng, best["lat"], best["lng"])
    return {
        "name_he": best["name_he"],
        "lat": best["lat"],
        "lng": best["lng"],
        "distance_km": round(d, 2),
    }
