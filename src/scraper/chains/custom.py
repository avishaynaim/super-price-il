"""Per-chain scrapers for portals that don't fit the binaprojects / publishedprices /
laibcatalog / shufersal patterns.

Currently:
  - Mega (Carrefour): publishprice.mega.co.il 301-redirects to prices.carrefour.co.il.
    The landing page embeds `const path = 'YYYYMMDD';` and `const files = [...]`
    in an inline <script>. Download URL is `/<path>/<filename>`.
  - Hazi Hinam: shop.hazi-hinam.co.il/Prices is a server-rendered HTML table with
    direct links to hazihinamprod01.blob.core.windows.net/regulatories/<filename>.

Keshet (publishprice.mehadrin.co.il) is stubbed — the subdomain is NXDOMAIN as of
2026-04-21; the main site keshet-teamim.co.il also times out from this env.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _store_from_filename(filename: str) -> str | None:
    # Price7290055700007-0062-202604202300.gz          → '0062'
    # Price7290700100008-000-208-20260421-060615.gz   → '208' (hazi-hinam)
    stem = filename.split(".", 1)[0]
    parts = stem.split("-")
    if len(parts) >= 3 and parts[1] == "000" and parts[2].isdigit():
        return parts[2]  # hazi-hinam triple-dash form
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    return None


# -- Mega / Carrefour --------------------------------------------------------

MEGA_BASE = "https://prices.carrefour.co.il"
MEGA_PATH = re.compile(r"const\s+path\s*=\s*['\"]([0-9]{8})['\"]")
MEGA_FILES = re.compile(r"const\s+files\s*=\s*(\[[\s\S]*?\])\s*;", re.M)


class MegaScraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        r = await self.client.get(f"{MEGA_BASE}/")
        r.raise_for_status()
        m_path = MEGA_PATH.search(r.text)
        m_files = MEGA_FILES.search(r.text)
        if not (m_path and m_files):
            raise RuntimeError("mega: could not extract `path`/`files` from landing page")
        path = m_path.group(1)
        files = json.loads(m_files.group(1))

        for f in files:
            fname = f.get("name")
            if not fname or not fname.endswith(".gz"):
                continue
            published = None
            try:
                # "05:09 21-04-2026"
                published = datetime.strptime(f["modified"], "%H:%M %d-%m-%Y").replace(
                    tzinfo=timezone.utc
                )
            except Exception:
                pass
            if since and published and published < since:
                continue
            yield RemoteFile(
                url=f"{MEGA_BASE}/{path}/{fname}",
                filename=fname,
                kind=_classify(fname),
                store_code=_store_from_filename(fname),
                published_at=published,
            )


def make_client_for_mega() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"},
        timeout=120,
        follow_redirects=True,
        verify=False,
    )


# -- Hazi Hinam --------------------------------------------------------------

HAZI_LIST = "https://shop.hazi-hinam.co.il/Prices"
HAZI_LINK = re.compile(
    r"""href=["'](https://hazihinamprod01\.blob\.core\.windows\.net/regulatories/[^"']+\.gz)["']""",
    re.I,
)
HAZI_ROW_DATE = re.compile(r"<span>(\d{2}-\d{2}-\d{4})</span>\s*<span>(\d{2}:\d{2})</span>")


class HaziHinamScraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        r = await self.client.get(HAZI_LIST)
        r.raise_for_status()
        seen: set[str] = set()
        for match in HAZI_LINK.finditer(r.text):
            url = match.group(1)
            fname = url.rsplit("/", 1)[-1]
            if fname in seen:
                continue
            seen.add(fname)
            published = _hazi_date_from_filename(fname)
            if since and published and published < since:
                continue
            yield RemoteFile(
                url=url,
                filename=fname,
                kind=_classify(fname),
                store_code=_store_from_filename(fname),
                published_at=published,
            )


def _hazi_date_from_filename(fname: str) -> datetime | None:
    # Price7290700100008-000-208-20260421-060615.gz → 2026-04-21 06:06:15
    stem = fname.split(".", 1)[0]
    parts = stem.split("-")
    if len(parts) >= 5:
        date_s, time_s = parts[-2], parts[-1]
        try:
            return datetime.strptime(date_s + time_s, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def make_client_for_hazi_hinam() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"},
        timeout=120,
        follow_redirects=True,
        verify=False,
    )


# -- Super-Pharm -------------------------------------------------------------

SUPERPHARM_BASE = "http://prices.super-pharm.co.il"
SUPERPHARM_LINK = re.compile(r'href="(/Download/[^"]+\.gz\?[^"]+)"', re.I)
SUPERPHARM_PAGES = re.compile(r'data-page="(\d+)"')
SUPERPHARM_DATE = re.compile(
    r'<td[^>]*>\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(?::\d{2})?)\s*</td>'
)


def _superpharm_published(fname: str) -> datetime | None:
    # PriceFull7290172900007-006-202604210707.gz → 2026-04-21 07:07
    stem = fname.split(".", 1)[0]
    parts = stem.split("-")
    # last part is a timestamp like 202604210707 (12 chars) or 20260421-070712 (date-time)
    tail = parts[-1]
    try:
        if len(tail) == 12:
            return datetime.strptime(tail, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        if len(tail) == 6 and len(parts) >= 2 and len(parts[-2]) == 8:
            return datetime.strptime(parts[-2] + tail, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None


class SuperPharmScraper(BaseChainScraper):
    """Super-Pharm portal: paginated HTML grid with direct /Download/* links.

    ~124 pages, ~20 rows per page → ~2,500 file entries. We walk pages until
    `since` is exceeded or the link set stops growing.
    """

    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        seen: set[str] = set()
        page = 1
        max_page = 1
        while page <= max_page:
            r = await self.client.get(f"{SUPERPHARM_BASE}/?page={page}")
            r.raise_for_status()
            html = r.text
            # capture the max page number once
            if page == 1:
                pages = [int(m) for m in SUPERPHARM_PAGES.findall(html)]
                if pages:
                    max_page = max(pages)
            older_only = True  # if every link on this page is older than `since`, stop
            for link in SUPERPHARM_LINK.findall(html):
                # link = "/Download/PriceFull...gz?bucketName=..."
                fname = link.split("/Download/", 1)[1].split("?", 1)[0]
                if fname in seen:
                    continue
                seen.add(fname)
                published = _superpharm_published(fname)
                if since and published and published < since:
                    continue
                older_only = False
                yield RemoteFile(
                    url=f"{SUPERPHARM_BASE}{link}",
                    filename=fname,
                    kind=_classify(fname),
                    store_code=_store_from_filename(fname),
                    published_at=published,
                )
            # cheap heuristic: rows come in descending date order on page 1, but
            # subsequent pages may interleave Price/PriceFull/Promo/PromoFull.
            # Keep walking until we've scanned all pages; `since` filter drops
            # old ones. Cap the walk to avoid runaway if layout changes.
            page += 1
            if page > 200:
                break
            if since and older_only and page > 3:
                # three consecutive pages with only stale links → done
                break


def make_client_for_superpharm() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Language": "he-IL,he;q=0.9,en;q=0.8"},
        timeout=60,
        follow_redirects=True,
        verify=False,
    )


# -- Wolt --------------------------------------------------------------------

WOLT_BASE = "https://wm-gateway.wolt.com/isr-prices/public/v1"
WOLT_DATE_HREF = re.compile(r'href="(\d{4}-\d{2}-\d{2})\.html"')
WOLT_FILE_HREF = re.compile(r'href="(download/\d{4}-\d{2}-\d{2}/[^"]+\.gz)"')


class WoltScraper(BaseChainScraper):
    """Wolt-Market price portal: plain directory listing per day."""

    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        # grab the per-day index — each <li> links to a day page
        r = await self.client.get(f"{WOLT_BASE}/index.html")
        r.raise_for_status()
        dates = WOLT_DATE_HREF.findall(r.text)
        # recent dates first
        dates.sort(reverse=True)
        for d in dates:
            try:
                day_dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if since and day_dt < since.replace(hour=0, minute=0, second=0, microsecond=0):
                break
            rd = await self.client.get(f"{WOLT_BASE}/{d}.html")
            rd.raise_for_status()
            for href in WOLT_FILE_HREF.findall(rd.text):
                fname = href.rsplit("/", 1)[-1]
                published = _hazi_date_from_filename(fname) or day_dt
                if since and published and published < since:
                    continue
                yield RemoteFile(
                    url=f"{WOLT_BASE}/{href}",
                    filename=fname,
                    kind=_classify(fname),
                    store_code=_store_from_filename(fname),
                    published_at=published,
                )


def make_client_for_wolt() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": UA},
        timeout=60,
        follow_redirects=True,
        verify=False,
    )
