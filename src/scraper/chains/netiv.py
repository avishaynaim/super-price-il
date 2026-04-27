"""Scraper for Netiv Hahesed (סופר חסד / ברכל).

Portal lives at the bare IP http://141.226.203.152/ — old-school IIS classic
directory listing, no auth. Two endpoints:

  GET / (root)          — today's snapshot, ~234 KB. Inline `<a href="prices/<file>.gz">`
                          for the latest of each kind, plus an /Prices/Archive
                          subdir link.
  GET /Prices/          — full historic listing, all kinds (Price, PriceFull,
                          Promo, PromoFull, StoresFull) for chain 7290058160839
                          across all stores, ~2 MB.

For daily scraping the root page is enough — it gives one of each kind per
store at the latest reporting time. For backfill > 1 day, /Prices/ is the
authoritative paginated listing.

File format: <Kind>7290058160839-<store>-<YYYYMMDDHHMM>.gz, e.g.
  PriceFull7290058160839-001-202604270507.gz   (chain, store=001, 2026-04-27 05:07)
  StoresFull7290058160839-000-202603290510.gz  (chain-wide stores file)
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile

DEFAULT_BASE = "http://141.226.203.152"

# Match files like Price...gz inside an <a href="..."> on either the root
# index (relative `prices/...`) or the /Prices/ listing (absolute `/Prices/...`).
HREF_RE = re.compile(
    r'href="(?:/?Prices/|prices/)((?:Price|Promo|Stores)(?:Full)?7290\d+-\d+-\d{12}\.gz)"',
    re.IGNORECASE,
)


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _parts(filename: str) -> tuple[str | None, datetime | None]:
    """Filename: <Kind><chainId>-<store>-<YYYYMMDDHHMM>.gz"""
    stem = filename.split(".", 1)[0]
    bits = stem.split("-")
    if len(bits) < 3:
        return None, None
    store = bits[1] if bits[1].isdigit() else None
    published = None
    try:
        published = datetime.strptime(bits[2][:12], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    return store, published


class NetivScraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        base = (self.spec.portal_url or DEFAULT_BASE).rstrip("/")
        # Root page first — covers today's files; fall back to /Prices/ for
        # historic when caller needs more than one day back.
        urls = [f"{base}/", f"{base}/Prices/"]
        seen: set[str] = set()
        for url in urls:
            try:
                resp = await self.client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError:
                continue
            for m in HREF_RE.finditer(resp.text):
                fname = m.group(1)
                if fname in seen:
                    continue
                seen.add(fname)
                store, published = _parts(fname)
                if since and published and published < since:
                    continue
                yield RemoteFile(
                    url=f"{base}/Prices/{fname}",
                    filename=fname,
                    kind=_classify(fname),
                    store_code=store,
                    published_at=published,
                )


def make_client_for_netiv() -> httpx.AsyncClient:
    # Plain HTTP, no cert chain to worry about. UA helps to avoid an iis
    # default-page redirect.
    return httpx.AsyncClient(
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
        },
        timeout=120,
        follow_redirects=True,
        verify=False,
    )
