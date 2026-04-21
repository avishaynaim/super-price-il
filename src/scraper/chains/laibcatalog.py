"""Scraper for chains hosted on laibcatalog.co.il (Nibit ERP).

Landing page is one ~4MB HTML blob with every chain's latest files as direct
<a href> links. No auth. Files live at:
    /CompetitionRegulationsFiles/latest/<chainId>/<filename>.xml.gz

We extract links matching the chain_id from the spec and yield them.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile

BASE = "https://laibcatalog.co.il"


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _parts(filename: str) -> tuple[str | None, datetime | None]:
    # Price7290696200003-089-202604201921-001.xml.gz
    stem = filename.split(".", 1)[0]
    bits = stem.split("-")
    store = bits[1] if len(bits) >= 2 and bits[1].isdigit() else None
    published = None
    for b in bits[2:]:
        if b.isdigit() and len(b) >= 12:
            try:
                published = datetime.strptime(b[:12], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                break
            except ValueError:
                pass
    return store, published


class LaibcatalogScraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        chain_id = self.spec.chain_id
        if not chain_id:
            raise RuntimeError(f"{self.spec.code}: chain_id required for laibcatalog")

        resp = await self.client.get(f"{BASE}/")
        resp.raise_for_status()
        pattern = re.compile(
            rf"""CompetitionRegulationsFiles[\\/]+latest[\\/]+{chain_id}[\\/]+([^\s"'<>]+\.xml\.gz)""",
            re.I,
        )
        seen: set[str] = set()
        for match in pattern.finditer(resp.text):
            fname = match.group(1)
            if fname in seen:
                continue
            seen.add(fname)
            store, published = _parts(fname)
            if since and published and published < since:
                continue
            yield RemoteFile(
                url=f"{BASE}/CompetitionRegulationsFiles/latest/{chain_id}/{fname}",
                filename=fname,
                kind=_classify(fname),
                store_code=store,
                published_at=published,
            )


def make_client_for_laibcatalog() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": "super-price-il/0.1 (research)"},
        timeout=120,
        follow_redirects=True,
        verify=False,
    )
