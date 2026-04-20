"""Shufersal scraper.

Portal: https://prices.shufersal.co.il/
No auth. Listing is a paginated HTML table that can be driven by category id.
catID values (observed): 0=all, 1=PriceFull, 2=Promos, 3=PriceFull (alt), 4=PromoFull,
5=StoreFull. Exact mapping has drifted historically, so we fetch catID=0 and
classify by filename prefix.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..base import BaseChainScraper, RemoteFile

BASE = "https://prices.shufersal.co.il/"
LIST_URL = BASE + "FileObject/UpdateCategory"


def _classify(filename: str) -> str:
    name = filename.upper()
    if name.startswith("PRICEFULL"):
        return "PriceFull"
    if name.startswith("PROMOFULL"):
        return "PromoFull"
    if name.startswith("PRICE"):
        return "Price"
    if name.startswith("PROMO"):
        return "Promo"
    if name.startswith("STORESFULL") or name.startswith("STOREFULL"):
        return "StoresFull"
    if name.startswith("STORES"):
        return "Stores"
    return "Unknown"


def _store_code_from_filename(filename: str) -> str | None:
    # PriceFull7290027600007-001-202604200300.xml.gz → store '001'
    stem = filename.split(".", 1)[0]
    parts = stem.split("-")
    if len(parts) >= 3:
        return parts[1]
    return None


def _date_from_filename(filename: str) -> datetime | None:
    stem = filename.split(".", 1)[0]
    parts = stem.split("-")
    if len(parts) >= 3:
        tail = parts[-1]
        try:
            return datetime.strptime(tail[:12], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


class ShufersalScraper(BaseChainScraper):
    async def _fetch_page(self, page: int, cat_id: int = 0) -> str:
        resp = await self.client.get(
            LIST_URL,
            params={"catID": cat_id, "storeId": 0, "page": page},
        )
        resp.raise_for_status()
        return resp.text

    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        page = 1
        seen: set[str] = set()
        while True:
            html = await self._fetch_page(page)
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.find_all("tr")
            any_new = False
            for tr in rows:
                a = tr.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if ".gz" not in href:
                    continue
                url = urljoin(BASE, href)
                path_only = urlparse(url).path
                filename = path_only.rsplit("/", 1)[-1]   # e.g. Price...-202604201800.gz
                if filename in seen:
                    continue
                seen.add(filename)
                any_new = True
                published = _date_from_filename(filename)
                if since and published and published < since:
                    continue
                yield RemoteFile(
                    url=url,
                    filename=filename,
                    kind=_classify(filename),
                    store_code=_store_code_from_filename(filename),
                    published_at=published,
                )
            if not any_new:
                break
            page += 1
            if page > 200:
                break  # safety
