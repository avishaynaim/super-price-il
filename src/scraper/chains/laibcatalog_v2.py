"""Scraper for the *new* laibcatalog format (Nibit's 2026 rewrite).

Endpoint shape (verified 2026-04-27 against laibcatalog.co.il/{victory,mshuk}/index.html):

  GET https://laibcatalog.co.il/webapi/api/getbranches?edi=<chainId>
      → JSON [{number, name}, ...]
  GET https://laibcatalog.co.il/webapi/api/getfiles?edi=<chainId>
      → JSON [{fileName, fileType, fileSize, fileDate, branchNumber}, ...]
  GET https://laibcatalog.co.il/webapi/<chainId>/<fileName>
      → gzipped XML

This scraper covers chains that the legacy `LaibcatalogScraper` no longer
finds files for (the old landing page stopped exposing direct file links
mid-2026). When Nibit finishes migrating per-chain data into the new
`getfiles` endpoint, this scraper picks it up automatically — for now the
endpoint returns branches but zero files.

Eligible chains: Victory, Machsanei Hashuk, Cohen H. Switching is done by
flipping a chain's `auth_kind` to `"laibcatalog_v2"` in registry.py.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile

BASE = "https://laibcatalog.co.il/webapi"


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _parse_date(s: str | None) -> datetime | None:
    """API returns dates like '2026-04-27 06:00' or '27/04/2026 06:00'."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M", "%H:%M %d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


class LaibcatalogV2Scraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        edi = self.spec.chain_id
        if not edi:
            raise RuntimeError(f"{self.spec.code}: chain_id required for laibcatalog_v2")
        resp = await self.client.get(f"{BASE}/api/getfiles", params={"edi": edi})
        resp.raise_for_status()
        try:
            files = resp.json()
        except Exception as e:
            raise RuntimeError(f"laibcatalog_v2 {self.spec.code}: bad JSON: {e}")
        if not isinstance(files, list):
            return
        for f in files:
            fname = f.get("fileName") or f.get("FileName")
            if not fname:
                continue
            published = _parse_date(f.get("fileDate") or f.get("FileDate"))
            if since and published and published < since:
                continue
            store_code = None
            bn = f.get("branchNumber") or f.get("BranchNumber")
            if bn is not None:
                # API may return branch as int or string; normalize to str
                store_code = str(bn)
            yield RemoteFile(
                url=f"{BASE}/{edi}/{fname}",
                filename=fname,
                kind=_classify(fname),
                store_code=store_code,
                published_at=published,
            )


def make_client_for_laibcatalog_v2() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
            "Referer": "https://laibcatalog.co.il/",
        },
        timeout=120,
        follow_redirects=True,
        verify=False,
    )
