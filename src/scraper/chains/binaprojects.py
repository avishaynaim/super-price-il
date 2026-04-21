"""Scraper for chains hosted on *.binaprojects.com.

API shape (verified 2026-04-21 against kingstore.binaprojects.com):

  POST /MainIO_Hok.aspx
    form: WStore=0, WDate='', WFileType=0   (0 = all)
    → JSON array of {FileNm, Company, Store, TypeFile, DateFile, PathLogo, ...}
    FileNm looks like 'PromoFull7290058108879-340-202604210509.gz'

  POST /Download.aspx?FileNm=<FileNm>
    → JSON [{"SPath": "https://<host>/Download/<FileNm>"}]

  GET <SPath>
    → gzipped XML

DateFile is a Hebrew-locale 'HH:MM DD/MM/YYYY' string.

Registered chains: King Store (and Osher Ad when its subdomain comes back).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _parse_datefile(s: str) -> datetime | None:
    # 'HH:MM DD/MM/YYYY'
    try:
        return datetime.strptime(s.strip(), "%H:%M %d/%m/%Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _store_code(filename: str) -> str | None:
    # PromoFull7290058108879-340-202604210509.gz → '340'
    stem = filename.split(".", 1)[0]
    parts = stem.split("-")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    return None


class BinaprojectsScraper(BaseChainScraper):
    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        base = self.spec.portal_url.rstrip("/")
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{base}/Main.aspx",
        }
        resp = await self.client.post(
            f"{base}/MainIO_Hok.aspx",
            data={"WStore": "0", "WDate": "", "WFileType": "0"},
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()
        for row in rows:
            fname = row.get("FileNm")
            if not fname:
                continue
            published = _parse_datefile(row.get("DateFile", ""))
            if since and published and published < since:
                continue
            # Resolve real URL via Download.aspx
            dl = await self.client.post(
                f"{base}/Download.aspx",
                params={"FileNm": fname},
                headers=headers,
            )
            dl.raise_for_status()
            try:
                spath = dl.json()[0]["SPath"]
            except Exception:
                continue
            yield RemoteFile(
                url=spath,
                filename=fname,
                kind=_classify(fname),
                store_code=_store_code(fname),
                published_at=published,
            )


def make_client_for_binaprojects() -> httpx.AsyncClient:
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
