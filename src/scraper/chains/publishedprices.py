"""Scraper for chains hosted on url.publishedprices.co.il.

Back end is a Cerberus FTP Web Client. Login returns a cftpSID cookie plus a
fresh csrftoken in a <meta> tag; file listing is AJAX JSON at /file/json/dir;
download is /file/d/<filename>.

Covers Rami Levi, Yohananof, Tiv Taam today. Add more by registering a chain
with auth_kind='publishedprices' and a username in registry.py.

SSL verification is disabled on this host's env because the Cerberus cert chain
doesn't validate against the proot distro's limited CA bundle. Acceptable for a
public read-only price feed; revisit when deploying elsewhere.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from ..base import BaseChainScraper, RemoteFile

DEFAULT_BASE = "https://url.publishedprices.co.il"
META_CSRF = re.compile(r'<meta name="csrftoken"[^>]*content="([^"]+)"', re.I)


def _classify(filename: str) -> str:
    n = filename.upper()
    if n.startswith("PRICEFULL"): return "PriceFull"
    if n.startswith("PROMOFULL"): return "PromoFull"
    if n.startswith("PRICE"):     return "Price"
    if n.startswith("PROMO"):     return "Promo"
    if n.startswith("STORESFULL") or n.startswith("STOREFULL"): return "StoresFull"
    if n.startswith("STORES"):    return "Stores"
    return "Unknown"


def _store_code(filename: str) -> str | None:
    # PriceFull7290058140886-001-070-20260420-070019.gz → '070' (3rd dash part = store)
    # naming varies: sometimes -NNN- once, sometimes twice. Pick the first 3-digit segment.
    parts = filename.split("-")
    for p in parts[1:]:
        if p.isdigit() and len(p) in (3, 4):
            return p
    return None


class PublishedPricesScraper(BaseChainScraper):
    _csrf: str = ""

    @property
    def _base(self) -> str:
        # Most chains live on url.publishedprices.co.il; Stop Market is on
        # url.retail.publishedprices.co.il. Derive from spec to support both.
        url = (self.spec.portal_url or "").rstrip("/")
        if not url:
            return DEFAULT_BASE
        # spec may be .../login — trim off trailing path
        for suffix in ("/login", "/file"):
            if url.endswith(suffix):
                url = url[: -len(suffix)]
                break
        return url or DEFAULT_BASE

    async def authenticate(self) -> None:
        # httpx verify is set at client construction time; we assume the caller
        # built an AsyncClient with verify=False for this chain.
        base = self._base
        r = await self.client.get(f"{base}/login")
        r.raise_for_status()
        m = META_CSRF.search(r.text)
        if not m:
            raise RuntimeError("publishedprices: csrftoken meta not found on /login")
        token = m.group(1)

        r2 = await self.client.post(
            f"{base}/login/user",
            data={
                "r": "",
                "username": self.spec.username or "",
                "password": self.spec.password or "",
                "Submit": "Sign in",
                "csrftoken": token,
            },
        )
        r2.raise_for_status()

        r3 = await self.client.get(f"{base}/file")
        r3.raise_for_status()
        m2 = META_CSRF.search(r3.text)
        if not m2:
            raise RuntimeError("publishedprices: no csrftoken after login (auth probably failed)")
        self._csrf = m2.group(1)

    async def _list_dir(self, cd: str) -> list[dict]:
        base = self._base
        r = await self.client.post(
            f"{base}/file/json/dir",
            data={
                "sEcho": "1",
                "iColumns": "5",
                "sColumns": "",
                "iDisplayStart": "0",
                "iDisplayLength": "100000",
                "cd": cd,
                "csrftoken": self._csrf,
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        r.raise_for_status()
        return r.json().get("aaData", []) or []

    async def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        # Most chains put files at the root. Super Yuda (and potentially others)
        # nest them one level deep in a subfolder (e.g. "/Yuda"). Walk a small
        # directory tree (max depth 2) to handle both.
        base = self._base
        queue = ["/"]
        seen: set[str] = set()
        depth = 0
        while queue and depth <= 2:
            next_queue: list[str] = []
            for cd in queue:
                if cd in seen:
                    continue
                seen.add(cd)
                try:
                    rows = await self._list_dir(cd)
                except Exception:
                    continue
                for row in rows:
                    rtype = row.get("type")
                    name = row.get("fname") or row.get("name") or ""
                    if rtype == "folder" and name and not name.startswith("."):
                        sub = (cd.rstrip("/") + "/" + name) if cd != "/" else "/" + name
                        next_queue.append(sub)
                        continue
                    if rtype != "file" or not name.endswith(".gz"):
                        continue
                    try:
                        published = datetime.fromisoformat(row["time"].replace("Z", "+00:00"))
                    except Exception:
                        published = None
                    if since and published and published < since:
                        continue
                    # Cerberus serves nested files at /file/d/<subdir>/<name>.
                    # Strip leading "/" from cd so we don't double-slash.
                    sub = cd.lstrip("/")
                    path = f"{sub}/{name}" if sub else name
                    yield RemoteFile(
                        url=f"{base}/file/d/{path}",
                        filename=name,
                        kind=_classify(name),
                        store_code=_store_code(name),
                        published_at=published,
                    )
            queue = next_queue
            depth += 1


def make_client_for_publishedprices() -> httpx.AsyncClient:
    """Helper: returns an AsyncClient preconfigured for this portal."""
    return httpx.AsyncClient(
        headers={"User-Agent": "super-price-il/0.1 (research)"},
        timeout=60,
        follow_redirects=True,
        verify=False,
    )
