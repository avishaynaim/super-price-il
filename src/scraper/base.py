from __future__ import annotations

import abc
import asyncio
import gzip
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import httpx

from .registry import ChainSpec

RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"

FileKind = str  # 'PriceFull' | 'Price' | 'PromoFull' | 'Promo' | 'Stores'


def _decompress(raw: bytes) -> bytes:
    """Dispatch by magic bytes. Binaprojects serves ZIPs with .gz extensions;
    other chains use real gzip. Inner ZIP members may themselves be gzipped."""
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    if raw[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            name = z.namelist()[0]
            inner = z.read(name)
        return gzip.decompress(inner) if inner[:2] == b"\x1f\x8b" else inner
    return raw


@dataclass
class RemoteFile:
    url: str
    filename: str
    kind: FileKind
    store_code: str | None
    published_at: datetime | None


@dataclass
class DownloadedFile:
    remote: RemoteFile
    path: Path  # local gz on disk
    xml_bytes: bytes  # decompressed content


class BaseChainScraper(abc.ABC):
    """Interface every chain scraper implements.

    Subclasses override ``list_files`` (how to enumerate the portal) and
    optionally ``authenticate``. Downloading, caching, and decompression
    are shared.
    """

    def __init__(self, spec: ChainSpec, client: httpx.AsyncClient):
        self.spec = spec
        self.client = client

    # Override: enumerate files on the portal, optionally filtered since `since`.
    @abc.abstractmethod
    def list_files(self, since: datetime | None = None) -> AsyncIterator[RemoteFile]:
        ...

    # Override if the chain needs a login. Default: no-op.
    async def authenticate(self) -> None:
        return None

    async def download(self, rf: RemoteFile) -> DownloadedFile:
        day = (rf.published_at or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
        target_dir = RAW_ROOT / self.spec.code / day
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / rf.filename

        if not path.exists():
            # Retry transient network failures with exponential backoff.
            # Shufersal's CDN and publishedprices both occasionally ReadTimeout
            # mid-response; one retry recovers most of them.
            attempts = 3
            for attempt in range(1, attempts + 1):
                try:
                    resp = await self.client.get(rf.url)
                    resp.raise_for_status()
                    path.write_bytes(resp.content)
                    break
                except (httpx.ReadTimeout, httpx.ConnectTimeout,
                        httpx.ReadError, httpx.RemoteProtocolError) as e:
                    if attempt == attempts:
                        raise
                    await asyncio.sleep(2 ** attempt)

        raw = path.read_bytes()
        xml = _decompress(raw)
        return DownloadedFile(remote=rf, path=path, xml_bytes=xml)

    async def run(
        self,
        since: datetime | None = None,
        concurrency: int = 6,
        limit: int | None = None,
        kinds: set[str] | None = None,
        on_listed: "callable | None" = None,
        on_downloaded: "callable | None" = None,
    ) -> AsyncIterator[DownloadedFile]:
        """Authenticate, list files, download in parallel; yield each as it lands.

        Yielding (instead of returning a list) lets the orchestrator parse
        and insert one file at a time and drop the decompressed XML, keeping
        peak memory ~one file (~3 MB) instead of the full chain (~1+ GB for
        shufersal/mega). Required to survive proot's memory/thread limits.

        Optional progress callbacks:
          on_listed(total: int) — fired once after listing.
          on_downloaded(done: int, total: int) — after EACH file lands.
        """
        await self.authenticate()
        sem = asyncio.Semaphore(concurrency)

        try:
            gen = self.list_files(since=since, kinds=kinds)
        except TypeError:
            gen = self.list_files(since=since)
        listed: list[RemoteFile] = []
        async for rf in gen:
            if kinds and rf.kind not in kinds:
                continue
            listed.append(rf)
            if limit and len(listed) >= limit:
                break

        # Per-store deduplication: keep only the latest published_at for each
        # (kind, store_code) pair. With a wide --days window (e.g. 7), this
        # ensures each store is represented by its newest available file —
        # if today's missing for store X, yesterday's is used; etc.
        # Files without store_code (Stores/StoresFull, header-only) are
        # untouched.
        keyed: dict[tuple, RemoteFile] = {}
        keepalways: list[RemoteFile] = []
        DEDUP_KINDS = {"PriceFull", "Price", "PromoFull", "Promo"}
        for rf in listed:
            if rf.store_code and rf.kind in DEDUP_KINDS:
                key = (rf.kind, rf.store_code)
                prev = keyed.get(key)
                if prev is None:
                    keyed[key] = rf
                else:
                    a, b = prev.published_at, rf.published_at
                    if b is not None and (a is None or b > a):
                        keyed[key] = rf
            else:
                keepalways.append(rf)
        listed = list(keyed.values()) + keepalways

        if on_listed is not None:
            try: on_listed(len(listed))
            except Exception: pass

        done = 0
        async def _fetch(rf: RemoteFile) -> DownloadedFile:
            nonlocal done
            async with sem:
                df = await self.download(rf)
            done += 1
            if on_downloaded is not None:
                try: on_downloaded(done, len(listed))
                except Exception: pass
            return df

        tasks = [asyncio.create_task(_fetch(rf)) for rf in listed]
        for t in asyncio.as_completed(tasks):
            try:
                yield await t
            except Exception:
                # Failed download — already reflected in `done` via callback.
                pass
