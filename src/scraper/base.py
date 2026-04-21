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
            resp = await self.client.get(rf.url)
            resp.raise_for_status()
            path.write_bytes(resp.content)

        raw = path.read_bytes()
        xml = _decompress(raw)
        return DownloadedFile(remote=rf, path=path, xml_bytes=xml)

    async def run(
        self,
        since: datetime | None = None,
        concurrency: int = 6,
        limit: int | None = None,
        kinds: set[str] | None = None,
    ) -> list[DownloadedFile]:
        await self.authenticate()
        sem = asyncio.Semaphore(concurrency)

        async def _fetch(rf: RemoteFile) -> DownloadedFile:
            async with sem:
                return await self.download(rf)

        tasks: list[asyncio.Task[DownloadedFile]] = []
        async for rf in self.list_files(since=since):
            if kinds and rf.kind not in kinds:
                continue
            tasks.append(asyncio.create_task(_fetch(rf)))
            if limit and len(tasks) >= limit:
                break
        return await asyncio.gather(*tasks)
