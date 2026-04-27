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
    ) -> list[DownloadedFile]:
        """Authenticate, list files, download in parallel.

        Optional progress callbacks let the orchestrator (cli/backfill.py)
        update the scrape_runs row in real time:
          on_listed(total: int) — fired once after listing is complete.
          on_downloaded(done: int, total: int) — fired after EACH file lands.
        Both callbacks are optional and synchronous; failures are swallowed.
        """
        await self.authenticate()
        sem = asyncio.Semaphore(concurrency)

        # Materialize the file list first so we can announce a stable `total`
        # to the orchestrator before any downloads start. This is critical for
        # live progress: the dashboard's "running_now" turns yellow as soon as
        # we know how many files we're going to fetch.
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

        if on_listed is not None:
            try: on_listed(len(listed))
            except Exception: pass

        done = 0
        results: list[DownloadedFile] = []
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
                results.append(await t)
            except Exception:
                # Surface as a count drop — the orchestrator's parse loop will
                # never see this file. We still count it via on_downloaded so
                # the progress denominator includes failed downloads.
                pass
        return results
