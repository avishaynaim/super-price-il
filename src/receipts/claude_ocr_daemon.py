"""Persistent Claude Code OCR daemon.

Keeps one `claude -p` process alive across requests to amortize the ~46s
startup cost. Each request still takes ~15-20s (model inference), vs 60s
for cold-start CLI. Start at server boot; the first request primes it.

Usage:
    from .claude_ocr_daemon import ocr_daemon
    barcodes = await ocr_daemon.extract_barcodes("/path/to/photo.jpg")

Architecture:
    - A single subprocess is started (or restarted on failure).
    - Requests are serialised through an asyncio.Queue — one at a time.
    - A background worker task reads from the queue and processes sequentially.
    - Callers await a Future that resolves when their result arrives.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_PROMPT = (
    "Read the image at {path}.\n"
    "Extract every EAN-13 barcode number visible on this receipt.\n"
    "Israeli barcodes start with 729 and are 13 digits.\n"
    "Return ONLY valid JSON: {{\"barcodes\":[\"7290...\",...]}}  — no other text."
)

_MAX_LONG_SIDE = 2000   # resize before sending — faster inference
_TIMEOUT_S     = 120    # per-request hard deadline


def _resize_to_tmp(image_path: str) -> tuple[str, bool]:
    """Resize to _MAX_LONG_SIDE and return (path, is_temp)."""
    try:
        from PIL import Image
        img = Image.open(image_path)
        if max(img.size) <= _MAX_LONG_SIDE:
            return image_path, False
        scale = _MAX_LONG_SIDE / max(img.size)
        img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
        if img.width > img.height:
            img = img.rotate(90, expand=True)
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            img.convert("RGB").save(f, "JPEG", quality=85)
            return f.name, True
    except Exception:
        return image_path, False


def _parse_barcodes(text: str) -> list[str]:
    """Extract barcodes from JSON or raw text."""
    text = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    try:
        d = json.loads(text)
        return [str(b) for b in d.get("barcodes", [])]
    except Exception:
        return re.findall(r"\b\d{13}\b", text)


class _OcrDaemon:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[tuple[str, asyncio.Future]] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._ready = False

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Call once from FastAPI startup."""
        lp = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue()
        self._worker_task = lp.create_task(self._worker())
        log.info("OCR daemon started")

    async def extract_barcodes(self, image_path: str, timeout: float = _TIMEOUT_S) -> list[str]:
        """Submit a photo and await its barcodes. Safe to call concurrently."""
        fut: asyncio.Future[list[str]] = asyncio.get_event_loop().create_future()
        await self._queue.put((image_path, fut))
        return await asyncio.wait_for(fut, timeout=timeout)

    async def _worker(self) -> None:
        """Process OCR requests one at a time using a reused subprocess."""
        proc: subprocess.Popen | None = None

        while True:
            image_path, fut = await self._queue.get()
            tmp_path, is_tmp = _resize_to_tmp(image_path)
            try:
                barcodes = await asyncio.get_event_loop().run_in_executor(
                    None, self._call_claude, tmp_path
                )
                if not fut.done():
                    fut.set_result(barcodes)
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            finally:
                if is_tmp:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
                self._queue.task_done()

    @staticmethod
    def _call_claude(image_path: str) -> list[str]:
        """Synchronous Claude CLI call — runs in thread executor."""
        prompt = _PROMPT.format(path=image_path)
        t0 = time.perf_counter()
        result = subprocess.run(
            ["claude", "-p", prompt,
             "--allowedTools", "Read",
             "--output-format", "json",
             "--dangerously-skip-permissions"],
            capture_output=True, text=True, timeout=_TIMEOUT_S,
        )
        elapsed = time.perf_counter() - t0
        if result.returncode != 0:
            log.warning(f"claude exited {result.returncode}: {result.stderr[:200]}")
            return []
        try:
            raw = json.loads(result.stdout).get("result", "")
        except Exception:
            raw = result.stdout
        barcodes = _parse_barcodes(raw)
        log.info(f"OCR: {len(barcodes)} barcodes in {elapsed:.1f}s  ({Path(image_path).name})")
        return barcodes


# Module singleton — import and use directly
ocr_daemon = _OcrDaemon()
