"""Live OCR WebSocket endpoint.

Client streams JPEG/PNG frames as base64 over WS; server runs RapidOCR on each
frame and replies with detected boxes + matched products. The frontend uses
the boxes to draw an overlay on the live <video> feed and the matched items
to populate a running side list.

Wire format:

    client → server:
        {"type": "frame", "image": "<base64 of JPEG bytes>", "id": "<frame id>"}
        {"type": "reset"}     # clears server-side dedup state

    server → client:
        {
          "type": "result",
          "id": "<echo frame id>",
          "latency_ms": int,
          "lines": [{"text": str, "box": [x, y, w, h], "conf": float, "kind": "barcode|price|qty|text"}],
          "items": [             # only NEW items since last frame
              {"barcode": str, "name": str, "line_total": float, "matched": bool, "box": [..]},
              ...
          ],
          "totals": {"distinct_items": int, "basket_total": float}
        }

Throttling: the OCR step is CPU-heavy (~0.5–2s per frame on Termux). The
server processes one frame at a time per connection; if the client sends
faster, we drop the older queued frame and keep the latest.
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..receipts.free_ocr import rapid_ocr_multipass
from ..receipts.parse_lines import BARCODE_RE, PRICE_RE, QTY_RE, _extract_barcodes


live_ocr_router = APIRouter()

# Live mode is latency-bound: the user moves the camera around and expects
# updates every second or two. A smaller max_long_side trades small-text
# recall for ~3× speedup. Receipts in the viewfinder are usually big enough
# that 800px is plenty.
LIVE_MAX_LONG_SIDE = 1600  # higher res = more digits readable; speed is secondary


def _classify(text: str) -> str:
    if _extract_barcodes(text):   # fuzzy: catches 1290→7290 and T290→7290 too
        return "barcode"
    if QTY_RE.search(text):
        return "qty"
    if PRICE_RE.search(text):
        return "price"
    return "text"


def _lookup_one(barcode: str) -> dict | None:
    from ..db.price_index import price_index
    info = price_index.lookup(barcode)
    if not info:
        return None
    cheapest = info.get("cheapest")
    priciest = info.get("priciest")
    return {
        "barcode":        barcode,
        "name":           info.get("name"),
        "manufacturer":   info.get("manufacturer"),
        "min_price":      cheapest["min_price"] if cheapest else None,
        "max_price":      priciest["max_price"] if priciest else None,
        "cheapest_chain": cheapest["chain_name_he"] if cheapest else None,
        "priciest_chain": priciest["chain_name_he"] if priciest else None,
        "all_chains":     info.get("chains", []),
    }


async def _claude_ocr_async(image_bytes: bytes, ws: "WebSocket") -> "OCRResult":
    """Run Claude Code OCR in a thread — sends heartbeats so WS stays alive."""
    import tempfile, os
    from ..receipts.free_ocr import OCRLine, OCRResult
    from ..receipts.claude_code_ocr import extract_barcodes as _claude_extract

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(image_bytes)
        tmp = f.name

    try:
        loop   = asyncio.get_event_loop()
        future = loop.run_in_executor(None, lambda: _claude_extract(tmp, timeout=180))

        # Poll every 4s — send heartbeat so browser knows we're alive
        elapsed = 0
        while not future.done():
            await asyncio.sleep(4)
            elapsed += 4
            if future.done():
                break
            try:
                await ws.send_text(json.dumps({"type": "processing",
                                               "engine": "claude",
                                               "elapsed_s": elapsed}))
            except Exception:
                future.cancel()
                raise WebSocketDisconnect()

        barcodes = future.result()
        lines = [OCRLine(text=bc, box=(0.0, 0.0, 1.0, 1.0), confidence=1.0)
                 for bc in barcodes]
        return OCRResult(lines=lines, provider="claude-code",
                         latency_ms=elapsed * 1000, image_size=(0, 0))
    finally:
        try: os.unlink(tmp)
        except OSError: pass


@live_ocr_router.websocket("/ws/live-ocr")
async def live_ocr(ws: WebSocket) -> None:
    await ws.accept()
    seen_barcodes: set[str] = set()
    basket_total: float = 0.0
    latest_frame: tuple[str, bytes, str] | None = None  # (id, raw, mode)
    current_mode: str = "rapid"

    async def reader():
        nonlocal latest_frame, current_mode
        try:
            while True:
                msg = await ws.receive_text()
                try:
                    obj = json.loads(msg)
                except json.JSONDecodeError:
                    continue
                if obj.get("type") == "reset":
                    seen_barcodes.clear()
                    nonlocal_basket_reset()
                    await ws.send_text(json.dumps({"type": "reset_ack"}))
                    continue
                if obj.get("type") != "frame":
                    continue
                b64 = obj.get("image", "")
                if "," in b64:
                    b64 = b64.split(",", 1)[1]
                try:
                    raw = base64.b64decode(b64)
                except (ValueError, TypeError):
                    continue
                mode = obj.get("mode", "rapid")
                current_mode = mode
                # Drop the previous queued frame; keep only the newest.
                latest_frame = (obj.get("id", ""), raw, mode)
        except WebSocketDisconnect:
            pass

    def nonlocal_basket_reset():
        nonlocal basket_total
        basket_total = 0.0

    async def worker():
        nonlocal latest_frame, basket_total
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(0.05)
            if latest_frame is None:
                continue
            frame_id, raw, mode = latest_frame
            latest_frame = None
            try:
                t0 = time.perf_counter()

                if mode == "claude":
                    ocr = await _claude_ocr_async(raw, ws)
                else:
                    ocr = await loop.run_in_executor(
                        None,
                        lambda: rapid_ocr_multipass(raw, max_long_side=LIVE_MAX_LONG_SIDE),
                    )
                lines_payload: list[dict[str, Any]] = []
                new_items: list[dict[str, Any]] = []
                for ln in ocr.lines:
                    kind = _classify(ln.text)
                    item_payload: dict[str, Any] = {
                        "text": ln.text,
                        "box": list(ln.box),
                        "conf": round(ln.confidence, 3),
                        "kind": kind,
                    }
                    lines_payload.append(item_payload)
                    if kind == "barcode":
                        for bc in _extract_barcodes(ln.text):
                            if bc in seen_barcodes:
                                continue
                            cat = _lookup_one(bc)
                            if not cat:
                                seen_barcodes.add(bc)
                                new_items.append({
                                    "barcode": bc,
                                    "name": None,
                                    "matched": False,
                                    "min_price": None,
                                    "box": list(ln.box),
                                })
                                continue
                            seen_barcodes.add(bc)
                            min_p = cat.get("min_price") or 0.0
                            basket_total += float(min_p or 0.0)
                            new_items.append({
                                "barcode":        bc,
                                "name":           cat.get("name"),
                                "manufacturer":   cat.get("manufacturer"),
                                "min_price":      min_p,
                                "max_price":      cat.get("max_price"),
                                "cheapest_chain": cat.get("cheapest_chain"),
                                "priciest_chain": cat.get("priciest_chain"),
                                "all_chains":     cat.get("all_chains", []),
                                "matched":        True,
                                "box":            list(ln.box),
                            })
                payload = {
                    "type": "result",
                    "id": frame_id,
                    "latency_ms": int((time.perf_counter() - t0) * 1000),
                    "lines": lines_payload,
                    "items": new_items,
                    "totals": {
                        "distinct_items": len(seen_barcodes),
                        "basket_total": round(basket_total, 2),
                    },
                }
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except WebSocketDisconnect:
                return
            except Exception as e:  # noqa: BLE001 — surface to client, keep loop alive
                try:
                    await ws.send_text(json.dumps({"type": "error", "error": str(e)}))
                except (WebSocketDisconnect, RuntimeError):
                    return

    try:
        await asyncio.gather(reader(), worker())
    except WebSocketDisconnect:
        pass
