"""Free OCR providers — local RapidOCR (PP-OCRv4 ONNX) and OCR.space web API.

Each provider returns a list of OCRLine: text + bounding box (normalized 0..1)
+ confidence. The bounding box matters for the live-OCR overlay so the frontend
can draw a rectangle over the camera feed at the right place.

Empirical ranking from /tmp/ocr_test (3 crumpled Hebrew receipts):
  1. RapidOCR raw                 36 valid 7290… barcodes  (best)
  2. OCR.space Engine 2 binarized 20 barcodes
  3. RapidOCR binarized           12 barcodes
  4. OCR.space Engine 3            10 barcodes (loops rows)
  5. Tesseract heb+eng              0 barcodes — useless

So default chain is rapid → ocrspace → claude.
"""
from __future__ import annotations

import base64
import io
import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Sequence


@dataclass
class OCRLine:
    text: str
    # box normalized to [0,1] image coords — (x, y, w, h) of axis-aligned rect
    box: tuple[float, float, float, float]
    confidence: float


@dataclass
class OCRResult:
    lines: list[OCRLine]
    provider: str
    latency_ms: int
    image_size: tuple[int, int]   # (w, h) of the image actually fed to OCR


# ----------------------------------------------------------------------------
# RapidOCR — local, fastest, best on these receipts.
# ----------------------------------------------------------------------------

_rapid_singleton = None


def _rapid():
    """Lazy single instance — loading models takes ~1s and they're ~50MB."""
    global _rapid_singleton
    if _rapid_singleton is not None:
        return _rapid_singleton
    # Silence onnxruntime stderr noise
    os.environ.setdefault("ORT_DISABLE_ALL_LOG_OUTPUT", "1")
    from rapidocr_onnxruntime import RapidOCR  # type: ignore[import-not-found]
    _rapid_singleton = RapidOCR()
    return _rapid_singleton


def _to_norm_box(quad, w: int, h: int) -> tuple[float, float, float, float]:
    """Convert RapidOCR's 4-point polygon to a normalized (x,y,w,h) rect."""
    xs = [p[0] for p in quad]
    ys = [p[1] for p in quad]
    x0, y0 = min(xs), min(ys)
    x1, y1 = max(xs), max(ys)
    return (x0 / w, y0 / h, (x1 - x0) / w, (y1 - y0) / h)


def rapid_ocr(
    image_bytes: bytes,
    max_long_side: int = 1600,
    center_crop: float | None = None,
) -> OCRResult:
    """Run RapidOCR on an image.

    `max_long_side` caps the input dimension before OCR. Smaller = faster, at
    the cost of small-text recall. 1600 for upload, ~800 for live frames.

    `center_crop` (0..1) keeps only the center fraction of width *and* height.
    Boxes returned are still in normalized [0,1] of the *cropped* region —
    callers using this for live mode should map them back if they need to
    overlay on the full uncropped frame.
    """
    from PIL import Image
    t0 = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if center_crop and 0 < center_crop < 1:
        cw, ch = img.size
        kw = int(cw * center_crop); kh = int(ch * center_crop)
        x = (cw - kw) // 2; y = (ch - kh) // 2
        img = img.crop((x, y, x + kw, y + kh))
    long_side = max(img.size)
    if long_side > max_long_side:
        scale = max_long_side / long_side
        img = img.resize(
            (int(img.size[0] * scale), int(img.size[1] * scale)),
            Image.LANCZOS,
        )
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=85)
    w, h = img.size

    ocr = _rapid()
    result, _meta = ocr(buf.getvalue())
    lines: list[OCRLine] = []
    for row in result or []:
        # row is (box, text, conf) in newer rapidocr; sometimes just (text, conf).
        if len(row) == 3:
            quad, text, conf = row
            box = _to_norm_box(quad, w, h)
        else:
            text, conf = row[0], row[1]
            box = (0.0, 0.0, 1.0, 0.05)
        try:
            c = float(conf)
        except (TypeError, ValueError):
            c = 0.0
        lines.append(OCRLine(text=str(text), box=box, confidence=c))
    return OCRResult(
        lines=lines,
        provider="rapidocr",
        latency_ms=int((time.perf_counter() - t0) * 1000),
        image_size=(w, h),
    )


# ----------------------------------------------------------------------------
# OCR.space — web API fallback; Engine 2 is the only usable one for Hebrew.
# Free tier with 'helloworld' key is rate-limited; users can override with
# OCRSPACE_API_KEY env var for their own quota.
# ----------------------------------------------------------------------------

def ocrspace_ocr(image_bytes: bytes, engine: int = 2) -> OCRResult:
    from PIL import Image
    t0 = time.perf_counter()
    # OCR.space free tier: 1MB max. Re-encode and downscale.
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    long_side = max(img.size)
    if long_side > 1600:
        scale = 1600 / long_side
        img = img.resize(
            (int(img.size[0] * scale), int(img.size[1] * scale)),
            Image.LANCZOS,
        )
    w, h = img.size
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=70, optimize=True)
    payload = buf.getvalue()
    if len(payload) > 1024 * 1024:
        # try harder
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=55, optimize=True)
        payload = buf.getvalue()

    key = os.environ.get("OCRSPACE_API_KEY", "helloworld")
    # Use curl rather than requests — keeps the dep surface tiny and matches
    # what we already validated in /tmp/ocr_test.
    p = subprocess.run(
        [
            "curl", "-s", "-X", "POST", "https://api.ocr.space/parse/image",
            "-H", f"apikey: {key}",
            "-F", f"OCREngine={engine}",
            "-F", "scale=true",
            "-F", "isOverlayRequired=true",
            "-F", "file=@-;filename=receipt.jpg;type=image/jpeg",
        ],
        input=payload, capture_output=True, timeout=60,
    )
    try:
        d = json.loads(p.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"OCR.space returned non-JSON: {p.stdout[:200]}") from e
    if d.get("IsErroredOnProcessing"):
        raise RuntimeError(f"OCR.space error: {d.get('ErrorMessage')}")

    lines: list[OCRLine] = []
    for r in d.get("ParsedResults") or []:
        overlay = r.get("TextOverlay") or {}
        for line in overlay.get("Lines") or []:
            text = line.get("LineText", "")
            words = line.get("Words") or []
            if not words:
                continue
            x = min(w_["Left"] for w_ in words)
            y = min(w_["Top"] for w_ in words)
            x1 = max(w_["Left"] + w_["Width"] for w_ in words)
            y1 = max(w_["Top"] + w_["Height"] for w_ in words)
            box = (x / w, y / h, (x1 - x) / w, (y1 - y) / h)
            lines.append(OCRLine(text=text, box=box, confidence=0.85))

    return OCRResult(
        lines=lines,
        provider=f"ocrspace-e{engine}",
        latency_ms=int((time.perf_counter() - t0) * 1000),
        image_size=(w, h),
    )


# ----------------------------------------------------------------------------
# Provider chooser
# ----------------------------------------------------------------------------

# Map provider name → attribute name on this module. Resolved at call time so
# tests can `monkeypatch.setattr(free_ocr, "rapid_ocr", stub)` and have it stick.
PROVIDER_FUNCS = {
    "rapid":    "rapid_ocr",
    "rapidocr": "rapid_ocr",
    "ocrspace": "ocrspace_ocr",
}


def run_provider(name: str, image_bytes: bytes) -> OCRResult:
    import sys
    attr = PROVIDER_FUNCS.get(name.lower())
    if attr is None:
        raise ValueError(f"unknown OCR provider: {name!r}; known: {sorted(PROVIDER_FUNCS)}")
    fn = getattr(sys.modules[__name__], attr)
    return fn(image_bytes)


def run_chain(image_bytes: bytes, chain: Sequence[str]) -> OCRResult:
    """Try each provider in order; return first non-empty result. Raises if all
    fail. The fallback is per-failure (exception or zero lines)."""
    last_exc: Exception | None = None
    for name in chain:
        try:
            r = run_provider(name, image_bytes)
            if r.lines:
                return r
        except Exception as e:  # noqa: BLE001 — we want to fall through
            last_exc = e
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"all OCR providers in chain {list(chain)} returned no lines")
