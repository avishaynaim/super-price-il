"""Free OCR providers — local RapidOCR (PP-OCRv4 ONNX) and OCR.space web API.

Each provider returns a list of OCRLine: text + bounding box (normalized 0..1)
+ confidence. The bounding box matters for the live-OCR overlay so the frontend
can draw a rectangle over the camera feed at the right place.

Empirical barcode recall (6 Israeli supermarket receipt photos, 20 GT barcodes):
  1. Claude vision (claude_code_ocr)   95%  — gold standard, needs running Claude session
  2. OCR.space Engine 2 (cloud)        85%  — free tier, rate-limited
  3. RapidOCR multipass (local)       ~75%  — 3 preprocessing modes unioned, no API cost
  4. RapidOCR single mode (local)      75%  — fast; multipass adds robustness
  5. Tesseract                         35%  — local, poor on thermal paper
  6. newocr.com                         0%  — site dead/changed

Default provider: 'rapid-multi' (local, no network needed).
Auto chain: rapid-multi → ocrspace → claude.
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


def _fix_orientation(img):
    """Apply EXIF rotation, then force portrait if still landscape.
    Receipts are always taller than wide — if we get a landscape image
    after EXIF correction, rotate it 90° CW so OCR reads rows correctly.
    """
    try:
        from PIL import ExifTags
        exif = img._getexif()
        if exif:
            for tag, val in exif.items():
                if ExifTags.TAGS.get(tag) == "Orientation":
                    rotations = {3: 180, 6: 270, 8: 90}
                    deg = rotations.get(val)
                    if deg:
                        img = img.rotate(deg, expand=True)
                    break
    except Exception:
        pass
    # If still landscape, rotate to portrait (receipts are always portrait)
    if img.width > img.height:
        img = img.rotate(90, expand=True)
    return img


def _enhance_receipt(img, mode: str = "sharpen"):
    """Boost contrast on faded thermal paper.

    mode='raw'      → no enhancement (just RGB conversion)
    mode='contrast' → strong contrast boost (Contrast 2.5), best for clear text
    mode='sharpen'  → contrast + sharpness (default, good for faded receipts)
    """
    from PIL import ImageEnhance
    if mode == "raw":
        return img.convert("RGB")
    img = img.convert("L")  # grayscale
    if mode == "contrast":
        img = ImageEnhance.Contrast(img).enhance(2.5)
    else:  # sharpen
        img = ImageEnhance.Contrast(img).enhance(2.0)
        img = ImageEnhance.Sharpness(img).enhance(2.0)
    return img.convert("RGB")


def rapid_ocr(
    image_bytes: bytes,
    max_long_side: int = 1600,
    center_crop: float | None = None,
    preprocess: bool = True,
    enhance_only: bool = False,
    mode: str = "sharpen",
) -> OCRResult:
    """Run RapidOCR on an image.

    preprocess=True  → full preprocessing (EXIF rotation + contrast boost) for uploads.
    enhance_only=True → only contrast boost, no rotation (for live camera frames).
    mode             → preprocessing mode: 'raw', 'contrast', or 'sharpen' (default).
    """
    from PIL import Image
    t0 = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if preprocess and not enhance_only:
        img = _fix_orientation(img)
        img = _enhance_receipt(img, mode=mode)
    elif enhance_only:
        img = _enhance_receipt(img, mode=mode)
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


def rapid_ocr_multipass(image_bytes: bytes, max_long_side: int = 1500) -> OCRResult:
    """Run RapidOCR with 3 preprocessing modes and concatenate all lines.

    Runs raw, contrast, and sharpen passes then concatenates all OCRLine results.
    Each mode renders digits differently so parse_lines gets the widest possible
    text coverage. Adds robustness over single-pass at the cost of 3× compute.
    Typical recall: ~75% on Israeli thermal receipts.
    """
    t0 = time.perf_counter()
    merged_lines: list[OCRLine] = []
    first_size = (0, 0)

    for i, mode in enumerate(("raw", "contrast", "sharpen")):
        try:
            r = rapid_ocr(image_bytes, max_long_side=max_long_side, preprocess=True, mode=mode)
            if i == 0:
                first_size = r.image_size
            merged_lines.extend(r.lines)
        except Exception:
            pass  # if one mode fails, continue with others

    return OCRResult(
        lines=merged_lines,
        provider="rapidocr-multipass",
        latency_ms=int((time.perf_counter() - t0) * 1000),
        image_size=first_size,
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
    img = Image.open(io.BytesIO(image_bytes))
    img = _fix_orientation(img)
    img = _enhance_receipt(img)
    img = img.convert("RGB")
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
    "rapid":         "rapid_ocr",
    "rapidocr":      "rapid_ocr",
    "rapid-multi":   "rapid_ocr_multipass",
    "rapidocr-multi":"rapid_ocr_multipass",
    "ocrspace":      "ocrspace_ocr",
}


def _claude_code_ocr_from_bytes(image_bytes: bytes) -> "OCRResult":
    """Write bytes to a temp file and delegate to claude_code_ocr."""
    import tempfile, os
    from . import claude_code_ocr
    suffix = ".jpg"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(image_bytes)
        tmp = f.name
    try:
        return claude_code_ocr.ocr_to_result(tmp)
    finally:
        os.unlink(tmp)


def run_provider(name: str, image_bytes: bytes) -> OCRResult:
    import sys
    if name.lower() in ("claude", "claude-code"):
        return _claude_code_ocr_from_bytes(image_bytes)
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
