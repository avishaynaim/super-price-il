"""OCR via the local Claude Code CLI — no API key required.

Uses `claude -p` in non-interactive mode with --allowedTools Read so Claude
can open the image file with its Read tool and return structured data.

This provider only works where the `claude` CLI is installed and authenticated
(same machine running Claude Code). It calls a subprocess so it's slower than
RapidOCR but vision-model accurate.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

try:
    from .free_ocr import OCRLine, OCRResult
except ImportError:
    # Allow running as a standalone script
    from free_ocr import OCRLine, OCRResult  # type: ignore


# ── prompts ──────────────────────────────────────────────────────────────────

_BARCODE_PROMPT = """Read the image at {path}.
Extract every barcode / product-code number visible on this receipt.
Israeli barcodes start with 729 and are 13 digits long.
Other EAN-13 barcodes are also 13 digits.

Return ONLY valid JSON — no markdown fences, no explanation:
{{"barcodes": ["7290...", "7290...", ...]}}"""

_BARCODE_BATCH_PROMPT = """Read each of these receipt images in order and extract all visible barcodes from all of them combined.
Images: {paths}

Israeli EAN-13 barcodes start with 729 and are 13 digits. Other 13-digit EAN codes are also valid.

Return ONLY valid JSON — no markdown fences, no explanation:
{{"barcodes": ["7290...", ...]}}"""

_FULL_RECEIPT_PROMPT = """Read the image at {path}.
This is a scanned Israeli supermarket receipt (thermal paper, Hebrew + numbers).

Return ONLY valid JSON — no markdown, no explanation:
{{
  "store": "<store name if visible>",
  "date": "<date if visible, YYYY-MM-DD>",
  "items": [
    {{"name": "<Hebrew or transliterated name>", "barcode": "<13-digit EAN or null>", "qty": <number>, "unit_price": <float>, "total_price": <float>}}
  ],
  "subtotal": <float or null>,
  "discount": <float or null>,
  "total": <float or null>,
  "raw_barcodes": ["<every 13-digit number visible, even if not matched to an item>"]
}}"""


# ── subprocess runner ─────────────────────────────────────────────────────────

def _run_claude(prompt: str, timeout: int = 120) -> str:
    """Run claude -p and return the text result. Raises on failure."""
    result = subprocess.run(
        [
            "claude", "-p", prompt,
            "--allowedTools", "Read",
            "--output-format", "json",
            "--dangerously-skip-permissions",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude CLI exited {result.returncode}: {result.stderr[:300]}")
    try:
        d = json.loads(result.stdout)
        return d.get("result", result.stdout)
    except json.JSONDecodeError:
        return result.stdout


def _parse_json_from_text(text: str) -> Any:
    """Extract JSON object from text that may have markdown fences."""
    text = text.strip()
    # Strip ```json ... ``` fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


# ── public API ────────────────────────────────────────────────────────────────

def _prepare_image(image_path: str | Path, max_long_side: int = 2000) -> tuple[str, bool]:
    """Return (path_to_use, is_temp). Downsizes if bigger than max_long_side."""
    import tempfile
    src = Path(image_path).resolve()
    try:
        from PIL import Image as _PIL
        img = _PIL.open(src)
        long = max(img.size)
        if long <= max_long_side:
            return str(src), False
        scale = max_long_side / long
        img = img.resize((int(img.size[0]*scale), int(img.size[1]*scale)), _PIL.LANCZOS)
        # Apply EXIF rotation so Claude reads the right orientation
        try:
            from PIL import ExifTags
            exif = img._getexif() or {}
            for tag, val in exif.items():
                if ExifTags.TAGS.get(tag) == "Orientation":
                    for deg, code in [(180, 3), (270, 6), (90, 8)]:
                        if val == code:
                            img = img.rotate(deg, expand=True)
        except Exception:
            pass
        if img.width > img.height:
            img = img.rotate(90, expand=True)
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            img.convert("RGB").save(f, "JPEG", quality=85)
            return f.name, True
    except Exception:
        return str(src), False


def extract_barcodes(image_path: str | Path, timeout: int = 120) -> list[str]:
    """Return list of EAN-13 barcode strings found in the image.

    Downsizes images >2000px before passing to Claude for faster processing.
    """
    path, is_temp = _prepare_image(image_path)
    try:
        prompt = _BARCODE_PROMPT.format(path=path)
        raw = _run_claude(prompt, timeout=timeout)
    finally:
        if is_temp:
            try:
                os.unlink(path)
            except OSError:
                pass
    try:
        d = _parse_json_from_text(raw)
        return [str(b) for b in d.get("barcodes", [])]
    except Exception:
        return re.findall(r"\b\d{13}\b", raw)


def extract_barcodes_batch(image_paths: list[str | Path], timeout_per_image: int = 150) -> list[str]:
    """Extract barcodes from multiple receipt images sequentially.

    Runs one `claude -p` call per image in order. Device resource constraints
    prevent concurrent calls from completing reliably. Failed images are skipped.
    Returns deduplicated list of all barcodes found across all images.
    """
    all_barcodes: list[str] = []
    for path in image_paths:
        try:
            barcodes = extract_barcodes(path, timeout=timeout_per_image)
            all_barcodes.extend(barcodes)
        except Exception:
            continue  # skip photos that time out or fail

    seen: set[str] = set()
    return [bc for bc in all_barcodes if not (bc in seen or seen.add(bc))]  # type: ignore[func-returns-value]


def parse_receipt(image_path: str | Path, timeout: int = 180) -> dict:
    """Return full structured receipt dict from an image.

    Slower path (~15–30s) — returns items, prices, barcodes, store, total.
    """
    path = str(Path(image_path).resolve())
    prompt = _FULL_RECEIPT_PROMPT.format(path=path)
    raw = _run_claude(prompt, timeout=timeout)
    try:
        return _parse_json_from_text(raw)
    except Exception:
        return {"raw_text": raw, "raw_barcodes": re.findall(r"\b\d{13}\b", raw)}


def ocr_to_result(image_path: str | Path, timeout: int = 120) -> OCRResult:
    """Wrap extract_barcodes into the standard OCRResult used by the pipeline.

    Each barcode becomes one OCRLine with a synthetic full-width bounding box.
    """
    t0 = time.perf_counter()
    path = Path(image_path).resolve()

    try:
        from PIL import Image
        img = Image.open(path)
        w, h = img.size
    except Exception:
        w, h = 1, 1

    barcodes = extract_barcodes(path, timeout=timeout)
    latency = int((time.perf_counter() - t0) * 1000)

    lines = [
        OCRLine(text=bc, box=(0.0, i / max(len(barcodes), 1), 1.0, 1 / max(len(barcodes), 1)), confidence=0.95)
        for i, bc in enumerate(barcodes)
    ]
    return OCRResult(
        lines=lines,
        provider="claude-code",
        latency_ms=latency,
        image_size=(w, h),
    )


# ── benchmark helper ──────────────────────────────────────────────────────────

def benchmark(image_paths: list[str | Path], ground_truth: set[str] | None = None) -> dict:
    """Run extract_barcodes on every photo and return a summary dict."""
    all_found: set[str] = set()
    results_per_photo = {}
    total_ms = 0

    for p in image_paths:
        t0 = time.perf_counter()
        try:
            barcodes = extract_barcodes(p)
            elapsed = int((time.perf_counter() - t0) * 1000)
            all_found |= set(barcodes)
            results_per_photo[str(Path(p).name)] = {"barcodes": barcodes, "ms": elapsed}
            total_ms += elapsed
        except Exception as e:
            results_per_photo[str(Path(p).name)] = {"error": str(e)}

    result = {
        "all_barcodes": sorted(all_found),
        "per_photo": results_per_photo,
        "total_ms": total_ms,
    }
    if ground_truth:
        hit = all_found & ground_truth
        result["recall_pct"] = len(hit) / len(ground_truth) * 100
        result["barcodes_hit"] = sorted(hit)
        result["barcodes_missed"] = sorted(ground_truth - all_found)

    return result


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="Claude Code OCR for receipt photos")
    parser.add_argument("images", nargs="+", help="Image file paths")
    parser.add_argument("--full", action="store_true", help="Full receipt parse (items + prices)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    for img in args.images:
        print(f"\n{'─'*60}")
        print(f"Image: {img}")
        if args.full:
            result = parse_receipt(img)
        else:
            barcodes = extract_barcodes(img)
            result = {"barcodes": barcodes, "count": len(barcodes)}

        if args.json or args.full:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            for bc in result.get("barcodes", []):
                print(f"  {bc}")
            print(f"  → {result['count']} barcodes found")
