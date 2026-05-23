#!/usr/bin/env python3
"""OCR engine benchmark — finds the best engine for reading Israeli receipt barcodes.

Tests every available free OCR engine on the 4 receipt photos taken tonight.
Ground truth barcodes extracted by human reading from the photos.
Outputs a ranked table of engines by recall (barcodes found / total known).

Run: python3 scripts/ocr_benchmark.py
Results written to: /tmp/ocr_benchmark_results.txt
"""
from __future__ import annotations
import re, time, io, subprocess, sys, traceback, os
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter

RECEIPT_PHOTOS = [
    "/sdcard/DCIM/Camera/20260503_231614.jpg",
    "/sdcard/DCIM/Camera/20260503_231616.jpg",
    "/sdcard/DCIM/Camera/20260503_231618.jpg",
    "/sdcard/DCIM/Camera/20260503_231621.jpg",
]

# Ground truth barcodes visible in these receipts (read by human from the photos)
GROUND_TRUTH = {
    "7290110115388", "7290000208428", "7290100243084", "7290004129156",
    "7290000474502", "7290010777525", "7290113195851", "7290018989333",
    "7290004131074", "7290002787891", "7290112963918", "7290000211503",
    "7290115203936", "7290011725006", "7290018825006", "7290110114909",
    "7290117034798", "7290117034828", "7422592419017", "7290000178911",
}

LOG = Path("/tmp/ocr_benchmark_results.txt")

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG.open("a") as f:
        f.write(line + "\n")

def preprocess(img: Image.Image, mode: str) -> Image.Image:
    if img.width > img.height:
        img = img.rotate(90, expand=True)
    if mode == "raw":
        return img
    if mode == "gray":
        return img.convert("L").convert("RGB")
    if mode == "contrast":
        img = img.convert("L")
        img = ImageEnhance.Contrast(img).enhance(2.5)
        return img.convert("RGB")
    if mode == "sharpen":
        img = img.convert("L")
        img = ImageEnhance.Contrast(img).enhance(2.0)
        img = ImageEnhance.Sharpness(img).enhance(2.0)
        return img.convert("RGB")
    if mode == "binarize":
        img = img.convert("L")
        img = img.point(lambda x: 0 if x < 120 else 255)
        return img.convert("RGB")
    if mode == "adaptive":
        import numpy as np
        img = img.convert("L")
        arr = np.array(img)
        from PIL import Image as PILImage
        # Simple adaptive threshold using local mean
        from scipy.ndimage import uniform_filter
        try:
            mean = uniform_filter(arr.astype(float), size=51)
            binary = ((arr.astype(float) > mean * 0.85) * 255).astype("uint8")
            return PILImage.fromarray(binary).convert("RGB")
        except Exception:
            return img.convert("RGB")
    return img

def extract_barcodes(text: str) -> set[str]:
    """Extract Israeli EAN-13 barcodes with fuzzy prefix correction."""
    text = text.replace(" ", "").replace("\n", "")
    # Fix common OCR errors: 1→7 at position 0, T→7, O→0, I→1
    normalized = re.sub(r"(?<!\d)1(290\d{9})(?!\d)", r"7\1", text)
    normalized = normalized.replace("T290", "7290").replace("O", "0").replace("I", "1")
    found = set()
    for m in re.finditer(r"729[0-9]{10}", normalized):
        found.add(m.group(0))
    return found

def resize_for_ocr(img: Image.Image, max_long: int = 3000) -> Image.Image:
    long = max(img.size)
    if long > max_long:
        scale = max_long / long
        img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
    return img

def load_photos() -> list[tuple[str, Image.Image]]:
    photos = []
    for p in RECEIPT_PHOTOS:
        try:
            img = Image.open(p)
            photos.append((Path(p).name, img))
            log(f"  Loaded {Path(p).name} {img.size}")
        except Exception as e:
            log(f"  Could not load {p}: {e}")
    return photos

# ─── OCR engines ────────────────────────────────────────────────────────────

def run_rapidocr(img: Image.Image) -> set[str]:
    os.environ.setdefault("ORT_DISABLE_ALL_LOG_OUTPUT", "1")
    from rapidocr_onnxruntime import RapidOCR
    ocr = RapidOCR()
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=95)
    result, _ = ocr(buf.getvalue())
    text = " ".join(r[1] for r in (result or []))
    return extract_barcodes(text)

def run_tesseract(img: Image.Image, psm: int = 6, lang: str = "eng") -> set[str]:
    import tempfile
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "PNG")
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(buf.getvalue())
        tmp = f.name
    try:
        r = subprocess.run(
            ["tesseract", tmp, "stdout", f"--psm", str(psm), lang],
            capture_output=True, text=True, timeout=60
        )
        return extract_barcodes(r.stdout)
    finally:
        os.unlink(tmp)

def run_tesseract_digits(img: Image.Image, psm: int = 6) -> set[str]:
    import tempfile
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "PNG")
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(buf.getvalue())
        tmp = f.name
    try:
        r = subprocess.run(
            ["tesseract", tmp, "stdout", f"--psm", str(psm), "eng",
             "-c", "tessedit_char_whitelist=0123456789"],
            capture_output=True, text=True, timeout=60
        )
        return extract_barcodes(r.stdout)
    finally:
        os.unlink(tmp)

def run_easyocr(img: Image.Image) -> set[str]:
    import easyocr
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=95)
    results = reader.readtext(buf.getvalue(), detail=0)
    return extract_barcodes(" ".join(results))

def run_paddleocr(img: Image.Image) -> set[str]:
    from paddleocr import PaddleOCR
    ocr = PaddleOCR(use_angle_cls=True, lang="en", use_gpu=False, show_log=False)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=95)
    result = ocr.ocr(buf.getvalue(), cls=True)
    text = " ".join(r[1][0] for line in (result or []) for r in (line or []))
    return extract_barcodes(text)

def run_doctr(img: Image.Image) -> set[str]:
    from doctr.io import DocumentFile
    from doctr.models import ocr_predictor
    model = ocr_predictor(pretrained=True)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=95)
    doc = DocumentFile.from_images([buf.getvalue()])
    result = model(doc)
    text = " ".join(
        word.value
        for page in result.pages
        for block in page.blocks
        for line in block.lines
        for word in line.words
    )
    return extract_barcodes(text)

def run_trocr(img: Image.Image) -> set[str]:
    from transformers import TrOCRProcessor, VisionEncoderDecoderModel
    processor = TrOCRProcessor.from_pretrained("microsoft/trocr-base-printed")
    model     = VisionEncoderDecoderModel.from_pretrained("microsoft/trocr-base-printed")
    # TrOCR works on single text-line crops — we split into horizontal strips
    import numpy as np
    arr = np.array(img.convert("RGB"))
    h, w = arr.shape[:2]
    strip_h = max(40, h // 30)
    found = set()
    for y in range(0, h - strip_h, strip_h // 2):
        strip = Image.fromarray(arr[y:y+strip_h])
        pixel_values = processor(strip, return_tensors="pt").pixel_values
        import torch
        with torch.no_grad():
            ids = model.generate(pixel_values)
        text = processor.batch_decode(ids, skip_special_tokens=True)[0]
        found |= extract_barcodes(text)
    return found

def run_surya(img: Image.Image) -> set[str]:
    from surya.ocr import run_ocr
    from surya.model.detection.model import load_model as load_det, load_processor as load_det_proc
    from surya.model.recognition.model import load_model as load_rec
    from surya.model.recognition.processor import load_processor as load_rec_proc
    langs = [["en"]]
    det_model, det_proc = load_det(), load_det_proc()
    rec_model, rec_proc  = load_rec(), load_rec_proc()
    results = run_ocr([img], langs, det_model, det_proc, rec_model, rec_proc)
    text = " ".join(l.text for r in results for l in r.text_lines)
    return extract_barcodes(text)

# ─── Engine registry ────────────────────────────────────────────────────────

ENGINES = [
    # (name, function, preprocess_mode, resize_px)
    ("rapidocr/raw",       run_rapidocr,         "raw",      2000),
    ("rapidocr/contrast",  run_rapidocr,         "contrast", 2000),
    ("rapidocr/gray",      run_rapidocr,         "gray",     2000),
    ("rapidocr/sharpen",   run_rapidocr,         "sharpen",  2000),
    ("tess-eng/psm3",      lambda i: run_tesseract(i, 3, "eng"), "contrast", 3000),
    ("tess-eng/psm6",      lambda i: run_tesseract(i, 6, "eng"), "contrast", 3000),
    ("tess-eng/psm11",     lambda i: run_tesseract(i,11, "eng"), "contrast", 3000),
    ("tess-heb/psm3",      lambda i: run_tesseract(i, 3, "heb"), "contrast", 3000),
    ("tess-heb/psm6",      lambda i: run_tesseract(i, 6, "heb"), "contrast", 3000),
    ("tess-digits/psm6",   lambda i: run_tesseract_digits(i, 6), "contrast", 3000),
    ("tess-digits/psm6-b", lambda i: run_tesseract_digits(i, 6), "binarize", 3000),
    ("tess-digits/psm11",  lambda i: run_tesseract_digits(i,11), "contrast", 3000),
    ("tess-eng/raw/psm6",  lambda i: run_tesseract(i, 6, "eng"), "raw",      3000),
    ("tess-eng/sharp/psm6",lambda i: run_tesseract(i, 6, "eng"), "sharpen",  3000),
]

OPTIONAL_ENGINES = [
    ("easyocr/raw",      run_easyocr,   "raw",      2000),
    ("easyocr/contrast", run_easyocr,   "contrast", 2000),
    ("paddleocr/raw",    run_paddleocr, "raw",      2000),
    ("doctr/raw",        run_doctr,     "raw",      2000),
    ("trocr/raw",        run_trocr,     "raw",      1500),
    ("surya/raw",        run_surya,     "raw",      2000),
]

def try_import(module: str) -> bool:
    try:
        __import__(module)
        return True
    except ImportError:
        return False

def build_engine_list():
    engines = list(ENGINES)
    if try_import("easyocr"):
        log("  ✓ EasyOCR available")
        engines += [e for e in OPTIONAL_ENGINES if e[0].startswith("easy")]
    if try_import("paddleocr"):
        log("  ✓ PaddleOCR available")
        engines += [e for e in OPTIONAL_ENGINES if e[0].startswith("paddle")]
    if try_import("doctr"):
        log("  ✓ doctr available")
        engines += [e for e in OPTIONAL_ENGINES if e[0].startswith("doctr")]
    if try_import("transformers"):
        log("  ✓ transformers available (TrOCR)")
        engines += [e for e in OPTIONAL_ENGINES if e[0].startswith("trocr")]
    if try_import("surya"):
        log("  ✓ surya available")
        engines += [e for e in OPTIONAL_ENGINES if e[0].startswith("surya")]
    return engines

# ─── Main benchmark loop ────────────────────────────────────────────────────

def run_benchmark(photos: list[tuple[str, Image.Image]], engines):
    results: dict[str, dict] = {}  # engine_name → {found, missed, time_ms}

    for eng_name, eng_fn, prep_mode, max_px in engines:
        log(f"\n{'─'*50}")
        log(f"ENGINE: {eng_name}")
        all_found: set[str] = set()
        total_ms = 0
        errors = 0
        for photo_name, img in photos:
            try:
                processed = preprocess(img.copy(), prep_mode)
                processed = resize_for_ocr(processed, max_px)
                t0 = time.perf_counter()
                found = eng_fn(processed)
                elapsed = int((time.perf_counter() - t0) * 1000)
                total_ms += elapsed
                all_found |= found
                gt_found = found & GROUND_TRUTH
                log(f"  {photo_name}: {len(gt_found)}/{len(GROUND_TRUTH)} in {elapsed}ms  {sorted(gt_found)[:3]}")
            except Exception as e:
                errors += 1
                log(f"  {photo_name}: ERROR — {e}")
                if errors >= 2:
                    log(f"  Too many errors, skipping engine")
                    break

        gt_found = all_found & GROUND_TRUTH
        recall = len(gt_found) / len(GROUND_TRUTH) * 100
        results[eng_name] = {
            "found": len(gt_found),
            "total": len(GROUND_TRUTH),
            "recall_pct": recall,
            "time_ms": total_ms,
            "barcodes": sorted(gt_found),
            "extra": sorted(all_found - GROUND_TRUTH),
        }
        log(f"  TOTAL: {len(gt_found)}/{len(GROUND_TRUTH)} = {recall:.0f}% recall | {total_ms}ms")

    return results

def print_ranking(results: dict):
    log("\n" + "═"*60)
    log("FINAL RANKING")
    log("═"*60)
    ranked = sorted(results.items(), key=lambda x: (-x[1]["recall_pct"], x[1]["time_ms"]))
    for i, (name, r) in enumerate(ranked, 1):
        log(f"#{i:2d}  {name:30s}  {r['found']:2d}/{r['total']} = {r['recall_pct']:5.1f}%  {r['time_ms']}ms")
    log("\nBest engine: " + ranked[0][0] if ranked else "none")
    log("Top 5 barcodes found by best:")
    if ranked:
        for bc in ranked[0][1]["barcodes"][:10]:
            log(f"  {bc}")

if __name__ == "__main__":
    LOG.write_text(f"OCR Benchmark started {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log("Loading receipt photos...")
    photos = load_photos()
    if not photos:
        log("No photos found! Exiting.")
        sys.exit(1)
    log(f"Loaded {len(photos)} photos. Ground truth: {len(GROUND_TRUTH)} barcodes")
    log("Building engine list...")
    engines = build_engine_list()
    log(f"Testing {len(engines)} engine configurations")
    results = run_benchmark(photos, engines)
    print_ranking(results)
    log(f"\nDone. Results in {LOG}")
