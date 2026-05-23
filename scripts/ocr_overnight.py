#!/usr/bin/env python3
"""Overnight OCR benchmark watcher.

Each run (every 10 min):
  1. Scans /sdcard/DCIM/Camera/ for receipt photos (today + yesterday).
  2. Loads cached results from /tmp/ocr_overnight_state.json.
  3. Runs only engines that haven't yet succeeded on the current photo set.
  4. Writes updated state + human-readable summary.

Morning report: python3 scripts/ocr_overnight.py --report
"""
from __future__ import annotations
import argparse, io, json, os, re, subprocess, sys, time, traceback
from pathlib import Path
from PIL import Image, ImageEnhance

STATE_FILE  = Path("/tmp/ocr_overnight_state.json")
LOG_FILE    = Path("/tmp/ocr_overnight.log")
CAMERA_DIR  = Path("/sdcard/DCIM/Camera")

# Ground truth barcodes from tonight's receipts (human-verified)
GROUND_TRUTH = {
    "7290110115388", "7290000208428", "7290100243084", "7290004129156",
    "7290000474502", "7290010777525", "7290113195851", "7290018989333",
    "7290004131074", "7290002787891", "7290112963918", "7290000211503",
    "7290115203936", "7290011725006", "7290018825006", "7290110114909",
    "7290117034798", "7290117034828", "7422592419017", "7290000178911",
}


# ── logging ──────────────────────────────────────────────────────────────────

def log(msg: str, also_print: bool = True):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    if also_print:
        print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


# ── image utils ──────────────────────────────────────────────────────────────

def load_image(path: str) -> Image.Image:
    img = Image.open(path)
    # EXIF rotation
    try:
        from PIL import ExifTags
        exif = img._getexif()
        if exif:
            for tag, val in exif.items():
                if ExifTags.TAGS.get(tag) == "Orientation":
                    for deg, code in [(180, 3), (270, 6), (90, 8)]:
                        if val == code:
                            img = img.rotate(deg, expand=True)
                    break
    except Exception:
        pass
    if img.width > img.height:
        img = img.rotate(90, expand=True)
    return img


def preprocess(img: Image.Image, mode: str) -> Image.Image:
    if mode == "raw":
        return img.convert("RGB")
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
    return img.convert("RGB")


def resize(img: Image.Image, max_long: int) -> Image.Image:
    long = max(img.size)
    if long > max_long:
        scale = max_long / long
        img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
    return img


def to_jpeg_bytes(img: Image.Image, quality: int = 90) -> bytes:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=quality)
    return buf.getvalue()


# ── barcode extraction ────────────────────────────────────────────────────────

def extract_barcodes(text: str) -> set[str]:
    text = text.replace(" ", "").replace("\n", "")
    text = re.sub(r"(?<!\d)1(290\d{9})(?!\d)", r"7\1", text)
    text = text.replace("T290", "7290").replace("O", "0").replace("I", "1")
    found = set()
    for m in re.finditer(r"729[0-9]{10}", text):
        found.add(m.group(0))
    # Also grab other EAN-13s (non-7290 barcodes on receipts)
    for m in re.finditer(r"\d{13}", text):
        found.add(m.group(0))
    return found


# ── OCR engines ──────────────────────────────────────────────────────────────

def _try_import(module: str) -> bool:
    try:
        __import__(module)
        return True
    except ImportError:
        return False


def _run_tesseract(img: Image.Image, psm: int, lang: str, whitelist: str | None = None) -> set[str]:
    import tempfile
    buf = to_jpeg_bytes(img)
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(buf); tmp = f.name
    try:
        cmd = ["tesseract", tmp, "stdout", "--psm", str(psm), lang]
        if whitelist:
            cmd += ["-c", f"tessedit_char_whitelist={whitelist}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return extract_barcodes(r.stdout)
    finally:
        os.unlink(tmp)


_rapid_inst = None
def _rapid():
    global _rapid_inst
    if _rapid_inst is None:
        os.environ.setdefault("ORT_DISABLE_ALL_LOG_OUTPUT", "1")
        from rapidocr_onnxruntime import RapidOCR
        _rapid_inst = RapidOCR()
    return _rapid_inst


def _run_rapidocr(img: Image.Image) -> set[str]:
    ocr = _rapid()
    result, _ = ocr(to_jpeg_bytes(img))
    return extract_barcodes(" ".join(r[1] for r in (result or [])))


def _run_easyocr(img: Image.Image) -> set[str]:
    import easyocr
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    results = reader.readtext(to_jpeg_bytes(img), detail=0)
    return extract_barcodes(" ".join(results))


def _run_paddleocr(img: Image.Image) -> set[str]:
    from paddleocr import PaddleOCR
    ocr = PaddleOCR(use_angle_cls=True, lang="en", use_gpu=False, show_log=False)
    result = ocr.ocr(to_jpeg_bytes(img), cls=True)
    text = " ".join(r[1][0] for line in (result or []) for r in (line or []))
    return extract_barcodes(text)


def _run_doctr(img: Image.Image) -> set[str]:
    from doctr.io import DocumentFile
    from doctr.models import ocr_predictor
    model = ocr_predictor(pretrained=True)
    doc = DocumentFile.from_images([to_jpeg_bytes(img)])
    result = model(doc)
    text = " ".join(
        word.value
        for page in result.pages
        for block in page.blocks
        for line in block.lines
        for word in line.words
    )
    return extract_barcodes(text)


def _run_trocr(img: Image.Image) -> set[str]:
    from transformers import TrOCRProcessor, VisionEncoderDecoderModel
    import torch, numpy as np
    processor = TrOCRProcessor.from_pretrained("microsoft/trocr-base-printed")
    model = VisionEncoderDecoderModel.from_pretrained("microsoft/trocr-base-printed")
    arr = np.array(img.convert("RGB"))
    h, w = arr.shape[:2]
    strip_h = max(40, h // 30)
    found = set()
    for y in range(0, h - strip_h, strip_h // 2):
        strip = Image.fromarray(arr[y:y+strip_h])
        pv = processor(strip, return_tensors="pt").pixel_values
        with torch.no_grad():
            ids = model.generate(pv)
        found |= extract_barcodes(processor.batch_decode(ids, skip_special_tokens=True)[0])
    return found


def _run_surya(img: Image.Image) -> set[str]:
    from surya.ocr import run_ocr
    from surya.model.detection.model import load_model as ld, load_processor as lp
    from surya.model.recognition.model import load_model as lr
    from surya.model.recognition.processor import load_processor as lrp
    results = run_ocr([img], [["en"]], ld(), lp(), lr(), lrp())
    return extract_barcodes(" ".join(l.text for r in results for l in r.text_lines))


def _run_ocrspace(img: Image.Image, engine: int = 2) -> set[str]:
    key = os.environ.get("OCRSPACE_API_KEY", "helloworld")
    buf = to_jpeg_bytes(img, quality=70)
    if len(buf) > 900_000:
        buf = to_jpeg_bytes(resize(img, 1200), quality=60)
    p = subprocess.run(
        ["curl", "-s", "-X", "POST", "https://api.ocr.space/parse/image",
         "-H", f"apikey: {key}", "-F", f"OCREngine={engine}",
         "-F", "scale=true", "-F", "file=@-;filename=r.jpg;type=image/jpeg"],
        input=buf, capture_output=True, timeout=60,
    )
    try:
        d = json.loads(p.stdout)
    except Exception:
        return set()
    if d.get("IsErroredOnProcessing"):
        return set()
    text = " ".join(
        r.get("ParsedText", "")
        for r in (d.get("ParsedResults") or [])
    )
    return extract_barcodes(text)


def _run_google_vision(img: Image.Image) -> set[str]:
    """Google Cloud Vision via REST — works with GOOGLE_APPLICATION_CREDENTIALS or API key."""
    import base64
    key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    creds_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not key and not creds_file:
        raise RuntimeError("no GOOGLE_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS")
    buf = base64.b64encode(to_jpeg_bytes(img)).decode()
    payload = json.dumps({"requests": [{"image": {"content": buf}, "features": [{"type": "TEXT_DETECTION"}]}]})
    url = f"https://vision.googleapis.com/v1/images:annotate?key={key}" if key else "https://vision.googleapis.com/v1/images:annotate"
    p = subprocess.run(["curl", "-s", "-X", "POST", url, "-H", "Content-Type: application/json", "-d", "@-"],
                       input=payload.encode(), capture_output=True, timeout=60)
    try:
        d = json.loads(p.stdout)
        text = d["responses"][0]["fullTextAnnotation"]["text"]
        return extract_barcodes(text)
    except Exception:
        return set()


def _run_azure_vision(img: Image.Image) -> set[str]:
    key = os.environ.get("AZURE_VISION_KEY", "")
    ep  = os.environ.get("AZURE_VISION_ENDPOINT", "")
    if not key or not ep:
        raise RuntimeError("AZURE_VISION_KEY / AZURE_VISION_ENDPOINT not set")
    buf = to_jpeg_bytes(img)
    url = ep.rstrip("/") + "/vision/v3.2/ocr"
    p = subprocess.run(["curl", "-s", "-X", "POST", url,
                        "-H", f"Ocp-Apim-Subscription-Key: {key}",
                        "-H", "Content-Type: application/octet-stream",
                        "--data-binary", "@-"],
                       input=buf, capture_output=True, timeout=60)
    try:
        d = json.loads(p.stdout)
        words = [w["text"] for reg in d.get("regions", []) for line in reg["lines"] for w in line["words"]]
        return extract_barcodes(" ".join(words))
    except Exception:
        return set()


def _run_llm_ocr(img: Image.Image) -> set[str]:
    """Claude vision as OCR fallback."""
    import base64
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    b64 = base64.b64encode(to_jpeg_bytes(img)).decode()
    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 512,
        "messages": [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
            {"type": "text", "text": "List every barcode number visible in this receipt image. Return ONLY the numeric strings, one per line."}
        ]}]
    })
    p = subprocess.run(["curl", "-s", "https://api.anthropic.com/v1/messages",
                        "-H", f"x-api-key: {key}",
                        "-H", "anthropic-version: 2023-06-01",
                        "-H", "Content-Type: application/json",
                        "-d", "@-"],
                       input=payload.encode(), capture_output=True, timeout=90)
    try:
        d = json.loads(p.stdout)
        text = d["content"][0]["text"]
        return extract_barcodes(text)
    except Exception:
        return set()


# ── Engine registry ───────────────────────────────────────────────────────────
# (name, fn, preprocess_mode, max_px, requires_module)

def _build_engine_list() -> list[tuple]:
    engines = [
        # Always-available (tesseract + rapidocr come pre-installed)
        ("rapidocr/raw",          _run_rapidocr,                            "raw",      2000, "rapidocr_onnxruntime"),
        ("rapidocr/contrast",     _run_rapidocr,                            "contrast", 2000, "rapidocr_onnxruntime"),
        ("rapidocr/gray",         _run_rapidocr,                            "gray",     2000, "rapidocr_onnxruntime"),
        ("rapidocr/sharpen",      _run_rapidocr,                            "sharpen",  2000, "rapidocr_onnxruntime"),
        ("tess-eng/psm3",         lambda i: _run_tesseract(i, 3, "eng"),   "contrast", 3000, None),
        ("tess-eng/psm6",         lambda i: _run_tesseract(i, 6, "eng"),   "contrast", 3000, None),
        ("tess-eng/psm11",        lambda i: _run_tesseract(i, 11,"eng"),   "contrast", 3000, None),
        ("tess-heb/psm3",         lambda i: _run_tesseract(i, 3, "heb"),   "contrast", 3000, None),
        ("tess-heb/psm6",         lambda i: _run_tesseract(i, 6, "heb"),   "contrast", 3000, None),
        ("tess-digits/psm6",      lambda i: _run_tesseract(i, 6, "eng", "0123456789"), "contrast", 3000, None),
        ("tess-digits/psm6-bin",  lambda i: _run_tesseract(i, 6, "eng", "0123456789"), "binarize", 3000, None),
        ("tess-digits/psm11",     lambda i: _run_tesseract(i, 11,"eng", "0123456789"), "contrast", 3000, None),
        ("tess-eng/raw/psm6",     lambda i: _run_tesseract(i, 6, "eng"),   "raw",      3000, None),
        ("tess-eng/sharp/psm6",   lambda i: _run_tesseract(i, 6, "eng"),   "sharpen",  3000, None),
        # Free web APIs
        ("ocrspace/e2/contrast",  lambda i: _run_ocrspace(i, 2),           "contrast", 1600, None),
        ("ocrspace/e3/contrast",  lambda i: _run_ocrspace(i, 3),           "contrast", 1600, None),
        ("ocrspace/e1/raw",       lambda i: _run_ocrspace(i, 1),           "raw",      1600, None),
        # Cloud (only include when credentials are present)
        *([("google-vision", _run_google_vision, "raw", 2000, None)]
          if os.environ.get("GOOGLE_VISION_API_KEY") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") else []),
        *([("azure-vision", _run_azure_vision, "raw", 2000, None)]
          if os.environ.get("AZURE_VISION_KEY") and os.environ.get("AZURE_VISION_ENDPOINT") else []),
        *([("claude-haiku", _run_llm_ocr, "raw", 2000, None)]
          if os.environ.get("ANTHROPIC_API_KEY") else []),
        # Optional local models
        ("easyocr/raw",           _run_easyocr,                             "raw",      2000, "easyocr"),
        ("easyocr/contrast",      _run_easyocr,                             "contrast", 2000, "easyocr"),
        ("paddleocr/raw",         _run_paddleocr,                           "raw",      2000, "paddleocr"),
        ("doctr/raw",             _run_doctr,                               "raw",      2000, "doctr"),
        ("trocr/raw",             _run_trocr,                               "raw",      1500, "transformers"),
        ("surya/raw",             _run_surya,                               "raw",      2000, "surya"),
    ]
    available = []
    for entry in engines:
        name, fn, prep, px, mod = entry
        if mod and not _try_import(mod):
            continue
        available.append((name, fn, prep, px))
    return available


# ── photo discovery ───────────────────────────────────────────────────────────

def find_receipt_photos() -> list[str]:
    """Return JPGs from camera dir taken today or yesterday."""
    today = time.strftime("%Y%m%d")
    yesterday = time.strftime("%Y%m%d", time.localtime(time.time() - 86400))
    photos = []
    if CAMERA_DIR.exists():
        for p in sorted(CAMERA_DIR.glob("*.jpg")) + sorted(CAMERA_DIR.glob("*.JPG")):
            stem = p.stem  # e.g. 20260503_231614
            date_part = stem[:8]
            if date_part in (today, yesterday):
                photos.append(str(p))
    return photos


# ── state management ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"results": {}, "photo_set_key": "", "iterations": 0}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── one benchmark iteration ──────────────────────────────────────────────────

def run_iteration():
    state = load_state()
    state["iterations"] = state.get("iterations", 0) + 1
    iteration = state["iterations"]
    log(f"\n{'='*60}")
    log(f"ITERATION {iteration}  —  {time.strftime('%Y-%m-%d %H:%M:%S')}")

    photos = find_receipt_photos()
    if not photos:
        log("No receipt photos found in camera roll for today/yesterday.")
        save_state(state)
        return

    photo_key = "|".join(sorted(photos))
    if state.get("photo_set_key") != photo_key:
        log(f"Photo set changed — resetting engine results.")
        state["results"] = {}
        state["photo_set_key"] = photo_key

    log(f"Photos ({len(photos)}): {[Path(p).name for p in photos]}")

    # Load images once
    loaded: list[tuple[str, Image.Image]] = []
    for p in photos:
        try:
            img = load_image(p)
            loaded.append((Path(p).name, img))
            log(f"  Loaded {Path(p).name} {img.size}")
        except Exception as e:
            log(f"  Could not load {p}: {e}")

    if not loaded:
        log("Could not load any photos.")
        save_state(state)
        return

    engines = _build_engine_list()
    log(f"Engines available: {len(engines)}")

    results = state.get("results", {})

    for eng_name, eng_fn, prep_mode, max_px in engines:
        if eng_name in results and results[eng_name].get("status") == "ok":
            log(f"  SKIP {eng_name} (already completed)")
            continue

        log(f"\n  ENGINE: {eng_name}")
        all_found: set[str] = set()
        total_ms = 0
        errors = 0

        last_error = ""
        for photo_name, img in loaded:
            try:
                proc = preprocess(img.copy(), prep_mode)
                proc = resize(proc, max_px)
                t0 = time.perf_counter()
                found = eng_fn(proc)
                elapsed = int((time.perf_counter() - t0) * 1000)
                total_ms += elapsed
                all_found |= found
                gt_found = found & GROUND_TRUTH
                log(f"    {photo_name}: {len(gt_found)}/{len(GROUND_TRUTH)} barcodes in {elapsed}ms")
            except Exception as e:
                errors += 1
                last_error = str(e)
                log(f"    {photo_name}: ERROR — {e}")
                if errors >= 2:
                    log(f"    Too many errors, skipping engine.")
                    break

        if errors >= 2:
            results[eng_name] = {"status": "error", "error": last_error}
        else:
            gt_found = all_found & GROUND_TRUTH
            recall = len(gt_found) / len(GROUND_TRUTH) * 100
            results[eng_name] = {
                "status": "ok",
                "found": len(gt_found),
                "total": len(GROUND_TRUTH),
                "recall_pct": recall,
                "time_ms": total_ms,
                "barcodes_hit": sorted(gt_found),
                "barcodes_extra": sorted(all_found - GROUND_TRUTH)[:10],
            }
            log(f"  RESULT: {len(gt_found)}/{len(GROUND_TRUTH)} = {recall:.0f}% recall | {total_ms}ms")

    state["results"] = results
    state["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(state)

    # Print mini ranking after each iteration
    ok = {k: v for k, v in results.items() if v.get("status") == "ok"}
    if ok:
        ranked = sorted(ok.items(), key=lambda x: (-x[1]["recall_pct"], x[1]["time_ms"]))
        log("\n  --- Current Top 5 ---")
        for i, (name, r) in enumerate(ranked[:5], 1):
            log(f"  #{i} {name:32s}  {r['found']}/{r['total']} = {r['recall_pct']:.0f}%  {r['time_ms']}ms")

    pending = [e[0] for e in engines if e[0] not in results or results[e[0]].get("status") != "ok"]
    log(f"\n  Engines still pending: {len(pending)} / {len(engines)}")
    if pending:
        log(f"  Pending: {pending[:8]}")


# ── morning report ────────────────────────────────────────────────────────────

def print_report():
    state = load_state()
    results = state.get("results", {})
    iterations = state.get("iterations", 0)
    last_run = state.get("last_run", "?")

    print("\n" + "═"*70)
    print(f"OCR OVERNIGHT REPORT  —  {iterations} iterations  —  last run {last_run}")
    print("═"*70)

    ok  = {k: v for k, v in results.items() if v.get("status") == "ok"}
    err = {k: v for k, v in results.items() if v.get("status") != "ok"}

    if not ok:
        print("No engines completed successfully.")
        return

    ranked = sorted(ok.items(), key=lambda x: (-x[1]["recall_pct"], x[1]["time_ms"]))

    print(f"\n{'RANK':<5} {'ENGINE':<35} {'FOUND':>6} {'RECALL':>8} {'TIME':>8}")
    print("-" * 70)
    for i, (name, r) in enumerate(ranked, 1):
        print(f"#{i:<4d} {name:<35} {r['found']:>3d}/{r['total']:<3d} {r['recall_pct']:>7.1f}%  {r['time_ms']:>6d}ms")

    best_name, best = ranked[0]
    print(f"\n{'─'*70}")
    print(f"WINNER: {best_name}")
    print(f"  Recall: {best['recall_pct']:.1f}%  ({best['found']}/{best['total']} ground-truth barcodes)")
    print(f"  Speed:  {best['time_ms']}ms total across all photos")
    print(f"  Barcodes found: {best['barcodes_hit']}")
    missed = sorted(set(GROUND_TRUTH) - set(best['barcodes_hit']))
    if missed:
        print(f"  Missed barcodes: {missed}")

    if err:
        print(f"\nFailed engines ({len(err)}): {sorted(err.keys())}")

    # Check for barcodes only found by certain engines
    union_all = set()
    for r in ok.values():
        union_all |= set(r["barcodes_hit"])
    print(f"\nUnion across all engines: {len(union_all)}/{len(GROUND_TRUTH)} barcodes")
    if len(union_all) < len(GROUND_TRUTH):
        still_missed = GROUND_TRUTH - union_all
        print(f"No engine found: {sorted(still_missed)}")

    print(f"\nFull log: {LOG_FILE}")
    print(f"Full state: {STATE_FILE}")
    print("═"*70)


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", action="store_true", help="Print morning report and exit")
    args = parser.parse_args()

    if args.report:
        print_report()
    else:
        run_iteration()
