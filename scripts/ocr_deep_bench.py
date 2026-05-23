#!/usr/bin/env python3
"""Deep OCR benchmark — tests every engine against all 6 receipt photos.

Engines covered:
  Local:   tesseract (12 configs), rapidocr (4 modes), easyocr (3 modes),
           paddleocr (2 modes), doctr
  Web API: ocr.space (engines 1-3), newocr.com
  Cloud:   Google Vision, Google Drive OCR, Claude Haiku direct API
           (each activated by env var, skipped if credentials absent)

Google Lens: not automatable — no public REST API; test manually.

Run:
  python3 scripts/ocr_deep_bench.py
  python3 scripts/ocr_deep_bench.py --resume        # skip already-done engines
  python3 scripts/ocr_deep_bench.py --report        # print saved results and exit

Results written to: /tmp/ocr_deep_bench.json  and  /tmp/ocr_deep_bench.log
"""
from __future__ import annotations
import argparse, base64, io, json, os, re, subprocess, sys, tempfile, time, traceback
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter

# ── photos & ground truth ─────────────────────────────────────────────────────

RECEIPT_PHOTOS = [
    "/sdcard/DCIM/Camera/20260503_183926.jpg",
    "/sdcard/DCIM/Camera/20260503_183941.jpg",
    "/sdcard/DCIM/Camera/20260503_231614.jpg",
    "/sdcard/DCIM/Camera/20260503_231616.jpg",
    "/sdcard/DCIM/Camera/20260503_231618.jpg",
    "/sdcard/DCIM/Camera/20260503_231621.jpg",
]

GROUND_TRUTH = {
    "7290110115388", "7290000208428", "7290100243084", "7290004129156",
    "7290000474502", "7290010777525", "7290113195851", "7290018989333",
    "7290004131074", "7290002787891", "7290112963918", "7290000211503",
    "7290115203936", "7290011725006", "7290018825006", "7290110114909",
    "7290117034798", "7290117034828", "7422592419017", "7290000178911",
}

STATE_FILE = Path("/tmp/ocr_deep_bench.json")
LOG_FILE   = Path("/tmp/ocr_deep_bench.log")


# ── logging ────────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


# ── image helpers ─────────────────────────────────────────────────────────────

def load_image(path: str) -> Image.Image:
    img = Image.open(path)
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
        img = img.point(lambda x: 0 if x < 128 else 255)
        return img.convert("RGB")
    if mode == "denoise":
        img = img.convert("L")
        img = ImageEnhance.Contrast(img).enhance(2.0)
        img = img.filter(ImageFilter.MedianFilter(3))
        return img.convert("RGB")
    if mode == "scale2x":
        img = img.convert("L")
        img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)
        img = ImageEnhance.Contrast(img).enhance(2.0)
        return img.convert("RGB")
    return img.convert("RGB")


def resize(img: Image.Image, max_long: int) -> Image.Image:
    long = max(img.size)
    if long > max_long:
        scale = max_long / long
        img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
    return img


def to_bytes(img: Image.Image, quality: int = 90) -> bytes:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "JPEG", quality=quality)
    return buf.getvalue()


def to_png_bytes(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, "PNG")
    return buf.getvalue()


# ── barcode extraction ─────────────────────────────────────────────────────────

def extract_barcodes(text: str) -> set[str]:
    text = text.replace(" ", "").replace("\n", "")
    text = re.sub(r"(?<!\d)1(290\d{9})(?!\d)", r"7\1", text)
    text = text.replace("T290", "7290").replace("O", "0").replace("I", "1")
    found = set()
    for m in re.finditer(r"729[0-9]{10}", text):
        found.add(m.group(0))
    for m in re.finditer(r"\d{13}", text):
        candidate = m.group(0)
        if candidate not in found:
            found.add(candidate)
    return found


# ── OCR engines ───────────────────────────────────────────────────────────────

_rapid_inst = None
def _run_rapidocr(img: Image.Image) -> set[str]:
    global _rapid_inst
    if _rapid_inst is None:
        os.environ.setdefault("ORT_DISABLE_ALL_LOG_OUTPUT", "1")
        from rapidocr_onnxruntime import RapidOCR
        _rapid_inst = RapidOCR()
    result, _ = _rapid_inst(to_bytes(img))
    return extract_barcodes(" ".join(r[1] for r in (result or [])))


_easy_inst = None
def _run_easyocr(img: Image.Image) -> set[str]:
    global _easy_inst
    if _easy_inst is None:
        import easyocr
        _easy_inst = easyocr.Reader(["en"], gpu=False, verbose=False)
    results = _easy_inst.readtext(to_bytes(img), detail=0)
    return extract_barcodes(" ".join(results))


_paddle_inst = None
def _run_paddleocr(img: Image.Image) -> set[str]:
    global _paddle_inst
    if _paddle_inst is None:
        from paddleocr import PaddleOCR
        _paddle_inst = PaddleOCR(use_angle_cls=True, lang="en", use_gpu=False, show_log=False)
    result = _paddle_inst.ocr(to_bytes(img), cls=True)
    text = " ".join(r[1][0] for line in (result or []) for r in (line or []))
    return extract_barcodes(text)


def _run_tesseract(img: Image.Image, psm: int, lang: str = "eng", whitelist: str | None = None) -> set[str]:
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(to_png_bytes(img))
        tmp = f.name
    try:
        cmd = ["tesseract", tmp, "stdout", "--psm", str(psm), lang, "--oem", "1"]
        if whitelist:
            cmd += ["-c", f"tessedit_char_whitelist={whitelist}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return extract_barcodes(r.stdout)
    finally:
        os.unlink(tmp)


def _run_doctr(img: Image.Image) -> set[str]:
    from doctr.io import DocumentFile
    from doctr.models import ocr_predictor
    model = ocr_predictor(pretrained=True)
    doc = DocumentFile.from_images([to_bytes(img)])
    result = model(doc)
    text = " ".join(
        word.value
        for page in result.pages
        for block in page.blocks
        for line in block.lines
        for word in line.words
    )
    return extract_barcodes(text)


def _run_ocrspace(img: Image.Image, engine: int = 2) -> set[str]:
    key = os.environ.get("OCRSPACE_API_KEY", "helloworld")
    buf = to_bytes(img, quality=70)
    if len(buf) > 900_000:
        buf = to_bytes(resize(img, 1200), quality=60)
    p = subprocess.run(
        ["curl", "-s", "-X", "POST", "https://api.ocr.space/parse/image",
         "-H", f"apikey: {key}",
         "-F", f"OCREngine={engine}",
         "-F", "scale=true",
         "-F", "isTable=false",
         "-F", "file=@-;filename=r.jpg;type=image/jpeg"],
        input=buf, capture_output=True, timeout=60,
    )
    try:
        d = json.loads(p.stdout)
    except Exception:
        return set()
    if d.get("IsErroredOnProcessing"):
        return set()
    text = " ".join(r.get("ParsedText", "") for r in (d.get("ParsedResults") or []))
    return extract_barcodes(text)


def _run_newocr(img: Image.Image) -> set[str]:
    """newocr.com — POST multipart form to their free OCR endpoint."""
    buf = to_bytes(img, quality=80)
    if len(buf) > 1_000_000:
        buf = to_bytes(resize(img, 1500), quality=75)
    # newocr.com accepts a multipart POST to /upload
    p = subprocess.run(
        ["curl", "-s", "-L",
         "-F", "file=@-;filename=receipt.jpg;type=image/jpeg",
         "-F", "lang=eng",
         "-F", "submit=Free+OCR",
         "https://newocr.com/ocr"],
        input=buf, capture_output=True, timeout=90,
    )
    html = p.stdout.decode("utf-8", errors="ignore")
    # Extract text from <textarea> result field
    m = re.search(r'<textarea[^>]*id=["\']?ocr["\']?[^>]*>(.*?)</textarea>', html, re.DOTALL | re.IGNORECASE)
    if not m:
        # Try any textarea
        m = re.search(r'<textarea[^>]*>(.*?)</textarea>', html, re.DOTALL | re.IGNORECASE)
    if m:
        text = re.sub(r"<[^>]+>", "", m.group(1))
        return extract_barcodes(text)
    return set()


def _run_google_vision(img: Image.Image) -> set[str]:
    key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not key and not creds:
        raise RuntimeError("GOOGLE_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS required")
    b64 = base64.b64encode(to_bytes(img)).decode()
    payload = json.dumps({
        "requests": [{"image": {"content": b64}, "features": [{"type": "TEXT_DETECTION"}]}]
    }).encode()
    url = f"https://vision.googleapis.com/v1/images:annotate?key={key}" if key \
          else "https://vision.googleapis.com/v1/images:annotate"
    p = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", "Content-Type: application/json", "-d", "@-"],
        input=payload, capture_output=True, timeout=60,
    )
    try:
        d = json.loads(p.stdout)
        text = d["responses"][0]["fullTextAnnotation"]["text"]
        return extract_barcodes(text)
    except Exception:
        return set()


def _run_google_drive_ocr(img: Image.Image) -> set[str]:
    """Upload image to Google Drive, open as Google Doc (triggers OCR), read text back."""
    key = os.environ.get("GOOGLE_DRIVE_API_KEY", "")
    token = os.environ.get("GOOGLE_OAUTH_TOKEN", "")
    if not key and not token:
        raise RuntimeError("GOOGLE_DRIVE_API_KEY or GOOGLE_OAUTH_TOKEN required")
    buf = to_bytes(img)
    auth_header = f"Authorization: Bearer {token}" if token else f"X-Goog-Api-Key: {key}"
    # Step 1: upload with OCR conversion
    upload = subprocess.run(
        ["curl", "-s", "-X", "POST",
         "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&convert=true",
         "-H", auth_header,
         "-F", "metadata={\"name\":\"ocr_tmp\",\"mimeType\":\"application/vnd.google-apps.document\"};type=application/json",
         "-F", f"file=@-;type=image/jpeg"],
        input=buf, capture_output=True, timeout=90,
    )
    try:
        file_id = json.loads(upload.stdout)["id"]
    except Exception:
        return set()
    # Step 2: export as plain text
    export = subprocess.run(
        ["curl", "-s", f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=text/plain",
         "-H", auth_header],
        capture_output=True, timeout=60,
    )
    # Step 3: delete the temp file
    subprocess.run(
        ["curl", "-s", "-X", "DELETE",
         f"https://www.googleapis.com/drive/v3/files/{file_id}",
         "-H", auth_header],
        capture_output=True, timeout=30,
    )
    text = export.stdout.decode("utf-8", errors="ignore")
    return extract_barcodes(text)


def _run_claude_haiku(img: Image.Image) -> set[str]:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    b64 = base64.b64encode(to_bytes(img)).decode()
    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
            {"type": "text", "text": (
                "This is an Israeli supermarket receipt. "
                "List EVERY barcode/product number visible (13-digit EAN-13 codes starting with 729 or 742). "
                "Return ONLY the numeric strings, one per line, nothing else."
            )}
        ]}]
    }).encode()
    p = subprocess.run(
        ["curl", "-s", "https://api.anthropic.com/v1/messages",
         "-H", f"x-api-key: {key}",
         "-H", "anthropic-version: 2023-06-01",
         "-H", "Content-Type: application/json",
         "-d", "@-"],
        input=payload, capture_output=True, timeout=90,
    )
    try:
        d = json.loads(p.stdout)
        text = d["content"][0]["text"]
        return extract_barcodes(text)
    except Exception:
        return set()


# ── engine registry ────────────────────────────────────────────────────────────

def build_engines() -> list[tuple]:
    """Returns list of (name, fn, preprocess_mode, max_px)."""
    W = "0123456789"
    engines = [
        # ── Tesseract: 12 configurations ──
        ("tess/eng-psm3-contrast",   lambda i: _run_tesseract(i,  3, "eng"),      "contrast", 3000),
        ("tess/eng-psm4-contrast",   lambda i: _run_tesseract(i,  4, "eng"),      "contrast", 3000),
        ("tess/eng-psm6-contrast",   lambda i: _run_tesseract(i,  6, "eng"),      "contrast", 3000),
        ("tess/eng-psm6-sharpen",    lambda i: _run_tesseract(i,  6, "eng"),      "sharpen",  3000),
        ("tess/eng-psm6-raw",        lambda i: _run_tesseract(i,  6, "eng"),      "raw",      3000),
        ("tess/eng-psm6-scale2x",    lambda i: _run_tesseract(i,  6, "eng"),      "scale2x",  2000),
        ("tess/eng-psm11-contrast",  lambda i: _run_tesseract(i, 11, "eng"),      "contrast", 3000),
        ("tess/eng-psm13-contrast",  lambda i: _run_tesseract(i, 13, "eng"),      "contrast", 3000),
        ("tess/heb-psm3-contrast",   lambda i: _run_tesseract(i,  3, "heb"),      "contrast", 3000),
        ("tess/heb-psm6-contrast",   lambda i: _run_tesseract(i,  6, "heb"),      "contrast", 3000),
        ("tess/digits-psm6-contrast",lambda i: _run_tesseract(i,  6, "eng", W),   "contrast", 3000),
        ("tess/digits-psm6-sharpen", lambda i: _run_tesseract(i,  6, "eng", W),   "sharpen",  3000),
        ("tess/digits-psm6-scale2x", lambda i: _run_tesseract(i,  6, "eng", W),   "scale2x",  2000),
        ("tess/digits-psm11-contrast",lambda i:_run_tesseract(i, 11, "eng", W),   "contrast", 3000),
        ("tess/digits-psm13-contrast",lambda i:_run_tesseract(i, 13, "eng", W),   "contrast", 3000),
        # ── RapidOCR ──
        ("rapidocr/raw",             _run_rapidocr,  "raw",      2000),
        ("rapidocr/gray",            _run_rapidocr,  "gray",     2000),
        ("rapidocr/contrast",        _run_rapidocr,  "contrast", 2000),
        ("rapidocr/sharpen",         _run_rapidocr,  "sharpen",  2000),
        ("rapidocr/scale2x",         _run_rapidocr,  "scale2x",  1500),
        # ── EasyOCR ──
        ("easyocr/raw",              _run_easyocr,   "raw",      2000),
        ("easyocr/contrast",         _run_easyocr,   "contrast", 2000),
        ("easyocr/sharpen",          _run_easyocr,   "sharpen",  2000),
        # ── PaddleOCR ──
        ("paddleocr/raw",            _run_paddleocr, "raw",      2000),
        ("paddleocr/contrast",       _run_paddleocr, "contrast", 2000),
        # ── doctr ──
        ("doctr/raw",                _run_doctr,     "raw",      2000),
        ("doctr/contrast",           _run_doctr,     "contrast", 2000),
        # ── ocr.space (free web API, rate-limited) ──
        ("ocrspace/e1-raw",          lambda i: _run_ocrspace(i, 1), "raw",      1600),
        ("ocrspace/e2-contrast",     lambda i: _run_ocrspace(i, 2), "contrast", 1600),
        ("ocrspace/e3-contrast",     lambda i: _run_ocrspace(i, 3), "contrast", 1600),
        # ── newocr.com ──
        ("newocr/raw",               _run_newocr,    "raw",      1600),
        ("newocr/contrast",          _run_newocr,    "contrast", 1600),
    ]
    # Cloud — only if credentials present
    if os.environ.get("GOOGLE_VISION_API_KEY") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        engines.append(("google-vision/raw", _run_google_vision, "raw", 2000))
    if os.environ.get("GOOGLE_DRIVE_API_KEY") or os.environ.get("GOOGLE_OAUTH_TOKEN"):
        engines.append(("google-drive-ocr/raw", _run_google_drive_ocr, "raw", 2000))
    if os.environ.get("ANTHROPIC_API_KEY"):
        engines.append(("claude-haiku/raw", _run_claude_haiku, "raw", 2000))
    return engines


# ── benchmark runner ──────────────────────────────────────────────────────────

def run_all(photos: list[tuple[str, Image.Image]], engines: list, resume: bool):
    state = {}
    if resume and STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            log(f"Resuming: {len(state)} engines already done")
        except Exception:
            pass

    for eng_name, eng_fn, prep_mode, max_px in engines:
        if resume and eng_name in state and state[eng_name].get("status") == "ok":
            log(f"  SKIP {eng_name} (already completed: {state[eng_name]['recall_pct']:.0f}%)")
            continue

        log(f"\n{'─'*55}")
        log(f"ENGINE: {eng_name}  [{prep_mode} / {max_px}px]")
        all_found: set[str] = set()
        total_ms = 0
        errors = 0
        last_error = ""

        for photo_name, img in photos:
            try:
                proc = preprocess(img.copy(), prep_mode)
                proc = resize(proc, max_px)
                t0 = time.perf_counter()
                found = eng_fn(proc)
                elapsed = int((time.perf_counter() - t0) * 1000)
                total_ms += elapsed
                all_found |= found
                gt_found = found & GROUND_TRUTH
                log(f"  {photo_name}: {len(gt_found):2d}/20 GT  raw_found={len(found)}  {elapsed}ms")
            except Exception as e:
                errors += 1
                last_error = str(e)
                log(f"  {photo_name}: ERROR — {e}")
                if errors >= 3:
                    log(f"  Too many errors, aborting engine.")
                    break

        if errors >= 3:
            state[eng_name] = {"status": "error", "error": last_error}
            log(f"  RESULT: ERROR")
        else:
            gt_found = all_found & GROUND_TRUTH
            recall = len(gt_found) / len(GROUND_TRUTH) * 100
            state[eng_name] = {
                "status": "ok",
                "found": len(gt_found),
                "total": len(GROUND_TRUTH),
                "recall_pct": recall,
                "time_ms": total_ms,
                "barcodes_hit": sorted(gt_found),
                "barcodes_extra": sorted(all_found - GROUND_TRUTH)[:15],
            }
            log(f"  RESULT: {len(gt_found)}/20 = {recall:.0f}%  {total_ms}ms")

        STATE_FILE.write_text(json.dumps(state, indent=2))

    return state


def print_report(state: dict):
    ok  = {k: v for k, v in state.items() if v.get("status") == "ok"}
    err = {k: v for k, v in state.items() if v.get("status") != "ok"}
    ranked = sorted(ok.items(), key=lambda x: (-x[1]["recall_pct"], x[1]["time_ms"]))

    log("\n" + "═"*70)
    log(f"FINAL RANKING  ({len(ok)} engines, {len(GROUND_TRUTH)} ground-truth barcodes)")
    log("═"*70)
    log(f"{'#':<4} {'ENGINE':<38} {'FOUND':>7} {'RECALL':>8} {'TIME':>9}")
    log("─"*70)
    for i, (name, r) in enumerate(ranked, 1):
        marker = " ★" if i == 1 else ""
        log(f"#{i:<3d} {name:<38} {r['found']:>3d}/20  {r['recall_pct']:>6.1f}%  {r['time_ms']:>7d}ms{marker}")

    if ranked:
        best_name, best = ranked[0]
        log(f"\nWINNER: {best_name}")
        log(f"  Recall: {best['recall_pct']:.1f}%  ({best['found']}/20 barcodes)")
        log(f"  Speed:  {best['time_ms']}ms across all photos")
        log(f"  Found:  {best['barcodes_hit']}")
        missed = sorted(GROUND_TRUTH - set(best["barcodes_hit"]))
        if missed:
            log(f"  Missed: {missed}")

    # Union coverage
    union = set()
    for r in ok.values():
        union |= set(r["barcodes_hit"])
    log(f"\nUnion of all engines: {len(union)}/20 barcodes covered")
    still_missed = sorted(GROUND_TRUTH - union)
    if still_missed:
        log(f"  Never found by any engine: {still_missed}")

    if err:
        log(f"\nFailed engines ({len(err)}): {sorted(err.keys())}")
        for k, v in sorted(err.items()):
            log(f"  {k}: {v.get('error','?')[:80]}")

    log(f"\nFull log:   {LOG_FILE}")
    log(f"Full state: {STATE_FILE}")
    log("\nNOTE: Google Lens — not automatable via public API. Test manually:")
    log("  Open Google Lens on phone → tap receipt photo → read all barcode numbers shown.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true", help="Skip already-completed engines")
    parser.add_argument("--report", action="store_true", help="Print saved report and exit")
    args = parser.parse_args()

    if args.report:
        if STATE_FILE.exists():
            state = json.loads(STATE_FILE.read_text())
            print_report(state)
        else:
            print("No results yet — run without --report first.")
        sys.exit(0)

    LOG_FILE.write_text(f"OCR Deep Benchmark started {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log(f"Ground truth: {len(GROUND_TRUTH)} barcodes")

    photos = []
    for p in RECEIPT_PHOTOS:
        try:
            img = load_image(p)
            photos.append((Path(p).name, img))
            log(f"  Loaded {Path(p).name}  {img.size}")
        except Exception as e:
            log(f"  SKIP {p}: {e}")

    if not photos:
        log("No photos found — exiting.")
        sys.exit(1)

    log(f"Photos loaded: {len(photos)}")
    engines = build_engines()
    log(f"Engines to test: {len(engines)}")
    for name, _, prep, px in engines:
        log(f"  {name}  [{prep}/{px}px]")

    state = run_all(photos, engines, resume=args.resume)
    print_report(state)
    log(f"\nDone. Results in {STATE_FILE}")
