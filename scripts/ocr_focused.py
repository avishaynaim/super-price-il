#!/usr/bin/env python3
"""Focused OCR benchmark — runs only engines NOT yet in overnight results.

Covers: rapidocr, easyocr, paddleocr, doctr, newocr.com
Uses max 1500px to run fast on low-memory Android/PRoot.

Results merged into /tmp/ocr_focused_results.json
Run: python3 scripts/ocr_focused.py
"""
from __future__ import annotations
import base64, io, json, os, re, subprocess, sys, tempfile, time, traceback
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter

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

STATE_FILE = Path("/tmp/ocr_focused_results.json")
LOG_FILE   = Path("/tmp/ocr_focused.log")


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


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


def extract_barcodes(text: str) -> set[str]:
    text = text.replace(" ", "").replace("\n", "")
    text = re.sub(r"(?<!\d)1(290\d{9})(?!\d)", r"7\1", text)
    text = text.replace("T290", "7290").replace("O", "0").replace("I", "1")
    found = set()
    for m in re.finditer(r"729[0-9]{10}", text):
        found.add(m.group(0))
    for m in re.finditer(r"\d{13}", text):
        found.add(m.group(0))
    return found


# ── engines ───────────────────────────────────────────────────────────────────

_rapid_inst = None
def run_rapidocr(img: Image.Image) -> set[str]:
    global _rapid_inst
    if _rapid_inst is None:
        os.environ.setdefault("ORT_DISABLE_ALL_LOG_OUTPUT", "1")
        from rapidocr_onnxruntime import RapidOCR
        _rapid_inst = RapidOCR()
    result, _ = _rapid_inst(to_bytes(img))
    return extract_barcodes(" ".join(r[1] for r in (result or [])))


_easy_inst = None
def run_easyocr(img: Image.Image) -> set[str]:
    global _easy_inst
    if _easy_inst is None:
        import easyocr
        log("  (loading EasyOCR model...)")
        _easy_inst = easyocr.Reader(["en"], gpu=False, verbose=False)
        log("  (EasyOCR model loaded)")
    results = _easy_inst.readtext(to_bytes(img), detail=0)
    return extract_barcodes(" ".join(results))


_paddle_inst = None
def run_paddleocr(img: Image.Image) -> set[str]:
    global _paddle_inst
    if _paddle_inst is None:
        from paddleocr import PaddleOCR
        log("  (loading PaddleOCR model...)")
        _paddle_inst = PaddleOCR(use_angle_cls=True, lang="en", use_gpu=False, show_log=False)
        log("  (PaddleOCR model loaded)")
    result = _paddle_inst.ocr(to_bytes(img), cls=True)
    text = " ".join(r[1][0] for line in (result or []) for r in (line or []))
    return extract_barcodes(text)


_doctr_model = None
def run_doctr(img: Image.Image) -> set[str]:
    global _doctr_model
    if _doctr_model is None:
        from doctr.models import ocr_predictor
        log("  (loading doctr model...)")
        _doctr_model = ocr_predictor(pretrained=True)
        log("  (doctr model loaded)")
    from doctr.io import DocumentFile
    doc = DocumentFile.from_images([to_bytes(img)])
    result = _doctr_model(doc)
    text = " ".join(
        word.value
        for page in result.pages
        for block in page.blocks
        for line in block.lines
        for word in line.words
    )
    return extract_barcodes(text)


def run_newocr(img: Image.Image) -> set[str]:
    buf = to_bytes(img, quality=80)
    if len(buf) > 900_000:
        buf = to_bytes(resize(img, 1200), quality=70)
    p = subprocess.run(
        ["curl", "-s", "-L", "--max-time", "60",
         "-A", "Mozilla/5.0",
         "-F", "file=@-;filename=receipt.jpg;type=image/jpeg",
         "-F", "lang=eng",
         "-F", "submit=Free+OCR",
         "https://newocr.com/ocr"],
        input=buf, capture_output=True, timeout=90,
    )
    html = p.stdout.decode("utf-8", errors="ignore")
    # Try various textarea patterns
    for pattern in [
        r'<textarea[^>]*id=["\']?ocr[^"\']*["\']?[^>]*>(.*?)</textarea>',
        r'<textarea[^>]*name=["\']?text["\']?[^>]*>(.*?)</textarea>',
        r'<textarea[^>]*>(.*?)</textarea>',
    ]:
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if m:
            text = re.sub(r"<[^>]+>", "", m.group(1))
            barcodes = extract_barcodes(text)
            if barcodes or len(text.strip()) > 10:
                return barcodes
    # Fallback: grab all text between certain markers
    m = re.search(r'OCR Result.*?<pre[^>]*>(.*?)</pre>', html, re.DOTALL | re.IGNORECASE)
    if m:
        return extract_barcodes(re.sub(r"<[^>]+>", "", m.group(1)))
    return set()


def run_tesseract_sharpen(img: Image.Image) -> set[str]:
    """Best tesseract config from overnight: psm6 + sharpen."""
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(to_png_bytes(img))
        tmp = f.name
    try:
        r = subprocess.run(
            ["tesseract", tmp, "stdout", "--psm", "6", "eng"],
            capture_output=True, text=True, timeout=60
        )
        return extract_barcodes(r.stdout)
    finally:
        os.unlink(tmp)


# ── engine list ───────────────────────────────────────────────────────────────

ENGINES = [
    # Best tesseract from overnight (35% recall)
    ("tess/eng-psm6-sharpen",   run_tesseract_sharpen, "sharpen", 1500),
    # RapidOCR
    ("rapidocr/raw",            run_rapidocr,          "raw",     1500),
    ("rapidocr/contrast",       run_rapidocr,          "contrast",1500),
    ("rapidocr/sharpen",        run_rapidocr,          "sharpen", 1500),
    # EasyOCR
    ("easyocr/raw",             run_easyocr,           "raw",     1500),
    ("easyocr/contrast",        run_easyocr,           "contrast",1500),
    # PaddleOCR
    ("paddleocr/raw",           run_paddleocr,         "raw",     1500),
    ("paddleocr/contrast",      run_paddleocr,         "contrast",1500),
    # doctr
    ("doctr/raw",               run_doctr,             "raw",     1500),
    ("doctr/contrast",          run_doctr,             "contrast",1500),
    # newocr.com
    ("newocr/raw",              run_newocr,            "raw",     1200),
    ("newocr/contrast",         run_newocr,            "contrast",1200),
]


# ── runner ────────────────────────────────────────────────────────────────────

def run_benchmark(photos, resume: bool = False):
    state = {}
    if resume and STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            log(f"Resuming: {len([k for k,v in state.items() if v.get('status')=='ok'])} engines already done")
        except Exception:
            pass

    for eng_name, eng_fn, prep_mode, max_px in ENGINES:
        if state.get(eng_name, {}).get("status") in ("ok", "skipped"):
            s = state[eng_name]
            if s["status"] == "ok":
                log(f"  SKIP {eng_name}  ({s['recall_pct']:.0f}%)")
            else:
                log(f"  SKIP {eng_name}  (skipped: {s.get('reason', '')})")
            continue

        log(f"\n{'─'*55}")
        log(f"ENGINE: {eng_name}  [{prep_mode}/{max_px}px]")
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
                gt = found & GROUND_TRUTH
                log(f"  {photo_name}: {len(gt):2d}/20 GT  ({len(found)} raw)  {elapsed}ms")
            except Exception as e:
                errors += 1
                last_error = str(e)
                log(f"  {photo_name}: ERROR — {e}")
                traceback.print_exc()
                if errors >= 3:
                    log("  Too many errors, skipping.")
                    break

        if errors >= 3:
            state[eng_name] = {"status": "error", "error": last_error}
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
    # Also pull in overnight data for context
    overnight_summary = {}
    overnight_path = Path("/tmp/ocr_overnight_state.json")
    if overnight_path.exists():
        try:
            ons = json.loads(overnight_path.read_text()).get("results", {})
            for k, v in ons.items():
                if v.get("status") == "ok":
                    overnight_summary[f"[overnight] {k}"] = v
        except Exception:
            pass

    all_results = {**overnight_summary, **{k: v for k, v in state.items() if v.get("status") == "ok"}}
    ranked = sorted(all_results.items(), key=lambda x: (-x[1]["recall_pct"], x[1]["time_ms"]))

    print("\n" + "═"*72)
    print(f"COMBINED OCR RANKING  (new={len(state)} + overnight={len(overnight_summary)} engines)")
    print(f"Ground truth: {len(GROUND_TRUTH)} barcodes")
    print("═"*72)
    print(f"{'#':<4} {'ENGINE':<40} {'FOUND':>6} {'RECALL':>8} {'TIME':>9}")
    print("─"*72)
    for i, (name, r) in enumerate(ranked, 1):
        marker = " ★" if i == 1 else ""
        print(f"#{i:<3d} {name:<40} {r['found']:>3d}/20  {r['recall_pct']:>6.1f}%  {r['time_ms']:>7d}ms{marker}")

    if ranked:
        best_name, best = ranked[0]
        print(f"\nBEST: {best_name}")
        print(f"  Recall: {best['recall_pct']:.1f}%  ({best['found']}/20)")
        missed = sorted(GROUND_TRUTH - set(best.get("barcodes_hit", [])))
        if missed:
            print(f"  Missed: {missed}")

    ok_local = {k: v for k, v in state.items() if v.get("status") == "ok"}
    union = set()
    for r in ok_local.values():
        union |= set(r.get("barcodes_hit", []))
    print(f"\nUnion (new engines only): {len(union)}/20 barcodes")

    err = {k: v for k, v in state.items() if v.get("status") == "error"}
    if err:
        print(f"\nErrors: {list(err.keys())}")
        for k, v in err.items():
            print(f"  {k}: {v.get('error','?')[:100]}")

    print(f"\nState: {STATE_FILE}")
    print("═"*72)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.report:
        if STATE_FILE.exists():
            print_report(json.loads(STATE_FILE.read_text()))
        else:
            print("No results yet.")
        sys.exit(0)

    LOG_FILE.write_text(f"Focused OCR Benchmark  {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
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
        log("No photos found.")
        sys.exit(1)

    log(f"Photos: {len(photos)}, Engines: {len(ENGINES)}")
    state = run_benchmark(photos, resume=args.resume)
    print_report(state)
