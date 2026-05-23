"""OCR text-line parser → ExtractedItem list.

The free-OCR engines (RapidOCR, OCR.space) return loose strings + boxes. They
don't structure receipts into rows or apply discounts. This module:

  1. Pulls 13-digit Israeli barcodes (start with 729) and decimal prices.
  2. Groups lines into rows by Y-coordinate proximity (one receipt row's
     barcode + price + qty are typically aligned to within ~half a line height).
  3. Looks each barcode up in `products` so we get the canonical Hebrew name.
  4. Builds ExtractedItem rows even when no name was OCR'd — which is the
     common case here, since RapidOCR doesn't read Hebrew. The DB lookup is
     the whole point: barcode → catalog name with full retail metadata.

Returns the same `Extracted` dataclass shape as pipeline.py so the rest of the
pipeline (match_items, basket_alternatives, store_receipt) is unchanged.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Iterable

from .free_ocr import OCRLine, OCRResult
from .pipeline import Extracted, ExtractedItem

# Israeli EAN-13: prefix 729 + 10 digits. Two strategies:
#   1. Exact: 13-digit sequence starting with 729, not surrounded by digits.
#   2. Fuzzy: extract any 13-digit run containing 729 if exact fails —
#      handles OCR noise that prepends/appends 1 stray digit.
BARCODE_RE = re.compile(r"(?<!\d)(729\d{10})(?!\d)")
BARCODE_FUZZY_RE = re.compile(r"\d*(729\d{10})\d*")


def _normalize_barcode_text(text: str) -> str:
    """Fix common OCR character substitutions in barcode regions.
    Thermal paper fonts cause: 7→1, 7→T, 7→l, 0→O, 1→I, etc.
    We only apply corrections when a 13-digit Israeli barcode is plausible.
    """
    # Replace characters that look like digits in OCR output
    return (text
            .replace("O", "0").replace("o", "0")
            .replace("I", "1").replace("l", "1")
            .replace("T", "7").replace("t", "7"))


def _extract_barcodes(text: str) -> list[str]:
    """Find all Israeli barcodes in text, with fuzzy fallback for OCR noise.

    Strategy:
    1. Exact match first.
    2. Normalize common OCR character errors (T→7, 1→7 for prefix) and retry.
    3. Fuzzy: look for 729x sequences and extract/reconstruct the 13-digit barcode.
    """
    exact = BARCODE_RE.findall(text)
    if exact:
        return exact

    # Normalize OCR errors and retry
    normalized = _normalize_barcode_text(text)
    # Also fix '1290' → '7290' prefix misread (7 looks like 1 on thermal paper)
    normalized = re.sub(r"(?<!\d)1(290\d{9})(?!\d)", r"7\1", normalized)
    exact2 = BARCODE_RE.findall(normalized)
    if exact2:
        return exact2

    # Fuzzy: find 729x + 9 digits even if embedded in noise
    found = []
    for src in (text, normalized):
        for m in re.finditer(r"[7T1][2I][9g][0Oo]\d{9}", src):
            candidate = _normalize_barcode_text(m.group(0))
            candidate = re.sub(r"^1(290)", r"7\1", candidate)
            if re.fullmatch(r"729\d{10}", candidate):
                found.append(candidate)
    return list(dict.fromkeys(found))  # deduplicate, preserve order
# Loose internal codes (sometimes 6-7 digits, e.g. PLU). We use these for
# weight items where there's no GTIN.
INTERNAL_RE = re.compile(r"(?<!\d)(\d{6,7})(?!\d)")
# Decimal price: NN.NN or N.NN; allow comma decimal too. Surrounding chars
# must not be digits to avoid grabbing chunks of barcodes.
PRICE_RE = re.compile(r"(?<!\d)(\d{1,4}[.,]\d{2})(?!\d)")
# Quantity hint: "2 x" or "3 X" or "2*"
QTY_RE = re.compile(r"(\d+)\s*[xX×*]\s*(?=\d|\s)")
# Weight multiplier on receipts: prints as "1.572 ק\"ג" or "2.186 קג"
WEIGHT_RE = re.compile(r"(\d{1,3}[.,]\d{2,3})\s*(?:ק[\"׳']?ג|kg)", re.IGNORECASE)


def _to_float(s: str) -> float | None:
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _row_y(line: OCRLine) -> float:
    return line.box[1] + line.box[3] / 2


def _row_x(line: OCRLine) -> float:
    return line.box[0] + line.box[2] / 2


def _auto_y_tol(lines: list[OCRLine]) -> float:
    """Pick y_tol based on the median line height — adapts to scale.

    On thermal receipts each item has: barcode line, then price line 1–1.5
    line-heights below. We use 1.3× median height so barcodes and their
    prices group together even when OCR splits them across adjacent rows.
    """
    if not lines:
        return 0.015
    heights = sorted(ln.box[3] for ln in lines)
    median = heights[len(heights) // 2]
    return max(0.010, min(0.040, median * 1.3))


def _group_rows(lines: list[OCRLine], y_tol: float | None = None) -> list[list[OCRLine]]:
    """Cluster lines that share approximately the same vertical center.

    Uses the group's FIRST line as anchor so that intermediate lines don't
    slide the window and merge separate receipt rows into one cluster.
    """
    if y_tol is None:
        y_tol = _auto_y_tol(lines)
    sorted_lines = sorted(lines, key=_row_y)
    rows: list[list[OCRLine]] = []
    for ln in sorted_lines:
        if rows and abs(_row_y(ln) - _row_y(rows[-1][0])) <= y_tol:
            rows[-1].append(ln)
        else:
            rows.append([ln])
    return rows


def _lookup_barcodes(barcodes: Iterable[str]) -> dict[str, dict]:
    bcs = list({b for b in barcodes if b})
    if not bcs:
        return {}
    try:
        from ..db.pg import cursor as _pg_cursor
        with _pg_cursor() as cur:
            cur.execute(
                "SELECT barcode, name, manufacturer FROM products WHERE barcode = ANY(%s)",
                (bcs,),
            )
            rows = cur.fetchall()
        return {r["barcode"]: dict(r) for r in rows}
    except Exception:
        from ..db.local import lookup_barcodes as _local_lookup
        return _local_lookup(bcs)


def _row_features(row: list[OCRLine]) -> dict:
    """Pull the structured signal out of a row: barcode tokens with X positions,
    price tokens with X positions, qty hints, weight hints. Position lets us
    pair each barcode with its nearest price even when a row has multiple
    items merged into one Y-cluster.
    """
    barcodes: list[tuple[float, str]] = []   # (x_center, barcode)
    prices:   list[tuple[float, float]] = [] # (x_center, value)
    qtys:     list[float] = []
    weights:  list[float] = []
    for ln in row:
        x = _row_x(ln)
        for bc in _extract_barcodes(ln.text):
            barcodes.append((x, bc))
        for p in PRICE_RE.findall(ln.text):
            v = _to_float(p)
            if v is not None:
                prices.append((x, v))
        for w in WEIGHT_RE.findall(ln.text):
            v = _to_float(w)
            if v is not None:
                weights.append(v)
        for q in QTY_RE.findall(ln.text):
            try:
                qtys.append(float(q))
            except ValueError:
                pass
    return {"barcodes": barcodes, "prices": prices, "qtys": qtys, "weights": weights}


def _pair_prices_to_barcodes(
    barcodes: list[tuple[float, str]],
    prices: list[tuple[float, float]],
) -> list[tuple[str, float | None, float | None]]:
    """Assign prices to barcodes.

    Strategy depends on barcode count:
      - 1 barcode: that barcode gets the LARGEST price as line_total; the
        next-largest (if smaller) becomes unit_price. Handles "2 × 9.45 = 18.90"
        and weight items with both per-kg and total prices.
      - 2+ barcodes: each barcode gets its NEAREST-X price as line_total
        (different items live in different X columns on a merged row), then
        we try to attach a smaller leftover price as unit_price by X distance.
    """
    out: list[tuple[str, float | None, float | None]] = []
    if len(barcodes) <= 1:
        for x, bc in barcodes:
            if not prices:
                out.append((bc, None, None)); continue
            sorted_p = sorted(prices, key=lambda p: -p[1])  # largest first
            line_total = sorted_p[0][1]
            unit_price = sorted_p[1][1] if len(sorted_p) > 1 and sorted_p[1][1] < line_total else None
            out.append((bc, line_total, unit_price))
        return out

    available = list(prices)
    # First pass: each barcode claims its closest-X price as line_total.
    chosen_totals: list[float | None] = [None] * len(barcodes)
    for i, (x, _bc) in enumerate(barcodes):
        if not available:
            continue
        idx = min(range(len(available)), key=lambda k: abs(available[k][0] - x))
        chosen_totals[i] = available.pop(idx)[1]
    # Second pass: any remaining smaller prices become unit_prices.
    chosen_unit: list[float | None] = [None] * len(barcodes)
    for i, (x, _bc) in enumerate(barcodes):
        lt = chosen_totals[i]
        if lt is None or not available:
            continue
        smaller = [(k, p) for k, p in enumerate(available) if p[1] < lt]
        if not smaller:
            continue
        idx = min(smaller, key=lambda kp: abs(kp[1][0] - x))[0]
        chosen_unit[i] = available.pop(idx)[1]
    for (_, bc), lt, up in zip(barcodes, chosen_totals, chosen_unit):
        out.append((bc, lt, up))
    return out


def parse_ocr_to_extracted(ocr: OCRResult) -> Extracted:
    """Turn an OCRResult into an Extracted.

    Strategy: sequential scan sorted by Y position.
    1. Collect all (y, barcode) and (y, price) events from every OCR line.
    2. Sort by Y. For each barcode, claim the nearest unclaimed price within
       a scan window of 3 line-heights (above or below).
    3. Barcodes resolved first; remaining prices become anonymous items.
    """
    if not ocr.lines:
        return Extracted(items=[], total_paid=None, chain_guess=None, city=None,
                         ai={"provider": ocr.provider, "latency_ms": ocr.latency_ms,
                             "ocr_lines": 0, "image_size": list(ocr.image_size)})

    y_tol = _auto_y_tol(ocr.lines)
    scan_window = y_tol * 3  # look up to 3 line-heights away for a price

    # --- collect all barcodes and prices with positions ---
    bc_events:    list[tuple[float, str, str]] = []   # (y, barcode, raw_line)
    price_events: list[tuple[float, float, str]] = [] # (y, value, raw_line)
    qty_events:   list[tuple[float, float]] = []       # (y, qty)

    all_barcodes: list[str] = []
    _seen_bc_pos: dict[str, float] = {}  # barcode → first Y seen; dedup multipass duplicates
    for ln in sorted(ocr.lines, key=_row_y):
        y = _row_y(ln)
        for bc in _extract_barcodes(ln.text):
            prev_y = _seen_bc_pos.get(bc)
            if prev_y is not None and abs(prev_y - y) <= y_tol:
                continue  # same barcode at same position from another OCR pass
            _seen_bc_pos[bc] = y
            bc_events.append((y, bc, ln.text))
            all_barcodes.append(bc)
        for p in PRICE_RE.findall(ln.text):
            v = _to_float(p)
            if v is not None:
                price_events.append((y, v, ln.text))
        for q in QTY_RE.findall(ln.text):
            try:
                qty_events.append((y, float(q)))
            except ValueError:
                pass

    catalog = _lookup_barcodes(all_barcodes)

    # --- greedily match each barcode to the nearest unclaimed price ---
    claimed = [False] * len(price_events)

    def _find_price(bc_y: float) -> tuple[int, float] | None:
        """Return (index, value) of nearest unclaimed price within scan_window."""
        best_idx, best_dist, best_val = -1, float("inf"), 0.0
        for i, (py, pv, _) in enumerate(price_events):
            if claimed[i]:
                continue
            dist = abs(py - bc_y)
            if dist <= scan_window and dist < best_dist:
                best_idx, best_dist, best_val = i, dist, pv
        return (best_idx, best_val) if best_idx >= 0 else None

    items: list[ExtractedItem] = []
    for bc_y, bc, raw in bc_events:
        cat = catalog.get(bc)
        name = cat["name"] if cat and cat.get("name") else None
        hit = _find_price(bc_y)
        if hit:
            idx, price = hit
            claimed[idx] = True
        else:
            price = 0.0
        # qty from nearby qty_events
        qty = next((q for qy, q in qty_events if abs(qy - bc_y) <= scan_window), None)
        items.append(ExtractedItem(
            raw_name=name or bc,
            barcode=bc,
            quantity=qty,
            unit_price=None,
            line_total=price,
        ))

    # --- remaining unclaimed prices become anonymous items ---
    for i, (py, pv, raw) in enumerate(price_events):
        if claimed[i]:
            continue
        cleaned = BARCODE_RE.sub("", PRICE_RE.sub("", raw)).strip(" \t·-")
        items.append(ExtractedItem(
            raw_name=cleaned,
            barcode=None,
            quantity=None,
            unit_price=None,
            line_total=pv,
        ))

    # Separate barcode items from price-only items.
    bc_items   = [it for it in items if it.barcode]
    anon_items = [it for it in items if not it.barcode]

    # If any price-only item is ≥90% of the barcode-items sum it's the grand
    # total — use it for total_paid but don't count it as a purchase.
    bc_sum = sum(it.line_total for it in bc_items if it.line_total > 0)
    grand_total: float | None = None
    leftover_anon: list[ExtractedItem] = []
    for it in anon_items:
        v = it.line_total
        if v > 999:
            pass  # barcode fragment — skip
        elif bc_sum > 0 and 0.85 <= v / bc_sum <= 1.15:
            grand_total = v  # matches sum of barcode items → receipt total
        elif bc_sum > 0 and v > bc_sum * 1.15:
            grand_total = grand_total or v  # keep largest as backup total
        else:
            leftover_anon.append(it)

    items = bc_items + leftover_anon
    total_paid = grand_total or (bc_sum if bc_sum else None)

    return Extracted(
        items=items,
        total_paid=total_paid,
        chain_guess=None,
        city=None,
        ai={
            "provider": ocr.provider,
            "latency_ms": ocr.latency_ms,
            "ocr_lines": len(ocr.lines),
            "image_size": list(ocr.image_size),
        },
    )
