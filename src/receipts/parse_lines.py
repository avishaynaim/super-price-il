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
from ..db.pg import cursor as _pg_cursor

# Israeli EAN-13: prefix 729 + 10 digits. Prefix 729 covers Israel; some chains
# emit 7290 (consumer) or 7296/7298 (own-brand). Allow a leading non-digit so
# we don't false-match inside longer numbers.
BARCODE_RE = re.compile(r"(?<!\d)(729\d{10})(?!\d)")
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
    """Pick y_tol based on the median line height — adapts to scale."""
    if not lines:
        return 0.012
    heights = sorted(ln.box[3] for ln in lines)
    median = heights[len(heights) // 2]
    # Two centers are 'same row' if within ~55% of one line height.
    return max(0.006, min(0.025, median * 0.55))


def _group_rows(lines: list[OCRLine], y_tol: float | None = None) -> list[list[OCRLine]]:
    """Cluster lines that share approximately the same vertical center.

    `y_tol` is in normalized image coords (1.0 = full height). When None,
    we pick a value from the median line height in the input.
    """
    if y_tol is None:
        y_tol = _auto_y_tol(lines)
    sorted_lines = sorted(lines, key=_row_y)
    rows: list[list[OCRLine]] = []
    for ln in sorted_lines:
        if rows and abs(_row_y(ln) - _row_y(rows[-1][-1])) <= y_tol:
            rows[-1].append(ln)
        else:
            rows.append([ln])
    return rows


def _lookup_barcodes(barcodes: Iterable[str]) -> dict[str, dict]:
    bcs = list({b for b in barcodes if b})
    if not bcs:
        return {}
    with _pg_cursor() as cur:
        cur.execute(
            "SELECT barcode, name, manufacturer FROM products WHERE barcode = ANY(%s)",
            (bcs,),
        )
        rows = cur.fetchall()
    return {r["barcode"]: dict(r) for r in rows}


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
        for bc in BARCODE_RE.findall(ln.text):
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
    """Turn an OCRResult into an Extracted. Only emits items where we found at
    least a barcode or a price — junk detections are dropped.

    Strategy:
      1. Group OCR lines into rows by Y proximity (auto y_tol from line height).
      2. Within each row, pair each barcode with its nearest-X price.
      3. Absorb orphan rows (price-only, no barcode) into the nearest
         barcode-bearing row above or below.
    """
    rows = _group_rows(ocr.lines)

    # First pass: lookup barcodes once.
    all_barcodes: list[str] = []
    row_features: list[dict] = []
    for row in rows:
        feat = _row_features(row)
        row_features.append(feat)
        all_barcodes.extend(bc for _, bc in feat["barcodes"])
    catalog = _lookup_barcodes(all_barcodes)

    # Second pass: absorb orphan price-only rows into an immediately-adjacent
    # barcode row IFF that host row has no price yet. The "no price yet" guard
    # prevents pulling the receipt's grand total down onto the last item — a
    # common mistake on Carrefour-style receipts where totals sit just below.
    has_bc = [bool(f["barcodes"]) for f in row_features]
    for ri, feat in enumerate(row_features):
        if has_bc[ri] or not feat["prices"]:
            continue
        # only consider directly-adjacent rows (above first, then below)
        host = None
        for j in (ri - 1, ri + 1):
            if 0 <= j < len(row_features) and has_bc[j] and not row_features[j]["prices"]:
                host = j
                break
        if host is None:
            continue
        row_features[host]["prices"].extend(feat["prices"])
        feat["prices"].clear()

    # Third pass: emit items.
    items: list[ExtractedItem] = []
    for ri, feat in enumerate(row_features):
        bcs = feat["barcodes"]
        prices = feat["prices"]
        qtys = feat["qtys"]
        weights = feat["weights"]
        joined = " ".join(ln.text for ln in rows[ri])

        if not bcs and not prices:
            continue

        qty: float | None = (
            weights[0] if weights else (qtys[0] if qtys else None)
        )

        if bcs:
            for bc, line_total, unit_price in _pair_prices_to_barcodes(bcs, prices):
                cat = catalog.get(bc)
                name = cat["name"] if cat and cat.get("name") else None
                items.append(ExtractedItem(
                    raw_name=name or BARCODE_RE.sub("", PRICE_RE.sub("", joined)).strip(" \t·-") or bc,
                    barcode=bc,
                    quantity=qty,
                    unit_price=unit_price,
                    line_total=line_total or 0.0,
                ))
        elif prices:
            # No barcode and we couldn't absorb upstream — emit a price-only
            # item so the user sees we noticed something there.
            biggest = max(prices, key=lambda p: p[1])[1]
            items.append(ExtractedItem(
                raw_name=BARCODE_RE.sub("", PRICE_RE.sub("", joined)).strip(" \t·-"),
                barcode=None, quantity=qty, unit_price=None, line_total=biggest,
            ))

    total_paid = sum(it.line_total for it in items if it.line_total > 0) or None

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
