"""Receipt ingest → product match → chain-comparison pipeline.

Flow:
  1. Accept a file (photo: image/*, digital: application/pdf).
  2. Extract line items (free OCR only — no Claude API calls):
       - PDF  → pypdf text extraction → local regex parser
               (scanned PDF) → pymupdf render → RapidOCR → local parser
       - image → RapidOCR (local) → OCR.space (free tier) → local parser
  3. Match each line to a product row:
       - if raw barcode parses: exact match
       - else: token-Jaccard fuzzy match on products.name
  4. Basket alternatives: for every active chain with data, sum the cheapest
     current_price per matched product (optionally filtered by city).
"""
from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..db.pg import connect as _pg_connect, cursor as _pg_cursor

JSON_FENCE = re.compile(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", re.I)


@dataclass
class ExtractedItem:
    raw_name: str
    barcode: str | None
    quantity: float | None
    unit_price: float | None
    line_total: float


@dataclass
class Extracted:
    items: list[ExtractedItem]
    total_paid: float | None
    chain_guess: str | None
    city: str | None
    ai: dict[str, Any] | None = field(default=None)


# ---------------- extraction ----------------

def extract_from_pdf(pdf_bytes: bytes) -> Extracted:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = "\n".join(p.extract_text() or "" for p in reader.pages)
    if text.strip():
        return _extract_from_pdf_text(text)
    # Scanned/encrypted PDF — render first page with pymupdf then run OCR.
    return _extract_from_scanned_pdf(pdf_bytes)


def _extract_from_pdf_text(text: str) -> Extracted:
    """Parse plain-text receipt (from pypdf) using the same regex pipeline as OCR."""
    from .free_ocr import OCRLine, OCRResult
    from .parse_lines import parse_ocr_to_extracted

    lines_text = [ln for ln in text.splitlines() if ln.strip()]
    n = len(lines_text) or 1
    ocr_lines = [
        OCRLine(text=ln, box=(0.0, i / n, 1.0, 1.0 / n), confidence=1.0)
        for i, ln in enumerate(lines_text)
    ]
    result = OCRResult(lines=ocr_lines, provider="pypdf", latency_ms=0, image_size=(800, 1200))
    return parse_ocr_to_extracted(result)


def _extract_from_scanned_pdf(pdf_bytes: bytes) -> Extracted:
    """Render the first page with pymupdf and run RapidOCR on it."""
    import fitz
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    pix = page.get_pixmap(dpi=150)
    img_bytes = pix.tobytes("png")
    doc.close()
    return extract_from_image(img_bytes, "image/png")


def extract_from_image(
    img_bytes: bytes,
    media_type: str,
    provider: str | None = None,
) -> Extracted:
    """Extract receipt items from an image using free OCR only.

    `provider`:
      - "rapid"    — local RapidOCR (default).
      - "ocrspace" — OCR.space web API (free tier).
      - "auto"     — try rapid → ocrspace; first non-empty wins.
    Falls back to OCR_PROVIDER env, then "rapid".
    """
    from .free_ocr import run_chain
    from .parse_lines import parse_ocr_to_extracted

    name = (provider or os.environ.get("OCR_PROVIDER") or "rapid").lower()
    chain = ("rapid", "ocrspace") if name == "auto" else (name,)
    ocr = run_chain(img_bytes, chain)
    return parse_ocr_to_extracted(ocr)


# ---------------- matching ----------------

_TOKEN = re.compile(r"[\w֐-׿]+", re.UNICODE)


def _tokens(s: str) -> set[str]:
    return {t for t in _TOKEN.findall(s.lower()) if len(t) > 1}


def match_items(items: list[ExtractedItem]) -> list[dict[str, Any]]:
    """Attach product_id + confidence to each extracted item.

    Strategy:
      - If barcode looks valid and exists, match with confidence 1.0.
      - Else, compute token-Jaccard similarity against every product.name. Pick
        the best ≥0.3. Ties broken by name-length difference (shorter wins).
    """
    with _pg_cursor() as cur:
        cur.execute(
            "SELECT id, barcode, name FROM products WHERE name IS NOT NULL AND length(name) > 2"
        )
        all_products = cur.fetchall()
    # pre-tokenize once
    toks = [(p["id"], p["barcode"], p["name"], _tokens(p["name"])) for p in all_products]

    out: list[dict[str, Any]] = []
    with _pg_cursor() as cur:
        for it in items:
            row: dict[str, Any] = {
                "raw_name": it.raw_name,
                "barcode": it.barcode,
                "name": None,
                "quantity": it.quantity,
                "unit_price": it.unit_price,
                "line_total": it.line_total,
                "product_id": None,
                "match_confidence": None,
            }
            if it.barcode and it.barcode.isdigit():
                cur.execute(
                    "SELECT id, name FROM products WHERE barcode = %s", (it.barcode,)
                )
                r = cur.fetchone()
                if r:
                    row["product_id"] = r["id"]; row["name"] = r["name"]; row["match_confidence"] = 1.0
                    out.append(row); continue

            qtok = _tokens(it.raw_name)
            if not qtok:
                out.append(row); continue
            best: tuple[float, int, str, str] | None = None
            for pid, bc, nm, ptok in toks:
                if not ptok:
                    continue
                inter = len(qtok & ptok)
                if not inter:
                    continue
                union = len(qtok | ptok)
                j = inter / union
                if j < 0.3:
                    continue
                penalty = abs(len(nm) - len(it.raw_name)) / 100.0
                score = j - penalty
                if not best or score > best[0]:
                    best = (score, pid, bc, nm)
            if best and best[0] >= 0.25:
                row["product_id"] = best[1]; row["barcode"] = row["barcode"] or best[2]
                row["name"] = best[3]; row["match_confidence"] = round(best[0], 3)
            out.append(row)
    return out


# ---------------- basket alternatives ----------------

def basket_alternatives(matched: list[dict[str, Any]], city: str | None) -> list[dict[str, Any]]:
    """For each chain, sum the cheapest current_price of each matched product.

    Only counts matched items (has product_id). Unmatched items contribute 0 —
    the UI should expose the coverage ratio so results are interpretable.

    City scope is *chain-level*: any chain that has at least one store in the
    requested city counts, even if its prices live on a logical master-store
    row with no city of its own (Tiv Taam-style chain-wide pricelist).
    """
    product_ids = [m["product_id"] for m in matched if m.get("product_id")]
    if not product_ids:
        return []

    params: list = list(product_ids)
    sql = """
        SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
               cp.product_id, MIN(cp.price) AS cheap
          FROM current_prices cp
          JOIN stores s  ON s.id = cp.store_id
          JOIN chains ch ON ch.id = s.chain_id
         WHERE cp.product_id = ANY(%s)
    """
    qparams: list = [product_ids]
    if city:
        sql += (
            " AND ch.id IN ("
            "SELECT DISTINCT chain_id FROM stores WHERE city ILIKE %s)"
        )
        qparams.append(f"%{city}%")
    sql += " GROUP BY ch.id, ch.code, ch.name_he, cp.product_id"
    with _pg_cursor() as cur:
        cur.execute(sql, qparams)
        rows = cur.fetchall()
    by_chain: dict[str, dict[str, Any]] = {}
    qty: dict[int, float] = {m["product_id"]: float(m.get("quantity") or 1) for m in matched if m.get("product_id")}
    for r in rows:
        c = by_chain.setdefault(r["chain_code"], {
            "chain_code": r["chain_code"],
            "chain_name_he": r["chain_name_he"],
            "basket_total": 0.0,
            "matched": 0,
        })
        c["basket_total"] += r["cheap"] * qty.get(r["product_id"], 1.0)
        c["matched"] += 1
    return sorted(by_chain.values(), key=lambda c: c["basket_total"])


def store_receipt(source: str, extracted: Extracted, matched: list[dict[str, Any]]) -> int:
    """Persist the receipt + items; return receipts.id."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = _pg_connect()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            import psycopg2.extras as _pge
            cur.execute(
                "INSERT INTO receipts(source_type, purchased_at, total, uploaded_at) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (source, None, extracted.total_paid, now),
            )
            rid = cur.fetchone()[0]
            if matched:
                _pge.execute_values(
                    cur,
                    """INSERT INTO receipt_items
                       (receipt_id, product_id, raw_name, raw_barcode, quantity,
                        unit_price, line_total, match_confidence)
                       VALUES %s""",
                    [
                        (rid, it.get("product_id"), it.get("raw_name"), it.get("barcode"),
                         it.get("quantity"), it.get("unit_price"), it.get("line_total"),
                         it.get("match_confidence"))
                        for it in matched
                    ],
                )
        conn.commit()
        return rid
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
