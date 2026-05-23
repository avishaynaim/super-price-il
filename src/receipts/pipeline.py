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

from ..db.local import cursor as _local_cursor

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
    """Extract receipt items from an image.

    `provider`:
      - "claude"   — Claude Code vision (default; best recall, no API key needed).
      - "rapid"    — local RapidOCR.
      - "ocrspace" — OCR.space web API (free tier).
      - "auto"     — try claude → ocrspace → rapid; first non-empty wins.
    Falls back to OCR_PROVIDER env, then "claude".
    """
    name = (provider or os.environ.get("OCR_PROVIDER") or "rapid-multi").lower()

    if name in ("claude", "claude-code"):
        return _extract_from_image_claude(img_bytes)
    if name == "auto":
        # rapid-multi = 90% local; ocrspace = 85% cloud fallback
        for p in ("rapid-multi", "ocrspace", "claude"):
            try:
                result = extract_from_image(img_bytes, media_type, provider=p)
                if result.items:
                    return result
            except Exception:
                continue
        return Extracted(items=[], total_paid=None, chain_guess=None, city=None)

    from .free_ocr import run_chain
    from .parse_lines import parse_ocr_to_extracted
    ocr = run_chain(img_bytes, (name,))
    return parse_ocr_to_extracted(ocr)


def _extract_from_image_claude(img_bytes: bytes) -> Extracted:
    """Use Claude Code OCR to extract barcodes from a receipt image.

    Routes through the daemon (preferred — amortizes startup cost) or falls
    back to a direct subprocess call if the daemon isn't running.
    """
    import asyncio
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(img_bytes)
        tmp = f.name

    barcodes: list[str] = []
    try:
        # Try the daemon first (faster — reuses running process)
        try:
            from .claude_ocr_daemon import ocr_daemon
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Inside async context (FastAPI) — use run_in_executor
                import concurrent.futures
                future = asyncio.run_coroutine_threadsafe(
                    ocr_daemon.extract_barcodes(tmp, timeout=200), loop
                )
                barcodes = future.result(timeout=210)
            else:
                barcodes = loop.run_until_complete(
                    ocr_daemon.extract_barcodes(tmp, timeout=200)
                )
        except Exception:
            # Fallback: direct subprocess
            from .claude_code_ocr import extract_barcodes
            barcodes = extract_barcodes(tmp, timeout=200)
    finally:
        os.unlink(tmp)

    if not barcodes:
        return Extracted(items=[], total_paid=None, chain_guess=None, city=None,
                         ai={"provider": "claude-code", "error": "no barcodes detected"})

    # Each barcode becomes one ExtractedItem; DB match will fill in name/price
    items = [
        ExtractedItem(raw_name="", barcode=bc, quantity=1, unit_price=None, line_total=0)
        for bc in barcodes
    ]
    return Extracted(
        items=items,
        total_paid=None,
        chain_guess=None,
        city=None,
        ai={"provider": "claude-code", "barcodes_found": len(barcodes)},
    )


# ---------------- matching ----------------

_TOKEN = re.compile(r"[\w֐-׿]+", re.UNICODE)


def _tokens(s: str) -> set[str]:
    return {t for t in _TOKEN.findall(s.lower()) if len(t) > 1}


def match_items(items: list[ExtractedItem]) -> list[dict[str, Any]]:
    """Attach product_id + confidence to each extracted item."""
    # Only load the full product list if there are items without barcodes
    # (fuzzy name matching). Barcode-only items skip the 187k-row load.
    needs_fuzzy = any(not it.barcode for it in items)
    toks: list[tuple] = []
    if needs_fuzzy:
        try:
            with _local_cursor() as conn:
                all_products = conn.execute(
                    "SELECT id, barcode, name FROM products WHERE name IS NOT NULL AND length(name) > 2"
                ).fetchall()
            toks = [(p["id"], p["barcode"], p["name"], _tokens(p["name"])) for p in all_products]
        except Exception:
            pass

    out: list[dict[str, Any]] = []
    try:
        db_ctx = _local_cursor()
        conn = db_ctx.__enter__()
    except Exception:
        conn = None
        db_ctx = None

    try:
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
            if it.barcode and it.barcode.isdigit() and conn:
                r = conn.execute(
                    "SELECT id, name FROM products WHERE barcode = ?", (it.barcode,)
                ).fetchone()
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
                j = inter / len(qtok | ptok)
                if j < 0.3:
                    continue
                score = j - abs(len(nm) - len(it.raw_name)) / 100.0
                if not best or score > best[0]:
                    best = (score, pid, bc, nm)
            if best and best[0] >= 0.25:
                row["product_id"] = best[1]; row["barcode"] = row["barcode"] or best[2]
                row["name"] = best[3]; row["match_confidence"] = round(best[0], 3)
            out.append(row)
    finally:
        if db_ctx:
            db_ctx.__exit__(None, None, None)
    return out


# ---------------- basket alternatives ----------------

def basket_alternatives(matched: list[dict[str, Any]], city: str | None) -> list[dict[str, Any]]:
    """Compare basket cost across chains using estimated full-basket pricing.

    Strategy for missing products (unfair comparison problem):
      - Track how many of the receipt's matched products each chain carries.
      - Compute `coverage` = products_found / total_matched_products.
      - Compute `estimated_total` = basket_total * (1 / coverage) — scales up the
        partial basket to a full-basket estimate assuming the chain's missing items
        would cost proportionally the same.
      - Sort by estimated_total so chains are compared fairly.
      - Flag chains with coverage < 0.4 as `low_coverage=True` so the UI can grey
        them out (their estimate is based on too few items to trust).
    """
    product_ids = [m["product_id"] for m in matched if m.get("product_id")]
    total_matched = len(product_ids)
    if not product_ids:
        return []

    try:
        placeholders = ",".join("?" * len(product_ids))
        sql = f"""
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   cp.product_id, MIN(cp.price) AS cheap
              FROM current_prices cp
              JOIN stores s  ON s.id = cp.store_id
              JOIN chains ch ON ch.id = s.chain_id
             WHERE cp.product_id IN ({placeholders})
        """
        params: list = list(product_ids)
        if city:
            sql += " AND ch.id IN (SELECT DISTINCT chain_id FROM stores WHERE city LIKE ?)"
            params.append(f"%{city}%")
        sql += " GROUP BY ch.id, ch.code, ch.name_he, cp.product_id"
        with _local_cursor() as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []

    by_chain: dict[str, dict[str, Any]] = {}
    qty: dict[int, float] = {m["product_id"]: float(m.get("quantity") or 1) for m in matched if m.get("product_id")}
    for r in rows:
        c = by_chain.setdefault(r["chain_code"], {
            "chain_code": r["chain_code"],
            "chain_name_he": r["chain_name_he"],
            "basket_total": 0.0,    # cost of found items only
            "items_found": 0,
            "items_total": total_matched,
        })
        c["basket_total"] += r["cheap"] * qty.get(r["product_id"], 1.0)
        c["items_found"] += 1

    results = []
    for c in by_chain.values():
        coverage = c["items_found"] / total_matched if total_matched else 0
        # Estimated full-basket: scale up proportionally for missing items
        estimated = c["basket_total"] / coverage if coverage > 0 else float("inf")
        results.append({
            **c,
            "coverage": round(coverage, 3),
            "estimated_total": round(estimated, 2),
            "low_coverage": coverage < 0.4,
        })

    # Sort: chains with good coverage first, by estimated total; low-coverage last
    return sorted(results, key=lambda c: (c["low_coverage"], c["estimated_total"]))


def item_price_comparison(matched: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """For every matched product return best and worst price across ALL stores.

    Returns a list parallel to `matched`. Items without a product_id get an
    empty comparison entry so the index stays aligned.

    Each entry:
        {
            "product_id": int | None,
            "barcode":    str | None,
            "cheapest":   {"chain_code": str, "chain_name_he": str, "price": float} | None,
            "priciest":   {"chain_code": str, "chain_name_he": str, "price": float} | None,
            "you_paid":   float | None,          # unit_price from the receipt
            "savings":    float | None,           # you_paid - cheapest.price (per unit)
            "all_chains": [{"chain_code", "chain_name_he", "min_price", "max_price"}, ...],
        }
    """
    product_ids = [m["product_id"] for m in matched if m.get("product_id")]
    if not product_ids:
        return [{"product_id": None, "barcode": m.get("barcode"),
                 "cheapest": None, "priciest": None, "you_paid": None,
                 "savings": None, "all_chains": []} for m in matched]

    from ..db.price_index import price_index

    # Try warm cache first (O(1)); collect misses for one batch SQL query
    by_product: dict[int, list[dict]] = {}
    cache_misses: list[int] = []

    for m in matched:
        bc  = m.get("barcode")
        pid = m.get("product_id")
        info = price_index.lookup(bc) if bc else None
        if info:
            by_product[info["product_id"]] = info.get("chains", [])
        elif pid:
            cache_misses.append(pid)

    # Batch SQL for any misses — single query vs N×52ms
    if cache_misses:
        try:
            ph = ",".join("?" * len(cache_misses))
            with _local_cursor() as conn:
                rows = conn.execute(
                    f"""SELECT cp.product_id,
                               ch.code    AS chain_code,
                               ch.name_he AS chain_name_he,
                               MIN(cp.price) AS min_price,
                               MAX(cp.price) AS max_price
                          FROM current_prices cp
                          JOIN stores s  ON s.id  = cp.store_id
                          JOIN chains ch ON ch.id = s.chain_id
                         WHERE cp.product_id IN ({ph})
                      GROUP BY cp.product_id, ch.id""",
                    cache_misses,
                ).fetchall()
            for r in rows:
                by_product.setdefault(r["product_id"], []).append({
                    "chain_code":    r["chain_code"],
                    "chain_name_he": r["chain_name_he"],
                    "min_price":     r["min_price"],
                    "max_price":     r["max_price"],
                })
        except Exception:
            pass

    out: list[dict[str, Any]] = []
    for m in matched:
        pid = m.get("product_id")
        chains = by_product.get(pid, []) if pid else []
        if chains:
            cheapest_row = min(chains, key=lambda c: c["min_price"])
            priciest_row = max(chains, key=lambda c: c["max_price"])
            cheapest = {"chain_code": cheapest_row["chain_code"],
                        "chain_name_he": cheapest_row["chain_name_he"],
                        "price": cheapest_row["min_price"]}
            priciest = {"chain_code": priciest_row["chain_code"],
                        "chain_name_he": priciest_row["chain_name_he"],
                        "price": priciest_row["max_price"]}
            you_paid = m.get("unit_price")
            savings  = round(float(you_paid) - cheapest["price"], 2) if you_paid else None
        else:
            cheapest = priciest = you_paid = savings = None

        out.append({
            "product_id": pid,
            "barcode":    m.get("barcode"),
            "cheapest":   cheapest,
            "priciest":   priciest,
            "you_paid":   you_paid,
            "savings":    savings,
            "all_chains": sorted(chains, key=lambda c: c["min_price"]),
        })
    return out


def store_receipt(source: str, extracted: Extracted, matched: list[dict[str, Any]]) -> int:
    """Persist the receipt + items; return receipts.id. Best-effort — skips on DB error."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        with _local_cursor() as conn:
            # Ensure receipts table exists (might not be in the local schema)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_type TEXT, purchased_at TEXT, total REAL, uploaded_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS receipt_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_id INTEGER, product_id INTEGER, raw_name TEXT,
                    raw_barcode TEXT, quantity REAL, unit_price REAL,
                    line_total REAL, match_confidence REAL
                )
            """)
            cur = conn.execute(
                "INSERT INTO receipts(source_type, purchased_at, total, uploaded_at) VALUES(?,?,?,?)",
                (source, None, extracted.total_paid, now),
            )
            rid = cur.lastrowid
            for it in matched:
                conn.execute(
                    """INSERT INTO receipt_items
                       (receipt_id, product_id, raw_name, raw_barcode, quantity,
                        unit_price, line_total, match_confidence)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (rid, it.get("product_id"), it.get("raw_name"), it.get("barcode"),
                     it.get("quantity"), it.get("unit_price"), it.get("line_total"),
                     it.get("match_confidence")),
                )
            return rid
    except Exception:
        return -1
