"""Receipt ingest → product match → chain-comparison pipeline.

Flow:
  1. Accept a file (photo: image/*, digital: application/pdf).
  2. Extract line items:
       - PDF  → pypdf text → Claude (extract JSON)
       - image → Claude vision (extract JSON directly, no local OCR)
  3. Match each line to a product row:
       - if raw barcode parses: exact match
       - else: token-Jaccard fuzzy match on products.name
  4. Basket alternatives: for every active chain with data, sum the cheapest
     current_price per matched product (optionally filtered by city). That's the
     "what would this basket cost at chain X" answer.

The pricing intuition: the reported basket price is the sum of the user's line
totals (truth from their receipt). The alternative per chain is the sum of the
cheapest store price per matched product within that chain/city. Unmatched items
contribute 0 to alternatives — the UI should show a match-confidence column so
the user can see what's uncounted.

Prompt caching: the extraction system prompt is large and identical across
requests; mark it ephemeral so repeated uploads hit cache.
"""
from __future__ import annotations

import base64
import io
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..db.connection import connect

MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

EXTRACT_SYSTEM = (
    "You read Israeli supermarket receipts and return a strictly-formatted JSON "
    "object. Never explain; never add prose; output JSON only.\n\n"
    "Schema:\n"
    "{\n"
    '  "chain_guess": string|null,   # chain name in Hebrew if visible\n'
    '  "city": string|null,          # city if visible on the header\n'
    '  "purchased_at": string|null,  # ISO 8601 if visible\n'
    '  "total_paid": number|null,    # grand total shown\n'
    '  "items": [\n'
    "    {\n"
    '      "raw_name": string,       # the product text as printed\n'
    '      "barcode":  string|null,  # if printed separately\n'
    '      "quantity": number|null,  # pieces or weight; default 1\n'
    '      "unit_price": number|null,\n'
    '      "line_total": number      # final amount for this line, after discounts\n'
    "    }\n"
    "  ]\n"
    "}\n\n"
    "Rules:\n"
    "- Skip deposit lines (פיקדון) and discount summary lines.\n"
    "- Numbers use '.' as decimal. Do not emit thousand separators.\n"
    "- If quantity is weight (kg) emit it as a float and keep unit_price per kg.\n"
    "- If you are unsure of a field, use null — never guess."
)

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
    if not text.strip():
        # encrypted or scanned PDF — fall back to sending pages as images
        return _call_claude_on_image(pdf_bytes, media_type="application/pdf")
    return _call_claude_on_text(text)


def extract_from_image(img_bytes: bytes, media_type: str) -> Extracted:
    return _call_claude_on_image(img_bytes, media_type=media_type)


def _parse_json_envelope(text: str) -> dict[str, Any]:
    m = JSON_FENCE.search(text)
    payload = m.group(1) if m else text.strip()
    i, j = payload.find("{"), payload.rfind("}")
    if i != -1 and j != -1 and j > i:
        payload = payload[i:j + 1]
    return json.loads(payload)


def _to_extracted(obj: dict[str, Any], ai: dict[str, Any] | None = None) -> Extracted:
    items = []
    for it in obj.get("items") or []:
        try:
            items.append(ExtractedItem(
                raw_name=str(it.get("raw_name") or "").strip(),
                barcode=(str(it["barcode"]) if it.get("barcode") else None),
                quantity=(float(it["quantity"]) if it.get("quantity") is not None else None),
                unit_price=(float(it["unit_price"]) if it.get("unit_price") is not None else None),
                line_total=float(it.get("line_total") or 0.0),
            ))
        except (TypeError, ValueError):
            continue
    return Extracted(
        items=items,
        total_paid=(float(obj["total_paid"]) if obj.get("total_paid") is not None else None),
        chain_guess=obj.get("chain_guess"),
        city=obj.get("city"),
        ai=ai,
    )


def _usage_dict(resp, model: str, latency_ms: int) -> dict[str, Any]:
    u = getattr(resp, "usage", None)
    return {
        "model": model,
        "latency_ms": latency_ms,
        "input_tokens": getattr(u, "input_tokens", None),
        "output_tokens": getattr(u, "output_tokens", None),
        "cache_read_input_tokens": getattr(u, "cache_read_input_tokens", None),
        "cache_creation_input_tokens": getattr(u, "cache_creation_input_tokens", None),
        "stop_reason": getattr(resp, "stop_reason", None),
    }


def _anthropic():
    from anthropic import Anthropic
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    return Anthropic(api_key=key)


def _call_claude_on_text(text: str) -> Extracted:
    client = _anthropic()
    t0 = time.perf_counter()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=[{"type": "text", "text": EXTRACT_SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": [
            {"type": "text", "text": "Extract items from this receipt text:\n\n" + text[:30_000]},
        ]}],
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)
    body = "".join(b.text for b in resp.content if b.type == "text")
    return _to_extracted(_parse_json_envelope(body), ai=_usage_dict(resp, MODEL, latency_ms))


def _call_claude_on_image(data: bytes, media_type: str) -> Extracted:
    client = _anthropic()
    b64 = base64.b64encode(data).decode()
    # vision supports jpeg/png/gif/webp; PDFs go through the documents endpoint
    block: dict[str, Any]
    if media_type == "application/pdf":
        block = {"type": "document",
                 "source": {"type": "base64", "media_type": media_type, "data": b64}}
    else:
        # normalize unusual image types to png via Pillow
        allowed = {"image/jpeg", "image/png", "image/gif", "image/webp"}
        if media_type not in allowed:
            from PIL import Image
            img = Image.open(io.BytesIO(data)).convert("RGB")
            out = io.BytesIO(); img.save(out, format="PNG")
            data = out.getvalue()
            media_type = "image/png"
            b64 = base64.b64encode(data).decode()
        block = {"type": "image",
                 "source": {"type": "base64", "media_type": media_type, "data": b64}}

    t0 = time.perf_counter()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=[{"type": "text", "text": EXTRACT_SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": [
            block,
            {"type": "text", "text": "Return the JSON object per schema."},
        ]}],
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)
    body = "".join(b.text for b in resp.content if b.type == "text")
    return _to_extracted(_parse_json_envelope(body), ai=_usage_dict(resp, MODEL, latency_ms))


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
    conn = connect()
    try:
        out: list[dict[str, Any]] = []
        all_products = conn.execute(
            "SELECT id, barcode, name FROM products WHERE name IS NOT NULL AND length(name) > 2"
        ).fetchall()
        # pre-tokenize once
        toks = [(p["id"], p["barcode"], p["name"], _tokens(p["name"])) for p in all_products]

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
    finally:
        conn.close()


# ---------------- basket alternatives ----------------

def basket_alternatives(matched: list[dict[str, Any]], city: str | None) -> list[dict[str, Any]]:
    """For each chain, sum the cheapest current_price of each matched product.

    Only counts matched items (has product_id). Unmatched items contribute 0 —
    the UI should expose the coverage ratio so results are interpretable.
    """
    product_ids = [m["product_id"] for m in matched if m.get("product_id")]
    if not product_ids:
        return []

    conn = connect()
    try:
        placeholders = ",".join(["?"] * len(product_ids))
        params: list = list(product_ids)
        sql = f"""
            SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                   cp.product_id, MIN(cp.price) AS cheap
              FROM current_prices cp
              JOIN stores s  ON s.id = cp.store_id
              JOIN chains ch ON ch.id = s.chain_id
             WHERE cp.product_id IN ({placeholders})
        """
        if city:
            sql += " AND s.city LIKE ?"; params.append(f"%{city}%")
        sql += " GROUP BY ch.id, cp.product_id"
        rows = conn.execute(sql, params).fetchall()
        by_chain: dict[str, dict[str, Any]] = {}
        # quantity lookup
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
    finally:
        conn.close()


def store_receipt(source: str, extracted: Extracted, matched: list[dict[str, Any]]) -> int:
    """Persist the receipt + items; return receipts.id."""
    conn = connect()
    try:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn.execute("BEGIN")
        cur = conn.execute(
            "INSERT INTO receipts(source_type, purchased_at, total, uploaded_at) VALUES (?, ?, ?, ?)",
            (source, None, extracted.total_paid, now),
        )
        rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for it in matched:
            conn.execute(
                """INSERT INTO receipt_items
                   (receipt_id, product_id, raw_name, raw_barcode, quantity,
                    unit_price, line_total, match_confidence)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (rid, it.get("product_id"), it.get("raw_name"), it.get("barcode"),
                 it.get("quantity"), it.get("unit_price"), it.get("line_total"),
                 it.get("match_confidence")),
            )
        conn.execute("COMMIT")
        return rid
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
