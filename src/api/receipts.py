"""Receipt upload endpoint. See src/receipts/pipeline.py for the pipeline."""
from __future__ import annotations

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from ..receipts.pipeline import (
    basket_alternatives,
    extract_from_image,
    extract_from_pdf,
    match_items,
    store_receipt,
)

receipts_router = APIRouter()


@receipts_router.post("/receipts")
async def upload_receipt(
    file: UploadFile = File(...),
    city: str | None = Form(None),
) -> dict:
    data = await file.read()
    if not data:
        raise HTTPException(400, "empty file")

    ct = (file.content_type or "").lower()
    try:
        if ct == "application/pdf" or file.filename.lower().endswith(".pdf"):
            source = "digital"
            ex = extract_from_pdf(data)
        elif ct.startswith("image/") or file.filename.lower().endswith(
            (".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic")
        ):
            source = "photo"
            ex = extract_from_image(data, ct if ct.startswith("image/") else "image/png")
        else:
            raise HTTPException(415, f"unsupported content-type: {ct}")
    except RuntimeError as e:
        raise HTTPException(500, str(e))

    matched = match_items(ex.items)
    alternatives = basket_alternatives(matched, city)
    rid = store_receipt(source, ex, matched)

    return {
        "receipt_id": rid,
        "source": source,
        "chain_guess": ex.chain_guess,
        "city": city or ex.city,
        "total_paid": ex.total_paid or sum(m["line_total"] for m in matched),
        "items": matched,
        "alternatives": alternatives,
        "ai": ex.ai,
    }
