"""Receipt upload endpoint. See src/receipts/pipeline.py for the pipeline."""
from __future__ import annotations

import asyncio
import os
import tempfile
import uuid

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile

from ..receipts.pipeline import (
    basket_alternatives,
    extract_from_image,
    extract_from_pdf,
    item_price_comparison,
    match_items,
    store_receipt,
    Extracted,
    ExtractedItem,
)

receipts_router = APIRouter()


@receipts_router.post("/receipts")
async def upload_receipt(
    file: UploadFile = File(...),
    city: str | None = Form(None),
    provider: str | None = Form(None),
) -> dict:
    """Upload a receipt photo or PDF.

    `provider` selects the OCR backend (overrides OCR_PROVIDER env):
      claude (default) | rapid | ocrspace | auto
    """
    data = await file.read()
    if not data:
        raise HTTPException(400, "empty file")

    ct = (file.content_type or "").lower()
    try:
        if ct == "application/pdf" or (file.filename or "").lower().endswith(".pdf"):
            source = "digital"
            ex = extract_from_pdf(data)
        elif ct.startswith("image/") or (file.filename or "").lower().endswith(
            (".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic")
        ):
            source = "photo"
            ex = extract_from_image(
                data,
                ct if ct.startswith("image/") else "image/png",
                provider=provider,
            )
        else:
            raise HTTPException(415, f"unsupported content-type: {ct}")
    except RuntimeError as e:
        msg = str(e)
        if "no lines" in msg or "ocr" in msg.lower():
            raise HTTPException(422, "לא זוהה טקסט בתמונה — נסה לצלם שוב בתאורה טובה יותר")
        raise HTTPException(500, msg)

    return _build_response(source, ex, city)


# In-memory job store for batch OCR jobs (process lifetime only)
_batch_jobs: dict[str, dict] = {}


@receipts_router.post("/receipts/multi")
async def upload_receipt_multi(
    files: list[UploadFile] = File(...),
    city: str | None = Form(None),
    provider: str | None = Form(None),
) -> dict:
    """Upload multiple photos of one receipt — fast synchronous merge with RapidOCR.

    Each photo is OCR'd independently, then barcodes are deduplicated across photos
    (handles overlapping sections). Returns a single merged receipt result.
    """
    if not files:
        raise HTTPException(400, "no files")

    all_items: list[ExtractedItem] = []
    seen_barcodes: set[str] = set()
    total_paid = None
    chain_guess = None
    photo_count = 0

    for f in files:
        data = await f.read()
        if not data:
            continue
        photo_count += 1
        ct = (f.content_type or "image/jpeg").lower()
        try:
            ex = extract_from_image(data, ct, provider=provider or "rapid-multi")
            # Merge: deduplicate barcodes, keep items unique across photos
            for item in ex.items:
                if item.barcode:
                    if item.barcode in seen_barcodes:
                        continue  # overlap — same barcode from adjacent photo
                    seen_barcodes.add(item.barcode)
                all_items.append(item)
            if ex.total_paid and not total_paid:
                total_paid = ex.total_paid
            if ex.chain_guess and not chain_guess:
                chain_guess = ex.chain_guess
        except Exception:
            continue  # skip failed photos

    if not all_items:
        raise HTTPException(422, "לא זוהו פריטים באף אחת מהתמונות — נסה לצלם מחדש בתאורה טובה יותר")

    merged = Extracted(
        items=all_items, total_paid=total_paid, chain_guess=chain_guess, city=city,
        ai={"provider": provider or "rapid-multi", "photos": photo_count,
            "barcodes_found": len(seen_barcodes)},
    )
    return _build_response("photo", merged, city)


@receipts_router.post("/receipts/batch")
async def upload_receipt_batch(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
    city: str | None = Form(None),
) -> dict:
    """Upload multiple receipt photos for async Claude OCR.

    Returns a job_id immediately. Poll GET /receipts/batch/{job_id} for results.
    Each photo takes ~2 min sequentially on this device.
    """
    if not files:
        raise HTTPException(400, "no files")

    tmp_paths: list[str] = []
    for f in files:
        data = await f.read()
        if not data:
            continue
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as t:
            t.write(data)
            tmp_paths.append(t.name)

    if not tmp_paths:
        raise HTTPException(400, "all files empty")

    job_id = str(uuid.uuid4())[:8]
    _batch_jobs[job_id] = {"status": "processing", "photos": len(tmp_paths), "city": city}

    background_tasks.add_task(_run_batch_job, job_id, tmp_paths, city)
    return {"job_id": job_id, "status": "processing", "photos": len(tmp_paths),
            "poll_url": f"/api/receipts/batch/{job_id}",
            "note": f"Claude OCR running on {len(tmp_paths)} photos. Each takes ~2 min — poll for results."}


@receipts_router.get("/receipts/batch/{job_id}")
async def get_batch_job(job_id: str) -> dict:
    """Poll for batch OCR results."""
    job = _batch_jobs.get(job_id)
    if not job:
        raise HTTPException(404, f"job {job_id!r} not found")
    return job


def _run_batch_job(job_id: str, tmp_paths: list[str], city: str | None) -> None:
    try:
        from ..receipts.claude_code_ocr import extract_barcodes_batch
        barcodes = extract_barcodes_batch(tmp_paths, timeout_per_image=150)
        items = [
            ExtractedItem(raw_name="", barcode=bc, quantity=1, unit_price=None, line_total=0)
            for bc in barcodes
        ]
        ex = Extracted(
            items=items, total_paid=None, chain_guess=None, city=None,
            ai={"provider": "claude-code-batch", "photos": len(tmp_paths), "barcodes_found": len(barcodes)},
        )
        result = _build_response("photo", ex, city)
        _batch_jobs[job_id] = {"status": "done", **result}
    except Exception as e:
        _batch_jobs[job_id] = {"status": "error", "error": str(e)}
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except OSError:
                pass


def _build_response(source: str, ex: Extracted, city: str | None) -> dict:
    # Only keep items where OCR found a full barcode — skip price-only noise lines
    barcode_items = [it for it in ex.items if it.barcode]
    matched      = match_items(barcode_items)
    alternatives = basket_alternatives(matched, city)
    comparisons  = item_price_comparison(matched)
    rid          = store_receipt(source, ex, matched)

    items_out = [{**item, "price_cmp": cmp} for item, cmp in zip(matched, comparisons)]

    return {
        "receipt_id":   rid,
        "source":       source,
        "chain_guess":  ex.chain_guess,
        "city":         city or ex.city,
        "total_paid":   ex.total_paid or sum(m["line_total"] for m in matched),
        "items":        items_out,
        "alternatives": alternatives,
        "ai":           ex.ai,
    }
