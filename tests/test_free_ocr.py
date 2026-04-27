"""Tests for the free-OCR pipeline (RapidOCR / OCR.space) and line parser.

We never actually run RapidOCR or hit OCR.space here — the providers are
stubbed at the boundary (`rapid_ocr`, `ocrspace_ocr`). These tests only
cover the deterministic glue: line grouping, barcode regex, DB lookup,
and the OCR_PROVIDER chain (auto = rapid → ocrspace → claude fallback).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.receipts import free_ocr as F
from src.receipts import parse_lines as PL
from src.receipts import pipeline as P
from src.receipts.free_ocr import OCRLine, OCRResult


# --------------------------------------------------------------------------
# DB fixture — same shape as test_receipt_ocr.seeded_db but local so the two
# files can run independently.
# --------------------------------------------------------------------------

@pytest.fixture
def stub_db(tmp_path, monkeypatch):
    db = tmp_path / "free.db"
    schema = (Path(__file__).resolve().parents[1] / "src" / "db" / "schema.sql").read_text()
    c = sqlite3.connect(str(db)); c.executescript(schema)
    c.executemany(
        "INSERT INTO products(barcode, name, manufacturer) VALUES (?, ?, ?)",
        [
            ("7290000123456", "חלב תנובה 3% 1 ליטר", "תנובה"),
            ("7290000234567", "לחם אחיד אנג'ל",       "אנג'ל"),
            ("7290000345678", "ביצים גדולות L",       "תנובה"),
        ],
    )
    c.commit(); c.close()

    def _fake_connect(*a, **kw):
        x = sqlite3.connect(str(db)); x.row_factory = sqlite3.Row; return x

    monkeypatch.setattr(PL, "connect", _fake_connect)
    return db


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _ocr(lines):
    return OCRResult(
        lines=[OCRLine(text=t, box=b, confidence=c) for (t, b, c) in lines],
        provider="stub", latency_ms=10, image_size=(1000, 1500),
    )


# --------------------------------------------------------------------------
# Line-parser regex coverage
# --------------------------------------------------------------------------

def test_barcode_re_matches_clean_israeli_ean13():
    assert PL.BARCODE_RE.findall("7290000123456") == ["7290000123456"]


def test_barcode_re_does_not_match_inside_longer_digit_run():
    # If the OCR produces "17290000123456789" we must not pull a false barcode.
    assert PL.BARCODE_RE.findall("17290000123456789") == []


def test_price_re_finds_decimals_but_not_barcode_chunks():
    s = "10.85 7290000123456 6.90"
    assert sorted(PL.PRICE_RE.findall(s)) == ["10.85", "6.90"]


def test_qty_re_picks_up_multiplier():
    assert PL.QTY_RE.findall("2 x 5.90") == ["2"]
    assert PL.QTY_RE.findall("3 X 4.50") == ["3"]


# --------------------------------------------------------------------------
# Row grouping by Y proximity
# --------------------------------------------------------------------------

def test_group_rows_clusters_aligned_lines():
    lines = [
        OCRLine("a", (0.10, 0.20, 0.05, 0.02), 0.9),
        OCRLine("b", (0.40, 0.205, 0.05, 0.02), 0.9),  # same row
        OCRLine("c", (0.10, 0.30, 0.05, 0.02), 0.9),   # next row
    ]
    rows = PL._group_rows(lines, y_tol=0.012)
    assert len(rows) == 2
    assert {ln.text for ln in rows[0]} == {"a", "b"}
    assert {ln.text for ln in rows[1]} == {"c"}


# --------------------------------------------------------------------------
# parse_ocr_to_extracted: real-receipt-shaped fixtures
# --------------------------------------------------------------------------

def test_parse_two_items_with_barcodes_and_prices(stub_db):
    """Two rows: each has barcode + price. Names come from the DB."""
    ocr = _ocr([
        ("7290000123456", (0.50, 0.20, 0.20, 0.02), 0.95),
        ("6.90",          (0.10, 0.205, 0.05, 0.02), 0.95),
        ("7290000234567", (0.50, 0.40, 0.20, 0.02), 0.95),
        ("5.50",          (0.10, 0.405, 0.05, 0.02), 0.95),
    ])
    ex = PL.parse_ocr_to_extracted(ocr)
    assert len(ex.items) == 2
    by_bc = {i.barcode: i for i in ex.items}
    assert by_bc["7290000123456"].raw_name == "חלב תנובה 3% 1 ליטר"
    assert by_bc["7290000123456"].line_total == pytest.approx(6.90)
    assert by_bc["7290000234567"].raw_name == "לחם אחיד אנג'ל"
    assert ex.total_paid == pytest.approx(12.40)


def test_parse_unknown_barcode_keeps_emit(stub_db):
    """Barcode not in DB → still emit item with raw_name = barcode."""
    ocr = _ocr([
        ("7290000999999", (0.50, 0.20, 0.20, 0.02), 0.9),
        ("12.00",         (0.10, 0.205, 0.05, 0.02), 0.9),
    ])
    ex = PL.parse_ocr_to_extracted(ocr)
    assert len(ex.items) == 1
    assert ex.items[0].barcode == "7290000999999"
    assert ex.items[0].line_total == 12.0


def test_parse_qty_multiplier_extracted(stub_db):
    """A row with '2 x' adds quantity=2 to the item."""
    ocr = _ocr([
        ("7290000345678", (0.50, 0.20, 0.20, 0.02), 0.9),
        ("2 x 9.45 = 18.90", (0.10, 0.205, 0.20, 0.02), 0.9),
    ])
    ex = PL.parse_ocr_to_extracted(ocr)
    assert len(ex.items) == 1
    it = ex.items[0]
    assert it.barcode == "7290000345678"
    assert it.quantity == 2.0
    # max price on the row = 18.90; first price 9.45 = unit price
    assert it.line_total == pytest.approx(18.90)
    assert it.unit_price == pytest.approx(9.45)


def test_parse_drops_rows_without_barcode_or_price(stub_db):
    ocr = _ocr([
        ("just hebrew header", (0.10, 0.10, 0.20, 0.02), 0.9),
        ("noise *** ###",       (0.10, 0.50, 0.20, 0.02), 0.9),
    ])
    ex = PL.parse_ocr_to_extracted(ocr)
    assert ex.items == []
    assert ex.total_paid is None


def test_parse_carries_provider_metadata(stub_db):
    ocr = _ocr([("7290000123456", (0.5, 0.2, 0.2, 0.02), 0.9),
                ("6.90",          (0.1, 0.205, 0.05, 0.02), 0.9)])
    ex = PL.parse_ocr_to_extracted(ocr)
    assert ex.ai["provider"] == "stub"
    assert ex.ai["ocr_lines"] == 2
    assert ex.ai["image_size"] == [1000, 1500]


# --------------------------------------------------------------------------
# Provider chain in pipeline.extract_from_image
# --------------------------------------------------------------------------

def test_extract_provider_rapid_uses_free_chain(stub_db, monkeypatch):
    canned = _ocr([("7290000123456", (0.5, 0.2, 0.2, 0.02), 0.9),
                   ("6.90",          (0.1, 0.205, 0.05, 0.02), 0.9)])
    monkeypatch.setattr(F, "rapid_ocr", lambda b: canned)
    ex = P.extract_from_image(b"\x00", "image/jpeg", provider="rapid")
    assert len(ex.items) == 1
    assert ex.items[0].barcode == "7290000123456"
    assert ex.ai["provider"] == "stub"


def test_extract_provider_auto_falls_back_to_claude(stub_db, monkeypatch):
    """Free chain returns empty → auto falls through to Claude (which we stub)."""
    empty = OCRResult(lines=[], provider="rapid-empty", latency_ms=1, image_size=(10, 10))
    monkeypatch.setattr(F, "rapid_ocr", lambda b: empty)
    monkeypatch.setattr(F, "ocrspace_ocr", lambda b, engine=2: empty)

    sentinel = P.Extracted(
        items=[P.ExtractedItem("מ", None, None, None, 1.0)],
        total_paid=1.0, chain_guess=None, city=None, ai={"model": "fake"},
    )
    monkeypatch.setattr(P, "_call_claude_on_image", lambda data, media_type: sentinel)
    ex = P.extract_from_image(b"\x00", "image/jpeg", provider="auto")
    assert ex is sentinel


def test_extract_provider_rapid_no_items_raises(stub_db, monkeypatch):
    """Single-provider mode with no items must NOT silently call Claude."""
    empty = OCRResult(lines=[], provider="rapid", latency_ms=1, image_size=(10, 10))
    monkeypatch.setattr(F, "rapid_ocr", lambda b: empty)
    with pytest.raises(RuntimeError, match="returned no lines|returned no items"):
        P.extract_from_image(b"\x00", "image/jpeg", provider="rapid")


def test_extract_provider_default_is_claude(stub_db, monkeypatch):
    """No provider arg + no env var → existing Claude path (preserves test stubs)."""
    monkeypatch.delenv("OCR_PROVIDER", raising=False)
    sentinel = P.Extracted(items=[], total_paid=None, chain_guess=None, city=None)
    called = []
    def fake_claude(data, media_type):
        called.append((data, media_type))
        return sentinel
    monkeypatch.setattr(P, "_call_claude_on_image", fake_claude)
    ex = P.extract_from_image(b"\x00", "image/jpeg")
    assert ex is sentinel
    assert called == [(b"\x00", "image/jpeg")]
