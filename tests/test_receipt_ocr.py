"""Receipt OCR pipeline tests.

Covers the photo-receipt path in src/receipts/pipeline.py — the route a phone
photo of an Israeli supermarket receipt takes through Claude vision, JSON
envelope parsing, dataclass coercion, and product matching.

The Claude API is stubbed at the boundary (_anthropic) — each test feeds a
canned Hebrew/RTL JSON response back through the pipeline, so the suite runs
offline and deterministically. Realistic Hebrew receipt patterns are covered:
RTL columns, deposit/discount lines, weight items, multi-pack promos, member
discounts, overflow product names, partial (no-total) receipts, returns.

Real phone photos: drop *.jpg/*.png/*.jpeg/*.webp into tests/fixtures/receipts/
and run with RUN_REAL_RECEIPT_TESTS=1 + ANTHROPIC_API_KEY set; the parametrized
test at the bottom will pick them up. Without those, that test auto-skips.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from src.receipts import pipeline as P
from src.receipts.pipeline import (
    Extracted,
    ExtractedItem,
    _parse_json_envelope,
    _to_extracted,
    extract_from_image,
    match_items,
)


# --------------------------------------------------------------------------
# Test doubles for the Anthropic client
# --------------------------------------------------------------------------

class _FakeUsage:
    input_tokens = 100
    output_tokens = 50
    cache_read_input_tokens = 0
    cache_creation_input_tokens = 0


class _FakeBlock:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _FakeResp:
    def __init__(self, text: str) -> None:
        self.content = [_FakeBlock(text)]
        self.usage = _FakeUsage()
        self.stop_reason = "end_turn"


class _FakeMessages:
    def __init__(self, text: str) -> None:
        self._text = text
        self.last_kwargs: dict | None = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        return _FakeResp(self._text)


class _FakeClient:
    def __init__(self, text: str) -> None:
        self.messages = _FakeMessages(text)


@pytest.fixture
def fake_claude(monkeypatch):
    """Stub _anthropic() with a client that returns a canned JSON string."""
    def _stub(json_text: str) -> _FakeClient:
        client = _FakeClient(json_text)
        monkeypatch.setattr(P, "_anthropic", lambda: client)
        return client
    return _stub


# A minimal but realistic Hebrew item payload Claude is told to emit.
def _item(raw_name, line_total, barcode=None, qty=None, unit_price=None):
    return {
        "raw_name": raw_name,
        "barcode": barcode,
        "quantity": qty,
        "unit_price": unit_price,
        "line_total": line_total,
    }


def _payload(items, total=None, chain=None, city=None, purchased_at=None):
    return {
        "chain_guess": chain,
        "city": city,
        "purchased_at": purchased_at,
        "total_paid": total,
        "items": items,
    }


# --------------------------------------------------------------------------
# Group A — JSON envelope parsing (5 tests)
# Claude sometimes wraps JSON in fences or prose; we must always recover it.
# --------------------------------------------------------------------------

def test_01_envelope_plain_json():
    obj = _parse_json_envelope('{"items": [], "total_paid": 0}')
    assert obj == {"items": [], "total_paid": 0}


def test_02_envelope_with_json_fence():
    text = '```json\n{"items": [], "total_paid": 12.5}\n```'
    assert _parse_json_envelope(text)["total_paid"] == 12.5


def test_03_envelope_with_bare_fence():
    text = '```\n{"items": [], "total_paid": null}\n```'
    assert _parse_json_envelope(text)["total_paid"] is None


def test_04_envelope_with_prose_prefix_and_suffix():
    # Claude occasionally adds "Here is the extracted JSON:" before the object.
    text = 'Here is the JSON:\n{"items":[],"total_paid":7.5}\nThanks!'
    assert _parse_json_envelope(text)["total_paid"] == 7.5


def test_05_envelope_preserves_hebrew_inside_strings():
    text = '{"items":[{"raw_name":"חלב תנובה 3%","line_total":6.9}],"total_paid":6.9}'
    obj = _parse_json_envelope(text)
    assert obj["items"][0]["raw_name"] == "חלב תנובה 3%"


# --------------------------------------------------------------------------
# Group B — Dataclass coercion (5 tests)
# Claude emits null for unknowns; the pipeline must turn that into clean Python
# values without crashing.
# --------------------------------------------------------------------------

def test_06_to_extracted_handles_null_total_paid():
    ex = _to_extracted({"items": [], "total_paid": None})
    assert ex.total_paid is None and ex.items == []


def test_07_to_extracted_coerces_string_numeric_total():
    # Claude may stringify numerics; the pipeline coerces.
    ex = _to_extracted({"items": [], "total_paid": "42.30"})
    assert ex.total_paid == pytest.approx(42.30)


def test_08_to_extracted_zero_when_line_total_null():
    ex = _to_extracted({"items": [_item("חלב", None)], "total_paid": None})
    assert len(ex.items) == 1 and ex.items[0].line_total == 0.0


def test_09_to_extracted_drops_item_with_invalid_quantity():
    bad = _item("חלב", 6.90, qty="not-a-number")
    good = _item("לחם", 5.50)
    ex = _to_extracted({"items": [bad, good], "total_paid": 12.40})
    assert [i.raw_name for i in ex.items] == ["לחם"]


def test_10_to_extracted_handles_missing_items_key():
    ex = _to_extracted({"total_paid": 0})
    assert ex.items == []


# --------------------------------------------------------------------------
# Group C — Receipt-format scenarios (15 tests)
# Each test feeds Claude's expected JSON output for a real Israeli receipt
# pattern through the full extract_from_image() flow.
# --------------------------------------------------------------------------

def _img() -> bytes:
    # Tiny valid PNG; Claude is stubbed so the bytes are never actually read.
    return (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xfc\xcf"
        b"\xc0P\x0f\x00\x05\x01\x01\xa3,\xea\x9b\xe5\x00\x00\x00\x00IEND\xaeB`\x82"
    )


def test_11_shufersal_simple_receipt(fake_claude):
    payload = _payload(
        [
            _item("חלב תנובה 3% 1ל", 6.90),
            _item("לחם אחיד אנג'ל", 5.50),
            _item("ביצים L 12 יח'", 18.90),
        ],
        total=31.30, chain="שופרסל", city="ירושלים",
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.chain_guess == "שופרסל"
    assert ex.total_paid == 31.30
    assert [i.raw_name for i in ex.items] == [
        "חלב תנובה 3% 1ל", "לחם אחיד אנג'ל", "ביצים L 12 יח'",
    ]


def test_12_rami_levi_with_barcode_column(fake_claude):
    payload = _payload(
        [
            _item("חלב תנובה 3% 1ל", 6.90, barcode="7290000000001"),
            _item("בננה", 8.30, barcode="7290000000002", qty=1.05, unit_price=7.90),
        ],
        total=15.20, chain="רמי לוי",
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/jpeg")
    assert [i.barcode for i in ex.items] == ["7290000000001", "7290000000002"]


def test_13_victory_member_discount_applied_in_line_total(fake_claude):
    # Claude is told to skip discount summary lines and bake the discount into
    # the item's line_total. Verify we accept the resulting shape.
    payload = _payload(
        [
            _item("שמפו הוואי 700מ\"ל", 22.90),  # original 26.90, discount applied
            _item("מרכך הוואי 700מ\"ל", 22.90),
        ],
        total=45.80, chain="ויקטורי",
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.total_paid == 45.80 == sum(i.line_total for i in ex.items)


def test_14_weight_item_kg_float_quantity(fake_claude):
    payload = _payload(
        [_item("בננה (ק\"ג)", 8.30, qty=1.05, unit_price=7.90)],
        total=8.30,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    it = ex.items[0]
    assert it.quantity == pytest.approx(1.05) and it.unit_price == pytest.approx(7.90)


def test_15_multipack_promo_two_for_ten(fake_claude):
    # "2 ב-10": two units at promo price. Claude collapses to one item with
    # quantity=2 and the discounted line_total.
    payload = _payload(
        [_item("קוטג' תנובה 5% 250ג", 10.00, qty=2, unit_price=5.00)],
        total=10.00,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    it = ex.items[0]
    assert it.quantity == 2 and it.line_total == 10.0


def test_16_percent_off_promo(fake_claude):
    # 25% off baked into line_total; we don't care about the discount math,
    # only that line_total is what we ingest.
    payload = _payload(
        [_item("שמן זית כתית 750מ\"ל", 29.93, qty=1, unit_price=39.90)],
        total=29.93,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.items[0].line_total == pytest.approx(29.93)


def test_17_mixed_hebrew_english_product_name(fake_claude):
    payload = _payload(
        [_item("Coca Cola זירו 1.5ל", 8.90)],
        total=8.90,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert "Coca Cola" in ex.items[0].raw_name
    assert "זירו" in ex.items[0].raw_name


def test_18_long_overflowed_product_name_kept_as_one_string(fake_claude):
    # Long Hebrew product names wrap to two lines on the printout but Claude
    # is expected to merge them. Verify the merged string survives the pipeline.
    long_name = "במבה אסם נוגט ושוקולד אישי 25 גרם מארז משפחתי"
    payload = _payload([_item(long_name, 12.90)], total=12.90)
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.items[0].raw_name == long_name


def test_19_unit_qty_explicit_three_times_price(fake_claude):
    # Receipt prints "3 X 5.90" then a line total. Claude returns the units.
    payload = _payload(
        [_item("יוגורט תות 150ג", 17.70, qty=3, unit_price=5.90)],
        total=17.70,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    it = ex.items[0]
    assert it.quantity == 3 and it.unit_price == pytest.approx(5.90)
    assert it.line_total == pytest.approx(17.70)


def test_20_no_barcode_field_on_receipt(fake_claude):
    payload = _payload([_item("מלפפון", 4.50)], total=4.50)
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.items[0].barcode is None


def test_21_no_chain_visible_in_partial_photo(fake_claude):
    payload = _payload([_item("חלב תנובה 3% 1ל", 6.90)], total=6.90, chain=None)
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.chain_guess is None


def test_22_partial_receipt_no_total(fake_claude):
    # User's photos are partial — total may be cropped out. Pipeline must accept.
    payload = _payload(
        [
            _item("חלב תנובה 3% 1ל", 6.90),
            _item("לחם אחיד", 5.50),
        ],
        total=None,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.total_paid is None and len(ex.items) == 2


def test_23_items_without_header_metadata(fake_claude):
    payload = _payload(
        [_item("גבינה לבנה 5%", 7.90)],
        total=7.90, chain=None, city=None, purchased_at=None,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert ex.chain_guess is None and ex.city is None
    assert ex.items[0].line_total == 7.90


def test_24_negative_line_total_for_return(fake_claude):
    # Refund/return rows print a negative amount.
    payload = _payload(
        [
            _item("חלב תנובה 3% 1ל", 6.90),
            _item("החזרה - חלב תנובה", -6.90),
        ],
        total=0.0,
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    assert sum(i.line_total for i in ex.items) == pytest.approx(0.0)


def test_25_purchased_at_iso_preserved(fake_claude):
    payload = _payload(
        [_item("חלב", 6.90)], total=6.90, purchased_at="2026-04-25T18:42:00",
    )
    fake_claude(json.dumps(payload, ensure_ascii=False))
    ex = extract_from_image(_img(), "image/png")
    # purchased_at isn't currently stored on Extracted but should round-trip
    # through the JSON envelope without breaking ingestion.
    assert ex.items[0].line_total == 6.90


# --------------------------------------------------------------------------
# Group D — DB-backed matching (5 tests)
# match_items() reads products from prices.db; we stand up a tiny temp DB
# and monkeypatch the connection.
# --------------------------------------------------------------------------

@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    """Create a minimal products table and patch pipeline.connect to use it."""
    db_path = tmp_path / "test.db"
    schema = (Path(__file__).resolve().parents[1] / "src" / "db" / "schema.sql").read_text()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(schema)
    conn.executemany(
        "INSERT INTO products(barcode, name) VALUES (?, ?)",
        [
            ("7290000000001", "חלב תנובה 3% 1 ליטר"),
            ("7290000000002", "לחם אחיד אנג'ל"),
            ("7290000000003", "ביצים גדולות L"),
            ("7290000000004", "במבה אסם 80 גרם"),
        ],
    )
    conn.commit()
    conn.close()

    import src.receipts.pipeline as pl

    def _fake_connect(*a, **kw):
        c = sqlite3.connect(str(db_path))
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(pl, "connect", _fake_connect)
    return db_path


def test_26_barcode_exact_match_yields_full_confidence(seeded_db):
    items = [ExtractedItem("חלב משהו", "7290000000001", None, None, 6.90)]
    out = match_items(items)
    assert out[0]["product_id"] is not None
    assert out[0]["match_confidence"] == 1.0


def test_27_unknown_barcode_falls_through_to_fuzzy(seeded_db):
    # Barcode doesn't exist; raw_name is close but not identical — should
    # match via fuzzy with confidence below 1.0.
    items = [ExtractedItem("חלב תנובה 3% טרי", "9999999999999", None, None, 6.90)]
    out = match_items(items)
    assert out[0]["product_id"] is not None
    assert out[0]["match_confidence"] is not None
    assert out[0]["match_confidence"] < 1.0


def test_28_fuzzy_hebrew_token_match(seeded_db):
    items = [ExtractedItem("לחם אחיד אנג'ל", None, None, None, 5.50)]
    out = match_items(items)
    assert out[0]["name"] == "לחם אחיד אנג'ל"
    assert out[0]["match_confidence"] is not None


def test_29_no_match_below_threshold(seeded_db):
    # Nothing in seed catalog resembles "טופו אורגני".
    items = [ExtractedItem("טופו אורגני", None, None, None, 14.90)]
    out = match_items(items)
    assert out[0]["product_id"] is None
    assert out[0]["match_confidence"] is None


def test_30_match_preserves_quantity_and_line_total(seeded_db):
    items = [ExtractedItem("חלב תנובה", "7290000000001", 2.0, 6.90, 13.80)]
    out = match_items(items)
    assert out[0]["quantity"] == 2.0
    assert out[0]["line_total"] == 13.80
    assert out[0]["product_id"] is not None


# --------------------------------------------------------------------------
# Real-photo runner — auto-discovers files in tests/fixtures/receipts/.
# Skipped unless RUN_REAL_RECEIPT_TESTS=1 and ANTHROPIC_API_KEY is set.
# --------------------------------------------------------------------------

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "receipts"
_PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
_real_photos = sorted(
    p for p in (_FIXTURE_DIR.glob("*") if _FIXTURE_DIR.exists() else [])
    if p.suffix.lower() in _PHOTO_EXTS
)


@pytest.mark.skipif(
    not _real_photos or os.environ.get("RUN_REAL_RECEIPT_TESTS") != "1"
    or not os.environ.get("ANTHROPIC_API_KEY"),
    reason=(
        "no fixture photos, or RUN_REAL_RECEIPT_TESTS!=1, or ANTHROPIC_API_KEY missing"
    ),
)
@pytest.mark.parametrize("photo_path", _real_photos, ids=lambda p: p.name)
def test_real_receipt_photo_smoke(photo_path):
    """End-to-end with a real phone photo — costs API tokens."""
    media = "image/jpeg" if photo_path.suffix.lower() in {".jpg", ".jpeg"} else "image/png"
    ex = extract_from_image(photo_path.read_bytes(), media)
    assert isinstance(ex, Extracted)
    # Even a partial receipt should produce at least one parsed item.
    assert len(ex.items) >= 1
    # Every line must have a non-empty raw_name and a numeric line_total.
    for it in ex.items:
        assert it.raw_name and it.raw_name.strip()
        assert isinstance(it.line_total, float)
