"""Streaming parser for chain PriceFull / Price XML files.

The schema varies slightly across chains, so we read tolerantly:
extract the fields we know by tag name anywhere under <Item>/<Product>.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from lxml import etree


ITEM_TAGS = {"Item", "Product"}  # chains disagree

FIELD_MAP = {
    "ItemCode": "barcode",
    "ItemName": "name",
    "ManufacturerName": "manufacturer",
    "ManufactureCountry": "country",
    "ManufacturerCountry": "country",
    "UnitQty": "unit_type",
    "UnitOfMeasure": "unit_type",
    "Quantity": "unit_qty",
    "UnitQtyInPackage": "unit_qty",
    "QtyInPackage": "unit_qty",
    "ItemPrice": "price",
    "UnitOfMeasurePrice": "unit_price",
    "PriceUpdateDate": "price_update",
    "bIsWeighted": "is_weighted",
    "blsWeighted": "is_weighted",
}


@dataclass
class PriceRow:
    barcode: str
    name: str | None
    manufacturer: str | None
    country: str | None
    unit_qty: float | None
    unit_type: str | None
    is_weighted: bool
    price: float
    unit_price: float | None
    price_update: str | None


@dataclass
class PriceHeader:
    chain_id: str | None
    sub_chain_id: str | None
    store_id: str | None


def _float(x: str | None) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def parse(xml_bytes: bytes) -> tuple[PriceHeader, Iterator[PriceRow]]:
    """Return (header, row iterator). Iterator must be consumed fully to free memory."""
    header = _parse_header(xml_bytes)
    return header, _iter_items(xml_bytes)


def _parse_header(xml_bytes: bytes) -> PriceHeader:
    ctx = etree.iterparse(_bio(xml_bytes), events=("end",), recover=True)
    chain_id = sub_chain_id = store_id = None
    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname
        if tag == "ChainId":
            chain_id = (elem.text or "").strip()
        elif tag == "SubChainId":
            sub_chain_id = (elem.text or "").strip()
        elif tag == "StoreId":
            store_id = (elem.text or "").strip()
        elif tag in ITEM_TAGS:
            break
        elem.clear()
    return PriceHeader(chain_id, sub_chain_id, store_id)


def _iter_items(xml_bytes: bytes) -> Iterator[PriceRow]:
    ctx = etree.iterparse(_bio(xml_bytes), events=("end",), tag=None, recover=True)
    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname
        if tag not in ITEM_TAGS:
            continue
        raw: dict[str, str] = {}
        for child in elem:
            ctag = etree.QName(child.tag).localname
            field = FIELD_MAP.get(ctag)
            if field and child.text is not None:
                raw[field] = child.text.strip()
        elem.clear()

        if "barcode" not in raw or "price" not in raw:
            continue

        yield PriceRow(
            barcode=raw["barcode"],
            name=raw.get("name"),
            manufacturer=raw.get("manufacturer"),
            country=raw.get("country"),
            unit_qty=_float(raw.get("unit_qty")),
            unit_type=raw.get("unit_type"),
            is_weighted=raw.get("is_weighted") in {"1", "true", "True"},
            price=_float(raw["price"]) or 0.0,
            unit_price=_float(raw.get("unit_price")),
            price_update=raw.get("price_update"),
        )


def _bio(xml_bytes: bytes):
    import io
    return io.BytesIO(xml_bytes)
