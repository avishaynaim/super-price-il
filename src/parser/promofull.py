"""Streaming parser for chain PromoFull / Promo XML files.

Schema varies slightly per chain (some use <Promotion> under <Promotions>, some
use <Sale> under <Sales>). We match case-insensitively on element names and pull
whatever fields we find.

Each promotion may carry many ItemCodes (the "10% off if you buy any of these").
We yield a single Promo object plus its list of item barcodes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

from lxml import etree


PROMO_TAGS = {"promotion", "sale"}
ITEM_TAGS = {"promotionitem", "saleitem", "item"}


@dataclass
class PromoRow:
    store_id: str | None
    promo_code: str
    description: str | None
    starts_at: str | None
    ends_at: str | None
    reward_type: str | None
    min_qty: float | None
    discount_price: float | None
    discount_rate: float | None
    item_barcodes: list[str] = field(default_factory=list)


@dataclass
class PromoHeader:
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


def parse(xml_bytes: bytes) -> tuple[PromoHeader, Iterator[PromoRow]]:
    header = _parse_header(xml_bytes)
    return header, _iter_promos(xml_bytes)


def _parse_header(xml_bytes: bytes) -> PromoHeader:
    import io
    ctx = etree.iterparse(io.BytesIO(xml_bytes), events=("end",), recover=True)
    chain_id = sub_chain_id = store_id = None
    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname.lower()
        if tag == "chainid" and chain_id is None:
            chain_id = (elem.text or "").strip() or None
        elif tag == "subchainid" and sub_chain_id is None:
            sub_chain_id = (elem.text or "").strip() or None
        elif tag == "storeid" and store_id is None:
            store_id = (elem.text or "").strip() or None
        elif tag in PROMO_TAGS:
            break
    return PromoHeader(chain_id, sub_chain_id, store_id)


def _iter_promos(xml_bytes: bytes) -> Iterator[PromoRow]:
    import io
    ctx = etree.iterparse(io.BytesIO(xml_bytes), events=("end",), recover=True)
    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname.lower()
        if tag not in PROMO_TAGS:
            continue

        fields: dict[str, str] = {}
        item_barcodes: list[str] = []

        # flat children
        for child in elem:
            ctag = etree.QName(child.tag).localname.lower()
            if ctag in {"promotionitems", "saleitems", "groups", "group"}:
                # dive one or two levels to find the item barcodes
                for code in child.iter():
                    itag = etree.QName(code.tag).localname.lower()
                    if itag == "itemcode" and code.text:
                        item_barcodes.append(code.text.strip())
                continue
            if ctag == "itemcode" and child.text:
                # Victory / laibcatalog: flat <ItemCode> under <Sale>
                item_barcodes.append(child.text.strip())
                continue
            if child.text is not None:
                fields[ctag] = child.text.strip()

        promo_code = fields.get("promotionid") or fields.get("saleid") or fields.get("id")
        if not promo_code:
            elem.clear()
            continue

        yield PromoRow(
            store_id=None,
            promo_code=promo_code,
            description=fields.get("promotiondescription") or fields.get("saledescription"),
            starts_at=fields.get("promotionstartdatetime") or fields.get("startdate") or fields.get("saledate"),
            ends_at=fields.get("promotionenddatetime") or fields.get("enddate"),
            reward_type=fields.get("rewardtype"),
            min_qty=_float(fields.get("minqty") or fields.get("minnoofitemoffered")),
            discount_price=_float(fields.get("discountedprice") or fields.get("saleprice")),
            discount_rate=_float(fields.get("discountrate")),
            item_barcodes=item_barcodes,
        )
        elem.clear()
