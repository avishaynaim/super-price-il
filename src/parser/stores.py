"""Parse chain Stores / StoreFull XML into normalized rows."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from lxml import etree


@dataclass
class StoreRow:
    chain_id: str | None
    sub_chain_id: str | None
    store_code: str
    name: str | None
    address: str | None
    city: str | None
    zip_code: str | None
    store_type: str | None


def parse(xml_bytes: bytes) -> Iterator[StoreRow]:
    """Parse a chain Stores/StoreFull XML.

    Formats in the wild vary: Shufersal uses UPPERCASE tags; Victory wraps each
    store in <Branch>; publishedprices chains use mixed case. Everything is
    matched case-insensitively and the inner element is one of
    Store / StoreFull / Branch / SubChainStore."""
    import io
    ctx = etree.iterparse(io.BytesIO(xml_bytes), events=("end",), recover=True)
    chain_id = None
    sub_chain_id: str | None = None

    STORE_TAGS = {"store", "storefull", "branch", "subchainstore"}

    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname.lower()
        if tag == "chainid":
            # skip when inside a <Store> (we handle those via the fields dict)
            if elem.getparent() is None or etree.QName(elem.getparent().tag).localname.lower() not in STORE_TAGS:
                chain_id = (elem.text or "").strip() or chain_id
        elif tag == "subchainid":
            if elem.getparent() is None or etree.QName(elem.getparent().tag).localname.lower() not in STORE_TAGS:
                sub_chain_id = (elem.text or "").strip() or sub_chain_id
        elif tag in STORE_TAGS:
            fields: dict[str, str] = {}
            for child in elem:
                ctag = etree.QName(child.tag).localname.lower()
                if child.text is not None:
                    fields[ctag] = child.text.strip()
            code = fields.get("storeid")
            if not code:
                elem.clear()
                continue
            yield StoreRow(
                chain_id=fields.get("chainid") or chain_id,
                sub_chain_id=fields.get("subchainid") or sub_chain_id,
                store_code=code,
                name=fields.get("storename"),
                address=fields.get("address"),
                city=fields.get("city"),
                zip_code=fields.get("zipcode"),
                store_type=fields.get("storetype"),
            )
            elem.clear()
