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
    import io
    ctx = etree.iterparse(io.BytesIO(xml_bytes), events=("end",), recover=True)
    chain_id = None
    sub_chain_id_stack: list[str | None] = [None]

    for _, elem in ctx:
        tag = etree.QName(elem.tag).localname
        if tag == "ChainId":
            chain_id = (elem.text or "").strip()
        elif tag == "SubChainId":
            sub_chain_id_stack[-1] = (elem.text or "").strip()
        elif tag in {"Store", "StoreFull"}:
            fields: dict[str, str] = {}
            for child in elem:
                ctag = etree.QName(child.tag).localname
                if child.text is not None:
                    fields[ctag] = child.text.strip()
            code = fields.get("StoreId") or fields.get("StoreID")
            if not code:
                elem.clear()
                continue
            yield StoreRow(
                chain_id=chain_id,
                sub_chain_id=sub_chain_id_stack[-1],
                store_code=code,
                name=fields.get("StoreName"),
                address=fields.get("Address"),
                city=fields.get("City"),
                zip_code=fields.get("ZipCode"),
                store_type=fields.get("StoreType"),
            )
            elem.clear()
