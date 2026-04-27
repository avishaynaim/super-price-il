"""Parse chain Stores / StoreFull XML into normalized rows."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
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


_CITIES_FILE = Path(__file__).resolve().parents[2] / "data" / "il_cities.json"
_NUMERIC_RE = re.compile(r"^\d{1,7}$")


@lru_cache(maxsize=1)
def _known_cities() -> list[str]:
    """Hebrew canonical city names from data/il_cities.json, longest-first
    so 'תל אביב יפו' wins over 'תל אביב' when matching against a store name."""
    try:
        data = json.loads(_CITIES_FILE.read_text())
    except Exception:
        return []
    names: set[str] = set()
    for c in data:
        n = c.get("name_he") or c.get("name") or c.get("canonical")
        if n:
            names.add(n)
        for a in c.get("aliases") or []:
            names.add(a)
    return sorted(names, key=len, reverse=True)


def _city_from_name(store_name: str | None) -> str | None:
    """Carrefour publishes postal codes in <City>; extract a Hebrew city from
    the store name as a fallback. We scan the name for any known city string."""
    if not store_name:
        return None
    for city in _known_cities():
        if city in store_name:
            return city
    return None


def _normalize_city(raw_city: str | None, store_name: str | None) -> str | None:
    """If <City> is purely numeric (postal code), try the store name; else
    return <City> as-is. Empty strings collapse to None."""
    if raw_city and not _NUMERIC_RE.match(raw_city.strip()):
        return raw_city.strip()
    return _city_from_name(store_name)


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
            store_name = fields.get("storename")
            yield StoreRow(
                chain_id=fields.get("chainid") or chain_id,
                sub_chain_id=fields.get("subchainid") or sub_chain_id,
                store_code=code,
                name=store_name,
                address=fields.get("address"),
                city=_normalize_city(fields.get("city"), store_name),
                zip_code=fields.get("zipcode"),
                store_type=fields.get("storetype"),
            )
            elem.clear()
