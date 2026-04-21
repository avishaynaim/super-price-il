"""Registry of supermarket chains under the Israeli Price Transparency Law.

Auth credentials listed here are the *public* values the chains themselves
publish — they are how the law is satisfied, not secrets.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AuthKind = Literal["none", "publishedprices", "binaprojects", "laibcatalog", "custom"]


@dataclass(frozen=True)
class ChainSpec:
    code: str
    name_he: str
    name_en: str
    portal_url: str
    auth_kind: AuthKind
    chain_id: str | None = None         # 13-digit GS1 chain prefix, matches filenames
    username: str | None = None
    password: str | None = None
    notes: str = ""


CHAINS: list[ChainSpec] = [
    ChainSpec(
        code="shufersal",
        name_he="שופרסל",
        name_en="Shufersal",
        portal_url="https://prices.shufersal.co.il/",
        auth_kind="none",
        chain_id="7290027600007",
    ),
    ChainSpec(
        code="rami_levi",
        name_he="רמי לוי",
        name_en="Rami Levi",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        chain_id="7290058140886",
        username="RamiLevi",
        password="",
    ),
    ChainSpec(
        code="victory",
        name_he="ויקטורי",
        name_en="Victory",
        portal_url="https://laibcatalog.co.il/",
        auth_kind="laibcatalog",
        chain_id="7290696200003",
        notes="Laibcatalog landing HTML embeds direct .xml.gz links per chain_id.",
    ),
    ChainSpec(
        code="yohananof",
        name_he="יוחננוף",
        name_en="Yohananof",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        chain_id="7290803800003",
        username="yohananof",
        password="",
    ),
    ChainSpec(
        code="tiv_taam",
        name_he="טיב טעם",
        name_en="Tiv Taam",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        chain_id="7290873900009",
        username="TivTaam",
        password="",
    ),
    ChainSpec(
        code="osher_ad",
        name_he="אושר עד",
        name_en="Osher Ad",
        portal_url="https://osherad.binaprojects.com/",
        auth_kind="binaprojects",
        notes="DISABLED 2026-04-21: subdomain NXDOMAIN on public DNS; may have moved hosts. Revisit.",
    ),
    ChainSpec(
        code="king_store",
        name_he="קינג סטור",
        name_en="King Store",
        portal_url="https://kingstore.binaprojects.com/",
        auth_kind="binaprojects",
    ),
    ChainSpec(
        code="mega",
        name_he="מגה",
        name_en="Mega / Carrefour",
        portal_url="https://prices.carrefour.co.il/",
        auth_kind="custom",
        notes="Rebranded Carrefour. publishprice.mega.co.il 301s here. "
              "Inline `const path`/`const files` on landing page; files at /<path>/<name>.",
    ),
    ChainSpec(
        code="keshet",
        name_he="קשת טעמים",
        name_en="Keshet",
        portal_url="https://publishprice.mehadrin.co.il/",
        auth_kind="custom",
        notes="DISABLED 2026-04-21: subdomain NXDOMAIN; keshet-teamim.co.il also times out. Revisit.",
    ),
    ChainSpec(
        code="hazi_hinam",
        name_he="חצי חינם",
        name_en="Hazi Hinam",
        portal_url="https://shop.hazi-hinam.co.il/Prices",
        auth_kind="custom",
    ),
]

BY_CODE: dict[str, ChainSpec] = {c.code: c for c in CHAINS}


def get(code: str) -> ChainSpec:
    if code not in BY_CODE:
        raise KeyError(f"unknown chain code: {code}")
    return BY_CODE[code]
