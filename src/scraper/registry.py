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
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="osherad",
        password="",
        notes="Legal entity: מרב-מזון כל בע\"מ. Migrated off binaprojects to Cerberus "
              "(url.publishedprices.co.il) — confirmed 2026-04-21 from gov.il.",
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
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="Keshet",
        password="",
        notes="Migrated off publishprice.mehadrin.co.il to Cerberus — confirmed 2026-04-21 from gov.il.",
    ),
    ChainSpec(
        code="hazi_hinam",
        name_he="חצי חינם",
        name_en="Hazi Hinam",
        portal_url="https://shop.hazi-hinam.co.il/Prices",
        auth_kind="custom",
    ),
    # --- binaprojects family added 2026-04-21 from gov.il registry ---
    ChainSpec(
        code="maayan2000",
        name_he="מעיין 2000",
        name_en="Maayan 2000",
        portal_url="http://maayan2000.binaprojects.com/",
        auth_kind="binaprojects",
        notes="Legal entity: ג.מ מעיין אלפיים (07) בע\"מ.",
    ),
    ChainSpec(
        code="good_pharm",
        name_he="גוד פארם",
        name_en="Good Pharm",
        portal_url="https://goodpharm.binaprojects.com/",
        auth_kind="binaprojects",
    ),
    ChainSpec(
        code="zolvebegadol",
        name_he="זול ובגדול",
        name_en="Zol Vebegadol",
        portal_url="http://zolvebegadol.binaprojects.com/",
        auth_kind="binaprojects",
    ),
    ChainSpec(
        code="supersapir",
        name_he="סופר ספיר",
        name_en="Super Sapir",
        portal_url="https://supersapir.binaprojects.com/",
        auth_kind="binaprojects",
    ),
    ChainSpec(
        code="superbareket",
        name_he="סופר ברקת",
        name_en="Super Bareket",
        portal_url="http://superbareket.binaprojects.com/",
        auth_kind="binaprojects",
        notes="Legal entity: עוף והודו ברקת - חנות המפעל בע\"מ.",
    ),
    ChainSpec(
        code="shuk_hayir",
        name_he="שוק העיר",
        name_en="Shuk Hayir",
        portal_url="http://shuk-hayir.binaprojects.com/",
        auth_kind="binaprojects",
        notes="Legal entity: שוק העיר (ט.ע.מ.ס) בע\"מ.",
    ),
    ChainSpec(
        code="shefa_berkat_hashem",
        name_he="שפע ברכת השם",
        name_en="Shefa Berkat Hashem",
        portal_url="http://shefabirkathashem.binaprojects.com/",
        auth_kind="binaprojects",
    ),
    ChainSpec(
        code="citymarket_kiryatgat",
        name_he="סיטי מרקט קרית גת",
        name_en="CityMarket Kiryat Gat",
        portal_url="https://citymarketkiryatgat.binaprojects.com/",
        auth_kind="binaprojects",
        notes="One of several סיטי מרקט portals on gov.il; others are separate entities.",
    ),
    ChainSpec(
        code="ktshivuk",
        name_he="משנת יוסף",
        name_en="Mishnat Yosef (KT Shivuk)",
        portal_url="https://ktshivuk.binaprojects.com/",
        auth_kind="binaprojects",
        notes="Legal entity: קיי.טי. יבוא ושיווק בע\"מ.",
    ),
    # --- publishedprices family added 2026-04-21 from gov.il registry ---
    ChainSpec(
        code="dor_alon",
        name_he="דור אלון",
        name_en="Dor Alon",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="doralon",
        password="",
        notes="Legal entity: דור אלון ניהול מתחמים קמעונאיים בע\"מ.",
    ),
    ChainSpec(
        code="super_cofix",
        name_he="סופר קופיקס",
        name_en="Super Cofix",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="SuperCofixApp",
        password="",
        notes="Sub-brand under Rami Levi legal entity but gov.il lists as its own login.",
    ),
    ChainSpec(
        code="politzer",
        name_he="פוליצר",
        name_en="Politzer",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="politzer",
        password="",
        notes="Legal entity: פוליצר חדרה (1982) בע\"מ.",
    ),
    ChainSpec(
        code="salach_dabah",
        name_he="סאלח דבאח",
        name_en="Salach Dabah",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="SalachD",
        password="12345",
        notes="Only chain on this portal with a non-empty password.",
    ),
    ChainSpec(
        code="freshmarket",
        name_he="פרשמרקט",
        name_en="Freshmarket (Paz)",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="freshmarket",
        password="",
        notes="Paz-group sub-brand.",
    ),
    ChainSpec(
        code="paz_yellow",
        name_he="יילו",
        name_en="Yellow (Paz)",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="Paz_bo",
        password="paz468",
        notes="Paz-group sub-brand (convenience stores at gas stations).",
    ),
    ChainSpec(
        code="super_yuda",
        name_he="סופר יודה",
        name_en="Super Yuda (Paz)",
        portal_url="https://url.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="yuda_ho",
        password="Yud@147",
        notes="Paz-group sub-brand.",
    ),
    ChainSpec(
        code="stop_market",
        name_he="סטופ מרקט",
        name_en="Stop Market",
        portal_url="https://url.retail.publishedprices.co.il/",
        auth_kind="publishedprices",
        username="Stop_Market",
        password="",
        notes="On a different Cerberus host (url.retail.publishedprices.co.il); "
              "scraper derives BASE from portal_url.",
    ),
]

BY_CODE: dict[str, ChainSpec] = {c.code: c for c in CHAINS}


def get(code: str) -> ChainSpec:
    if code not in BY_CODE:
        raise KeyError(f"unknown chain code: {code}")
    return BY_CODE[code]
