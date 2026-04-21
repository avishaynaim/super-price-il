from src.scraper.chains.shufersal import _classify as shufersal_classify
from src.scraper.chains.publishedprices import _classify as pp_classify, _store_code
from src.scraper.chains.laibcatalog import _classify as laib_classify


def test_shufersal_classify():
    assert shufersal_classify("PriceFull7290027600007-001-202604210300.gz") == "PriceFull"
    assert shufersal_classify("Price7290027600007-001-202604210200.gz") == "Price"
    assert shufersal_classify("PromoFull7290027600007-001-202604210300.gz") == "PromoFull"
    assert shufersal_classify("Promo7290027600007-001.gz") == "Promo"
    assert shufersal_classify("StoresFull7290027600007-000.gz") == "StoresFull"
    assert shufersal_classify("Stores7290027600007-000.gz") == "Stores"
    assert shufersal_classify("mystery.gz") == "Unknown"


def test_publishedprices_store_code():
    # first 3-digit segment after the chain_id prefix is the store code
    assert _store_code("PriceFull7290058140886-001-070-20260420-070019.gz") == "001"
    assert _store_code("Price7290058140886-070-20260420-070019.gz") == "070"
    assert _store_code("Stores7290058140886.gz") is None


def test_publishedprices_classify_case_insensitive():
    assert pp_classify("pricefull7290058140886.gz") == "PriceFull"
    assert pp_classify("STORES7290058140886.gz") == "Stores"


def test_laibcatalog_classify():
    assert laib_classify("PriceFull7290696200003-089.xml.gz") == "PriceFull"
    assert laib_classify("StoresFull7290696200003.xml.gz") == "StoresFull"
