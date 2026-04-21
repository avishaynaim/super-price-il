from src.parser import pricefull, promofull
from src.parser import stores as stores_parser


def test_pricefull_parses_basic_rows():
    xml = b"""<?xml version="1.0" encoding="utf-8"?>
    <root>
      <ChainId>7290027600007</ChainId>
      <StoreId>001</StoreId>
      <Items>
        <Item>
          <ItemCode>7290000000001</ItemCode>
          <ItemName>Milk 1L</ItemName>
          <ManufacturerName>Tnuva</ManufacturerName>
          <ItemPrice>6.90</ItemPrice>
          <bIsWeighted>0</bIsWeighted>
        </Item>
        <Item>
          <ItemCode>7290000000002</ItemCode>
          <ItemName>Bananas</ItemName>
          <ItemPrice>5.90</ItemPrice>
          <bIsWeighted>1</bIsWeighted>
        </Item>
      </Items>
    </root>"""
    header, rows = pricefull.parse(xml)
    rows = list(rows)
    assert header.chain_id == "7290027600007"
    assert header.store_id == "001"
    assert [r.barcode for r in rows] == ["7290000000001", "7290000000002"]
    assert rows[0].price == 6.90
    assert rows[1].is_weighted is True


def test_stores_parser_uppercase_shufersal():
    xml = """<?xml version="1.0"?><asx:abap xmlns:asx="http://www.sap.com/abapxml"><asx:values>
        <CHAINID>7290027600007</CHAINID>
        <STORES>
          <STORE>
            <SUBCHAINID>1</SUBCHAINID>
            <STOREID>1</STOREID>
            <STORENAME>שלי בן יהודה</STORENAME>
            <ADDRESS>בן יהודה 79</ADDRESS>
            <CITY>תל אביב</CITY>
            <ZIPCODE>6343504</ZIPCODE>
          </STORE>
        </STORES>
    </asx:values></asx:abap>""".encode("utf-8")
    rows = list(stores_parser.parse(xml))
    assert len(rows) == 1
    r = rows[0]
    assert r.store_code == "1"
    assert r.city == "תל אביב"
    assert r.name == "שלי בן יהודה"


def test_stores_parser_victory_branch_element():
    xml = """<?xml version="1.0"?>
    <Store>
      <Branch>
        <ChainID>7290696200003</ChainID>
        <SubChainID>001</SubChainID>
        <StoreID>026</StoreID>
        <StoreName>Rosh HaAyin</StoreName>
        <City>ראש העין</City>
        <ZIPCode>4802126</ZIPCode>
      </Branch>
    </Store>""".encode("utf-8")
    rows = list(stores_parser.parse(xml))
    assert len(rows) == 1
    assert rows[0].store_code == "026"
    assert rows[0].city == "ראש העין"
    assert rows[0].zip_code == "4802126"


def test_promofull_parser_victory_flat_itemcode():
    # Victory wraps each promo in <Sale> with a flat <ItemCode> child.
    xml = b"""<?xml version="1.0"?>
    <Promos>
      <ChainID>72906</ChainID>
      <StoreID>027</StoreID>
      <Sales>
        <Sale>
          <ItemCode>4014400917956</ItemCode>
          <PromotionID>247853</PromotionID>
          <PromotionDescription>24.90</PromotionDescription>
          <DiscountedPrice>24.90</DiscountedPrice>
        </Sale>
      </Sales>
    </Promos>"""
    _, promos = promofull.parse(xml)
    promos = list(promos)
    assert len(promos) == 1
    assert promos[0].item_barcodes == ["4014400917956"]
    assert promos[0].discount_price == 24.90


def test_promofull_parser_extracts_items():
    xml = b"""<?xml version="1.0"?>
    <root>
      <ChainID>7290027600007</ChainID>
      <StoreID>460</StoreID>
      <Promotions>
        <Promotion>
          <PromotionID>1861010</PromotionID>
          <PromotionDescription>10% off</PromotionDescription>
          <PromotionStartDateTime>2026-04-01T00:00:00</PromotionStartDateTime>
          <PromotionEndDateTime>2026-05-01T23:59:00</PromotionEndDateTime>
          <DiscountRate>10</DiscountRate>
          <Groups>
            <Group>
              <PromotionItems>
                <PromotionItem><ItemCode>7290000000001</ItemCode></PromotionItem>
                <PromotionItem><ItemCode>7290000000002</ItemCode></PromotionItem>
              </PromotionItems>
            </Group>
          </Groups>
        </Promotion>
      </Promotions>
    </root>"""
    header, promos = promofull.parse(xml)
    promos = list(promos)
    assert header.chain_id == "7290027600007"
    assert len(promos) == 1
    p = promos[0]
    assert p.promo_code == "1861010"
    assert p.discount_rate == 10.0
    assert p.item_barcodes == ["7290000000001", "7290000000002"]
