"""Tests for the local SQLite write path (src/db/local.py) on a temp DB."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.db import local
from src.parser.pricefull import PriceRow


@pytest.fixture()
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(local, "LOCAL_DB", tmp_path / "test.db")
    local._tls.conn = None
    yield local
    conn = getattr(local._tls, "conn", None)
    if conn is not None:
        conn.close()
    local._tls.conn = None


def _row(barcode="111", price=5.0):
    return PriceRow(barcode=barcode, name="x", manufacturer=None, country=None,
                    unit_qty=None, unit_type=None, is_weighted=False,
                    price=price, unit_price=None, price_update=None)


def _store(code, city):
    return SimpleNamespace(store_code=code, name="n", address="a",
                           city=city, zip_code=None, store_type=None)


def test_chain_id_for_code_is_idempotent(db):
    assert db.chain_id_for_code("c1") == db.chain_id_for_code("c1")
    assert db.chain_id_for_code("c2") != db.chain_id_for_code("c1")


def test_zero_padded_store_codes_match(db):
    cid = db.chain_id_for_code("c1")
    sid = db.get_or_create_store_by_code(cid, "062")
    assert db.get_or_create_store_by_code(cid, "0062") == sid
    assert db.get_or_create_store_by_code(cid, "62") == sid
    assert db.get_or_create_store_by_code(cid, "63") != sid


def test_zero_padding_does_not_cross_chains(db):
    a = db.chain_id_for_code("a")
    b = db.chain_id_for_code("b")
    assert db.get_or_create_store_by_code(a, "5") != db.get_or_create_store_by_code(b, "05")


def test_upsert_store_updates_existing(db):
    cid = db.chain_id_for_code("c1")
    s1 = db.upsert_store(cid, _store("007", "Haifa"))
    s2 = db.upsert_store(cid, _store("7", "Tel Aviv"))
    assert s1 == s2
    with db.cursor() as conn:
        city = conn.execute("SELECT city FROM stores WHERE id=?", (s1,)).fetchone()["city"]
    assert city == "Tel Aviv"


def test_insert_observations_upserts_price(db):
    cid = db.chain_id_for_code("c1")
    sid = db.get_or_create_store_by_code(cid, "1")
    cache = {}
    assert db.insert_observations(cid, sid, [_row(price=5.0)], "f", cache) == 1
    assert db.insert_observations(cid, sid, [_row(price=6.5)], "f", cache) == 1
    with db.cursor() as conn:
        rows = conn.execute("SELECT price FROM current_prices").fetchall()
    assert [r["price"] for r in rows] == [6.5]
    assert db.insert_observations(cid, sid, [], "f", cache) == 0


def test_delete_stale_uses_14_day_cutoff(db):
    cid = db.chain_id_for_code("c1")
    other = db.chain_id_for_code("c2")
    cache = {}
    old_s = db.get_or_create_store_by_code(cid, "1")
    new_s = db.get_or_create_store_by_code(cid, "2")
    other_s = db.get_or_create_store_by_code(other, "1")
    for s in (old_s, new_s, other_s):
        db.insert_observations(cid, s, [_row()], "f", cache)
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=15)).isoformat(timespec="seconds")
    recent = (now - timedelta(days=13)).isoformat(timespec="seconds")
    with db.cursor() as conn:
        conn.execute("UPDATE current_prices SET updated_at=? WHERE store_id=?", (old, old_s))
        conn.execute("UPDATE current_prices SET updated_at=? WHERE store_id=?", (recent, new_s))
        conn.execute("UPDATE current_prices SET updated_at=? WHERE store_id=?", (old, other_s))
    db.delete_stale_chain_prices(cid, "ignored")
    with db.cursor() as conn:
        left = {r["store_id"] for r in conn.execute("SELECT store_id FROM current_prices")}
    # stale row of this chain removed; recent kept; other chain untouched
    assert left == {new_s, other_s}


def test_delete_chain_current_prices(db):
    cid = db.chain_id_for_code("c1")
    sid = db.get_or_create_store_by_code(cid, "1")
    db.insert_observations(cid, sid, [_row()], "f", {})
    db.delete_chain_current_prices(cid)
    with db.cursor() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM current_prices").fetchone()["c"] == 0
