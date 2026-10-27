"""Unit tests for the source-agnostic futures storage/retrieval slice.

Covers the two new modules against the in-memory FakeDB (no PostgreSQL, no IB):

  * ``price_retrival.futures`` — root/contract registration (idempotent) and
    root -> children navigation.
  * ``price_retrival.ohlcv_store`` — the vendor-agnostic candle upsert/read that
    ``IBStockDataFetcher._save_bars_to_db`` now delegates to.

The candle upsert normally calls ``psycopg2.extras.execute_values`` against a
real cursor; here we monkeypatch the module-level ``_execute_values`` seam so it
writes into the FakeDB (which models ON CONFLICT last-wins) instead.
"""

from datetime import date, datetime, timezone

import pytest

from price_retrival import futures, ohlcv_store
from conftest import FakeDB


def _utc(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=timezone.utc)


@pytest.fixture
def fdb():
    """A fresh, empty FakeDB and its connection (no seeded equities)."""
    db = FakeDB()
    return db, db.conn()


@pytest.fixture
def priced(fdb, monkeypatch):
    """FakeDB whose candle upsert is wired to the in-memory ON CONFLICT model."""
    db, _ = fdb
    monkeypatch.setattr(
        ohlcv_store, "_execute_values",
        lambda cur, sql, records: db.apply_price_upsert(records),
    )
    return fdb


# --------------------------------------------------------------------------
# Registration + navigation (price_retrival.futures)
# --------------------------------------------------------------------------
def test_get_or_create_root_is_idempotent(fdb):
    db, conn = fdb
    first = futures.get_or_create_root(conn, "ES", name="E-mini S&P 500")
    second = futures.get_or_create_root(conn, "ES")
    assert first == second
    assert db.stock_rows[first]["sec_type"] == "FUTROOT"
    assert db.stock_rows[first]["parent_stock_id"] is None
    # Exactly one ES row exists.
    assert sum(1 for r in db.stock_rows.values() if r["symbol"] == "ES") == 1


def test_register_contract_links_to_root(fdb):
    db, conn = fdb
    root_id = futures.get_or_create_root(conn, "ES")
    cid = futures.register_contract(conn, "ES", "ESZ26", "2026-12-18", 50,
                                    ib_con_id=515416632)
    row = db.stock_rows[cid]
    assert row["sec_type"] == "FUT"
    assert row["parent_stock_id"] == root_id
    assert row["expiry"] == "2026-12-18"
    assert row["multiplier"] == 50
    assert row["ib_con_id"] == 515416632


def test_register_contract_creates_root_on_demand(fdb):
    db, conn = fdb
    cid = futures.register_contract(conn, "NQ", "NQZ26", "2026-12-18", 20)
    parent = db.stock_rows[cid]["parent_stock_id"]
    assert db.stock_rows[parent]["symbol"] == "NQ"
    assert db.stock_rows[parent]["sec_type"] == "FUTROOT"


def test_register_contract_is_idempotent_and_updates_metadata(fdb):
    db, conn = fdb
    futures.get_or_create_root(conn, "ES")
    first = futures.register_contract(conn, "ES", "ESZ26", "2026-12-18", 50)
    # Re-register with a corrected expiry + a newly-known conId.
    second = futures.register_contract(conn, "ES", "ESZ26", "2026-12-19", 50,
                                       ib_con_id=999)
    assert first == second  # no duplicate row
    assert sum(1 for r in db.stock_rows.values() if r["symbol"] == "ESZ26") == 1
    assert db.stock_rows[first]["expiry"] == "2026-12-19"
    assert db.stock_rows[first]["ib_con_id"] == 999


def test_get_child_contracts_ordered_by_expiry(fdb):
    db, conn = fdb
    futures.get_or_create_root(conn, "ES")
    # Register out of expiry order; navigation must sort ascending.
    futures.register_contract(conn, "ES", "ESH27", "2027-03-19", 50)
    futures.register_contract(conn, "ES", "ESZ26", "2026-12-18", 50)
    futures.register_contract(conn, "ES", "ESM27", "2027-06-18", 50)
    kids = futures.get_child_contracts(conn, "ES")
    assert [k[1] for k in kids] == ["ESZ26", "ESH27", "ESM27"]


def test_get_child_contracts_unknown_root_returns_empty(fdb):
    _, conn = fdb
    assert futures.get_child_contracts(conn, "NOPE") == []


def test_resolve_stock_id(fdb):
    _, conn = fdb
    cid = futures.register_contract(conn, "ES", "ESZ26", "2026-12-18", 50)
    assert futures.resolve_stock_id(conn, "ESZ26") == cid
    assert futures.resolve_stock_id(conn, "ESX99") is None


# --------------------------------------------------------------------------
# Candle storage/retrieval (price_retrival.ohlcv_store)
# --------------------------------------------------------------------------
def _candles():
    return [
        (_utc(2026, 10, 1, 9),  4500, 4510, 4495, 4505, 1000),
        (_utc(2026, 10, 1, 10), 4505, 4520, 4500, 4515, 1200),
        (_utc(2026, 10, 1, 11), 4515, 4525, 4510, 4512, 900),
    ]


def test_build_records_dedups_last_wins_and_sorts():
    # Pure function: an in-batch duplicate timestamp keeps the last value,
    # and records come out sorted by timestamp regardless of input order.
    candles = [
        (_utc(2026, 10, 1, 11), 4515, 4525, 4510, 4512, 900),
        (_utc(2026, 10, 1, 10), 4505, 4520, 4500, 4515, 1200),
        (_utc(2026, 10, 1, 10), 4505, 4520, 4500, 4518, 1250),  # dup -> wins
    ]
    recs = ohlcv_store._build_records(7, candles)
    assert [r[1] for r in recs] == [_utc(2026, 10, 1, 10), _utc(2026, 10, 1, 11)]
    assert recs[0][5] == 4518 and recs[0][6] == 1250  # last-wins close/volume
    assert all(r[0] == 7 for r in recs)  # stock_id stamped in


def test_build_records_empty():
    assert ohlcv_store._build_records(1, []) == []


def test_upsert_and_read_roundtrip(priced):
    db, conn = priced
    n = ohlcv_store.upsert_hourly_bars(conn, 42, _candles())
    assert n == 3
    rows = ohlcv_store.read_hourly_bars(conn, 42)
    assert len(rows) == 3
    assert [r[1] for r in rows] == [c[0] for c in _candles()]  # ordered by ts


def test_upsert_is_idempotent(priced):
    db, conn = priced
    ohlcv_store.upsert_hourly_bars(conn, 42, _candles())
    ohlcv_store.upsert_hourly_bars(conn, 42, _candles())  # re-run
    assert len(db.prices[42]) == 3  # ON CONFLICT, no duplicate rows


def test_upsert_overwrites_on_conflict(priced):
    db, conn = priced
    ohlcv_store.upsert_hourly_bars(conn, 42, _candles())
    corrected = [(_utc(2026, 10, 1, 10), 4505, 4520, 4500, 9999, 1)]
    ohlcv_store.upsert_hourly_bars(conn, 42, corrected)
    row = [r for r in db.prices[42] if r[1] == _utc(2026, 10, 1, 10)][0]
    assert row[5] == 9999  # close updated in place
    assert len(db.prices[42]) == 3  # still no new row


def test_upsert_empty_is_noop(priced):
    db, conn = priced
    assert ohlcv_store.upsert_hourly_bars(conn, 42, []) == 0
    assert 42 not in db.prices


# --------------------------------------------------------------------------
# Symbol normalisation (price_retrival.futures.normalise_future_symbol)
# Pure, IB-free: the IB -> canonical-symbol mapping used by the fetcher.
# --------------------------------------------------------------------------
def test_normalise_future_symbol_from_yyyymmdd():
    # IB's localSymbol would be the ambiguous 'ESZ6'; we build 'ESZ26'.
    assert futures.normalise_future_symbol("ES", "20261218") == "ESZ26"


def test_normalise_future_symbol_quarterly_month_codes():
    # ES quarterly cycle H/M/U/Z = Mar/Jun/Sep/Dec.
    assert futures.normalise_future_symbol("ES", "20270319") == "ESH27"
    assert futures.normalise_future_symbol("ES", "20270617") == "ESM27"
    assert futures.normalise_future_symbol("ES", "20270917") == "ESU27"


def test_normalise_future_symbol_accepts_yyyymm_and_date():
    assert futures.normalise_future_symbol("CL", "202601") == "CLF26"
    assert futures.normalise_future_symbol("ES", datetime(2026, 12, 18)) == "ESZ26"


def test_normalise_future_symbol_rejects_bad_input():
    with pytest.raises(ValueError):
        futures.normalise_future_symbol("ES", "2026")       # too short
    with pytest.raises(ValueError):
        futures.normalise_future_symbol("ES", "20261318")   # month 13


# --------------------------------------------------------------------------
# IB contract -> registration mapping (Phase B, pure / IB-free)
# --------------------------------------------------------------------------
class _FakeContract:
    """Minimal stand-in for an ib_insync Contract (attribute access)."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _es_contract(expiry, con_id, local):
    return dict(symbol="ES", lastTradeDateOrContractMonth=expiry, multiplier="50",
                conId=con_id, exchange="CME", currency="USD", localSymbol=local)


def test_normalise_ib_future_from_dict():
    out = futures.normalise_ib_future(_es_contract("20261218", 515416632, "ESZ6"))
    assert out == {
        "root": "ES", "symbol": "ESZ26", "expiry": date(2026, 12, 18),
        "multiplier": 50.0, "ib_con_id": 515416632,
        "exchange": "CME", "currency": "USD",
    }


def test_normalise_ib_future_from_object():
    c = _FakeContract(symbol="ES", lastTradeDateOrContractMonth="20270319",
                      multiplier="50", conId=649180684, exchange="CME", currency="USD")
    out = futures.normalise_ib_future(c)
    assert out["symbol"] == "ESH27" and out["expiry"] == date(2027, 3, 19)
    assert out["multiplier"] == 50.0 and out["ib_con_id"] == 649180684


def test_normalise_ib_future_missing_fields_raises():
    with pytest.raises(ValueError):
        futures.normalise_ib_future({"symbol": "ES"})       # no expiry


def test_register_term_structure_registers_root_and_contracts_in_expiry_order(fdb):
    db, conn = fdb
    contracts = [
        futures.normalise_ib_future(_es_contract("20271217", 586139767, "ESZ7")),
        futures.normalise_ib_future(_es_contract("20261218", 515416632, "ESZ6")),
        futures.normalise_ib_future(_es_contract("20270319", 649180684, "ESH7")),
    ]
    results = futures.register_term_structure(conn, "ES", contracts)
    # Ordered by expiry ascending.
    assert [sym for sym, _ in results] == ["ESZ26", "ESH27", "ESZ27"]
    # Root exists once, each contract links to it.
    root_id = futures.resolve_stock_id(conn, "ES")
    assert db.stock_rows[root_id]["sec_type"] == "FUTROOT"
    for _, cid in results:
        assert db.stock_rows[cid]["parent_stock_id"] == root_id
        assert db.stock_rows[cid]["sec_type"] == "FUT"
    # Navigation returns them expiry-ordered with conId carried through.
    children = futures.get_child_contracts(conn, "ES")
    assert [c[1] for c in children] == ["ESZ26", "ESH27", "ESZ27"]
    assert children[0][4] == 515416632   # ib_con_id


def test_register_term_structure_is_idempotent(fdb):
    db, conn = fdb
    contracts = [futures.normalise_ib_future(_es_contract("20261218", 515416632, "ESZ6"))]
    first = futures.register_term_structure(conn, "ES", contracts)
    second = futures.register_term_structure(conn, "ES", contracts)
    assert first == second
    # Exactly one root + one contract row.
    assert sum(1 for r in db.stock_rows.values() if r["symbol"] == "ES") == 1
    assert sum(1 for r in db.stock_rows.values() if r["symbol"] == "ESZ26") == 1


def test_register_term_structure_empty_is_noop(fdb):
    db, conn = fdb
    assert futures.register_term_structure(conn, "ES", []) == []
    assert not any(r["symbol"] == "ES" for r in db.stock_rows.values())


# --------------------------------------------------------------------------
# Front-month selection (Phase C — futures.get_front_contract)
# --------------------------------------------------------------------------
def _seed_es_chain(conn):
    """Register ES with three quarterly contracts (Mar/Jun/Sep 2027)."""
    contracts = [
        futures.normalise_ib_future(_es_contract("20270319", 649180684, "ESH7")),
        futures.normalise_ib_future(_es_contract("20270617", 649180661, "ESM7")),
        futures.normalise_ib_future(_es_contract("20270917", 649180681, "ESU7")),
    ]
    futures.register_term_structure(conn, "ES", contracts)


def test_get_front_contract_picks_nearest_unexpired(fdb):
    db, conn = fdb
    _seed_es_chain(conn)
    # Before the first expiry → front is the Mar contract.
    front = futures.get_front_contract(conn, "ES", as_of=date(2027, 1, 1))
    assert front[1] == "ESH27"
    assert front[4] == 649180684   # ib_con_id carried through


def test_get_front_contract_rolls_past_expired(fdb):
    db, conn = fdb
    _seed_es_chain(conn)
    # After Mar expiry but before Jun → front is the Jun contract.
    front = futures.get_front_contract(conn, "ES", as_of=date(2027, 4, 1))
    assert front[1] == "ESM27"
    # On the expiry date itself, the contract is still considered front (>=).
    assert futures.get_front_contract(conn, "ES", as_of=date(2027, 3, 19))[1] == "ESH27"


def test_get_front_contract_accepts_string_as_of(fdb):
    db, conn = fdb
    _seed_es_chain(conn)
    assert futures.get_front_contract(conn, "ES", as_of="2027-07-01")[1] == "ESU27"


def test_get_front_contract_all_expired_returns_none(fdb):
    db, conn = fdb
    _seed_es_chain(conn)
    assert futures.get_front_contract(conn, "ES", as_of=date(2028, 1, 1)) is None


def test_get_front_contract_unknown_root_returns_none(fdb):
    db, conn = fdb
    assert futures.get_front_contract(conn, "NOPE", as_of=date(2027, 1, 1)) is None
