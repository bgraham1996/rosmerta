"""Stock lookups in the trades and dividends fetchers must match on symbol alone.

Regression (2026-11-12): ``stocks`` is UNIQUE(symbol) since the dedupe migration,
and our rows carry the routing exchange ``SMART`` while IB Flex reports the
listing exchange (``NYSE``). Both ``_get_or_create_stock`` helpers looked up by
symbol AND exchange, missed the existing row, and the placeholder INSERT then
violated ``uq_stocks_symbol`` — rolling back the entire trade ingest (84 trades
downloaded, 0 stored). Network- and DB-free: a tiny fake models the ``stocks``
table including the unique constraint.
"""

import re

import pytest

from price_retrival import dividends_api
from price_retrival.trades_api import IBTradesFetcher


class UniqueViolation(Exception):
    pass


class _StocksTable:
    """In-memory ``stocks`` with UNIQUE(symbol), answering the lookup SQL."""

    def __init__(self, rows):
        # rows: [(stock_id, symbol, exchange, ib_con_id)]
        self.rows = {r[0]: list(r) for r in rows}
        self.inserts = 0

    def execute(self, sql, params):
        s = " ".join(sql.split())
        if s.startswith("SELECT stock_id FROM stocks WHERE ib_con_id"):
            hit = [r for r in self.rows.values() if r[3] == params[0]]
            return [(hit[0][0],)] if hit else []
        if s.startswith("SELECT stock_id") and "WHERE symbol" in s:
            want_exch = "AND exchange" in s
            hit = [r for r in self.rows.values()
                   if r[1] == params[0] and (not want_exch or r[2] == params[1])]
            if not hit:
                return []
            return [(hit[0][0], hit[0][3])] if "ib_con_id FROM" in s else [(hit[0][0],)]
        if s.startswith("UPDATE stocks SET ib_con_id"):
            self.rows[params[1]][3] = params[0]
            return []
        if s.startswith("INSERT INTO stocks"):
            symbol = params[0]
            if any(r[1] == symbol for r in self.rows.values()):
                raise UniqueViolation(f'duplicate key "uq_stocks_symbol" ({symbol})')
            new_id = max(self.rows, default=0) + 1
            con_id = params[4] if len(params) > 4 else None
            self.rows[new_id] = [new_id, symbol, params[2], con_id]
            self.inserts += 1
            return [(new_id,)]
        raise AssertionError(f"unexpected SQL: {s}")


class _Cur:
    def __init__(self, table):
        self.t, self._res = table, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._res = self.t.execute(sql, params)

    def fetchone(self):
        return self._res[0] if self._res else None


class _Conn:
    closed = False

    def __init__(self, table):
        self.t = table

    def cursor(self):
        return _Cur(self.t)

    def commit(self):
        pass


@pytest.fixture
def table():
    # ABT as it exists in prod: routing exchange SMART, no ib_con_id yet.
    return _StocksTable([(7, "ABT", "SMART", None)])


def _trades_fetcher(table):
    f = IBTradesFetcher(token="t", query_id="q", db_config={"dummy": True})
    f.db_conn = _Conn(table)
    return f


def test_trades_lookup_matches_existing_symbol_on_other_exchange(table):
    f = _trades_fetcher(table)
    # Flex reports the listing exchange; must resolve to the existing SMART row.
    assert f._get_or_create_stock("ABT", "NYSE", "USD", con_id=4065) == 7
    assert table.inserts == 0
    # ...and backfills the con_id it learned.
    assert table.rows[7][3] == 4065


def test_trades_lookup_still_inserts_genuinely_new_symbol(table):
    f = _trades_fetcher(table)
    sid = f._get_or_create_stock("NEWCO", "NASDAQ", "USD", con_id=99)
    assert sid != 7 and table.inserts == 1


def test_dividends_lookup_matches_existing_symbol_on_other_exchange(table):
    conn = _Conn(table)
    assert dividends_api._get_or_create_stock(conn, "ABT", exchange="NYSE") == 7
    assert table.inserts == 0


def test_dividends_lookup_inserts_new_symbol(table):
    conn = _Conn(table)
    assert dividends_api._get_or_create_stock(conn, "NEWCO") != 7
    assert table.inserts == 1
