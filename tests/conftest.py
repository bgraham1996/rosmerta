"""Shared fixtures for the rosmerta test suite.

These tests are deliberately **DB-free and network-free** so they run anywhere
without a populated PostgreSQL or an Interactive Brokers gateway. The analysis
layer (``data_models.Asset`` / ``data_models.Market``) only ever *reads* the DB
through a tiny ``conn.cursor()`` → ``execute`` / ``fetchone`` / ``fetchall``
interface, so we stand a small in-memory fake in for psycopg2's connection.

``FakeConnection`` routes each SQL statement by recognising a substring of the
query the production code issues (resolve symbol, load price_hourly, dividends,
fundamentals, watchlist members) and answers it from canned Python data. The
fake is intentionally permissive about the date-range ``WHERE`` clauses: tests
supply only in-range rows, so the fake returns everything it holds for a given
stock and lets the assertions focus on the computation, not the SQL filter.
"""

from datetime import datetime, timezone

import pytest


def _utc(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=timezone.utc)


class FakeCursor:
    """Minimal psycopg2-cursor stand-in driven by an in-memory ``FakeDB``."""

    def __init__(self, db):
        self._db = db
        self._result = None

    # used as ``with conn.cursor() as cur:``
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=()):
        s = " ".join(sql.split())  # collapse whitespace for stable matching
        if "FROM stocks WHERE symbol" in s:
            symbol = params[0]
            self._result = [self._db.stocks.get(symbol)]  # (stock_id, name) or None
        elif "FROM price_hourly" in s:
            stock_id = params[0]
            self._result = list(self._db.prices.get(stock_id, []))
        elif "FROM dividends" in s:
            stock_id = params[0]
            self._result = list(self._db.dividends.get(stock_id, []))
        elif "FROM fundamentals" in s:
            stock_id = params[0]
            self._result = list(self._db.fundamentals.get(stock_id, []))
        elif "FROM watchlist_members" in s:
            list_name = params[0]
            self._result = [(sym,) for sym in self._db.watchlists.get(list_name, [])]
        else:
            raise AssertionError(f"FakeCursor got an unrecognised query: {s}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result or [])


class FakeConnection:
    """Connection wrapper whose ``cursor()`` yields a :class:`FakeCursor`."""

    def __init__(self, db):
        self._db = db

    def cursor(self):
        return FakeCursor(self._db)


class FakeDB:
    """Container of canned table data the fake cursor answers from.

    Attributes mirror the relevant tables:

    - ``stocks``: ``{symbol: (stock_id, name)}``
    - ``prices``: ``{stock_id: [(stock_id, ts, open, high, low, close, volume), ...]}``
    - ``dividends``: ``{stock_id: [(dividend_id, ex_date, pay_date, decl_date, type, amount), ...]}``
    - ``fundamentals``: ``{stock_id: [(fundamental_id, period_end, net_income), ...]}``
    - ``watchlists``: ``{list_name: [symbol, ...]}``
    """

    def __init__(self):
        self.stocks = {}
        self.prices = {}
        self.dividends = {}
        self.fundamentals = {}
        self.watchlists = {}

    def conn(self):
        return FakeConnection(self)


@pytest.fixture
def utc():
    """Helper to build tz-aware UTC datetimes: ``utc(2024, 1, 2, 10)``."""
    return _utc


@pytest.fixture
def sample_prices():
    """Five consecutive hourly OHLCV bars for one stock (stock_id=1).

    Rows match the column order ``data_models.Asset.get_prices`` expects:
    ``(stock_id, timestamp, open, high, low, close, volume)``.
    """
    return [
        (1, _utc(2024, 1, 2, 9),  10.0, 11.0,  9.5, 10.5, 100.0),
        (1, _utc(2024, 1, 2, 10), 10.5, 12.0, 10.0, 11.0, 200.0),
        (1, _utc(2024, 1, 2, 11), 11.0, 11.5, 10.5, 10.8, 150.0),
        (1, _utc(2024, 1, 2, 12), 10.8, 11.2, 10.2, 11.2, 300.0),
        (1, _utc(2024, 1, 2, 13), 11.2, 13.0, 11.0, 12.5, 250.0),
    ]


@pytest.fixture
def db(sample_prices):
    """A :class:`FakeDB` pre-seeded with one stock (``ACME``) and its bars."""
    d = FakeDB()
    d.stocks["ACME"] = (1, "Acme Corp")
    d.prices[1] = sample_prices
    return d


@pytest.fixture
def conn(db):
    """A :class:`FakeConnection` over the seeded :class:`FakeDB`."""
    return db.conn()


# Fixed string bounds wide enough to cover the sample data.
START = "2024-01-01 00:00:00"
END = "2024-12-31 00:00:00"


@pytest.fixture
def date_bounds():
    return START, END
