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
        if "INSERT INTO stocks" in s:
            # get_or_create_root / register_contract upsert (ON CONFLICT (symbol)).
            # Distinguished by whether the contract-only parent_stock_id column
            # is written. Models RETURNING stock_id.
            self._result = [(self._db.upsert_stock(s, params),)]
        elif "FROM stocks WHERE parent_stock_id" in s:
            # Root -> children navigation, ordered by expiry.
            self._result = self._db.child_contracts(params[0])
        elif "FROM stocks WHERE symbol" in s:
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
    """Connection wrapper whose ``cursor()`` yields a :class:`FakeCursor`.

    ``commit`` / ``rollback`` are no-ops: the fake mutates in-memory dicts
    immediately, so the write-path code (``ohlcv_store`` / ``futures``) can call
    them without a real transaction.
    """

    def __init__(self, db):
        self._db = db

    def cursor(self):
        return FakeCursor(self._db)

    def commit(self):
        pass

    def rollback(self):
        pass


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
        # Full stock rows keyed by stock_id, for the futures write/nav path:
        # {stock_id: {symbol, name, sec_type, parent_stock_id, expiry,
        #             multiplier, ib_con_id, is_active}}
        self.stock_rows = {}
        self._next_stock_id = 1

    def conn(self):
        return FakeConnection(self)

    # -- futures write path -------------------------------------------------
    def upsert_stock(self, sql, params):
        """Model ``INSERT INTO stocks ... ON CONFLICT (symbol) ... RETURNING``.

        Returns the (new or existing) stock_id. ``sql`` is the whitespace-
        collapsed statement; a contract insert is recognised by the
        ``parent_stock_id`` column it writes.
        """
        is_contract = "parent_stock_id" in sql
        symbol, name = params[0], params[1]
        existing = self.stocks.get(symbol)
        if existing:
            stock_id = existing[0]
        else:
            stock_id = self._next_stock_id
            self._next_stock_id += 1
        row = self.stock_rows.get(stock_id, {"is_active": True})
        row.update(symbol=symbol, name=name)
        if is_contract:
            # params: symbol, name, exchange, currency, parent_stock_id,
            #         expiry, multiplier, ib_con_id
            row.update(sec_type="FUT", parent_stock_id=params[4],
                       expiry=params[5], multiplier=params[6],
                       ib_con_id=params[7])
        else:
            row.update(sec_type="FUTROOT", parent_stock_id=None)
        self.stock_rows[stock_id] = row
        self.stocks[symbol] = (stock_id, name)
        return stock_id

    def child_contracts(self, root_id):
        """Rows ``(stock_id, symbol, expiry, multiplier, ib_con_id)`` under a
        root, active only, ordered by expiry — the nav query's result."""
        kids = [
            (sid, r["symbol"], r["expiry"], r["multiplier"], r.get("ib_con_id"))
            for sid, r in self.stock_rows.items()
            if r.get("parent_stock_id") == root_id and r.get("is_active", True)
        ]
        return sorted(kids, key=lambda k: k[2])

    def apply_price_upsert(self, records):
        """Model ``price_hourly`` ON CONFLICT (stock_id, timestamp) last-wins.

        Bound to ``ohlcv_store._execute_values`` via monkeypatch in tests so the
        real ``upsert_hourly_bars`` writes here instead of PostgreSQL. ``records``
        are ``(stock_id, ts, o, h, l, c, v)`` tuples.
        """
        for rec in records:
            stock_id, ts = rec[0], rec[1]
            bucket = self.prices.setdefault(stock_id, [])
            # replace any existing row for this (stock_id, ts)
            for i, existing in enumerate(bucket):
                if existing[1] == ts:
                    bucket[i] = rec
                    break
            else:
                bucket.append(rec)
            bucket.sort(key=lambda r: r[1])


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
