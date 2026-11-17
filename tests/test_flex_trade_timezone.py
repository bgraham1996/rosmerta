"""Flex execution times are US/Eastern wall-clock and must be stored as UTC.

Regression (2026-11-17): ``_save_trades_to_db`` passed the naive Flex
``dateTime`` (e.g. ``20260529;095303``) straight into ``trade.trade_datetime``
(TIMESTAMPTZ), so PostgreSQL stamped it with the session zone (Europe/London) —
every stored execution was 4–5 hours early. Evidence the source zone is Eastern:
all 39 STK fills fall in 09:30–16:00, and the report's ``whenGenerated`` matches
IB's EST response timestamp.
"""

from datetime import date, datetime, timezone

import pandas as pd

from price_retrival.trades_api import IBTradesFetcher

UTC = timezone.utc


def test_summer_fill_is_edt_plus_4():
    # ABT 2026-05-29 09:53:03 EDT → 13:53:03 UTC
    assert IBTradesFetcher._parse_exec_dt("20260529;095303") == datetime(2026, 5, 29, 13, 53, 3, tzinfo=UTC)


def test_winter_fill_is_est_plus_5():
    # BMY 2026-11-04 15:25:13 EST → 20:25:13 UTC
    assert IBTradesFetcher._parse_exec_dt("20261104;152513") == datetime(2026, 11, 4, 20, 25, 13, tzinfo=UTC)


def test_us_dst_gap_week_is_edt_not_london():
    # KO 2026-03-27: US already on EDT (since 03-08), UK still GMT until 03-29.
    assert IBTradesFetcher._parse_exec_dt("20260327;144251") == datetime(2026, 3, 27, 18, 42, 51, tzinfo=UTC)


def test_result_is_tz_aware_utc_and_other_formats():
    dt = IBTradesFetcher._parse_exec_dt("2026-07-16 09:59:03")
    assert dt.tzinfo is not None and dt.utcoffset().total_seconds() == 0
    assert dt == datetime(2026, 7, 16, 13, 59, 3, tzinfo=UTC)


def test_aware_input_is_converted_not_relocalised():
    aware = datetime(2026, 7, 16, 13, 59, 3, tzinfo=UTC)
    assert IBTradesFetcher._parse_exec_dt(aware) == aware


def test_unparseable_is_none():
    assert IBTradesFetcher._parse_exec_dt(None) is None
    assert IBTradesFetcher._parse_exec_dt("") is None


class _Cur:
    def __init__(self, log):
        self.log, self._ret = log, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.log.append((s, params))
        self._ret = (len(self.log),) if "RETURNING" in s else None

    def fetchone(self):
        return self._ret

    def fetchall(self):
        return []


class _Conn:
    closed = False

    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        pass

    def rollback(self):
        pass


def test_save_writes_utc_datetime_and_local_trade_date():
    f = IBTradesFetcher(token="t", query_id="q", db_config=None)
    f.db_conn = _Conn()
    f._get_or_create_stock = lambda *a, **k: 1
    # Late-evening fill: UTC date rolls over; transaction_date must stay the ET date.
    df = pd.DataFrame([dict(assetCategory="STK", symbol="BMY", tradeID="1", buySell="BUY",
                            conid="1", listingExchange="NYSE", currency="USD",
                            dateTime="20261104;193000", quantity="2", tradePrice="63.79")])
    summary = f._save_trades_to_db(df)
    assert summary["inserted"] == 1
    tx = next(p for s, p in f.db_conn.log if s.startswith("INSERT INTO transaction"))
    tr = next(p for s, p in f.db_conn.log if s.startswith("INSERT INTO trade"))
    assert tx[1] == date(2026, 11, 4)
    assert tr[5] == datetime(2026, 11, 5, 0, 30, tzinfo=UTC)
