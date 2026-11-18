"""Dividends derived from IB adjusted vs unadjusted daily closes.

DB- and network-free: the derivation is pure, the IB fetcher gets a fake
``ib`` object, and the store path gets a recording fake connection.
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest

from price_retrival import dividends_api as d


def _series(days, dividends, start_price=50.0, drift=0.0):
    """Build (raw, adjusted) close series with cash dividends on given dates.

    ``adjusted`` follows IB's convention: closes before each ex-date are
    scaled by (1 - div / prev_close), so the latest close is unadjusted.
    """
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(days)]
    raw = [start_price + drift * i for i in range(days)]
    factors = [1.0] * days
    for ex, amount in dividends.items():
        i = dates.index(ex)
        k = 1 - amount / raw[i - 1]
        for j in range(i):
            factors[j] *= k
    adj = [r * f for r, f in zip(raw, factors)]
    return list(zip(dates, raw)), list(zip(dates, adj))


def test_recovers_ex_dates_and_amounts():
    divs = {date(2024, 1, 20): 0.43, date(2024, 4, 20): 0.43,
            date(2024, 7, 20): 0.43, date(2024, 10, 20): 0.44}
    raw, adj = _series(330, divs, drift=0.01)
    recs = d.derive_dividends_from_bars(raw, adj)
    assert [r.ex_date for r in recs] == sorted(divs)
    assert [r.amount for r in recs] == [Decimal('0.430')] * 3 + [Decimal('0.440')]
    assert {r.source for r in recs} == {'ib_adjusted'}
    assert {r.frequency for r in recs} == {'quarterly'}
    assert {r.dividend_type for r in recs} == {'regular'}


def test_non_payer_and_noise_give_nothing():
    raw, adj = _series(200, {})
    assert d.derive_dividends_from_bars(raw, adj) == []
    # float noise in the adjusted closes (relative ~1e-6) is not a dividend
    noisy = [(dt, c * (1 + (1e-6 if i % 2 else -1e-6))) for i, (dt, c) in enumerate(adj)]
    assert d.derive_dividends_from_bars(raw, noisy) == []


def test_only_overlapping_dates_are_used():
    raw, adj = _series(120, {date(2024, 3, 1): 0.5})
    recs = d.derive_dividends_from_bars(raw[10:], adj[:-10])
    assert [(r.ex_date, r.amount) for r in recs] == [(date(2024, 3, 1), Decimal('0.500'))]


def test_special_flag_and_monthly_frequency():
    monthly = {date(2024, m, 15): 0.27 for m in range(2, 12)}
    monthly[date(2024, 6, 15)] = 1.50   # one-off, > 3x the median
    raw, adj = _series(360, monthly)
    recs = {r.ex_date: r for r in d.derive_dividends_from_bars(raw, adj)}
    assert recs[date(2024, 6, 15)].dividend_type == 'special'
    assert recs[date(2024, 5, 15)].dividend_type == 'regular'
    assert {r.frequency for r in recs.values()} == {'monthly'}


@pytest.mark.parametrize('value,expected', [
    (0.42997, '0.430'), (0.43011, '0.430'), (0.4849, '0.485'),
    (0.5903, '0.5903'), (2.13221, '2.1322'),
])
def test_snap_amount(value, expected):
    assert d._snap_amount(value) == Decimal(expected)


def test_ib_duration():
    assert d._ib_duration(None) == '10 Y'
    two_years_ago = (date.today() - timedelta(days=730)).isoformat()
    assert d._ib_duration(two_years_ago) == '3 Y'


class _Bar:
    def __init__(self, dt, close):
        self.date, self.close = dt, close


class _FakeIB:
    def __init__(self, raw, adj, qualify=True):
        self.series = {'TRADES': raw, 'ADJUSTED_LAST': adj}
        self.qualify = qualify
        self.requests = []

    def qualifyContracts(self, c):
        return [c] if self.qualify else []

    def reqHistoricalData(self, contract, endDateTime, durationStr,
                          barSizeSetting, whatToShow, useRTH):
        self.requests.append((durationStr, barSizeSetting, whatToShow))
        return [_Bar(dt, c) for dt, c in self.series[whatToShow]]


def _fetcher(ib):
    f = d.IBDividendsFetcher()
    f.ib = ib
    return f


def test_fetcher_requests_daily_bars_and_filters_window():
    divs = {date(2024, 2, 1): 0.3, date(2024, 5, 1): 0.3, date(2024, 8, 1): 0.3}
    raw, adj = _series(300, divs)
    ib = _FakeIB(raw, adj)
    recs = _fetcher(ib).get_dividends('KO', start_date='2024-03-01', end_date='2024-07-01')
    assert [r.ex_date for r in recs] == [date(2024, 5, 1)]
    assert {(b, w) for _, b, w in ib.requests} == {('1 day', 'TRADES'), ('1 day', 'ADJUSTED_LAST')}


def test_fetcher_raises_when_ib_has_no_data():
    with pytest.raises(RuntimeError, match='no daily bars'):
        _fetcher(_FakeIB([], [])).get_dividends('KO')
    with pytest.raises(RuntimeError, match='qualify'):
        _fetcher(_FakeIB([], [], qualify=False)).get_dividends('ZZZZ')


class _RecCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((' '.join(sql.split()), params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _RecConn:
    def __init__(self):
        self.log, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return _RecCursor(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_replace_deletes_same_source_window_then_inserts(monkeypatch):
    inserted = []
    monkeypatch.setattr(d, 'execute_values',
                        lambda cur, sql, rows: inserted.extend(rows))
    raw, adj = _series(300, {date(2024, 2, 1): 0.3, date(2024, 8, 1): 0.31})
    recs = d.derive_dividends_from_bars(raw, adj)
    conn = _RecConn()
    assert d._replace_derived_dividends(conn, 42, recs) == 2
    sql, params = conn.log[0]
    assert sql.startswith('DELETE FROM dividends')
    assert params == (42, 'ib_adjusted', date(2024, 2, 1), date(2024, 8, 1))
    assert [r[1] for r in inserted] == [date(2024, 2, 1), date(2024, 8, 1)]
    assert conn.commits == 1


def test_get_dividends_no_db_never_touches_postgres(monkeypatch):
    raw, adj = _series(200, {date(2024, 3, 1): 0.5})

    class _Ctx(d.IBDividendsFetcher):
        def __enter__(self):
            self.ib = _FakeIB(raw, adj)
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(d, 'IBDividendsFetcher', _Ctx)
    monkeypatch.setattr(d.psycopg2, 'connect',
                        lambda **kw: pytest.fail('connected to Postgres'))
    recs, source = d.get_dividends('pfe', db_config=None, save_to_db=False)
    assert source == 'ib_adjusted'
    assert [(r.ex_date, r.amount) for r in recs] == [(date(2024, 3, 1), Decimal('0.500'))]
