"""Unit tests for the analysis layer (``data_models.Asset`` / ``Market``).

These exercise the *computation* in ``Asset``/``Market`` against the in-memory
``FakeConnection`` from ``conftest.py`` — no live PostgreSQL required. The
fetcher modules are out of scope here; this layer only reads and analyses.
"""

import math

import pandas as pd
import pytest

from data_models import Asset, Market


# --- Asset construction & price loading ------------------------------------


def test_unknown_ticker_raises(conn, date_bounds):
    start, end = date_bounds
    with pytest.raises(ValueError, match="No ticker for"):
        Asset(conn, "NOPE", start, end)


def test_asset_metadata(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    meta = a.asset_metadata()
    assert meta["ticker"] == "ACME"
    assert meta["stock_id"] == 1
    assert meta["timeframe"] == "hourly"
    assert meta["asset_id"] == f"ACME-{start}-{end}-hourly"


def test_get_prices_loads_and_caches(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    prices = a.get_prices(conn)
    assert len(prices) == 5
    assert list(prices.columns) == [
        "stock_id", "timestamp", "open", "high", "low", "close", "volume",
    ]
    # numeric columns coerced to float
    for col in ["open", "high", "low", "close", "volume"]:
        assert prices[col].dtype == float
    # returns the same cached object on a second call
    assert a.get_prices(conn) is prices


def test_get_dates_requires_prices(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    with pytest.raises(ValueError, match="No prices available"):
        a.get_dates()
    a.get_prices(conn)
    assert len(a.get_dates()) == 5


# --- Asset computations -----------------------------------------------------


def test_get_stats(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    a.get_prices(conn)
    stats = a.get_stats()
    closes = [10.5, 11.0, 10.8, 11.2, 12.5]
    assert stats["high"] == max(closes)
    assert stats["low"] == min(closes)
    assert stats["range"] == max(closes) - min(closes)
    assert stats["first"] == closes[0]
    assert stats["last"] == closes[-1]
    assert stats["total_change"] == pytest.approx(closes[-1] - closes[0])
    assert stats["total_change_pct"] == pytest.approx((closes[-1] / closes[0] - 1) * 100)
    # cached
    assert a.get_stats() is stats


def test_clear_stats_recomputes(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    a.get_prices(conn)
    first = a.get_stats()
    assert a.clear_stats() is True
    assert a._stats_cache is None
    assert a.get_stats() is not first


def test_calc_bar_avg_price(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    a.get_prices(conn)
    avg = a.calc_bar_avg_price()
    first = (10.0 + 11.0 + 9.5 + 10.5) / 4
    assert avg.iloc[0] == pytest.approx(first)
    assert "avg_price" in a._prices_cache.columns


def test_get_growth(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    a.get_prices(conn)
    growth = a.get_growth()
    assert list(growth.columns) == ["timestamp", "period_pct_change"]
    assert math.isnan(growth["period_pct_change"].iloc[0])
    assert growth["period_pct_change"].iloc[1] == pytest.approx(11.0 / 10.5 - 1)


def test_get_growth_requires_prices(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    with pytest.raises(ValueError, match="No prices available"):
        a.get_growth()


def test_get_price_levels(conn, date_bounds):
    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    a.get_prices(conn)
    levels = a.get_price_levels(split=4)
    assert {"volume", "time", "price", "density"} <= set(levels.columns)
    # NOTE: bins are built over the close min/max but applied to each bar's
    # avg_price. Bar 0's avg_price (10.25) sits below the close low (10.5), so it
    # falls outside every bin and is dropped — only 4 of the 5 bars are counted.
    # (Surfaced as a likely latent bug; this asserts current behaviour.)
    assert levels["time"].sum() == 4
    assert levels["volume"].sum() == pytest.approx(900.0)  # 1000 - bar0's 100
    assert (levels["density"] == levels["volume"] * levels["time"]).all()


# --- Indicator integration via Asset ---------------------------------------


def test_add_and_get_indicator(conn, date_bounds):
    from indicators import Indicator

    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    sma = Indicator("sma", window=3)
    returned = a.add_indicator(sma, conn)
    assert returned is sma
    assert sma._result is not None
    # adding the same key again returns the cached indicator, not a new compute
    again = a.add_indicator(Indicator("sma", window=3), conn)
    assert again is sma
    assert a.get_indicator("sma", window=3) is sma
    assert a.get_indicator("sma", window=99) is None


def test_add_indicator_multi_column_source(conn, date_bounds):
    from indicators import Indicator

    start, end = date_bounds
    a = Asset(conn, "ACME", start, end)
    obv = Indicator("obv")
    a.add_indicator(obv, conn, source=["close", "volume"])
    assert obv._result is not None
    assert len(obv._result) == 5


# --- Market ----------------------------------------------------------------


@pytest.fixture
def market_db(db, utc):
    """Extend the shared fake DB with a second stock and a watchlist."""
    db.stocks["BETA"] = (2, "Beta Inc")
    db.prices[2] = [
        (2, utc(2024, 1, 2, 9), 20.0, 21.0, 19.0, 20.5, 400.0),
        (2, utc(2024, 1, 2, 10), 20.5, 22.0, 20.0, 21.0, 500.0),
        (2, utc(2024, 1, 2, 11), 21.0, 21.5, 20.5, 20.8, 350.0),
    ]
    db.watchlists["core"] = ["ACME", "BETA"]
    return db


def test_market_reads_watchlist(market_db, date_bounds):
    start, end = date_bounds
    m = Market(market_db.conn(), start, end, stock_list="core")
    assert set(m.assets.keys()) == {"ACME", "BETA"}
    assert all(v is None for v in m.assets.values())


def test_market_seed_and_populate(market_db, date_bounds):
    start, end = date_bounds
    conn = market_db.conn()
    m = Market(conn, start, end, stock_list="core")
    assert m.seed_assets(conn) is True
    assert all(isinstance(v, Asset) for v in m.assets.values())
    assert m.populate_assets(conn) is True
    # every asset now has its prices and growth cached
    for asset in m.assets.values():
        assert asset._prices_cache is not None
        assert asset._growth_cache is not None


def test_market_get_panel(market_db, date_bounds):
    start, end = date_bounds
    conn = market_db.conn()
    m = Market(conn, start, end, stock_list="core")
    m.seed_assets(conn)
    m.populate_assets(conn)  # get_panel needs prices loaded (it reads get_dates)
    panel = m.get_panel(conn, "close")
    assert "timestamp" in panel.columns
    assert {"ACME", "BETA"} <= set(panel.columns)
    # union of all timestamps (ACME has 5 bars, BETA has 3, sharing 3) → 5 rows
    assert len(panel) == 5
    # BETA has no bars for the later ACME timestamps → NaN there
    assert panel["BETA"].isna().sum() == 2
    # cached per field
    assert m.get_panel(conn, "close") is panel


def test_market_get_panel_invalid_field(market_db, date_bounds):
    start, end = date_bounds
    conn = market_db.conn()
    m = Market(conn, start, end, stock_list="core")
    m.seed_assets(conn)
    with pytest.raises(ValueError, match="Invalid field"):
        m.get_panel(conn, "not_a_field")


def test_market_get_market_stats(market_db, date_bounds):
    start, end = date_bounds
    conn = market_db.conn()
    m = Market(conn, start, end, stock_list="core")
    m.seed_assets(conn)
    m.populate_assets(conn)
    m.get_panel(conn, "close")
    stats = m.get_market_stats(conn, agg_option="close")
    assert list(stats.columns) == ["timestamp", "avg", "count", "std"]
    # `avg` is computed over the symbol columns before any are mutated, so it is
    # correct: ACME=10.5, BETA=20.5 → 15.5 at the first timestamp.
    assert stats["avg"].iloc[0] == pytest.approx((10.5 + 20.5) / 2)
    # KNOWN BUG (get_market_stats is WIP): `count` is computed *after* the `avg`
    # column has been appended, so it counts avg too — inflating every count by 1.
    # First timestamp has 2 real symbols present → reported as 3; the last two
    # timestamps have only ACME → reported as 2.
    assert stats["count"].iloc[0] == 3
    assert stats["count"].iloc[-1] == 2
