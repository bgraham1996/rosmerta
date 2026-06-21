"""Unit tests for OHLCV resampling (``utils/bars.py``)."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from utils.bars import OHLCV_AGG, RESAMPLE_RULES, VALID_TIMEFRAMES, resample_ohlcv


def _bar(ts, o, h, low, c, v, stock_id=None):
    row = {"timestamp": ts, "open": o, "high": h, "low": low, "close": c, "volume": v}
    if stock_id is not None:
        row = {"stock_id": stock_id, **row}
    return row


@pytest.fixture
def two_day_hourly():
    """Hourly bars across two business days (2024-01-02 and 2024-01-03)."""
    def t(d, h):
        return datetime(2024, 1, d, h, tzinfo=timezone.utc)

    rows = [
        _bar(t(2, 9), 10, 11, 9, 10.5, 100),
        _bar(t(2, 10), 10.5, 12, 10, 11, 200),
        _bar(t(2, 11), 11, 13, 10.5, 12, 150),
        _bar(t(3, 9), 12, 12.5, 11, 11.5, 300),
        _bar(t(3, 10), 11.5, 14, 11, 13, 250),
    ]
    return pd.DataFrame(rows)


def test_resample_daily_aggregates_correctly(two_day_hourly):
    out = resample_ohlcv(two_day_hourly, RESAMPLE_RULES["daily"])
    assert len(out) == 2  # two business days

    day1 = out.iloc[0]
    assert day1["open"] == 10        # first bar's open
    assert day1["high"] == 13        # max high
    assert day1["low"] == 9          # min low
    assert day1["close"] == 12       # last bar's close
    assert day1["volume"] == 450     # 100 + 200 + 150

    day2 = out.iloc[1]
    assert day2["open"] == 12
    assert day2["high"] == 14
    assert day2["close"] == 13
    assert day2["volume"] == 550


def test_resample_preserves_stock_id(two_day_hourly):
    df = two_day_hourly.copy()
    df.insert(0, "stock_id", 7)
    out = resample_ohlcv(df, RESAMPLE_RULES["daily"])
    assert (out["stock_id"] == 7).all()
    assert list(out.columns)[0] == "stock_id"


def test_resample_timestamp_restored_as_column(two_day_hourly):
    out = resample_ohlcv(two_day_hourly, RESAMPLE_RULES["daily"])
    assert "timestamp" in out.columns
    assert out.index.name != "timestamp"


def test_resample_empty_returns_input():
    empty = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    out = resample_ohlcv(empty, RESAMPLE_RULES["daily"])
    assert out.empty


def test_resample_drops_periods_without_data(two_day_hourly):
    # Weekly resampling rolls both business days into one week-ending-Friday bar,
    # and must not emit empty bars for intervening weeks.
    out = resample_ohlcv(two_day_hourly, RESAMPLE_RULES["weekly"])
    assert len(out) == 1
    assert out.iloc[0]["open"] == 10
    assert out.iloc[0]["close"] == 13
    assert out.iloc[0]["volume"] == 1000


def test_valid_timeframes_membership():
    assert "hourly" in VALID_TIMEFRAMES
    assert {"daily", "weekly", "monthly"} <= VALID_TIMEFRAMES
    assert set(OHLCV_AGG) == {"open", "high", "low", "close", "volume"}
