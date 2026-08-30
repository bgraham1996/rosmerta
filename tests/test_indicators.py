"""Unit tests for the indicator registry (``indicators.py``).

Pure pandas/numpy maths — no DB. Each indicator is checked against small,
hand-verifiable inputs so a regression in the formula is caught, plus tests for
the ``Indicator`` wrapper (registry lookup, key identity, compute caching).
"""

import math

import numpy as np
import pandas as pd
import pytest

from indicators import (
    Indicator, _REGISTRY, sma, ema, rsi, bollinger, obv, vwap, days_offset_gain,
    trading_days_offset_gain, rsi_ema_cross,
)


@pytest.fixture
def closes():
    return pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])


def test_sma_window_3(closes):
    result = sma(closes, window=3)
    expected = [math.nan, math.nan, 2.0, 3.0, 4.0]
    assert result.iloc[:2].isna().all()
    pd.testing.assert_series_equal(
        result.iloc[2:], pd.Series(expected[2:], index=[2, 3, 4]), check_names=False
    )


def test_ema_adjust_false():
    # alpha = 2/(span+1) = 2/3 for span=2; adjust=False seeds with the first value.
    s = pd.Series([1.0, 2.0, 3.0])
    result = ema(s, window=2)
    a = 2 / 3
    e0 = 1.0
    e1 = a * 2 + (1 - a) * e0
    e2 = a * 3 + (1 - a) * e1
    np.testing.assert_allclose(result.to_numpy(), [e0, e1, e2])


def test_rsi_all_gains_is_100():
    # Strictly increasing series → no losses → RSI saturates at 100.
    s = pd.Series(np.arange(1, 30, dtype=float))
    result = rsi(s, window=14)
    # min_periods=window means the first valid value lands on index window-1 (13);
    # indices 0..12 are NaN, and every value from 13 on pins to 100.
    assert result.iloc[:13].isna().all()
    np.testing.assert_allclose(result.iloc[13:].to_numpy(), 100.0)


def test_rsi_bounds_on_mixed_series():
    rng = np.random.default_rng(0)
    s = pd.Series(np.cumsum(rng.standard_normal(200)) + 100)
    result = rsi(s, window=14).dropna()
    assert ((result >= 0) & (result <= 100)).all()


def test_bollinger_structure_and_bands(closes):
    bands = bollinger(closes, window=3, num_std=2)
    assert list(bands.columns) == ["upper", "middle", "lower"]
    # middle band is the SMA; bands are symmetric about it by num_std * rolling std.
    pd.testing.assert_series_equal(
        bands["middle"], sma(closes, window=3), check_names=False
    )
    std = closes.rolling(3).std()
    np.testing.assert_allclose(
        (bands["upper"] - bands["middle"]).to_numpy(), (2 * std).to_numpy()
    )
    np.testing.assert_allclose(
        (bands["middle"] - bands["lower"]).to_numpy(), (2 * std).to_numpy()
    )


def test_obv_accumulates_by_direction():
    data = pd.DataFrame(
        {
            "close": [10.0, 11.0, 10.5, 10.5, 12.0],
            "volume": [100.0, 200.0, 150.0, 300.0, 250.0],
        }
    )
    result = obv(data)
    # diff sign: [nan, +, -, 0, +] → +200, -150, +0, +250 cumulatively.
    # First element is NaN (no prior close); cumsum carries NaN forward.
    assert math.isnan(result.iloc[0])
    np.testing.assert_allclose(result.iloc[1:].to_numpy(), [200.0, 50.0, 50.0, 300.0])


def test_vwap_small_window():
    data = pd.DataFrame(
        {
            "close": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "volume": [100.0, 300.0],
        }
    )
    result = vwap(data, window=2)
    tp = (data["close"] + data["high"] + data["low"]) / 3
    expected = (tp * data["volume"]).sum() / data["volume"].sum()
    assert math.isnan(result.iloc[0])  # not enough bars for the window yet
    np.testing.assert_allclose(result.iloc[1], expected)


# --- days_offset_gain (forward-looking) --------------------------------------


@pytest.fixture
def offset_frame():
    """Six hourly bars; with ``bars_per_day=2`` each 'day' is two bars."""
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2024-01-02 09:00", periods=6, freq="h", tz="UTC"
            ),
            "close": [10.0, 20.0, 40.0, 10.0, 30.0, 60.0],
        }
    )


def test_days_offset_gain_pct(offset_frame):
    result = days_offset_gain(offset_frame, days_ahead=1, bars_per_day=2)
    # Two bars ahead: (40-10)/10, (10-20)/20, (30-40)/40, (60-10)/10.
    np.testing.assert_allclose(result.iloc[:4].to_numpy(), [3.0, -0.5, -0.25, 5.0])
    # The last days_ahead*bars_per_day bars have no future bar to compare to.
    assert result.iloc[4:].isna().all()


def test_days_offset_gain_abs(offset_frame):
    result = days_offset_gain(offset_frame, days_ahead=1, bars_per_day=2, mode="abs")
    np.testing.assert_allclose(result.iloc[:4].to_numpy(), [30.0, -10.0, -10.0, 50.0])
    assert result.iloc[4:].isna().all()


def test_days_offset_gain_fractional_bars_per_day(offset_frame):
    # Weekly-style fraction: 10 days at 1/5 bars per day = the same 2-bar offset.
    frac = days_offset_gain(offset_frame, days_ahead=10, bars_per_day=1 / 5)
    whole = days_offset_gain(offset_frame, days_ahead=1, bars_per_day=2)
    pd.testing.assert_series_equal(frac, whole)


def test_days_offset_gain_sorts_by_timestamp(offset_frame):
    # An out-of-order frame must give each bar the same value it gets when
    # sorted; results come back aligned to the caller's index.
    shuffled = offset_frame.sample(frac=1, random_state=0)
    result = days_offset_gain(shuffled, days_ahead=1, bars_per_day=2)
    expected = days_offset_gain(offset_frame, days_ahead=1, bars_per_day=2)
    for idx in offset_frame.index:
        a, b = result.loc[idx], expected.loc[idx]
        assert (math.isnan(a) and math.isnan(b)) or a == b


def test_days_offset_gain_unknown_mode_raises(offset_frame):
    with pytest.raises(ValueError, match="unknown mode"):
        days_offset_gain(offset_frame, mode="median")


# --- trading_days_offset_gain (same-time, business-day lookahead) -------------


@pytest.fixture
def tradingday_frame():
    """Two bars/day (09:00, 15:00) over four trading days spanning a weekend.

    Thu 04, Fri 05, Mon 08, Tue 09 Jan 2024 — Sat/Sun (06/07) have no bars, so
    the same-time-of-day shift skips them without any calendar bookkeeping.
    """
    ts = pd.to_datetime(
        [
            "2024-01-04 09:00", "2024-01-04 15:00",
            "2024-01-05 09:00", "2024-01-05 15:00",
            "2024-01-08 09:00", "2024-01-08 15:00",
            "2024-01-09 09:00", "2024-01-09 15:00",
        ]
    ).tz_localize("UTC")
    return pd.DataFrame(
        {"timestamp": ts, "close": [10.0, 100.0, 20.0, 200.0, 40.0, 400.0, 80.0, 800.0]}
    )


def test_trading_days_offset_gain_skips_weekends(tradingday_frame):
    out = trading_days_offset_gain(tradingday_frame, days_ahead=2)
    assert list(out.columns) == ["abs", "pct"]
    # 09:00 group [10,20,40,80] shifted 2 trading days (Thu->Mon over the
    # weekend) -> future [40,80,nan,nan]; 15:00 group [100,200,400,800] ->
    # [400,800,nan,nan]. Rows interleave 09:00,15:00,...
    np.testing.assert_allclose(
        out["abs"].to_numpy(), [30.0, 300.0, 60.0, 600.0, np.nan, np.nan, np.nan, np.nan]
    )
    np.testing.assert_allclose(
        out["pct"].to_numpy(), [3.0, 3.0, 3.0, 3.0, np.nan, np.nan, np.nan, np.nan]
    )


def test_trading_days_offset_gain_exact_time_only(tradingday_frame):
    # Drop the Mon 09:00 bar: the Thu 09:00 -> (2 trading days) target no longer
    # exists at 09:00, so within the 09:00 group the shift now reaches Tue 09:00
    # (still exact same time). The point: values only ever come from same-time bars.
    frame = tradingday_frame.drop(index=4).reset_index(drop=True)  # remove Mon 09:00
    out = trading_days_offset_gain(frame, days_ahead=2)
    # 09:00 group is now [Thu 10, Fri 20, Tue 80]; shift(-2): Thu->Tue(80), rest nan.
    nine = out.loc[frame["timestamp"].dt.strftime("%H:%M:%S") == "09:00:00", "abs"]
    np.testing.assert_allclose(nine.to_numpy(), [70.0, np.nan, np.nan])


def test_trading_days_offset_gain_realigns_shuffled_input(tradingday_frame):
    shuffled = tradingday_frame.sample(frac=1, random_state=0)
    result = trading_days_offset_gain(shuffled, days_ahead=2)
    expected = trading_days_offset_gain(tradingday_frame, days_ahead=2)
    for idx in tradingday_frame.index:
        for col in ("abs", "pct"):
            a, b = result.loc[idx, col], expected.loc[idx, col]
            assert (math.isnan(a) and math.isnan(b)) or a == b


def test_trading_days_offset_gain_empty():
    empty = pd.DataFrame({"timestamp": pd.to_datetime([]), "close": []})
    out = trading_days_offset_gain(empty)
    assert list(out.columns) == ["abs", "pct"]
    assert len(out) == 0


def test_trading_days_offset_gain_aligns_across_dst():
    # One session bar (10:00 America/New_York) spanning the 2024 spring-forward
    # (10 Mar). In UTC that bar is 15:00 before DST (EST) but 14:00 after (EDT),
    # so grouping by UTC time-of-day would split it into two groups and strand the
    # pre-DST bar; grouping by Eastern time keeps them one session so the shift
    # connects across the boundary.
    ts = pd.to_datetime(
        [
            "2024-03-08 15:00",  # Fri, EST -> 10:00 ET
            "2024-03-11 14:00",  # Mon, EDT -> 10:00 ET
            "2024-03-12 14:00",  # Tue, EDT -> 10:00 ET
            "2024-03-13 14:00",  # Wed, EDT -> 10:00 ET
        ]
    ).tz_localize("UTC")
    frame = pd.DataFrame({"timestamp": ts, "close": [10.0, 20.0, 40.0, 80.0]})
    out = trading_days_offset_gain(frame, days_ahead=1)
    # All four are the same 10:00 ET session -> one group -> shift(-1):
    # future [20,40,80,nan]. The pre-DST Fri bar (idx 0) MUST be non-NaN.
    np.testing.assert_allclose(out["abs"].to_numpy(), [10.0, 20.0, 40.0, np.nan])
    np.testing.assert_allclose(out["pct"].to_numpy(), [1.0, 1.0, 1.0, np.nan])


# --- rsi_ema_cross (bullish EMA cross gated by oversold RSI) ------------------


@pytest.fixture
def oversold_cross_closes():
    """A long steady decline (RSI-14 deeply oversold) then a brisk uptick that a
    fast 2/4 EMA pair crosses on while RSI is still down in oversold territory.

    Engineered so the bullish cross and the oversold reading coincide on one
    bar (index 31) — the setup the indicator is meant to flag.
    """
    down = list(np.linspace(100, 55, 30))
    up = [57.0, 60.0, 64.0, 69.0]
    return pd.Series(down + up)


def test_rsi_ema_cross_is_binary_int(oversold_cross_closes):
    out = rsi_ema_cross(oversold_cross_closes, s_window=2, l_window=4)
    assert set(pd.unique(out)).issubset({0, 1})
    assert out.dtype.kind in ("i", "u")


def test_rsi_ema_cross_fires_on_oversold_cross(oversold_cross_closes):
    out = rsi_ema_cross(
        oversold_cross_closes, s_window=2, l_window=4, rsi_window=14, oversold=30
    )
    # Exactly one trigger, on the bar where the bullish cross meets oversold RSI.
    assert out.sum() == 1
    assert out.iloc[31] == 1
    # That bar is a genuine cross-up (spread turns positive) with RSI < 30.
    spread = ema(oversold_cross_closes, window=2) - ema(oversold_cross_closes, window=4)
    assert spread.iloc[31] > 0 and spread.iloc[30] <= 0
    assert rsi(oversold_cross_closes, window=14).iloc[31] < 30


def test_rsi_ema_cross_rsi_gate_blocks(oversold_cross_closes):
    # RSI at the cross is ~22.7; a stricter oversold=20 gate is unmet, so the
    # cross is suppressed and the whole series reads 0.
    out = rsi_ema_cross(
        oversold_cross_closes, s_window=2, l_window=4, rsi_window=14, oversold=20
    )
    assert (out == 0).all()


def test_rsi_ema_cross_isolates_crosses_when_gate_open(oversold_cross_closes):
    # An always-true RSI gate (< 101) reduces the signal to the raw bullish
    # cross-ups, isolating the cross-detection half of the logic.
    out = rsi_ema_cross(oversold_cross_closes, s_window=2, l_window=4, oversold=101)
    spread = ema(oversold_cross_closes, window=2) - ema(oversold_cross_closes, window=4)
    cross_up = ((spread > 0) & (spread.shift(1) <= 0)).astype(int)
    pd.testing.assert_series_equal(out, cross_up, check_names=False)


def test_rsi_ema_cross_matches_manual_and():
    # General cross-check: the output is exactly the elementwise AND of a bullish
    # cross and an oversold RSI, on an independent reversal series.
    closes = pd.Series(np.concatenate([np.linspace(100, 60, 25), np.linspace(61, 90, 25)]))
    out = rsi_ema_cross(closes, s_window=9, l_window=20, rsi_window=14, oversold=60)
    spread = ema(closes, window=9) - ema(closes, window=20)
    cross_up = (spread > 0) & (spread.shift(1) <= 0)
    oversold = rsi(closes, window=14) < 60
    expected = (cross_up & oversold).astype(int)
    pd.testing.assert_series_equal(out, expected, check_names=False)


def test_rsi_ema_cross_uptrend_no_signal():
    # A pure uptrend is never oversold and has no post-warmup bullish cross.
    out = rsi_ema_cross(pd.Series(np.linspace(10.0, 100.0, 60)))
    assert (out == 0).all()


# --- Indicator wrapper -----------------------------------------------------


def test_indicator_unknown_name_raises():
    with pytest.raises(ValueError, match="Unknown Indicator"):
        Indicator("not_a_real_indicator")


def test_indicator_key_is_order_independent():
    a = Indicator("bollinger", window=20, num_std=2)
    b = Indicator("bollinger", num_std=2, window=20)
    assert a.key == b.key
    assert a.key == ("bollinger", (("num_std", 2), ("window", 20)))


def test_indicator_compute_caches_result(closes):
    ind = Indicator("sma", window=3)
    first = ind.compute(closes)
    # Second call must return the identical cached object, not recompute.
    second = ind.compute(pd.Series([99.0, 99.0, 99.0, 99.0, 99.0]))
    assert first is second
    assert ind._result is first


def test_builtins_are_registered():
    for name in ["sma", "ema", "rsi", "bollinger", "obv", "vwap", "days_offset_gain",
                 "rsi_ema_cross"]:
        assert name in _REGISTRY
