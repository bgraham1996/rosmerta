"""Unit tests for the indicator registry (``indicators.py``).

Pure pandas/numpy maths — no DB. Each indicator is checked against small,
hand-verifiable inputs so a regression in the formula is caught, plus tests for
the ``Indicator`` wrapper (registry lookup, key identity, compute caching).
"""

import math

import numpy as np
import pandas as pd
import pytest

from indicators import Indicator, _REGISTRY, sma, ema, rsi, bollinger, obv, vwap


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
    for name in ["sma", "ema", "rsi", "bollinger", "obv", "vwap"]:
        assert name in _REGISTRY
