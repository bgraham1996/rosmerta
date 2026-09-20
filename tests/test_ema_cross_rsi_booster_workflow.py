"""Unit tests for the ema_cross_rsi_booster workflow's signal logic.

DB-free: exercises ``_asset_signal`` / ``_resample_close`` on synthetic hourly
frames. Covers the two properties that matter — (1) with equal timeframes and a
zero lookback the workflow reproduces the ``rsi_ema_cross`` indicator exactly,
and (2) the oversold lookback widens the confirmation window so a bullish cross
that follows a *recent* oversold dip fires even when RSI has recovered by the
cross bar.
"""

import contextlib

import numpy as np
import pandas as pd
import pytest

from indicators import rsi_ema_cross
from workflows.basic_feature_generation import ema_cross_rsi_booster as wf


@contextlib.contextmanager
def _params(**overrides):
    """Temporarily set the workflow's module-level parameters, then restore."""
    saved = {k: getattr(wf, k) for k in overrides}
    for k, v in overrides.items():
        setattr(wf, k, v)
    try:
        yield
    finally:
        for k, v in saved.items():
            setattr(wf, k, v)


def _hourly(prices):
    ts = pd.date_range("2024-01-01 14:00", periods=len(prices), freq="h", tz="UTC")
    return pd.DataFrame({"timestamp": ts, "close": np.asarray(prices, dtype="float64")})


@pytest.fixture
def reversal():
    # Long decline (RSI-14 goes oversold) then a recovery; the slow 9/20 cross
    # fires only after RSI has climbed back above 30 — so the cross bar itself is
    # not oversold, but a recent bar was.
    return _hourly(np.concatenate([np.linspace(100, 55, 40), np.linspace(56, 95, 40)]))


def test_same_timeframe_zero_lookback_matches_indicator(reversal):
    # Engineered fast-cross series where the cross and oversold coincide, so even
    # lookback=0 fires — and the workflow must equal the indicator bar-for-bar.
    closes = _hourly(list(np.linspace(100, 55, 30)) + [57.0, 60.0, 64.0, 69.0])
    with _params(
        EMA_TIMEFRAME="hourly", RSI_TIMEFRAME="hourly",
        S_WINDOW=2, L_WINDOW=4, RSI_WINDOW=14, OVERSOLD=30, OVERSOLD_LOOKBACK=0,
    ):
        got = wf._asset_signal(closes).reset_index(drop=True)
    ref = rsi_ema_cross(closes["close"], s_window=2, l_window=4, rsi_window=14, oversold=30)
    assert got.sum() == 1
    np.testing.assert_array_equal(got.to_numpy(), ref.to_numpy())


def test_lookback_confirms_recent_oversold(reversal):
    with _params(
        EMA_TIMEFRAME="hourly", RSI_TIMEFRAME="hourly",
        S_WINDOW=9, L_WINDOW=20, RSI_WINDOW=14, OVERSOLD=30,
    ):
        with _params(OVERSOLD_LOOKBACK=0):
            strict = wf._asset_signal(reversal)
        with _params(OVERSOLD_LOOKBACK=30):
            widened = wf._asset_signal(reversal)
    # The cross bar isn't oversold, so strict never fires; a generous lookback
    # picks up the earlier oversold dip and lets the cross through.
    assert strict.sum() == 0
    assert widened.sum() >= 1
    assert set(pd.unique(widened)).issubset({0, 1})


def test_resample_close_labels_at_true_close_time():
    # Two trading days of hourly bars; daily resample must label each bar at the
    # last underlying hourly timestamp (its close), not the period start.
    frame = _hourly(np.arange(16.0))  # 16 hourly bars spanning >1 day
    out = wf._resample_close(frame, "daily")
    assert list(out.columns) == ["timestamp", "close"]
    # Every labelled timestamp must be an actual hourly timestamp from the input.
    src = set(pd.to_datetime(frame["timestamp"], utc=True))
    assert set(out["timestamp"]).issubset(src)


def test_cross_timeframe_runs_and_is_binary():
    # daily EMA cross vs hourly RSI: mainly a smoke test that mixed timeframes
    # align without error and yield a clean 0/1 series on the EMA timeframe.
    frame = _hourly(np.concatenate([np.linspace(100, 60, 200), np.linspace(61, 120, 200)]))
    with _params(
        EMA_TIMEFRAME="daily", RSI_TIMEFRAME="hourly",
        S_WINDOW=3, L_WINDOW=6, RSI_WINDOW=14, OVERSOLD=40, OVERSOLD_LOOKBACK=20,
    ):
        out = wf._asset_signal(frame)
    assert set(pd.unique(out)).issubset({0, 1})
    assert out.dtype.kind in ("i", "u")
