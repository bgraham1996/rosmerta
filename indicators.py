"""Technical indicator registry.

Indicators are plain functions registered under a string name via the
``@register("name")`` decorator into the module-level ``_REGISTRY``. The
``Indicator`` class wraps a registered function together with its parameters,
caching the computed result and exposing a hashable ``key`` (name + sorted
params) used by ``Asset`` to store/look up indicators.

Add a new indicator by writing a ``@register``-decorated function that takes a
price series (or OHLCV DataFrame, e.g. ``obv``/``vwap``) plus keyword params and
returns a Series/DataFrame aligned to the input index. No other wiring needed.

Built-ins: ``sma``, ``ema``, ``rsi``, ``bollinger``, ``obv``, ``vwap``.
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt

_REGISTRY ={}

def register(name):
    """Decorator that registers ``fn`` under ``name`` in ``_REGISTRY``."""
    def decorator(fn):
        _REGISTRY[name] = fn
        return fn
    return decorator

class Indicator:
    """A registered indicator function bound to a set of parameters.

    Wraps a function from ``_REGISTRY`` with its ``params`` and a lazily
    computed, cached ``_result``. Identified by ``key`` so the same
    indicator+params pair is only computed once per ``Asset``.
    """
    def __init__(self, name, **params):
        if name not in _REGISTRY:
            raise ValueError(f"Unknown Indicator: {name}")
        self.name = name
        self.params = params
        self._result = None

    @property
    def key(self):
        """Hashable identity: ``(name, sorted params tuple)``."""
        return (self.name, tuple(sorted(self.params.items())))

    def compute(self, input_series):
        """Compute (and cache) the indicator over ``input_series``.

        ``input_series`` is a price Series for most indicators, or an OHLCV
        DataFrame for indicators that need multiple columns (``obv``, ``vwap``).
        Returns the cached result on subsequent calls.
        """
        if self._result is None:
            self._result = _REGISTRY[self.name](input_series, **self.params)
        return self._result

    def __repr__(self):
        p = ". ".join(f"{k}={v}" for k, v in self.params.items())
        return f"Indicator({self.name}, {p})"


@register("sma")
def sma(prices, window):
    """Simple moving average over ``window`` periods."""
    return prices.rolling(window).mean()

@register("rsi")
def rsi(prices, window = 14):
    """Relative Strength Index using Wilder's smoothing (EWM, ``alpha=1/window``)."""
    delta = prices.diff()
    gains = delta.where(delta>0, 0.0)
    losses = -delta.where(delta<0,0.0)
    avg_gain = gains.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

@register("ema")
def ema(prices, window):
    """Exponential moving average with span ``window`` (``adjust=False``)."""
    return prices.ewm(span=window, adjust=False).mean()


@register("ema_arb")
def ema_arb(prices, s_window=9, l_window=20):
    """Spread between a short- and long-span EMA (``short - long``).

    Positive when the short EMA sits above the long EMA (bullish alignment),
    negative when below. Returns a Series aligned to ``prices``' index.
    """
    s = prices.ewm(span=s_window, adjust=False).mean()
    l = prices.ewm(span=l_window, adjust=False).mean()
    return s - l

@register("bollinger")
def bollinger(prices, window=20, num_std=2):
    """Bollinger Bands as a DataFrame of ``upper``/``middle``/``lower``.

    ``middle`` is the ``window``-period SMA; the bands sit ``num_std`` rolling
    standard deviations above/below it.
    """
    middle = prices.rolling(window).mean()
    std = prices.rolling(window).std()
    upper = middle + (std * num_std)
    lower = middle - (std * num_std)
    return pd.DataFrame({
        'upper': upper,
        'middle': middle,
        'lower': lower
    })

@register("obv")
def obv(data, window=None):
    """On-Balance Volume from an OHLCV DataFrame.

    Adds/subtracts each bar's volume by the sign of the close-to-close change
    and returns the running cumulative total. ``window`` is unused (kept for a
    uniform signature).
    """
    direction = np.sign(data['close'].diff())
    return (data['volume'] * direction).cumsum()

@register("vwap")
def vwap(data, window=20):
    """Rolling Volume-Weighted Average Price over ``window`` bars.

    Uses the typical price ``(close + high + low) / 3`` and an OHLCV DataFrame.
    """
    typical_price = (data['close'] + data['high'] + data['low']) / 3
    return (typical_price * data['volume']).rolling(window).sum() / data['volume'].rolling(window).sum()


# None typucal 'indicators, ie just things I want to calculate'

@register("days_offset_gain")
def days_offset_gain(data, days_ahead=20, bars_per_day=8, mode='pct', offset_column='close'):
    """Forward-looking gain: for each bar, the change in ``offset_column`` when
    looking ``days_ahead`` days ahead, measured in bars.

    The lookahead is applied at the bar level: it offsets by
    ``days_ahead * bars_per_day`` bars, so ``bars_per_day`` must match the
    current timeframe (e.g. 8 for hourly, 1 for daily, 1/5 for weekly —
    fractions are fine, the product is truncated to whole bars). Requires an OHLCV
    DataFrame with a ``timestamp`` column (call via ``source=['timestamp',
    <col>]``). ``mode='pct'`` returns the fractional return, ``mode='abs'`` the
    raw price difference. Bars whose offset runs past the end of the series
    yield ``NaN``.
    """
    if mode not in ('pct', 'abs'):
        raise ValueError(f"days_offset_gain: unknown mode {mode!r}; use 'pct' or 'abs'")

    if data.empty:
        return pd.Series(dtype='float64', index=data.index)

    # Sort by timestamp first so the positional shift below always looks
    # strictly forward in time regardless of the caller's row order; the
    # original index labels are preserved so we can realign at the end.
    dt = data[['timestamp', offset_column]].sort_values('timestamp')
    current = dt[offset_column]

    # Offset by whole bars: days_ahead * bars_per_day, truncated (e.g. 1/5
    # bars/day for weekly is fine — only the product matters).
    future = current.shift(-int(days_ahead * bars_per_day))

    if mode == 'pct':
        values = (future - current) / current
    else:  # 'abs'
        values = future - current

    # Realign to the order the caller handed us.
    return values.reindex(data.index)


@register("trading_days_offset_gain")
def trading_days_offset_gain(data, days_ahead=20, offset_column='close'):
    """Forward gain ``days_ahead`` *trading days* ahead at the **same bar time**.

    For each bar the target is the bar with the identical time-of-day
    ``days_ahead`` market-open days later. Trading days are taken from the data
    itself: bars are grouped by time-of-day and shifted ``days_ahead`` positions
    within each (date-ordered) group, so weekends and holidays — which have no
    bars to occupy a slot — are skipped automatically. Only exact same-time
    matches count: where the target bar is absent (the tail of the series, or a
    DST/data gap that splits the time-of-day group) the value is NaN.

    Requires an OHLCV DataFrame with a ``timestamp`` column (call via
    ``source=['timestamp', <col>]``). Returns a DataFrame with two columns,
    ``abs`` (raw price difference) and ``pct`` (fractional return), aligned to
    the caller's index.
    """
    if data.empty:
        return pd.DataFrame(
            {'abs': pd.Series(dtype='float64'), 'pct': pd.Series(dtype='float64')},
            index=data.index,
        )

    current = data[offset_column].astype('float64')

    # Sort by timestamp so each time-of-day group is date-ordered, then look
    # days_ahead bars ahead *within the same time-of-day*. The original index
    # labels are preserved so we can realign to the caller's row order at the end.
    ordered = pd.DataFrame(
        {'ts': pd.to_datetime(data['timestamp'], utc=True), 'price': current}
    ).sort_values('ts', kind='stable')
    tod = ordered['ts'].dt.strftime('%H:%M:%S')
    future = ordered.groupby(tod, sort=False)['price'].shift(-days_ahead)
    future = future.reindex(data.index)

    abs_gain = future - current
    pct_gain = abs_gain / current
    return pd.DataFrame({'abs': abs_gain, 'pct': pct_gain}, index=data.index)

