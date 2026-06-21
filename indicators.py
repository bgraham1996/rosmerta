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

