import pandas as pd
import numpy as np
from datetime import datetime as dt

_REGISTRY ={}

def register(name):
    def decorator(fn):
        _REGISTRY[name] = fn
        return fn
    return decorator

class Indicator:
    def __init__(self, name, **params):
        if name not in _REGISTRY:
            raise ValueError(f"Unknown Indicator: {name}")
        self.name = name
        self.params = params
        self._result = None

    @property
    def key(self):
        return (self.name, tuple(sorted(self.params.items())))

    def compute(self, input_series):
        if self._result is None:
            self._result = _REGISTRY[self.name](input_series, **self.params)
        return self._result

    def __repr__(self):
        p = ". ".join(f"{k}={v}" for k, v in self.params.items())
        return f"Indicator({self.name}, {p})"


@register("sma")
def sma(prices, window):
    return prices.rolling(window).mean()

@register("rsi")
def rsi(prices, window = 14):
    delta = prices.diff()
    gains = delta.where(delta>0, 0.0)
    losses = -delta.where(delta<0,0.0)
    avg_gain = gains.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

@register("ema")
def ema(prices, window):
    return prices.ewm(span=window, adjust=False).mean()


@register("bollinger")
def bollinger(prices, window=20, num_std=2):
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
    direction = np.sign(data['close'].diff())
    return (data['volume'] * direction).cumsum()

@register("vwap")
def vwap(data, window=20):
    typical_price = (data['close'] + data['high'] + data['low']) / 3
    return (typical_price * data['volume']).rolling(window).sum() / data['volume'].rolling(window).sum()

