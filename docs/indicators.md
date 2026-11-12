# Indicators — reference & guide

How the indicator system works, how to use it from analysis code, how to add a
new one, and a full catalog of every indicator currently registered.

- **Code:** `indicators.py` (the registry) and `data_models.py` (`Asset`
  consumes indicators).
- **Boundary:** indicators only *read and compute*. They never touch the DB or
  the network — that is the fetchers' job (`price_retrival/`). Keep that line.

---

## 1. How it works

### The registry

Every indicator is a plain function registered under a string name with the
`@register("name")` decorator, which stores it in the module-level `_REGISTRY`
dict:

```python
@register("sma")
def sma(prices, window):
    return prices.rolling(window).mean()
```

That is the *entire* wiring. There is no base class to subclass, no list to
append to, no config to edit — importing `indicators.py` populates `_REGISTRY`,
and the function is immediately usable by name.

### The `Indicator` wrapper

You rarely call the raw functions directly. `Indicator(name, **params)` binds a
registered function to a specific set of parameters and adds two things:

- **`key`** — a hashable identity `(name, sorted params tuple)`. Two `Indicator`s
  with the same name and params (in any order) compare equal, so the same
  indicator+params pair is only ever computed once per asset.
- **caching** — `compute(input)` computes on the first call and returns the
  cached result on every call after that (it ignores its argument once cached).

```python
from indicators import Indicator

ind = Indicator("rsi", window=14)
ind.key                      # ('rsi', (('window', 14),))
series = ind.compute(closes) # computed once, then cached on `ind`
```

Constructing an `Indicator` with an unregistered name raises `ValueError`.

### The input contract: Series vs OHLCV DataFrame

Each indicator declares, by how it's written, what it expects as input:

- **Price-series indicators** take a single pandas `Series` (usually `close`):
  `sma`, `ema`, `rsi`, `macd`, `bollinger`.
- **OHLCV indicators** take a `DataFrame` and read the columns they need by
  name: `obv`, `vwap`, `atr`, `stochastic`, `adx`.
- **Forward-looking stats** take a `DataFrame` that must include a `timestamp`
  column (they sort by it internally): `days_offset_gain`, `max_forward_gain`,
  `max_forward_drawdown`, `bars_to_target`.

Outputs are always aligned to the input index. Most return a `Series`; the
multi-line indicators (`macd`, `bollinger`, `stochastic`, `adx`) return a
`DataFrame` with named columns (see the catalog).

### How `Asset` consumes them

`Asset.add_indicator(indicator, conn, source='close')` is the normal entry
point. It loads the price frame (`get_prices`, columns: `stock_id, timestamp,
open, high, low, close, volume`), slices it with `source`, computes the
indicator, and caches it on the asset by its `key`:

- `source='close'` → passes the `close` **Series** (default; for price-series
  indicators).
- `source=['high', 'low', 'close']` → passes a **DataFrame** slice (for OHLCV
  indicators; include exactly the columns that indicator needs).

`add_indicator` returns the (possibly already-cached) `Indicator`; retrieve it
later with `get_indicator(name, **params)`, which returns `None` if it was never
added. Because lookups are keyed on `(name, params)`, adding the same
indicator+params twice is a no-op that returns the existing one.

---

## 2. Using indicators

### Standalone (no DB)

```python
import pandas as pd
from indicators import Indicator, rsi

closes = pd.Series([...])
rsi(closes, window=14)                    # call the function directly, or
Indicator("rsi", window=14).compute(closes)  # via the wrapper (adds caching)
```

### Through an `Asset` (the usual path)

```python
from indicators import Indicator

asset = Asset(conn, "AAPL", start, end, timeframe="daily")

# price-series indicator — default source='close'
asset.add_indicator(Indicator("rsi", window=14), conn)

# OHLCV indicator — pass the columns it reads
asset.add_indicator(Indicator("atr", window=14), conn,
                    source=["high", "low", "close"])

# multi-column indicator returns a DataFrame
macd = asset.get_indicator("macd", fast=12, slow=26, signal=9)
macd.compute(...)   # already cached from add_indicator; -> DataFrame

# retrieve a cached indicator later
rsi_ind = asset.get_indicator("rsi", window=14)   # None if never added
```

### `source` cheat-sheet

| Indicator group                                       | `source=`                                   |
|-------------------------------------------------------|---------------------------------------------|
| `sma`, `ema`, `rsi`, `macd`, `bollinger`              | `'close'` (default)                         |
| `atr`, `stochastic`, `adx`                            | `['high', 'low', 'close']`                  |
| `obv`                                                 | `['close', 'volume']`                       |
| `vwap`                                                | `['high', 'low', 'close', 'volume']`        |
| `days_offset_gain`                                    | `['timestamp', 'close']`                    |
| `max_forward_gain`, `max_forward_drawdown`, `bars_to_target` | `['timestamp', 'high', 'low', 'close']` |

### Forward-looking stats and `get_offset_growth`

The forward-looking family answers *"if I had bought here, what happened next?"*
over a **bar-count** lookahead (see the catalog for the shared conventions).
`Asset.get_offset_growth(conn, days=20, mode='pct', column='close')` is a
convenience wrapper over `days_offset_gain` that fills in `bars_per_day` from the
asset's timeframe (8 hourly, 1 daily, 1/5 weekly), so `days` reads in trading
days rather than bars:

```python
growth = asset.get_offset_growth(conn, days=20)   # DataFrame: timestamp, gain_20d
```

The other three (`max_forward_gain`, `max_forward_drawdown`, `bars_to_target`)
are used through `add_indicator` with `source=['timestamp', 'high', 'low',
'close']` and take `days_ahead` / `bars_per_day` directly.

---

## 3. Adding a new indicator

Write a `@register`-decorated function that takes its input and returns a
Series/DataFrame aligned to the input index. No other wiring is needed — it is
immediately constructable via `Indicator("your_name", ...)` and usable through
`add_indicator`.

```python
@register("my_thing")
def my_thing(prices, window=10):
    """One-line purpose. Document the params and the output shape."""
    return prices.rolling(window).mean() - prices
```

Conventions to match the existing set:

- **EMAs use `adjust=False`**; Wilder-smoothed indicators use
  `ewm(alpha=1/window, min_periods=window, adjust=False)` (see `rsi`, `atr`,
  `adx`). This keeps a consistent smoothing convention across the registry.
- **Need multiple columns?** Take the OHLCV `DataFrame` and read columns by name
  (like `obv`/`vwap`), then call it with a list `source`.
- **Forward-looking?** Require a `timestamp` column, sort by it, compute
  positionally, and realign to the caller's index; leave bars whose lookahead
  runs past the end as `NaN` (see `_forward_windows` and `days_offset_gain`).
- **Add a unit test** in `tests/test_indicators.py` against small,
  hand-verifiable inputs, and add the name to `test_*_registered`. The suite is
  DB-free (`uv run pytest`).

---

## 4. Catalog

> **Note (2026-11-12):** this guide was written on 2026-08-01. `ema_arb`, `rsi_ema_cross` and
> `trading_days_offset_gain` landed on `main` afterwards. They're in the table below but have no
> detailed section yet. See their docstrings in `indicators.py`.

Every registered indicator. **P** = takes a price Series; **O** = takes an OHLCV
DataFrame; **F** = forward-looking (needs a `timestamp` column).

| Name | In | Output | Purpose |
|------|----|--------|---------|
| `sma` | P | Series | Simple moving average |
| `ema` | P | Series | Exponential moving average |
| `ema_arb` | P | Series | Short − long EMA spread (`s_window=9`, `l_window=20`); > 0 = bullish alignment |
| `rsi_ema_cross` | P | Series (int 0/1) | `1` on a bullish short/long EMA cross where RSI is oversold (`< oversold`) |
| `macd` | P | DataFrame `macd/signal/histogram` | Trend/momentum crossover |
| `rsi` | P | Series | Momentum oscillator (0–100) |
| `bollinger` | P | DataFrame `upper/middle/lower` | Volatility bands |
| `stochastic` | O | DataFrame `k/d` | Momentum oscillator (0–100) |
| `atr` | O | Series | Volatility (price units) |
| `adx` | O | DataFrame `adx/plus_di/minus_di` | Trend strength + direction |
| `obv` | O | Series | Volume flow |
| `vwap` | O | Series | Volume-weighted average price |
| `days_offset_gain` | F | Series | Point-in-time forward return |
| `trading_days_offset_gain` | F | DataFrame `abs/pct` | Forward return N *trading days* ahead at the same (US/Eastern) bar time |
| `max_forward_gain` | F | Series | Best-case forward return (MFE) |
| `max_forward_drawdown` | F | Series | Worst-case forward drawdown (MAE) |
| `bars_to_target` | F | Series | Bars until a +X% target is hit |

### Trend / moving averages

**`sma(prices, window)`** — Simple moving average over `window` periods.
`window` is required.

**`ema(prices, window)`** — Exponential moving average with span `window`
(`adjust=False`, so it seeds from the first value). `window` is required.

**`macd(prices, fast=12, slow=26, signal=9)`** — Returns a DataFrame:
- `macd` = `EMA(fast) − EMA(slow)`
- `signal` = `EMA(macd, signal)`
- `histogram` = `macd − signal`

All EMAs use `adjust=False` to match `ema`. Read a rising histogram / a
`macd`-crossing-`signal` as momentum turning up.

### Momentum / oscillators

**`rsi(prices, window=14)`** — Relative Strength Index using Wilder's smoothing
(`ewm(alpha=1/window)`). Ranges 0–100; the first valid value lands at index
`window − 1`. Conventionally <30 oversold, >70 overbought.

**`stochastic(data, k=14, d=3)`** — Returns a DataFrame of `k` (%K) and `d`
(%D). `%K = 100·(close − lowest_low) / (highest_high − lowest_low)` over the
`k`-bar range; `%D` is the `d`-period SMA of `%K`. Both range 0–100; low = near
the bottom of the recent range. Needs `high`, `low`, `close`.

### Volatility

**`bollinger(prices, window=20, num_std=2)`** — Returns a DataFrame of
`upper/middle/lower`. `middle` is the `window`-period SMA; the bands sit
`num_std` rolling standard deviations above/below it.

**`atr(data, window=14)`** — Average True Range (Wilder-smoothed). True Range is
`max(high−low, |high−prev_close|, |low−prev_close|)`; ATR is its
`ewm(alpha=1/window)` mean. A volatility measure in price units — useful for
sizing stops (e.g. `stop = entry − 2·ATR`). Needs `high`, `low`, `close`.

### Trend strength

**`adx(data, window=14)`** — Average Directional Index with directional
indicators. Returns a DataFrame:
- `adx` — trend *strength*, 0–100 (higher = stronger trend, regardless of
  direction).
- `plus_di` / `minus_di` — directional movement; `plus_di > minus_di` indicates
  an up-trend.

Directional movement and the true range are Wilder-smoothed. A common uptrend
filter is `adx > 25 and plus_di > minus_di`. Needs `high`, `low`, `close`.

### Volume

**`obv(data, window=None)`** — On-Balance Volume: the running cumulative total of
each bar's volume signed by the close-to-close direction. `window` is unused
(kept for a uniform signature). Needs `close`, `volume`.

**`vwap(data, window=20)`** — Rolling Volume-Weighted Average Price over `window`
bars, using the typical price `(close + high + low) / 3`. Needs `high`, `low`,
`close`, `volume`.

### Forward-looking / non-typical stats

These are not classic technical indicators — they answer *"if I had bought here,
what happened next?"* Shared conventions:

- **Bar-count lookahead.** The window is `days_ahead × bars_per_day` **whole
  bars**, so `bars_per_day` must match the timeframe: **8** hourly, **1** daily,
  **1/5** weekly (fractions are fine; the product is truncated). This is a
  deliberate choice over calendar-day lookahead.
- **Timestamp-ordered.** They sort by `timestamp` internally and realign to the
  caller's index, so row order doesn't matter — but a `timestamp` column is
  required.
- **`NaN` past the end.** A bar whose *full* forward window runs past the end of
  the series yields `NaN` (no partial-window results).

**`days_offset_gain(data, days_ahead=20, bars_per_day=8, mode='pct', offset_column='close')`**
— The change in `offset_column` exactly `days_ahead` days ahead (a single point,
not a window). `mode='pct'` returns the fractional return, `mode='abs'` the raw
difference. Usually driven via `Asset.get_offset_growth`. The raw material for
questions like *"how often is price within ±0.2% thirty days after
investment?"*

**`max_forward_gain(data, days_ahead=20, bars_per_day=8)`** — Maximum Favorable
Excursion (MFE): the best fractional gain achievable if bought at this bar's
`close` and held for the window: `(max future high − close) / close`. Uses the
intra-bar `high` (the best price actually touched). Always ≥ 0 where defined.

**`max_forward_drawdown(data, days_ahead=20, bars_per_day=8)`** — Maximum Adverse
Excursion (MAE): the worst fractional drawdown suffered over the window:
`(min future low − close) / close`, using the intra-bar `low`. *Typically* ≤ 0 —
but positive where price gaps up and never trades back to the entry close (a dip
you never had to sit through). Useful for sizing stops.

**`bars_to_target(data, target=0.05, days_ahead=20, bars_per_day=8)`** — The
number of bars ahead (1-based) until `high` first reaches `close · (1 + target)`
within the window; `NaN` if the target is never hit inside the window. Answers
*"how long until my +`target`% exit fills?"*

---

## See also

- `indicators.py` — the registry and all indicator functions.
- `data_models.py` — `Asset.add_indicator` / `get_indicator` /
  `get_offset_growth`.
- `tests/test_indicators.py` — hand-verifiable unit tests for every indicator.
- `tests/README.md` — the DB-free test suite.
