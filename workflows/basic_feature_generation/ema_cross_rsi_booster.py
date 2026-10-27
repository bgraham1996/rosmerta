"""RSI-boosted EMA-cross signal, with independent EMA / RSI timeframes.

For every asset in the market this flags (``1``) each bar where the short-span
EMA crosses *above* the long-span EMA while RSI is still oversold, and ``0``
everywhere else -- a long-entry setup (a bullish cross arriving while the stock
is still beaten down).

Unlike the single-series ``rsi_ema_cross`` indicator, this workflow lets the EMA
cross and the RSI confirmation live on **different timeframes**.

Why the RSI check is a *lookback*, not an exact coincidence
-----------------------------------------------------------
A bullish EMA cross is, by construction, a moment momentum has already turned up,
so RSI is above the midline there: across 97 stocks over 2020-2024 the *minimum*
RSI at a daily 9/20 cross was ~48 -- RSI is never actually oversold (< 30) *at*
the cross, on any timeframe (hourly, daily, or weekly all give 0 exact matches).
So the useful signal is a **recovery-from-oversold entry**: a bullish cross that
fires while RSI was oversold at any point within the last ``OVERSOLD_LOOKBACK``
RSI-timeframe bars. ``OVERSOLD_LOOKBACK = 0`` restores the strict
cross-and-oversold-on-the-same-bar reading (a near-null event); the default of 10
(daily) fires ~168 times across 76/97 stocks over 2020-2024. The cross timeframe,
RSI timeframe, and lookback are all independently configurable below.

How the two timeframes are combined
-----------------------------------
Each asset is pulled once at the hourly base and resampled to the EMA timeframe
and the RSI timeframe separately (``utils.bars`` conventions: business day /
week-ending-Fri / business-month-end). Resampled bars are labelled at their
**actual close time** (the last underlying hourly bar), so aligning the RSI onto
the cross with a *backward* ``merge_asof`` is lookahead-free: each EMA-cross bar
is paired with the most recent RSI bar that had already closed at that instant.
When both timeframes are equal the alignment is an exact match and the result
reproduces the ``rsi_ema_cross`` indicator.

Result
------
``main`` returns ``(market, signals)`` where ``market`` is the hourly
``Market`` and ``signals`` is ``{ticker: Series}``. Each Series is int 0/1 on the
EMA timeframe, indexed by the EMA bar's close timestamp; ``1`` marks a candidate
entry bar:

    market, signals = main('2023-01-01 00:00:00', '2025-01-01 00:00:00')
    sig = signals['AAPL']
    entries = sig[sig == 1]        # timestamps of triggered entry bars
"""


from db_config import *
from data_models import *
from indicators import ema, rsi
from utils.bars import RESAMPLE_RULES, VALID_TIMEFRAMES
import pandas as pd
import psycopg2 as db


# --- Signal parameters (tune here) -------------------------------------------
# Timeframes are independent: the EMA cross runs on EMA_TIMEFRAME, the RSI
# oversold check on RSI_TIMEFRAME. Each is one of utils.bars.VALID_TIMEFRAMES
# ('hourly', 'daily', 'weekly', 'monthly'). Window sizes are counted in bars of
# their own timeframe (so S_WINDOW=9 on 'daily' is a 9-day EMA).
EMA_TIMEFRAME = 'daily'
RSI_TIMEFRAME = 'daily'
S_WINDOW = 9
L_WINDOW = 20
RSI_WINDOW = 14
OVERSOLD = 30
# The cross qualifies if RSI was oversold within the last OVERSOLD_LOOKBACK bars
# of the RSI timeframe (0 = must be oversold on the exact cross bar; see module
# docstring for why that is essentially never true).
OVERSOLD_LOOKBACK = 10


def _resample_close(hourly_df, timeframe):
    """Resample the hourly close to ``timeframe``, labelled at the true close time.

    Returns a two-column frame ``timestamp`` / ``close`` where each row's
    ``timestamp`` is the last underlying hourly bar of the period (not the
    period-start label), so downstream backward as-of alignment never peeks into
    a bar that had not yet closed. ``'hourly'`` passes the bars through unchanged.
    """
    # DB timestamps arrive as object-dtype datetimes; coerce so the index is a
    # real DatetimeIndex (required for resample and merge_asof).
    ts = pd.to_datetime(hourly_df['timestamp'], utc=True)
    if timeframe == 'hourly':
        return pd.DataFrame(
            {'timestamp': ts.to_numpy(), 'close': hourly_df['close'].to_numpy()}
        )

    rule = RESAMPLE_RULES[timeframe]
    grouped = pd.Series(hourly_df['close'].to_numpy(), index=ts.to_numpy()).resample(rule)
    out = pd.DataFrame(
        {
            'timestamp': grouped.apply(lambda bars: bars.index.max()),
            'close': grouped.last(),
        }
    )
    # Empty periods (weekends/holidays) have no close -> drop them.
    return out.dropna(subset=['close']).reset_index(drop=True)


def _asset_signal(hourly_df):
    """Compute the cross-timeframe RSI-boosted EMA-cross signal for one asset.

    Returns an int 0/1 Series on the EMA timeframe, indexed by each EMA bar's
    close timestamp.
    """
    ema_bars = _resample_close(hourly_df, EMA_TIMEFRAME)
    rsi_bars = _resample_close(hourly_df, RSI_TIMEFRAME)

    # Bullish cross on the EMA timeframe: short-long spread turns positive.
    spread = ema(ema_bars['close'], window=S_WINDOW) - ema(ema_bars['close'], window=L_WINDOW)
    cross_up = (spread > 0) & (spread.shift(1) <= 0)

    # Oversold on the RSI timeframe (NaN warm-up compares False -> never fires),
    # then widened to "oversold within the last OVERSOLD_LOOKBACK bars" so a cross
    # can be confirmed by a recent oversold reading, not only a simultaneous one.
    oversold_bar = rsi(rsi_bars['close'], window=RSI_WINDOW) < OVERSOLD
    oversold = oversold_bar.rolling(OVERSOLD_LOOKBACK + 1, min_periods=1).max().astype(bool)

    left = pd.DataFrame({'timestamp': ema_bars['timestamp'], 'cross': cross_up.to_numpy()})
    right = pd.DataFrame({'timestamp': rsi_bars['timestamp'], 'oversold': oversold.to_numpy()})
    merged = pd.merge_asof(
        left.sort_values('timestamp'),
        right.sort_values('timestamp'),
        on='timestamp',
        direction='backward',
    )
    signal = (merged['cross'].fillna(False) & merged['oversold'].fillna(False)).astype(int)
    signal.index = merged['timestamp']
    return signal


def main(start_date, end_date, stock_list='core'):
    if EMA_TIMEFRAME not in VALID_TIMEFRAMES:
        raise ValueError(f"EMA_TIMEFRAME {EMA_TIMEFRAME!r} not in {sorted(VALID_TIMEFRAMES)}")
    if RSI_TIMEFRAME not in VALID_TIMEFRAMES:
        raise ValueError(f"RSI_TIMEFRAME {RSI_TIMEFRAME!r} not in {sorted(VALID_TIMEFRAMES)}")

    db_config = get_db_config()
    connection = db.connect(**db_config)
    connection.autocommit = False

    signals = {}
    with connection as conn:
        # Base market is hourly; each asset is resampled to the EMA and RSI
        # timeframes inside _asset_signal.
        market = Market(conn, start_date, end_date, stock_list, timeframe='hourly')
        market.seed_assets(conn)
        market.populate_assets(conn)

        for ticker, asset in market.assets.items():
            hourly_df = asset.get_prices(conn)
            if hourly_df is None or hourly_df.empty:
                continue
            signals[ticker] = _asset_signal(hourly_df)

    return market, signals
