"""Utilities for working with OHLCV bar data.

Currently provides timeframe aggregation (resampling hourly bars to coarser
timeframes). Designed to be consumed by Asset and any future class that
holds bar-shaped data (e.g. Pair spreads, synthetic instruments).
"""

from pandas import DataFrame


# OHLCV aggregation rules — applied column-wise during resampling.
# These are the standard conventions for rolling up bar data to a coarser
# timeframe and should not be changed without good reason.
OHLCV_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}

# Analysis timeframes that correspond directly to a stored ``price_bars``
# ``bar_size`` (the IB barSizeSetting string). Under "read-what-you-ask"
# (board item "Bar size in the analysis layer") these are read straight from
# the DB at that granularity — no resampling. Both ``'1min'`` and ``'minute'``
# are accepted spellings for 1-minute.
TIMEFRAME_BAR_SIZE = {
    "1min":   "1 min",
    "minute": "1 min",
    "5min":   "5 mins",
    "15min":  "15 mins",
    "30min":  "30 mins",
    "hourly": "1 hour",
}

# Pandas resample rules for each supported coarser (calendar) timeframe.
# These are resampled UP from the hourly base, as the analysis layer always
# has. 'B' — business day; 'W-FRI' — week ending Friday; 'BME' — business
# month end.
RESAMPLE_RULES = {
    "daily":   "B",
    "weekly":  "W-FRI",
    "monthly": "BME",
}

VALID_TIMEFRAMES = set(TIMEFRAME_BAR_SIZE) | set(RESAMPLE_RULES)


def resample_ohlcv(df: DataFrame, rule: str) -> DataFrame:
    """Resample hourly OHLCV bars to a coarser timeframe.

    Expects a DataFrame with a 'timestamp' column and OHLCV columns. May
    optionally carry a 'stock_id' column, which will be preserved on the
    output. Returns a DataFrame with the same column shape, resampled and
    with 'timestamp' restored as a column (not the index). Empty input
    returns the input unchanged.
    """
    if df.empty:
        return df

    has_stock_id = "stock_id" in df.columns
    stock_id_value = df["stock_id"].iloc[0] if has_stock_id else None

    indexed = df.set_index("timestamp")
    resampled = indexed.resample(rule).agg(OHLCV_AGG)
    # Drop periods with no underlying data (weekends, holidays).
    resampled = resampled.dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()

    if has_stock_id and not resampled.empty:
        resampled.insert(0, "stock_id", stock_id_value)
    return resampled
