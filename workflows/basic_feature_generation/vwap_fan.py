"""VWAP fan: rolling volume-weighted average price at several lookbacks.

The volume-weighted counterpart to ``daily_ema_fan``. For every asset in the
market it attaches a *fan* of rolling VWAP indicators — one per window in
``VWAP_WINDOWS`` — so each asset carries short-through-long volume-weighted price
lines side by side. Because VWAP weights price by traded volume, a fan of them
shows where the bulk of trading has clustered over progressively longer horizons
(short windows track recent value, long windows the broader accepted range).

Unlike the EMA fan, VWAP needs the full OHLCV bar (it uses the typical price
``(close + high + low) / 3`` weighted by ``volume``), so the indicator is applied
with a multi-column ``source`` rather than the default ``'close'``.

Result
------
``main`` returns the ``Market``; each asset has the fan cached, looked up by
name + window:

    market = main('2024-01-01 00:00:00', '2025-01-01 00:00:00')
    vwap50 = market.assets['AAPL'].get_indicator('vwap', window=50)._result
"""


from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db
import pandas as pd
import numpy as np


# Rolling VWAP lookbacks (in bars of the market timeframe), mirroring the EMA
# fan's window set so the two fans line up horizon-for-horizon.
VWAP_WINDOWS = [9, 20, 50, 100, 200]

# VWAP is computed over the OHLCV bar, not just the close.
VWAP_SOURCE = ['close', 'high', 'low', 'volume']


def main(start_date, end_date, stock_list='core'):
    db_config = get_db_config()
    connection = db.connect(**db_config)
    connection.autocommit = False

    with connection:
        market = Market(connection, start_date, end_date, stock_list)
        market.seed_assets(connection)
        market.populate_assets(connection)
        for window in VWAP_WINDOWS:
            market.add_indicators(
                connection,
                Indicator('vwap', window=window),
                source=VWAP_SOURCE,
            )

    return market
