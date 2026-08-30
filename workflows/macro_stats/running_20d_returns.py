"""Running 20-trading-day forward returns.

For every hourly bar, look 20 market-open days ahead at the SAME bar time and
record both the absolute price change and the percentage change. Weekends and
holidays are skipped because they have no bars; trading days are counted from
the data itself (see ``trading_days_offset_gain`` in ``indicators.py``). Only
exact same-time matches count -- if the 20-days-ahead same-time bar is missing
(tail of the series, DST boundary, or a data gap) the value is NaN.

The indicator is attached to every asset in the market and produces the two
series requested -- absolute gain/loss and % change -- as the ``abs`` and
``pct`` columns of one DataFrame:

    market = main('2024-01-01 00:00:00', '2025-01-01 00:00:00')
    ind = market.assets['AAPL'].get_indicator('trading_days_offset_gain', days_ahead=20)
    returns = ind._result        # DataFrame with 'abs' and 'pct' columns
    abs_series = returns['abs']
    pct_series = returns['pct']
"""

from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db


LOOKAHEAD_DAYS = 20


def main(start_date, end_date, stock_list='core'):
    connection = db.connect(**get_db_config())
    connection.autocommit = False

    with connection as conn:
        market = Market(conn, start_date, end_date, stock_list)
        market.seed_assets(conn)
        market.populate_assets(conn)

        offset = Indicator('trading_days_offset_gain', days_ahead=LOOKAHEAD_DAYS)
        market.add_indicators(conn, offset, source=['timestamp', 'close'])

    return market
