from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db
import pandas as pd

db_config = get_db_config()

connection = db.connect(**db_config)
connection.autocommit = False

stock_list = 'core'
start_date = '2020-01-01 00:00:00'
end_date = '2026-06-30 00:00:00'

# forward-looking gain horizons, in trading days (5, 10, ... 100)
OFFSET_DAYS = list(range(5, 101, 5))

# hourly market -> 8 bars per trading day (see Asset.get_offset_growth)
BARS_PER_DAY = 8

with connection as conn:
    core_stocks = Market(conn, start_date, end_date, stock_list)
    core_stocks.seed_assets(conn)
    core_stocks.populate_assets(conn)

    for days in OFFSET_DAYS:
        growth = Indicator('days_offset_gain', days_ahead=days, bars_per_day=BARS_PER_DAY)
        core_stocks.add_indicators(conn, growth, source=['timestamp', 'close'])
