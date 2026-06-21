from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

stock_list = 'dividends'

db_config = get_db_config()

conn = db.connect(**db_config)
conn.autocommit = False

start_date = '2024-01-01 00:00:00'
end_date = '2026-03-01 00:00:00'

with conn:
    market = Market(conn, start_date, end_date, stock_list)
    market.seed_assets(conn)
    market.populate_assets(conn)
    market.get_panel(conn, 'close')
    print(market.get_panel(conn, 'close').head(5))

    market.get_market_stats(conn)
    print(market._market_stats['close'].head(5))


with conn:
    market.get_growth(conn)
    market.get_panel(conn, 'volume')
    market.get_market_stats(conn, agg_option='volume')
    print(market._market_stats['volume'].head(5))
