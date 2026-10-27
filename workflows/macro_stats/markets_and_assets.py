
# this is a basic workflow for both testing the idea
# and getting a general idea of the different growth opportunities
# in this market
# this is not meant to be a screener but instead a historic info



import pandas as pd
from datetime import datetime as dt, timedelta as td
import psycopg2 as db

from data_models import *
from db_config import *
from indicators import *



db_config = get_db_config()
connection = db.connect(**db_config)
connection.autocommit = False 


# establish last 5 year timeframe
now = dt.now()
start = now - td(days=365 * 5)
start_date = start.strftime('%Y-%m-%d %H:%M:%S')
end_date = now.strftime('%Y-%m-%d %H:%M:%S')


# creating market
stock_list = 'core'

with connection as conn:
    market = Market(conn, start_date, end_date, stock_list)
    market.seed_assets(conn)
    market.populate_assets(conn)

    # market.get_growth(10d)
    # market.get_growth(20d)
    # market.get_growth(40d)
    # market.get_growth(60d)



