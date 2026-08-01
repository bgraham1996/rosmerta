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

with connection as conn:
    core_stocks = Market(conn, start_date, end_date, stock_list)
    core_stocks.seed_assets(conn)
    market.populate_assets(conn)

d5_growth = Indicator('days_offset_gain', days=5)
d10_growth = Indicator('days_offset_gain', days=10)
d15_growth = Indicator('days_offset_gain', days=15)
d20_growth = Indicator('days_offset_gain', days=20)
d25_growth = Indicator('days_offset_gain', days=25)
d30_growth = Indicator('days_offset_gain', days=30)
d35_growth = Indicator('days_offset_gain', days=35)
d40_growth = Indicator('days_offset_gain', days=40)
d45_growth = Indicator('days_offset_gain', days=45)
d50_growth = Indicator('days_offset_gain', days=50)
d55_growth = Indicator('days_offset_gain', days=55)
d60_growth = Indicator('days_offset_gain', days=60)
d65_growth = Indicator('days_offset_gain', days=65)
d70_growth = Indicator('days_offset_gain', days=70)
d75_growth = Indicator('days_offset_gain', days=75)
d80_growth = Indicator('days_offset_gain', days=80)
d85_growth = Indicator('days_offset_gain', days=85)
d90_growth = Indicator('days_offset_gain', days=90)
d95_growth = Indicator('days_offset_gain', days=95)
d100_growth = Indicator('days_offset_gain', days=100)

with connection as conn:
    core_stocks.add_indicators(d5_growth, conn)
    core_stocks.add_indicators(d10_growth, conn)
    core_stocks.add_indicators(d15_growth, conn)
    core_stocks.add_indicators(d20_growth, conn)
    core_stocks.add_indicators(d25_growth, conn)
    core_stocks.add_indicators(d30_growth, conn)
    core_stocks.add_indicators(d35_growth, conn)
    core_stocks.add_indicators(d40_growth, conn)
    core_stocks.add_indicators(d45_growth, conn)
    core_stocks.add_indicators(d50_growth, conn)
    core_stocks.add_indicators(d55_growth, conn)
    core_stocks.add_indicators(d60_growth, conn)
    core_stocks.add_indicators(d65_growth, conn)
    core_stocks.add_indicators(d70_growth, conn)
    core_stocks.add_indicators(d75_growth, conn)
    core_stocks.add_indicators(d80_growth, conn)
    core_stocks.add_indicators(d85_growth, conn)
    core_stocks.add_indicators(d90_growth, conn)
    core_stocks.add_indicators(d95_growth, conn)
    core_stocks.add_indicators(d100_growth, conn)


