"""
    this file will build a market object where
    each asset has 5 ema indicators added at different timeframes
"""


from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db
import pandas as pd
import numpy as np










EMA_WINDOWS = [9, 20, 50, 100, 200]


def main(start_date, end_date, stock_list='core'):
    db_config = get_db_config()
    connection = db.connect(**db_config)
    connection.autocommit = False

    with connection:
        market = Market(connection, start_date, end_date, stock_list)
        market.seed_assets(connection)
        market.populate_assets(connection)
        for window in EMA_WINDOWS:
            market.add_indicators(connection, Indicator('ema', window=window))

    return market
