

from pandas import DataFrame, concat, cut, merge
from datetime import datetime as dt
from datetime import timezone
import numpy as np

from utils.bars import RESAMPLE_RULES, VALID_TIMEFRAMES, resample_ohlcv

class Asset:
    def __init__(self, conn, ticker, start_date, end_date, timeframe = 'hourly'):
        self.ticker = ticker
        self.timeframe = timeframe
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_id, name FROM stocks WHERE symbol = %s",
                (ticker,)
            )
            row = cur.fetchone()
        if row is None: 
            raise ValueError(f"No ticker for {ticker}")
        else:
            self.stock_id = row[0]
            self.name = row[1]
            self.start_date = start_date
            self.end_date = end_date
            self.asset_id = f"{ticker}-{start_date}-{end_date}-{timeframe}"
            self._prices_cache = None
            self._indicators = {}
            self._stats_cache = None
            self._dividends_cache = None
            self._net_income_cache = None
            self._dates_cache = None
            self._growth_cache = None
            self._levels_cache = None
            print(f"Asset Object {self.asset_id} initialised")

    def get_prices(self, conn):
        if self._prices_cache is None:
            start = dt.strptime(self.start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            end = dt.strptime(self.end_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT stock_id, timestamp, open, high, low, close, volume "
                    "FROM price_hourly WHERE stock_id = %s AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp ASC",
                    (self.stock_id, start, end)
                )
                data = cur.fetchall()
            columns=['stock_id', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            data = DataFrame(data, columns=columns)
            for col in ['open', 'close', 'high', 'low', 'volume']:
                data[col] = data[col].astype(float)
            if self.timeframe == 'hourly':
                self._prices_cache = data
            elif self.timeframe in ['daily', 'weekly']:
                rule = RESAMPLE_RULES[self.timeframe]
                self._prices_cache = resample_ohlcv(data, rule)
            else:
                raise ValueError(f"None applicable timeframe aggregation applied to {self.asset_id}")
            return self._prices_cache
        else:
            return self._prices_cache

    def get_dates(self):
        if self._prices_cache is None:
            raise ValueError(f"No prices available for {self.asset_id}")
        else:
            self._dates_cache = self._prices_cache['timestamp'].to_list()
            return self._dates_cache

    def get_growth(self):
        if self._prices_cache is None:
            raise ValueError(f"No prices available for {self.asset_id}")
        else:
            data = self._prices_cache.copy()
            data['period_pct_change'] = data['close'].pct_change()
            data2 = DataFrame()
            data2['timestamp'] = data['timestamp']
            data2['period_pct_change'] = data['period_pct_change']
            self._growth_cache = data2
            return data2

    def add_indicator(self, indicator, conn, source='close'):
        prices = self.get_prices(conn)
        if indicator.key in self._indicators:
            return self._indicators[indicator.key]
        if isinstance(source, list):
            input_data = prices[source]
        else:
            input_data = prices[source]
        indicator.compute(input_data)
        self._indicators[indicator.key] = indicator
        return indicator

    def get_indicator(self, name, **params):
        key = (name, tuple(sorted(params.items())))
        return self._indicators.get(key)

    def clear_price_cache(self):
        self._prices_cache = None
        return True

    def calc_bar_avg_price(self):
        if self._prices_cache is None:
            raise ValueError(f"No price data for {self.asset_id}")
        else:
            self._prices_cache['avg_price'] = (self._prices_cache['open'] + self._prices_cache['high'] + self._prices_cache['low'] + self._prices_cache['close']) / 4
            return self._prices_cache['avg_price']

    def asset_metadata(self):
        return {'ticker': self.ticker, 'stock_id': self.stock_id, 'asset_id': self.asset_id, 'timeframe': self.timeframe}

    def get_stats(self):
        if self._stats_cache is None:
            if self._prices_cache is None:
                raise ValueError(f"No price data for {self.asset_id}")
            else:
                high = max(self._prices_cache['close'])
                low = min(self._prices_cache['close'])
                total_range = high - low
                mean = float(self._prices_cache['close'].mean())
                median = float(self._prices_cache['close'].median())
                std = float(self._prices_cache['close'].std())
                first = self._prices_cache['close'].iloc[0]
                last = self._prices_cache['close'].iloc[-1]
                total_change = last - first
                range_pct = total_range/low *100
                total_change_pct = ((last/first)-1)*100
            stats = {'high': high, 'low': low, 'range': total_range,
                     'mean': mean, 'median': median, 'std': std,
                     'first': first, 'last': last, 'total_change': total_change,
                     'range_pct': range_pct, 'total_change_pct': total_change_pct}
            self._stats_cache = stats
            return stats
        else:
            return self._stats_cache
    
    def clear_stats(self):
        self._stats_cache = None
        return True

    def get_dividends(self, conn):
        if self._dividends_cache is None:
            start = dt.strptime(self.start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            end = dt.strptime(self.end_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT dividend_id, ex_date, pay_date, declaration_date, dividend_type, amount "
                    "FROM dividends "
                    "WHERE stock_id = %s AND ex_date >= %s AND ex_date <= %s",
                    (self.stock_id, start, end)
                )
                data = cur.fetchall()
            cols = ['dividend_id', 'ex_date', 'pay_date', 'declaration_date', 'dividend_type', 'amount']
            data = DataFrame(data, columns=cols)
            self._dividends_cache = data
            return data
        else:
            return self._dividends_cache

    def get_net_income(self, conn):
        if self._net_income_cache is None:
            start = dt.strptime(self.start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            end = dt.strptime(self.end_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fundamental_id, period_end, net_income "
                    "FROM fundamentals "
                    "WHERE stock_id = %s AND period_end >= %s and period_end <= %s",
                    (self.stock_id, start, end)
                )
                data = cur.fetchall()
            cols = ['fundamental_id', 'period_end', 'net_income']
            data = DataFrame(data, columns = cols)
            self._net_income_cache = data
            return data
        else:
            return self._net_income_cache

    def get_price_levels(self, split = 1000):
        if self._levels_cache is None:
            if self._prices_cache is None:
                raise ValueError(f"No Price Data for {self.asset_id}")
            else:
                if 'avg_price' not in self._prices_cache.columns:
                    self.calc_bar_avg_price()
                remove_stats = False
                if self._stats_cache is None:
                    self.get_stats()

                low = self._stats_cache['low']
                high = self._stats_cache['high']

                edges = np.linspace(low, high, split + 1)
                bins = cut(self._prices_cache['avg_price'], bins = edges, include_lowest = True)
                grouped = self._prices_cache.groupby(bins, observed = False)['volume'].agg(
                    volume = 'sum',
                    time = 'count'
                )
                grouped['price'] = [interval.mid for interval in grouped.index]
                grouped['density'] = grouped['volume'] * grouped['time']
                self._levels_cache = grouped
                return grouped
        else:
            return self._levels_cache
                

class Market:
    def __init__(self, conn, start_date, end_date, stock_list = 'core', timeframe = 'hourly'):
        self.timeframe = timeframe
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        self.assets = {}
        self.market_id = f"{stock_list}-{start_date}-{end_date}-{timeframe}"
        self._panels_cache = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': None,
            'avg_price': None
        }
        with conn.cursor() as cur:
            cur.execute(
                "select s.symbol "
                "FROM watchlist_members wm "
                "JOIN stocks s ON s.stock_id = wm.stock_id "
                "WHERE wm.list_name = %s",
                (self.stock_list,)
            )
            symbol_list = cur.fetchall()
        symbol_list = [row[0] for row in symbol_list]
        for symbol in symbol_list:
            self.assets[symbol] = None

    def seed_assets(self, conn):
        for symbol in list(self.assets.keys()):
            self.assets[symbol] = Asset(conn, symbol, self.start_date, self.end_date, self.timeframe)
        return True

    def populate_assets(self, conn):
        for key, value in self.assets.items():
            value.get_prices(conn)
        return True

    def get_growth(self, conn, clear_price_cache = False):
        for key, value in self.assets.items():
            value.get_prices(conn)
            value.get_growth()
            if clear_price_cache is True:
                value.clear_price_cache()
        return True

    def get_panel(self, conn, field = 'close'):
        cols = ['open', 'high', 'low', 'close', 'volume', 'avg_price']
        if field not in cols:
            raise ValueError(f"Invalid field: {field} for market object: {self.market_id}")
        else:
            if self._panels_cache[field] is None:
                dates = set()
                for symbol in self.assets:
                    dates.update(self.assets[symbol].get_dates())
                panel = DataFrame({'timestamp': sorted(dates)})
                keep_cols = ['timestamp', field]
                for symbol in list(self.assets.keys()):
                    data = self.assets[symbol].get_prices(conn)
                    data = data[keep_cols]
                    data = data.rename(columns={field: symbol})
                    panel = merge(panel, data, on = 'timestamp', how = 'left')
                self._panels_cache[field] = panel
                return self._panels_cache[field]
            else: 
                return self._panels_cache[field]
                   
    def remove_panel(self, field):
        self._panels_cache[field] = None
        return True

    def get_market_averages(self, conn, clear_price_cache = False):
        for symbol in list(self.assets.keys()):
            print('method not implemted yet')
        return True









