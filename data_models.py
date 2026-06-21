"""Analysis layer over data already stored in PostgreSQL.

This module is read-only with respect to the database: it loads price,
dividend, and fundamental rows that the ``price_retrival`` fetchers have
already persisted, and computes statistics, growth, volume-by-price levels, and
indicators on top of them. It never writes to the DB — keep that boundary.

Two classes:

- ``Asset`` — a single ticker over a date range and timeframe. Heavily
  lazy-cached: each ``get_*`` method computes on first call, stores the result
  in a ``self._*_cache`` field, and returns the cache thereafter; the matching
  ``clear_*`` methods reset a cache.
- ``Market`` — a collection of ``Asset``s drawn from a watchlist
  (``watchlist_members``), used to build cross-sectional panels and
  market-wide statistics.

Dates are passed as ``'%Y-%m-%d %H:%M:%S'`` strings and treated as UTC.
"""

from pandas import DataFrame, concat, cut, merge
from datetime import datetime as dt
from datetime import timezone
import numpy as np

from utils.bars import RESAMPLE_RULES, VALID_TIMEFRAMES, resample_ohlcv

class Asset:
    """Price/fundamental analysis for a single ticker over a date window.

    Loads data already present in the DB and derives stats, growth, indicators,
    and volume-by-price levels. All heavy results are lazily computed and cached
    on ``self._*_cache`` fields; call the relevant ``clear_*`` method to force
    recomputation (e.g. after changing the underlying price cache).

    Args:
        conn: Open psycopg2 connection (used to resolve ``stock_id``/``name``).
        ticker: Stock symbol; must exist in the ``stocks`` table.
        start_date / end_date: UTC bounds as ``'%Y-%m-%d %H:%M:%S'`` strings.
        timeframe: ``'hourly'`` (raw bars) or ``'daily'``/``'weekly'``
            (resampled from hourly via ``utils.bars``).

    Raises:
        ValueError: if ``ticker`` is not found in ``stocks``.
    """
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
        """Load (and cache) OHLCV bars for this asset's window.

        Reads ``price_hourly`` between ``start_date`` and ``end_date``. For
        ``daily``/``weekly`` timeframes the hourly bars are resampled via
        ``resample_ohlcv``. Returns the cached price DataFrame on later calls.
        """
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
        """Return (and cache) the list of bar timestamps.

        Requires ``get_prices`` to have been called first; raises ``ValueError``
        if no price cache exists.
        """
        if self._prices_cache is None:
            raise ValueError(f"No prices available for {self.asset_id}")
        else:
            self._dates_cache = self._prices_cache['timestamp'].to_list()
            return self._dates_cache

    def get_growth(self):
        """Per-bar close-to-close percent change.

        Returns (and caches) a DataFrame of ``timestamp`` + ``period_pct_change``.
        Requires ``get_prices`` to have been called first.
        """
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
        """Compute an ``Indicator`` over this asset and cache it by its key.

        ``source`` selects the input column(s) from the price frame (default
        ``'close'``). Returns the existing indicator if its key is already
        cached, otherwise computes and stores it. Look results up later with
        ``get_indicator``.
        """
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
        """Return a previously added indicator by name + params, or ``None``."""
        key = (name, tuple(sorted(params.items())))
        return self._indicators.get(key)

    def clear_price_cache(self):
        """Drop the cached price frame so the next ``get_prices`` re-queries."""
        self._prices_cache = None
        return True

    def calc_bar_avg_price(self):
        """Add an ``avg_price`` column = mean of OHLC per bar; return the Series."""
        if self._prices_cache is None:
            raise ValueError(f"No price data for {self.asset_id}")
        else:
            self._prices_cache['avg_price'] = (self._prices_cache['open'] + self._prices_cache['high'] + self._prices_cache['low'] + self._prices_cache['close']) / 4
            return self._prices_cache['avg_price']

    def asset_metadata(self):
        """Return a small dict identifying this asset (ticker/ids/timeframe)."""
        return {'ticker': self.ticker, 'stock_id': self.stock_id, 'asset_id': self.asset_id, 'timeframe': self.timeframe}

    def get_stats(self):
        """Summary statistics over the close price for the window.

        Returns (and caches) a dict with high/low/range, mean/median/std,
        first/last, absolute and percentage total change, and range percent.
        Requires a price cache; raises ``ValueError`` otherwise.
        """
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
        """Drop the cached stats dict so the next ``get_stats`` recomputes."""
        self._stats_cache = None
        return True

    def get_dividends(self, conn):
        """Load (and cache) dividend rows whose ex-date falls in the window."""
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
        """Load (and cache) net income by period_end from ``fundamentals``."""
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
        """Volume-by-price profile across ``split`` evenly spaced price bins.

        Bins each bar's ``avg_price`` between the window low and high, then
        aggregates ``volume`` (sum) and bar ``time`` (count) per bin, adding the
        bin midpoint ``price`` and a ``density`` (volume * time) column. Computes
        ``avg_price``/``stats`` first if needed. Returns (and caches) the
        resulting DataFrame indexed by price interval.
        """
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
    """A panel of ``Asset``s drawn from a named watchlist.

    Resolves the members of ``stock_list`` via ``watchlist_members`` at
    construction (storing ``{symbol: None}``), then builds the assets and
    cross-sectional views on demand. Typical workflow:

        market = Market(conn, start, end, stock_list='core')
        market.seed_assets(conn)       # build an Asset per symbol
        market.populate_assets(conn)   # load prices + growth into each
        market.get_panel(conn, 'close')        # symbol-by-timestamp matrix
        market.get_market_stats(conn, 'close') # avg/count/std across symbols

    Panels and market stats are cached per field in ``self._panels_cache`` /
    ``self._market_stats``; ``remove_panel(field)`` invalidates a panel.

    Args:
        conn: Open psycopg2 connection.
        start_date / end_date: UTC bounds as ``'%Y-%m-%d %H:%M:%S'`` strings.
        stock_list: Watchlist name in ``watchlist_members.list_name``.
        timeframe: Passed through to each ``Asset``.
    """
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
            'avg_price': None,
            'period_pct_change': None
        }
        self._market_stats = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': None,
            'avg_price': None,
            'period_pct_change': None
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
        """Instantiate an ``Asset`` for each watchlist symbol."""
        for symbol in list(self.assets.keys()):
            self.assets[symbol] = Asset(conn, symbol, self.start_date, self.end_date, self.timeframe)
        return True

    def populate_assets(self, conn):
        """Load prices and growth into every seeded asset."""
        for key, value in self.assets.items():
            value.get_prices(conn)
            value.get_growth()
        return True

    def get_growth(self, conn, clear_price_cache = False):
        """Compute per-asset growth across the panel.

        Loads prices then growth for each asset. If ``clear_price_cache`` is
        True, drops each asset's price cache afterwards to save memory.
        """
        for key, value in self.assets.items():
            value.get_prices(conn)
            value.get_growth()
            if clear_price_cache is True:
                value.clear_price_cache()
        return True

    def get_panel(self, conn, field = 'close'):
        """Build a cross-sectional panel for one field.

        Returns (and caches per field) a DataFrame with a ``timestamp`` column
        and one column per symbol, holding that symbol's ``field`` value at each
        timestamp (outer-aligned on the union of all assets' dates, left-merged
        so missing bars are NaN). ``field`` must be one of
        open/high/low/close/volume/avg_price.
        """
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
        """Invalidate the cached panel for ``field``."""
        self._panels_cache[field] = None
        return True

    def get_market_stats(self, conn, agg_option = 'close', clear_price_cache = False):
        """Cross-sectional stats across symbols for one field.

        Intended to reduce the ``agg_option`` panel to a per-timestamp summary
        of ``avg``/``count``/``std`` taken across all symbols, cached in
        ``self._market_stats[agg_option]``.

        Note: still under development — building this out is the current
        work-in-progress on ``Market`` (the panel must be populated first via
        ``get_panel``).
        """
        if self._panels_cache is None:
            self.get_panel(conn, 'agg_option')
        else:
            data = self._panels_cache[agg_option].copy()

            data['avg'] = data.mean(axis=1, numeric_only=True)
            data['count'] = data.count(axis=1, numeric_only=True)
            data['std'] = data.std(axis=1, numeric_only=True)

            cols = ['timestamp', 'avg', 'count', 'std']
            data = data[cols]
        self._market_stats[agg_option] = data
        return self._market_stats[agg_option]






