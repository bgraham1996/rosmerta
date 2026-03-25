"""
IB Stock Data Fetcher - Hourly Bars with PostgreSQL Integration
===============================================================
Fetches hourly OHLCV data from Interactive Brokers and stores
it in the PostgreSQL database on the Ubuntu server.

Key changes from the original 1-minute bar fetcher:
  - Bar size changed to '1 hour'
  - Chunking increased to 30 days per request (IB allows up to 365 days for hourly)
  - Added PostgreSQL integration for direct database inserts
  - Currency parameter added to support future multi-market expansion
  - Default port set to 4001 (IB Gateway) since gateway runs on the Mac
"""

import pandas as pd
from datetime import datetime, timedelta
from ib_insync import *
import logging
import nest_asyncio
import psycopg2
from psycopg2.extras import execute_values

# Allow nested event loops (needed if running from Jupyter/IPython)
nest_asyncio.apply()


class IBStockDataFetcher:
    """
    Fetches hourly OHLCV bar data from Interactive Brokers
    and optionally stores it directly into PostgreSQL.
    """

    def __init__(self, host='127.0.0.1', port=4001, client_id=1, db_config=None):
        """
        Args:
            host:       IB Gateway host (Mac Mini IP if running from server).
            port:       4001 = IB Gateway paper, 4000 = live.
            client_id:  Unique client ID for this connection.
            db_config:  Dict with PostgreSQL connection params, e.g.:
                        {'host': '192.168.x.x', 'port': 5432,
                         'dbname': 'stocks', 'user': 'stock_user',
                         'password': '...'}
                        If None, database features are disabled.
        """
        self.host = host
        self.port = port
        self.client_id = client_id
        self.db_config = db_config
        self.ib = None
        self.db_conn = None

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self._initialize_ib()

    # ---------------------------------------------------------
    # IB Connection
    # ---------------------------------------------------------
    def _initialize_ib(self):
        """Initialize IB instance with proper event loop handling."""
        try:
            get_ipython()
            self.ib = IB()
            self.ib.RaiseRequestErrors = True
        except NameError:
            self.ib = IB()

    def connect(self):
        """Connect to IB Gateway/TWS. Returns True on success."""
        try:
            if self.ib is None:
                self._initialize_ib()

            if not self.ib.isConnected():
                self.ib.connect(self.host, self.port,
                                clientId=self.client_id, timeout=10)
                self.logger.info(f"Connected to IB on {self.host}:{self.port}")
                self.ib.sleep(1)
                return True
            else:
                self.logger.info("Already connected to IB")
                return True
        except Exception as e:
            self.logger.error(f"Failed to connect to IB: {e}")
            try:
                if self.ib:
                    self.ib.disconnect()
            except Exception:
                pass
            return False

    def disconnect(self):
        """Disconnect from IB and close DB connection."""
        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
                self.logger.info("Disconnected from IB")
        except Exception as e:
            self.logger.error(f"Error during IB disconnect: {e}")

        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
                self.logger.info("Closed DB connection")
        except Exception as e:
            self.logger.error(f"Error closing DB connection: {e}")

    # ---------------------------------------------------------
    # Database Connection
    # ---------------------------------------------------------
    def connect_db(self):
        """Establish PostgreSQL connection. Returns True on success."""
        if not self.db_config:
            self.logger.warning("No db_config provided — database features disabled")
            return False
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            self.db_conn.autocommit = False
            self.logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def _get_or_create_stock(self, symbol, exchange, currency):
        """
        Look up a stock by symbol+exchange. If it doesn't exist,
        insert a placeholder row. Returns the stock_id.
        """
        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return None

        with self.db_conn.cursor() as cur:
            cur.execute(
                "SELECT stock_id FROM stocks WHERE symbol = %s AND exchange = %s",
                (symbol, exchange)
            )
            row = cur.fetchone()
            if row:
                return row[0]

            # Insert new stock (name can be updated later)
            cur.execute(
                """INSERT INTO stocks (symbol, name, exchange, currency)
                   VALUES (%s, %s, %s, %s) RETURNING stock_id""",
                (symbol, symbol, exchange, currency)
            )
            stock_id = cur.fetchone()[0]
            self.db_conn.commit()
            self.logger.info(f"Created stock record: {symbol} ({exchange}) -> id {stock_id}")
            return stock_id

    def _save_bars_to_db(self, stock_id, df):
        """
        Upsert hourly bars into the price_hourly table.
        Uses ON CONFLICT to handle re-fetches gracefully.
        """
        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return False

        records = [
            (stock_id, row.date, row.open, row.high, row.low, row.close, int(row.volume))
            for row in df.itertuples()
        ]

        insert_sql = """
            INSERT INTO price_hourly (stock_id, timestamp, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (stock_id, timestamp)
            DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
        """

        try:
            with self.db_conn.cursor() as cur:
                execute_values(cur, insert_sql, records)
            self.db_conn.commit()
            self.logger.info(f"Saved {len(records)} bars to database for stock_id={stock_id}")
            return True
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"Error saving bars to DB: {e}")
            return False

    def _log_fetch(self, stock_id, data_type, fetch_start, fetch_end,
                   bars_fetched, status='success', error_message=None):
        """Record a fetch operation in the fetch_log table."""
        if not self.db_conn or self.db_conn.closed:
            return

        try:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO fetch_log
                       (stock_id, data_type, fetch_start, fetch_end,
                        bars_fetched, status, error_message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (stock_id, data_type, fetch_start, fetch_end,
                     bars_fetched, status, error_message)
                )
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging fetch: {e}")
            self.db_conn.rollback()

    # ---------------------------------------------------------
    # Data Fetching
    # ---------------------------------------------------------
    def get_hourly_bars(self, ticker, start_date, end_date,
                        exchange='SMART', currency='USD',
                        save_to_db=True):
        """
        Fetch hourly OHLCV bar data for a stock.

        Args:
            ticker:     Stock ticker symbol (e.g. 'AAPL', 'MSFT').
            start_date: Start date as 'YYYY-MM-DD' string or datetime.
            end_date:   End date as 'YYYY-MM-DD' string or datetime.
            exchange:   IB exchange routing. Default 'SMART'.
            currency:   Contract currency. Default 'USD'.
            save_to_db: If True and db_config was provided, save bars
                        directly to PostgreSQL.

        Returns:
            pd.DataFrame with columns: date, open, high, low, close, volume.
            None on error.
        """
        try:
            # Ensure IB connection
            if not self.ib.isConnected():
                if not self.connect():
                    return None

            # Parse dates
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')

            # Create and qualify the contract
            contract = Stock(ticker, exchange, currency)
            qualified = self.ib.qualifyContracts(contract)
            if not qualified:
                self.logger.error(f"Could not qualify contract for {ticker}")
                return None
            contract = qualified[0]

            # Fetch in chunks — IB allows up to 365 days for hourly bars,
            # but 30-day chunks are more reliable and kinder to rate limits.
            all_bars = []
            current_end = end_date

            while current_end > start_date:
                request_days = min(30, (current_end - start_date).days + 1)
                duration_str = f"{request_days} D"

                self.logger.info(
                    f"Fetching {ticker} hourly bars: "
                    f"chunk ending {current_end.strftime('%Y%m%d %H:%M:%S')}, "
                    f"duration {duration_str}"
                )

                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime=current_end.strftime('%Y%m%d %H:%M:%S'),
                    durationStr=duration_str,
                    barSizeSetting='1 hour',
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1,
                    keepUpToDate=False,
                    timeout=60
                )

                if bars:
                    all_bars.extend(bars)
                    # Move window back past the earliest bar we just received
                    current_end = bars[0].date.replace(tzinfo=None) - timedelta(hours=1)
                else:
                    self.logger.warning(f"No data for period ending {current_end}")
                    break

                # Rate-limit courtesy
                self.ib.sleep(1)

                if current_end <= start_date:
                    break

            if not all_bars:
                self.logger.warning(f"No data retrieved for {ticker}")
                return None

            # Build DataFrame
            df = util.df(all_bars)
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
            df['date'] = pd.to_datetime(df['date'])

            # Filter to requested range
            mask = (df['date'].dt.date >= start_date.date()) & \
                   (df['date'].dt.date <= end_date.date())
            df = df[mask].sort_values('date').reset_index(drop=True)

            self.logger.info(f"Retrieved {len(df)} hourly bars for {ticker}")

            # Persist to database if configured
            if save_to_db and self.db_config:
                if not self.db_conn or self.db_conn.closed:
                    self.connect_db()

                stock_id = self._get_or_create_stock(
                    ticker, contract.exchange, currency
                )
                if stock_id:
                    self._save_bars_to_db(stock_id, df)
                    self._log_fetch(
                        stock_id, 'price_hourly',
                        start_date, end_date,
                        len(df), 'success'
                    )

            return df

        except Exception as e:
            self.logger.error(f"Error fetching data for {ticker}: {e}")
            return None

    # ---------------------------------------------------------
    # Context Manager
    # ---------------------------------------------------------
    def __enter__(self):
        self.connect()
        if self.db_config:
            self.connect_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass


# =============================================================
# Convenience function
# =============================================================
def fetch_hourly_bars(ticker, start_date, end_date,
                      ib_host='127.0.0.1', ib_port=4001,
                      db_config=None):
    """
    Quick function to fetch hourly bars without managing the class.

    Args:
        ticker:     Stock symbol
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'
        ib_host:    IB Gateway host (Mac Mini IP from server's perspective)
        ib_port:    4001 for paper, 4000 for live
        db_config:  PostgreSQL connection dict, or None to skip DB

    Returns:
        pd.DataFrame or None
    """
    fetcher = IBStockDataFetcher(
        host=ib_host, port=ib_port, db_config=db_config
    )
    try:
        return fetcher.get_hourly_bars(ticker, start_date, end_date)
    finally:
        fetcher.disconnect()


# =============================================================
# Example usage
# =============================================================
if __name__ == "__main__":
    # -- Configuration --
    # IB Gateway runs on Mac Mini; adjust IP if running from server
    IB_HOST = '127.0.0.1'  # or Mac Mini's IP if running from Ubuntu
    IB_PORT = 4001          # Gateway paper trading

    # PostgreSQL on the Ubuntu server
    from db_config import get_db_config
    DB_CONFIG = get_db_config()

    # -- Fetch and store --
    with IBStockDataFetcher(host=IB_HOST, port=IB_PORT,
                            db_config=DB_CONFIG) as fetcher:

        df = fetcher.get_hourly_bars(
            ticker='AAPL',
            start_date='2024-01-02',
            end_date='2024-03-31',
        )

        if df is not None:
            print(f"\nShape: {df.shape}")
            print(f"\nFirst rows:\n{df.head()}")
            print(f"\nLast rows:\n{df.tail()}")
