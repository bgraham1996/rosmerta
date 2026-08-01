"""
IB Flex Trades Fetcher — Historical executions into PostgreSQL
=============================================================
Pulls executed stock trades from Interactive Brokers' **Flex Web Service**
and persists them into the ``transaction`` + ``trade`` tables.

Why Flex (and not the Gateway)?
  IB's live ``reqExecutions`` API only returns roughly the last 7 days of
  fills. The Flex Web Service serves the full configured history of a saved
  *Flex Query* over HTTPS, so this fetcher needs **no** IB Gateway
  connection — only a Flex token, a Trades query id, and the DB.

Setup (one-time, in IB Account Management):
  1. Reports → Flex Queries → create an *Activity Flex Query* that includes
     the **Trades** section. Note its **Query ID**.
  2. Settings → enable the **Flex Web Service** and generate a **token**.
  3. Put both in ``.env``:
         IB_FLEX_TOKEN=...
         IB_FLEX_TRADES_QUERY_ID=...

Each IB ``Trade`` row maps to one ``transaction`` (type ``buy``/``sell``)
plus one ``trade`` row (stock_id, quantity, price, datetime). The Flex
``tradeID`` is stored in ``trade.ib_trade_id`` (see the
``add_trade_ib_id`` migration) so re-runs are idempotent.

Scope: stock (``STK``) trades only — the ``trade`` table is keyed to
``stocks``. Options/forex/other asset categories are skipped and logged.
"""

import logging
import time
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError
import xml.etree.ElementTree as et

import pandas as pd
import psycopg2
from ib_insync import FlexReport

# IB's Flex Web Service. ib_insync's FlexReport.download() hard-codes the old
# ``gdcdyn`` ("g" data center) host, which no longer resolves on some networks
# (it CNAMEs to Akamai but the local resolver may fail it), while the ``ndcdyn``
# mirror resolves fine. IB's SendRequest response also hands back a GetStatement
# URL on gdcdyn, so we drive the exchange ourselves and rewrite the host. See
# _download_flex_report below.
FLEX_HOST = "https://ndcdyn.interactivebrokers.com"
_FLEX_SEND_PATH = "/Universal/servlet/FlexStatementService.SendRequest"

# IB throttles repeat SendRequests for a query ("Statement could not be
# generated at this time. Please try again shortly.") for several minutes: a
# cold run succeeds at once, but a re-run soon after another request has to wait
# it out. These are the waits (seconds) between successive SendRequest attempts;
# attempts = len + 1 and total patience is their sum (~10 min here). A hard
# "too many failed attempts" lockout is never retried — that only deepens it.
_FLEX_SEND_BACKOFF = (15, 30, 60, 120, 180, 240)


class IBTradesFetcher:
    """
    Downloads a Trades Flex report from IB and stores stock executions
    into the ``transaction`` + ``trade`` tables.

    Mirrors ``IBStockDataFetcher``'s shape (DB connect, _get_or_create_stock,
    _save_*_to_db, context manager) but talks to the Flex Web Service over
    HTTPS instead of the IB Gateway.
    """

    def __init__(self, token, query_id, db_config=None):
        """
        Args:
            token:     IB Flex Web Service token.
            query_id:  Saved Trades Flex Query id.
            db_config: psycopg2 connection dict (see db_config.get_db_config).
                       If None, database features are disabled.
        """
        self.token = token
        self.query_id = query_id
        self.db_config = db_config
        self.db_conn = None
        self._stock_id_cache = {}

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

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

    def disconnect(self):
        """Close the DB connection if open."""
        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
                self.logger.info("Closed DB connection")
        except Exception as e:
            self.logger.error(f"Error closing DB connection: {e}")

    def _get_or_create_stock(self, symbol, exchange, currency, con_id=None):
        """
        Resolve a stock to its stock_id, matching by IB con_id first, then by
        symbol+exchange. Inserts a placeholder row if not found. Backfills
        ib_con_id on existing rows when we learn it. Returns the stock_id.
        """
        if con_id and con_id in self._stock_id_cache:
            return self._stock_id_cache[con_id]

        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return None

        with self.db_conn.cursor() as cur:
            # Prefer the IB contract id — it's stable across symbol changes.
            if con_id:
                cur.execute(
                    "SELECT stock_id FROM stocks WHERE ib_con_id = %s",
                    (con_id,),
                )
                row = cur.fetchone()
                if row:
                    self._stock_id_cache[con_id] = row[0]
                    return row[0]

            cur.execute(
                "SELECT stock_id, ib_con_id FROM stocks WHERE symbol = %s AND exchange = %s",
                (symbol, exchange),
            )
            row = cur.fetchone()
            if row:
                stock_id = row[0]
                # Backfill the con_id if we have it and the row is missing it.
                if con_id and row[1] is None:
                    cur.execute(
                        "UPDATE stocks SET ib_con_id = %s WHERE stock_id = %s",
                        (con_id, stock_id),
                    )
                    self.db_conn.commit()
                if con_id:
                    self._stock_id_cache[con_id] = stock_id
                return stock_id

            cur.execute(
                """INSERT INTO stocks (symbol, name, exchange, currency, ib_con_id)
                   VALUES (%s, %s, %s, %s, %s) RETURNING stock_id""",
                (symbol, symbol, exchange, currency, con_id),
            )
            stock_id = cur.fetchone()[0]
            self.db_conn.commit()
            self.logger.info(
                f"Created stock record: {symbol} ({exchange}) -> id {stock_id}"
            )
            if con_id:
                self._stock_id_cache[con_id] = stock_id
            return stock_id

    # ---------------------------------------------------------
    # Flex download
    # ---------------------------------------------------------
    def _download_flex_report(self):
        """
        Drive the two-step Flex Web Service exchange against ``FLEX_HOST`` and
        return a populated ``FlexReport``.

        Reimplements ib_insync's ``FlexReport.download`` so the request goes to
        the resolvable ``ndcdyn`` host (see FLEX_HOST note above) and so the
        GetStatement URL IB returns — which points back at ``gdcdyn`` — is
        rewritten to the same host. Retries the transient "Statement could not
        be generated at this time" throttle with backoff. Returns a FlexReport
        with ``root``/``data`` set, ready for ``topics()``/``df()``.
        """
        send_url = f"{FLEX_HOST}{_FLEX_SEND_PATH}?t={self.token}&q={self.query_id}&v=3"

        # Step 1: SendRequest → ReferenceCode + statement URL. The happy path
        # returns Success on the first call. IB throttles repeat requests for a
        # query with "could not be generated" and — if you keep hammering —
        # escalates to a "Too many failed attempts" token lockout that only more
        # waiting clears. So retry the soft throttle a couple of times with a
        # generous gap, but bail immediately on the lockout rather than deepen it.
        reference_code = base_url = None
        waits = _FLEX_SEND_BACKOFF
        attempts = len(waits) + 1
        for attempt in range(attempts):
            try:
                root = et.fromstring(urlopen(send_url, timeout=30).read())
                status = root.findtext("Status")
            except (URLError, TimeoutError, et.ParseError) as e:
                # Transient network/parse blip — treat like the soft throttle and retry.
                status, error_msg = None, f"network error: {e}"
            else:
                if status == "Success":
                    reference_code = root.findtext("ReferenceCode")
                    base_url = root.findtext("Url")
                    break
                error_msg = root.findtext("ErrorMessage") or root.findtext("ErrorCode") or "unknown error"
                if "too many" in error_msg.lower():
                    # A hard lockout, not the soft throttle — retrying only deepens it.
                    raise RuntimeError(
                        f"Flex token locked out by IB ({error_msg}). Wait ~15 min "
                        "without requesting, then retry."
                    )

            if attempt == attempts - 1:
                raise RuntimeError(
                    f"Flex SendRequest still failing after {attempts} attempts "
                    f"(~{sum(waits) // 60} min of retries): {error_msg}"
                )
            wait = waits[attempt]
            self.logger.warning(
                f"Flex SendRequest not ready ({error_msg}); attempt "
                f"{attempt + 1}/{attempts}, retrying in {wait}s"
            )
            time.sleep(wait)

        # IB returns a gdcdyn GetStatement URL; pin it to the resolvable host.
        base_url = base_url.replace("gdcdyn.interactivebrokers.com", "ndcdyn.interactivebrokers.com")

        # Step 2: poll GetStatement until the statement is ready.
        for _ in range(20):
            time.sleep(2)
            data = urlopen(f"{base_url}?q={reference_code}&t={self.token}").read()
            root = et.fromstring(data)
            if root.tag == "FlexQueryResponse":
                report = FlexReport()
                report.data = data
                report.root = root
                return report
            msg = (root[0].text if len(root) else root.text) or ""
            if "in progress" in msg or "generation in progress" in msg:
                continue
            raise RuntimeError(f"Flex GetStatement error: {msg}")
        raise RuntimeError("Flex GetStatement timed out waiting for the statement")

    def download_trades(self):
        """
        Download the Trades Flex report and return its ``Trade`` rows as a
        DataFrame (one row per execution). Returns None on failure or if the
        report has no Trade section.
        """
        try:
            report = self._download_flex_report()
        except Exception as e:
            self.logger.error(f"Flex download failed: {e}")
            return None

        topics = report.topics()
        if 'Trade' not in topics:
            self.logger.warning(
                f"No 'Trade' section in Flex report (topics: {sorted(topics)}). "
                "Check the query includes the Trades section."
            )
            return None

        df = report.df('Trade')
        if df is None or df.empty:
            self.logger.warning("Flex report contained no trades")
            return None

        self.logger.info(f"Downloaded {len(df)} trade rows from Flex")
        return df

    # ---------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------
    @staticmethod
    def _parse_dt(value):
        """Parse a Flex date/time value into a datetime, tolerant of formats.

        Flex emits dates/times in whatever format the query is configured for,
        commonly ``20240115``, ``20240115;103000`` or ``2024-01-15 10:30:00``.
        """
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).replace(';', ' ').strip()
        ts = pd.to_datetime(text, errors='coerce')
        if pd.isna(ts):
            ts = pd.to_datetime(text, format='%Y%m%d', errors='coerce')
        return None if pd.isna(ts) else ts.to_pydatetime()

    def _existing_trade_ids(self):
        """Return the set of ib_trade_id values already in the trade table."""
        with self.db_conn.cursor() as cur:
            cur.execute("SELECT ib_trade_id FROM trade WHERE ib_trade_id IS NOT NULL")
            return {row[0] for row in cur.fetchall()}

    def _save_trades_to_db(self, df):
        """
        Insert new stock trades as transaction + trade pairs.

        Skips rows whose ``tradeID`` is already stored (idempotent) and rows
        that aren't stock executions. Returns a summary dict with counts.
        """
        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return None

        existing = self._existing_trade_ids()
        inserted = skipped_dup = skipped_non_stock = errors = 0

        try:
            with self.db_conn.cursor() as cur:
                for row in df.itertuples(index=False):
                    asset_cat = str(getattr(row, 'assetCategory', '') or '').upper()
                    if asset_cat and asset_cat != 'STK':
                        skipped_non_stock += 1
                        continue

                    trade_id = str(getattr(row, 'tradeID', '') or '')
                    if trade_id and trade_id in existing:
                        skipped_dup += 1
                        continue

                    symbol = getattr(row, 'symbol', None)
                    buy_sell = str(getattr(row, 'buySell', '') or '').upper()
                    if not symbol or buy_sell not in ('BUY', 'SELL'):
                        self.logger.warning(
                            f"Skipping unusable trade row (symbol={symbol}, "
                            f"buySell={buy_sell}, tradeID={trade_id})"
                        )
                        errors += 1
                        continue

                    con_id = getattr(row, 'conid', None)
                    con_id = int(con_id) if con_id not in (None, '') else None
                    exchange = (
                        getattr(row, 'listingExchange', None)
                        or getattr(row, 'exchange', None)
                        or 'SMART'
                    )
                    currency = getattr(row, 'currency', None) or 'USD'

                    stock_id = self._get_or_create_stock(
                        symbol, exchange, currency, con_id=con_id
                    )
                    if not stock_id:
                        errors += 1
                        continue

                    trade_dt = self._parse_dt(
                        getattr(row, 'dateTime', None) or getattr(row, 'tradeDate', None)
                    )
                    # Some Trades queries emit only dateTime (no tradeDate column);
                    # fall back to the execution date so transaction_date isn't NULL.
                    trade_date = self._parse_dt(getattr(row, 'tradeDate', None)) or trade_dt
                    quantity = int(abs(float(getattr(row, 'quantity', 0) or 0)))
                    price = getattr(row, 'tradePrice', None)

                    # transaction (parent) → trade (stock leg)
                    cur.execute(
                        """INSERT INTO transaction (transaction_type, transaction_date)
                           VALUES (%s, %s) RETURNING transaction_id""",
                        (buy_sell.lower(),
                         trade_date.date() if trade_date else None),
                    )
                    transaction_id = cur.fetchone()[0]

                    cur.execute(
                        """INSERT INTO trade
                               (transaction_id, trade_type, stock_id, stock_quantity,
                                stock_price, trade_datetime, ib_trade_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (transaction_id, buy_sell, stock_id, quantity,
                         price, trade_dt, trade_id or None),
                    )
                    inserted += 1
                    if trade_id:
                        existing.add(trade_id)

            self.db_conn.commit()
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"Error saving trades to DB (rolled back): {e}")
            return None

        summary = {
            'inserted': inserted,
            'skipped_duplicate': skipped_dup,
            'skipped_non_stock': skipped_non_stock,
            'errors': errors,
        }
        self.logger.info(f"Trade ingest summary: {summary}")
        return summary

    def get_trades(self, start_date=None, end_date=None, save_to_db=True):
        """
        Download trades from Flex, optionally filter to a date window, and
        persist them.

        Args:
            start_date / end_date: optional 'YYYY-MM-DD' bounds applied to the
                trade date (the Flex query itself defines the overall period).
            save_to_db: persist to PostgreSQL when a db_config was provided.

        Returns:
            (DataFrame of trades, summary dict | None). The DataFrame is the
            filtered set of trades; the summary is the DB ingest result, or
            None when not saved.
        """
        df = self.download_trades()
        if df is None or df.empty:
            return None, None

        # Optional client-side date filtering on top of the query's period.
        if (start_date or end_date) and 'tradeDate' in df.columns:
            dates = df['tradeDate'].map(self._parse_dt)
            mask = pd.Series(True, index=df.index)
            if start_date:
                mask &= dates >= pd.to_datetime(start_date)
            if end_date:
                mask &= dates <= pd.to_datetime(end_date)
            df = df[mask].reset_index(drop=True)

        summary = None
        if save_to_db and self.db_config:
            if not self.db_conn or self.db_conn.closed:
                self.connect_db()
            summary = self._save_trades_to_db(df)

        return df, summary

    # ---------------------------------------------------------
    # Context Manager
    # ---------------------------------------------------------
    def __enter__(self):
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
def fetch_trades(token, query_id, start_date=None, end_date=None, db_config=None):
    """
    Quick one-shot Flex trades fetch without managing the class.

    Args:
        token:      IB Flex Web Service token.
        query_id:   Trades Flex Query id.
        start_date: optional 'YYYY-MM-DD' lower bound on trade date.
        end_date:   optional 'YYYY-MM-DD' upper bound on trade date.
        db_config:  PostgreSQL connection dict, or None to skip DB.

    Returns:
        (DataFrame, summary dict | None)
    """
    fetcher = IBTradesFetcher(token=token, query_id=query_id, db_config=db_config)
    try:
        return fetcher.get_trades(start_date, end_date)
    finally:
        fetcher.disconnect()


# =============================================================
# Example usage
# =============================================================
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from db_config import get_db_config

    load_dotenv()
    TOKEN = os.getenv('IB_FLEX_TOKEN')
    QUERY_ID = os.getenv('IB_FLEX_TRADES_QUERY_ID')
    DB_CONFIG = get_db_config()

    df, summary = fetch_trades(TOKEN, QUERY_ID, db_config=DB_CONFIG)
    if df is not None:
        print(f"\nTrades: {df.shape}")
        print(df.head())
        print(f"\nIngest: {summary}")
