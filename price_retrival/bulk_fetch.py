"""
Bulk fetch logic for Rosmerta.

Provides watchlist resolution and batch fetching for prices and fundamentals.
No CLI or display dependencies — pure data operations.
"""

import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """Result of a single ticker fetch."""
    symbol: str
    status: str       # 'success', 'empty', 'error', 'skipped'
    records: int      # bars for price, periods for fundamentals
    error: str | None = None


def get_watchlist_tickers(conn, list_name):
    """
    Fetch all active tickers for a given watchlist name.

    Returns list of dicts with stock_id, symbol, exchange, currency.
    Returns None if the list doesn't exist.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM watchlist_members WHERE list_name = %s",
            (list_name,)
        )
        if cur.fetchone()[0] == 0:
            return None

        cur.execute(
            """SELECT s.stock_id, s.symbol, s.exchange, s.currency
               FROM watchlist_members wm
               JOIN stocks s ON s.stock_id = wm.stock_id
               WHERE wm.list_name = %s
                 AND s.is_active = TRUE
               ORDER BY s.symbol""",
            (list_name,)
        )
        columns = ['stock_id', 'symbol', 'exchange', 'currency']
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_available_lists(conn):
    """Return list of (list_name, member_count) tuples."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT wm.list_name, COUNT(*) as member_count
               FROM watchlist_members wm
               JOIN stocks s ON s.stock_id = wm.stock_id
               WHERE s.is_active = TRUE
               GROUP BY wm.list_name
               ORDER BY wm.list_name"""
        )
        return cur.fetchall()


def get_last_fetch_times(conn, symbols, data_type='fundamentals'):
    """
    Query fetch_log for the most recent successful fetch per ticker.

    Does a single query for all symbols (efficient for large watchlists).

    Args:
        conn: psycopg2 connection
        symbols: list of ticker symbol strings
        data_type: fetch_log data_type to check (default: 'fundamentals')

    Returns:
        dict: {symbol: fetched_at_datetime} for tickers with a successful fetch.
              Tickers with no successful fetch are absent from the dict.
    """
    if not symbols:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """SELECT s.symbol, MAX(fl.fetched_at) as last_fetch
               FROM fetch_log fl
               JOIN stocks s ON s.stock_id = fl.stock_id
               WHERE s.symbol = ANY(%s)
                 AND fl.data_type = %s
                 AND fl.status = 'success'
               GROUP BY s.symbol""",
            (symbols, data_type)
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def bulk_fetch_prices(
    tickers,
    start,
    end,
    db_config,
    ib_host='127.0.0.1',
    ib_port=4001,
    exchange='SMART',
    delay=2,
    on_ticker_start=None,
    on_ticker_complete=None,
):
    """
    Fetch hourly price bars for a list of tickers.

    Args:
        tickers: list of dicts with 'symbol', 'exchange', 'currency' keys
        start: start date string (YYYY-MM-DD)
        end: end date string (YYYY-MM-DD)
        db_config: database config dict
        ib_host: IB Gateway host
        ib_port: IB Gateway port
        exchange: IB exchange routing
        delay: seconds between tickers
        on_ticker_start: optional callback(index, symbol) called before each fetch
        on_ticker_complete: optional callback(index, result) called after each fetch

    Returns:
        list of FetchResult
    """
    from price_retrival.ib_api import IBStockDataFetcher

    results = []

    for i, ticker_info in enumerate(tickers):
        symbol = ticker_info['symbol']
        currency = ticker_info['currency']

        if on_ticker_start:
            on_ticker_start(i, symbol)

        result = _fetch_single_ticker(
            symbol=symbol,
            currency=currency,
            start=start,
            end=end,
            db_config=db_config,
            ib_host=ib_host,
            ib_port=ib_port,
            exchange=exchange,
        )
        results.append(result)

        if on_ticker_complete:
            on_ticker_complete(i, result)

        # Delay between tickers to avoid IB rate limits
        if i < len(tickers) - 1:
            time.sleep(delay)

    return results


def _fetch_single_ticker(symbol, currency, start, end, db_config,
                         ib_host, ib_port, exchange):
    """Fetch a single ticker with its own IB connection. Returns FetchResult."""
    from price_retrival.ib_api import IBStockDataFetcher

    try:
        with IBStockDataFetcher(
            host=ib_host,
            port=ib_port,
            db_config=db_config,
        ) as fetcher:
            df = fetcher.get_hourly_bars(
                ticker=symbol,
                start_date=start,
                end_date=end,
                exchange=exchange,
                currency=currency,
                save_to_db=True,
            )

        if df is not None and not df.empty:
            return FetchResult(symbol=symbol, status='success', records=len(df))
        else:
            return FetchResult(symbol=symbol, status='empty', records=0,
                               error='No data returned')

    except Exception as e:
        logger.error(f"Failed to fetch {symbol}: {e}")
        return FetchResult(symbol=symbol, status='error', records=0, error=str(e))


# =============================================================
# Bulk Fundamentals Fetch
# =============================================================

def bulk_fetch_fundamentals(
    tickers,
    db_config,
    user_agent,
    start=None,
    end=None,
    max_age_days=30,
    force=False,
    on_ticker_start=None,
    on_ticker_complete=None,
):
    """
    Fetch fundamental data from SEC EDGAR for a list of tickers.

    Uses a single EdgarFundamentalsFetcher instance for the whole batch
    since EDGAR is HTTP-based (no persistent connection like IB Gateway).

    Supports incremental fetching: tickers with a successful fetch within
    `max_age_days` are skipped unless `force` is True.

    Args:
        tickers: list of dicts with 'symbol' key (from get_watchlist_tickers)
        db_config: database config dict
        user_agent: SEC-required user agent string
        start: start date string (YYYY-MM-DD) or None for all available
        end: end date string (YYYY-MM-DD) or None for up to present
        max_age_days: skip tickers fetched within this many days (default 30)
        force: if True, fetch all tickers regardless of freshness
        on_ticker_start: optional callback(index, symbol) called before each fetch
        on_ticker_complete: optional callback(index, result) called after each fetch

    Returns:
        list of FetchResult
    """
    from price_retrival.edgar_api import EdgarFundamentalsFetcher
    import psycopg2

    results = []

    # --- Freshness check (single upfront query) ---
    skip_symbols = set()
    if not force and max_age_days > 0:
        try:
            conn = psycopg2.connect(**db_config)
            try:
                symbols = [t['symbol'] for t in tickers]
                last_fetches = get_last_fetch_times(
                    conn, symbols, data_type='fundamentals'
                )
                cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
                for symbol, fetched_at in last_fetches.items():
                    if fetched_at >= cutoff:
                        skip_symbols.add(symbol)
                        days_ago = (datetime.now(timezone.utc) - fetched_at).days
                        logger.info(
                            f"Skipping {symbol} — last fetched {days_ago} day(s) ago "
                            f"(within {max_age_days}-day window)"
                        )
            finally:
                conn.close()
        except Exception as e:
            logger.warning(
                f"Could not check fetch freshness: {e}. "
                "Proceeding with full fetch."
            )

    # --- Fetch loop ---
    with EdgarFundamentalsFetcher(
        db_config=db_config,
        user_agent=user_agent,
    ) as fetcher:
        for i, ticker_info in enumerate(tickers):
            symbol = ticker_info['symbol']

            if on_ticker_start:
                on_ticker_start(i, symbol)

            # Skip if fresh
            if symbol in skip_symbols:
                result = FetchResult(
                    symbol=symbol, status='skipped', records=0,
                    error=f'Fetched within last {max_age_days} days'
                )
                results.append(result)
                if on_ticker_complete:
                    on_ticker_complete(i, result)
                continue

            try:
                records = fetcher.get_fundamentals(
                    ticker=symbol,
                    start_date=start,
                    end_date=end,
                    save_to_db=True,
                )

                if records:
                    result = FetchResult(
                        symbol=symbol, status='success', records=len(records)
                    )
                else:
                    result = FetchResult(
                        symbol=symbol, status='empty', records=0,
                        error='No fundamental data found'
                    )

            except Exception as e:
                logger.error(f"Failed to fetch fundamentals for {symbol}: {e}")
                result = FetchResult(
                    symbol=symbol, status='error', records=0, error=str(e)
                )

            results.append(result)

            if on_ticker_complete:
                on_ticker_complete(i, result)

    return results
