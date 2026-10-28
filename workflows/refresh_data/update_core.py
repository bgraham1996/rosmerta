"""Bring the ``core`` watchlist's price data current (hourly by default).

When invoked this refreshes every active member of a watchlist (``core`` by
default) up to *tomorrow*: for each ticker it looks up the last bar of the
requested ``bar_size`` already stored in ``price_bars``, then fetches from that
point through tomorrow's date and upserts the missing bars. Pass
``main(bar_size='1 min')`` (or ``'5 mins'``/``'15 mins'``/``'30 mins'``) for a
sub-hourly refresh; everything defaults to ``'1 hour'`` for back-compat.

Why tomorrow, not today
-----------------------
The end date is set to tomorrow (UTC) so the most recent *complete* session is
always captured regardless of the caller's timezone. IB's ``useRTH`` request
never returns bars past the last real session, so overshooting the end is
harmless.

Why start at the last stored bar's *day*
----------------------------------------
Each ticker starts from the calendar day of its last stored bar, so no session
falls through a gap. Re-fetching that (possibly partial) final day is safe:
``price_bars`` is written ``ON CONFLICT (stock_id, bar_size, timestamp)`` (see
``ib_api._save_bars_to_db``), so overlapping bars are idempotent. A ticker with
no history yet backfills from ``_default_start_for(bar_size)`` — full history
for hourly, a bounded recent window for sub-hourly.

Each ticker is fetched over its own fresh IB connection (via
``bulk_fetch._fetch_single_ticker``) for isolation, exactly as ``bulk price``
does.

Result
------
``main`` returns a list of ``FetchResult`` (one per ticker,
``status`` in ``success`` / ``empty`` / ``error``):

    results = main()                       # refresh 'core' up to tomorrow
    failed = [r for r in results if r.status == 'error']
"""

import time
import logging
from datetime import datetime, timedelta, timezone

from db_config import get_db_config
from price_retrival.bulk_fetch import get_watchlist_tickers, _fetch_single_ticker
import psycopg2 as db

logger = logging.getLogger(__name__)


# --- Refresh parameters (tune here) ------------------------------------------
# Fallback start for a ticker that has no stored bars yet (nothing to extend
# from) — kept generous so a newly added core name backfills fully on first run.
DEFAULT_START = '2020-01-01'
IB_HOST = '127.0.0.1'
IB_PORT = 4001
EXCHANGE = 'SMART'          # SMART routing, matching `bulk price`
DELAY = 2                   # seconds between tickers (hourly), to respect IB pacing
BAR_SIZE = '1 hour'         # default timeframe to refresh

# Sub-hourly first-backfill window (days). 1-minute history is enormous and IB
# paces historical requests hard (~60 requests / 10 min), so a brand-new ticker
# backfills only a recent window at fine granularity rather than from
# DEFAULT_START. Steady-state runs fetch just the gap since the last stored bar
# of that size, so they stay small regardless.
DEFAULT_LOOKBACK_DAYS = {'1 min': 10, '5 mins': 30, '15 mins': 60, '30 mins': 90}
# Seconds between tickers for sub-hourly (more chunks/ticker → more requests, so
# give IB's historical-data pacing more room than the hourly DELAY).
SUBHOURLY_DELAY = 5


def _tomorrow(now=None):
    """Tomorrow's date as a ``YYYY-MM-DD`` string (UTC)."""
    now = now or datetime.now(timezone.utc)
    return (now + timedelta(days=1)).strftime('%Y-%m-%d')


def _start_for(last_ts, default=DEFAULT_START):
    """Start date (``YYYY-MM-DD``) for a ticker given its last stored bar.

    Starts on the calendar day of ``last_ts`` so no session is skipped; falls
    back to ``default`` when the ticker has no history yet (``last_ts`` None).
    """
    return last_ts.strftime('%Y-%m-%d') if last_ts is not None else default


def _default_start_for(bar_size, now=None):
    """Fallback start date (``YYYY-MM-DD``) for a ticker with no stored bars of
    ``bar_size``.

    Hourly backfills from ``DEFAULT_START`` (full history); sub-hourly backfills
    only a recent window (``DEFAULT_LOOKBACK_DAYS``) because 1-minute history is
    enormous and IB paces historical requests — a years-long 1-min backfill
    would emit thousands of requests. Sizes not in the lookback map fall back to
    ``DEFAULT_START``.
    """
    lookback = DEFAULT_LOOKBACK_DAYS.get(bar_size)
    if lookback is None:
        return DEFAULT_START
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(days=lookback)).strftime('%Y-%m-%d')


def get_last_price_dates(conn, list_name='core', bar_size='1 hour'):
    """Return ``{symbol: last stored timestamp at bar_size}`` for active members.

    A single query over the whole watchlist, scoped to one ``bar_size`` so the
    incremental start is per-timeframe (the last 1-min bar, not the last hourly
    bar). Members with no stored bars of that size map to ``None`` (LEFT JOIN),
    so callers can distinguish "up to date" from "never fetched".
    """
    with conn.cursor() as cur:
        cur.execute(
            """SELECT s.symbol, MAX(p.timestamp)
               FROM stocks s
               JOIN watchlist_members wm
                 ON wm.stock_id = s.stock_id AND wm.list_name = %s
               LEFT JOIN price_bars p
                 ON p.stock_id = s.stock_id AND p.bar_size = %s
               WHERE s.is_active = TRUE
               GROUP BY s.symbol""",
            (list_name, bar_size)
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def main(stock_list='core', bar_size=BAR_SIZE):
    """Refresh ``stock_list``'s prices at ``bar_size`` up to tomorrow.

    Returns a list of ``FetchResult``. Each ticker is extended incrementally
    from its last stored bar *of this bar size*; a ticker with none backfills
    from ``_default_start_for(bar_size)`` (a bounded window for sub-hourly).
    """
    db_config = get_db_config()
    end = _tomorrow()
    fallback_start = _default_start_for(bar_size)
    delay = DELAY if bar_size == '1 hour' else SUBHOURLY_DELAY

    # Read the watchlist and each member's last bar in one short-lived
    # connection; the per-ticker fetch below manages its own IB + DB connections.
    connection = db.connect(**db_config)
    try:
        with connection as conn:
            tickers = get_watchlist_tickers(conn, stock_list)
            if not tickers:
                raise ValueError(
                    f"Watchlist {stock_list!r} is empty or does not exist"
                )
            last_dates = get_last_price_dates(conn, stock_list, bar_size=bar_size)
    finally:
        connection.close()

    results = []
    total = len(tickers)
    for i, info in enumerate(tickers, start=1):
        symbol = info['symbol']
        start = _start_for(last_dates.get(symbol), default=fallback_start)

        print(f"[{i}/{total}] {symbol} [{bar_size}]: fetching {start} -> {end}")
        result = _fetch_single_ticker(
            symbol=symbol,
            currency=info['currency'],
            start=start,
            end=end,
            db_config=db_config,
            ib_host=IB_HOST,
            ib_port=IB_PORT,
            exchange=EXCHANGE,
            bar_size=bar_size,
        )
        results.append(result)
        note = f" — {result.error}" if result.error else ""
        print(f"    {result.status} ({result.records} bars){note}")

        # Pace requests to stay under IB rate limits (skip after the last one).
        if i < total:
            time.sleep(delay)

    ok = sum(1 for r in results if r.status == 'success')
    empty = sum(1 for r in results if r.status == 'empty')
    errors = sum(1 for r in results if r.status == 'error')
    bars = sum(r.records for r in results)
    print(
        f"\nDone: {ok} updated, {empty} empty, {errors} error(s); "
        f"{bars} bars written across {total} tickers."
    )
    return results


if __name__ == '__main__':
    main()
