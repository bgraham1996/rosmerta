"""
Source-agnostic OHLCV candle storage & retrieval
=================================================
The candle read/write path, decoupled from any single vendor. Previously
the only way to write ``price_hourly`` was ``IBStockDataFetcher._save_bars_to_db``,
which is bound to an ``ib-insync`` DataFrame and an IB connection. Futures
(and any future non-IB source) need to write the *same* table from synthetic
or third-party candles, so the actual upsert lives here as a plain function
over a psycopg2 connection and a list of ``(ts, o, h, l, c, v)`` tuples.

Design principle (see project board "Futures OHLCV storage and retrieval"):
the DB is the source of truth and vendor-agnostic. This module knows nothing
about IB, EDGAR, or contract semantics — it just upserts/reads candles keyed
by ``stock_id`` and a ``bar_size``.

Bar size (board item "Bar-size storage model", option (a)): candles live in a
single generalised ``price_bars`` table with an explicit ``bar_size`` column
(the IB barSizeSetting string, e.g. ``'1 hour'``, ``'1 min'``). Row identity is
``(stock_id, bar_size, timestamp)``, so the same instant can hold both an hourly
and a 1-minute bar without collision. ``upsert_bars`` / ``read_bars`` are the
general path; the ``*_hourly`` functions are thin ``bar_size='1 hour'`` wrappers
kept for back-compat with existing callers.

Nothing in here manages its own connection or transaction *scope* beyond the
single upsert it performs: callers pass a live connection, and ``upsert_bars``
commits its own write (matching the old behaviour) so a caller can loop over
contracts without one failure poisoning the rest.
"""

from psycopg2.extras import execute_values as _execute_values

# ``_execute_values`` is referenced through the module namespace on purpose:
# tests monkeypatch ``ohlcv_store._execute_values`` to model ON CONFLICT
# semantics against the in-memory FakeDB without a live PostgreSQL.

_HOURLY = "1 hour"

_UPSERT_SQL = """
    INSERT INTO price_bars (stock_id, timestamp, bar_size, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (stock_id, bar_size, timestamp)
    DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume
"""


def _build_records(stock_id, bar_size, candles):
    """Normalise ``candles`` into ``price_bars`` row tuples.

    ``candles`` is any iterable of ``(timestamp, open, high, low, close, volume)``.
    Rows are de-duplicated by timestamp (**last value wins**, so re-supplying a
    corrected bar in the same batch overrides the earlier one) and returned
    sorted by timestamp for deterministic writes. Each record carries the
    ``bar_size`` and matches the INSERT column order
    ``(stock_id, timestamp, bar_size, open, high, low, close, volume)``.
    """
    by_ts = {}
    for ts, o, h, l, c, v in candles:
        by_ts[ts] = (stock_id, ts, bar_size, o, h, l, c, int(v))
    return [by_ts[ts] for ts in sorted(by_ts)]


def upsert_bars(conn, stock_id, candles, bar_size=_HOURLY):
    """Upsert OHLCV candles for ``stock_id`` at ``bar_size`` into ``price_bars``.

    Source-agnostic: ``candles`` may come from IB, a synthetic seed, or any
    third party. Uses an ``ON CONFLICT (stock_id, bar_size, timestamp)`` upsert
    so re-writes are idempotent and different timeframes for the same instant
    coexist. Commits on success and rolls back on failure. Returns the number
    of rows written.
    """
    records = _build_records(stock_id, bar_size, candles)
    if not records:
        return 0
    try:
        with conn.cursor() as cur:
            _execute_values(cur, _UPSERT_SQL, records)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(records)


def read_bars(conn, stock_id, bar_size=_HOURLY, start=None, end=None):
    """Read a stock's / contract's candles at ``bar_size`` from ``price_bars``.

    Returns rows as ``(stock_id, timestamp, open, high, low, close, volume)``
    ordered by timestamp — the same shape ``data_models.Asset`` consumes
    (``bar_size`` is a filter, not an output column). ``start`` / ``end`` are
    optional inclusive bounds (datetimes or ``'%Y-%m-%d %H:%M:%S'`` strings).
    """
    sql = ("SELECT stock_id, timestamp, open, high, low, close, volume "
           "FROM price_bars WHERE stock_id = %s AND bar_size = %s")
    params = [stock_id, bar_size]
    if start is not None:
        sql += " AND timestamp >= %s"
        params.append(start)
    if end is not None:
        sql += " AND timestamp <= %s"
        params.append(end)
    sql += " ORDER BY timestamp ASC"
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def upsert_hourly_bars(conn, stock_id, candles):
    """Back-compat wrapper: upsert hourly candles (``bar_size='1 hour'``)."""
    return upsert_bars(conn, stock_id, candles, bar_size=_HOURLY)


def read_hourly_bars(conn, stock_id, start=None, end=None):
    """Back-compat wrapper: read hourly candles (``bar_size='1 hour'``)."""
    return read_bars(conn, stock_id, bar_size=_HOURLY, start=start, end=end)
