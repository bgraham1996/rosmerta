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
by ``stock_id``. ``IBStockDataFetcher._save_bars_to_db`` now delegates here.

Nothing in here manages its own connection or transaction *scope* beyond the
single upsert it performs: callers pass a live connection, and ``upsert_hourly_bars``
commits its own write (matching the old behaviour) so a caller can loop over
contracts without one failure poisoning the rest.
"""

from psycopg2.extras import execute_values as _execute_values

# ``_execute_values`` is referenced through the module namespace on purpose:
# tests monkeypatch ``ohlcv_store._execute_values`` to model ON CONFLICT
# semantics against the in-memory FakeDB without a live PostgreSQL.

_UPSERT_SQL = """
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


def _build_records(stock_id, candles):
    """Normalise ``candles`` into ``price_hourly`` row tuples.

    ``candles`` is any iterable of ``(timestamp, open, high, low, close, volume)``.
    Rows are de-duplicated by timestamp (**last value wins**, so re-supplying a
    corrected bar in the same batch overrides the earlier one) and returned
    sorted by timestamp for deterministic writes.
    """
    by_ts = {}
    for ts, o, h, l, c, v in candles:
        by_ts[ts] = (stock_id, ts, o, h, l, c, int(v))
    return [by_ts[ts] for ts in sorted(by_ts)]


def upsert_hourly_bars(conn, stock_id, candles):
    """Upsert OHLCV candles for ``stock_id`` into ``price_hourly``.

    Source-agnostic: ``candles`` may come from IB, a synthetic seed, or any
    third party. Uses the same ``ON CONFLICT (stock_id, timestamp)`` upsert as
    the original IB path, so re-writes are idempotent. Commits on success and
    rolls back on failure. Returns the number of rows written.
    """
    records = _build_records(stock_id, candles)
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


def read_hourly_bars(conn, stock_id, start=None, end=None):
    """Read a contract's / stock's candles by ``stock_id``.

    Returns rows as ``(stock_id, timestamp, open, high, low, close, volume)``
    ordered by timestamp — the same shape ``data_models.Asset`` consumes.
    ``start`` / ``end`` are optional inclusive bounds (datetimes or
    ``'%Y-%m-%d %H:%M:%S'`` strings).
    """
    sql = ("SELECT stock_id, timestamp, open, high, low, close, volume "
           "FROM price_hourly WHERE stock_id = %s")
    params = [stock_id]
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
