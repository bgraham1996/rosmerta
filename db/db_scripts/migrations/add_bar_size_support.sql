-- =============================================================
-- Migration: Configurable bar size -- generalised `price_bars` table
-- =============================================================
-- Lets multiple timeframes (1-minute, 1-hour, ...) coexist for the same
-- stock. Today the timeframe is implicit in the table name: `price_hourly`
-- assumes exactly one row per (stock_id, timestamp) at hourly granularity,
-- so a 1-minute and an hourly bar at the same instant would COLLIDE on the
-- primary key. This makes the timeframe part of the row identity.
--
-- Decision (board item "Bar-size storage model", option (a), 2026-10-28):
--   a single generalised bars table with an explicit `bar_size` column,
--   rather than a table-per-timeframe. DB-as-source-of-truth: the timeframe
--   is stored explicitly, one read/write path serves every size.
--
-- Model
-- -----
--   * Rename `price_hourly` -> `price_bars`, unchanged OHLCV columns.
--   * Add `bar_size` (IB barSizeSetting string, e.g. '1 hour', '1 min').
--     Existing rows backfill to '1 hour' (they are all hourly today).
--   * New PK (stock_id, bar_size, timestamp): bar_size before timestamp so
--     the PK index directly serves the dominant size-scoped range read
--     (read_bars(stock_id, bar_size, start, end)). Uniqueness is identical
--     to adding bar_size to the old 2-col key.
--   * A `price_hourly` VIEW (bar_size = '1 hour') preserves read back-compat
--     for any code / ad-hoc SQL still selecting the hourly table. NOTE the
--     write path (ohlcv_store) moves to `price_bars` directly: INSERT ...
--     ON CONFLICT is not supported through a view, so the view is read-only
--     in practice.
--
-- Pure additive + idempotent apart from the one-time rename, which is
-- guarded so a re-run is a no-op. It does NOT touch db/init.sql (reconciled
-- separately later, board item "Reconcile DB schema").
--
-- Run as: psql -U stocks -d rosmerta__prod -h localhost \
--           -f db/db_scripts/migrations/add_bar_size_support.sql
-- =============================================================

BEGIN;

-- -------------------------------------------------------------
-- M1. One-time rename price_hourly (base table) -> price_bars.
--     Guarded on "price_hourly is still a BASE TABLE and price_bars does
--     not exist yet" so a re-run (price_hourly now a VIEW) is a no-op.
-- -------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_name = 'price_hourly' AND table_type = 'BASE TABLE')
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_name = 'price_bars' AND table_type = 'BASE TABLE') THEN
        ALTER TABLE price_hourly RENAME TO price_bars;
    END IF;
END $$;

-- Carry the old timestamp index over under the new name (guarded).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_price_hourly_ts' AND relkind = 'i')
       AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_price_bars_ts' AND relkind = 'i') THEN
        ALTER INDEX idx_price_hourly_ts RENAME TO idx_price_bars_ts;
    END IF;
END $$;

-- -------------------------------------------------------------
-- M2. bar_size column. IB barSizeSetting string ('1 hour', '1 min',
--     '5 mins', '15 mins', ...). NOT NULL with a '1 hour' default so
--     existing rows backfill to hourly and legacy hourly inserts need
--     no change.
-- -------------------------------------------------------------
ALTER TABLE price_bars
    ADD COLUMN IF NOT EXISTS bar_size VARCHAR(16) NOT NULL DEFAULT '1 hour';

-- -------------------------------------------------------------
-- M3. Swap the primary key to include bar_size. The original inline PK was
--     named price_hourly_pkey (the rename does not rename constraints).
-- -------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'price_hourly_pkey'
                 AND conrelid = 'price_bars'::regclass) THEN
        ALTER TABLE price_bars DROP CONSTRAINT price_hourly_pkey;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'price_bars_pkey'
                     AND conrelid = 'price_bars'::regclass) THEN
        ALTER TABLE price_bars
            ADD CONSTRAINT price_bars_pkey PRIMARY KEY (stock_id, bar_size, timestamp);
    END IF;
END $$;

-- -------------------------------------------------------------
-- M4. Read back-compat: a view that looks exactly like the old
--     price_hourly table (hourly rows, no bar_size column).
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW price_hourly AS
    SELECT stock_id, timestamp, open, high, low, close, volume
    FROM price_bars
    WHERE bar_size = '1 hour';

COMMENT ON TABLE price_bars IS
    'OHLCV candles at an explicit timeframe. One row per (stock_id, bar_size, timestamp). bar_size is the IB barSizeSetting string.';
COMMENT ON COLUMN price_bars.bar_size IS
    'Timeframe as an IB barSizeSetting string: ''1 hour'' (default/legacy), ''1 min'', ''5 mins'', ''15 mins'', ...';
COMMENT ON VIEW price_hourly IS
    'Read back-compat view: hourly rows of price_bars. Writes go to price_bars directly (ON CONFLICT is unsupported through a view).';

-- -------------------------------------------------------------
-- Post-condition assertions. Any failure raises and rolls back.
-- -------------------------------------------------------------
DO $$
DECLARE
    null_sizes int;
    pk_cols    text;
    has_view   int;
BEGIN
    SELECT count(*) INTO null_sizes FROM price_bars WHERE bar_size IS NULL;
    IF null_sizes > 0 THEN
        RAISE EXCEPTION 'Backfill incomplete: % price_bars row(s) with NULL bar_size', null_sizes;
    END IF;

    SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
      INTO pk_cols
      FROM pg_constraint c
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
     WHERE c.conname = 'price_bars_pkey' AND c.conrelid = 'price_bars'::regclass;
    IF pk_cols IS DISTINCT FROM 'stock_id,bar_size,timestamp' THEN
        RAISE EXCEPTION 'Unexpected price_bars PK columns: %', pk_cols;
    END IF;

    SELECT count(*) INTO has_view FROM information_schema.views WHERE table_name = 'price_hourly';
    IF has_view <> 1 THEN
        RAISE EXCEPTION 'price_hourly back-compat view missing';
    END IF;

    RAISE NOTICE 'Bar-size schema OK: price_bars(stock_id,bar_size,timestamp), existing rows = ''1 hour'', price_hourly view present.';
END $$;

COMMIT;

-- =============================================================
-- Rollback (manual down-migration; only safe while no non-hourly rows exist):
--   DROP VIEW IF EXISTS price_hourly;
--   ALTER TABLE price_bars DROP CONSTRAINT IF EXISTS price_bars_pkey;
--   ALTER TABLE price_bars ADD CONSTRAINT price_hourly_pkey PRIMARY KEY (stock_id, timestamp);
--   ALTER TABLE price_bars DROP COLUMN IF EXISTS bar_size;
--   ALTER INDEX idx_price_bars_ts RENAME TO idx_price_hourly_ts;
--   ALTER TABLE price_bars RENAME TO price_hourly;
-- =============================================================
