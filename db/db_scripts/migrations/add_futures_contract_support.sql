-- =============================================================
-- Migration: Futures support -- contracts-as-stocks under a self-parent
-- =============================================================
-- Extends the equities-only `stocks` table so futures can live in it as
-- "contracts-as-stocks" (design: project board item
-- "Futures schema - contracts as stocks" / parent
-- "Futures asset data retrival and storage", decided 2026-09-20).
--
-- Model
-- -----
--   * Each specific contract (e.g. ESH26) is its OWN `stocks` row.
--   * The root (e.g. ES) is ALSO a `stocks` row (an organizing parent).
--   * Contracts point at their root via a new self-referential FK,
--     `parent_stock_id`. Equities and roots keep parent_stock_id = NULL.
--   * OHLCV is UNCHANGED: a contract's candles are just `price_hourly`
--     rows keyed by its stock_id. No new price table.
--
-- This does NOT clash with uq_stocks_symbol UNIQUE(symbol) (from the
-- dedupe migration): every contract carries a distinct normalised symbol
-- (ESH26 != ESM26 != ES).
--
-- Design principle: the DB is the source of truth and API/vendor-agnostic.
-- Store the NORMALISED symbol (ESH26) in stocks.symbol -- never IB's
-- localSymbol (ESH6), which stays mapped into ib_con_id at ingestion.
--
-- Scope of this migration: M1 (parent link) + M2 (contract metadata),
-- both confirmed in scope. Pure additive, idempotent DDL -- safe to
-- re-run. It does NOT touch db/init.sql (reconciled separately later,
-- board item "Reconcile DB schema").
--
-- Run as: psql -U stocks -d rosmerta__prod -h localhost \
--           -f db/db_scripts/migrations/add_futures_contract_support.sql
-- =============================================================

BEGIN;

-- -------------------------------------------------------------
-- M1. Parent link: self-referential FK. Nullable; equities and roots
--     stay NULL, a contract points at its FUTROOT row. Indexed so
--     root -> children navigation (the only model function needed now)
--     is cheap.
-- -------------------------------------------------------------
ALTER TABLE stocks
    ADD COLUMN IF NOT EXISTS parent_stock_id INTEGER REFERENCES stocks(stock_id);

CREATE INDEX IF NOT EXISTS idx_stocks_parent_stock_id
    ON stocks (parent_stock_id);

-- -------------------------------------------------------------
-- M2. Contract metadata.
--   * sec_type   -- distinguishes an equity from a futures root/contract.
--                   'STK' | 'FUTROOT' | 'FUT'. Backfilled to 'STK' for
--                   all existing rows (they are equities), NOT NULL with
--                   a 'STK' default so future equity inserts need no change.
--   * expiry     -- contract expiry (IB lastTradeDateOrContractMonth) as
--                   a DATE. NULL for equities and for roots.
--   * multiplier -- contract multiplier (e.g. ES = 50). NUMERIC to allow
--                   fractional multipliers. NULL for equities and roots.
-- -------------------------------------------------------------
ALTER TABLE stocks
    ADD COLUMN IF NOT EXISTS sec_type   VARCHAR(10) NOT NULL DEFAULT 'STK',
    ADD COLUMN IF NOT EXISTS expiry     DATE,
    ADD COLUMN IF NOT EXISTS multiplier NUMERIC(20,4);

-- Constrain sec_type to the known kinds. Guarded so a re-run does not
-- fail on the already-present constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_stocks_sec_type'
          AND conrelid = 'stocks'::regclass
    ) THEN
        ALTER TABLE stocks
            ADD CONSTRAINT ck_stocks_sec_type
            CHECK (sec_type IN ('STK', 'FUTROOT', 'FUT'));
    END IF;
END $$;

COMMENT ON COLUMN stocks.parent_stock_id IS
    'Self-FK: a FUT contract points at its FUTROOT row. NULL for equities and roots.';
COMMENT ON COLUMN stocks.sec_type IS
    'Row kind: STK (equity) | FUTROOT (futures root, organizing parent) | FUT (specific contract).';
COMMENT ON COLUMN stocks.expiry IS
    'Futures contract expiry (IB lastTradeDateOrContractMonth) as a DATE. NULL for STK/FUTROOT.';
COMMENT ON COLUMN stocks.multiplier IS
    'Futures contract multiplier (e.g. ES=50). NULL for STK/FUTROOT.';

-- -------------------------------------------------------------
-- Post-condition assertions. Any failure raises and rolls back.
-- -------------------------------------------------------------
DO $$
DECLARE
    non_stk int;
BEGIN
    -- Every pre-existing row must have been backfilled to 'STK'.
    SELECT count(*) INTO non_stk FROM stocks WHERE sec_type IS NULL;
    IF non_stk > 0 THEN
        RAISE EXCEPTION 'Backfill incomplete: % row(s) with NULL sec_type', non_stk;
    END IF;
    RAISE NOTICE 'Futures schema OK: parent_stock_id + sec_type/expiry/multiplier in place, existing rows = STK.';
END $$;

COMMIT;

-- =============================================================
-- Rollback (before COMMIT, or as a manual down-migration):
--   ALTER TABLE stocks DROP CONSTRAINT IF EXISTS ck_stocks_sec_type;
--   DROP INDEX IF EXISTS idx_stocks_parent_stock_id;
--   ALTER TABLE stocks
--       DROP COLUMN IF EXISTS parent_stock_id,
--       DROP COLUMN IF EXISTS sec_type,
--       DROP COLUMN IF EXISTS expiry,
--       DROP COLUMN IF EXISTS multiplier;
-- =============================================================
