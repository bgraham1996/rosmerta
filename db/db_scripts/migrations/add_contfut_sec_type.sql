-- =============================================================
-- Migration: allow CONTFUT in stocks.sec_type
-- =============================================================
-- Extends the sec_type check constraint added by
-- add_futures_contract_support.sql (STK / FUTROOT / FUT) to also permit
-- 'CONTFUT' -- a continuous-futures series modelled as another child row of
-- the root (board item "Futures contract aggregation and continuous series").
--
-- A CONTFUT row (e.g. ES.c1) is a derived, rebuildable series: its candles are
-- ordinary price_hourly rows produced by volume-roll + ratio back-adjustment of
-- the raw FUT contracts. The raw contracts remain the source of truth; the
-- CONTFUT row is the gap-free series the analysis layer consumes.
--
-- Pure DDL, idempotent, additive (only widens the allowed set). Does NOT touch
-- db/init.sql (reconciled separately, board item "Reconcile DB schema").

ALTER TABLE stocks DROP CONSTRAINT IF EXISTS ck_stocks_sec_type;
ALTER TABLE stocks
    ADD CONSTRAINT ck_stocks_sec_type
    CHECK (sec_type IN ('STK', 'FUTROOT', 'FUT', 'CONTFUT'));

COMMENT ON COLUMN stocks.sec_type IS
    'Row kind: STK (equity), FUTROOT (futures root/organizing parent), '
    'FUT (specific futures contract), CONTFUT (derived continuous series).';

DO $$
BEGIN
    RAISE NOTICE 'sec_type now allows STK/FUTROOT/FUT/CONTFUT.';
END $$;
