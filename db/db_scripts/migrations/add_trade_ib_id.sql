-- =============================================================
-- Migration: Add IB trade identifier to the trade table
-- =============================================================
-- The trade table (db/add_positions.sql) has no natural key tying a
-- row back to its source execution, so re-running the IB Flex trades
-- fetcher (price_retrival/trades_api.py) would insert duplicate rows.
--
-- IB's Flex "Trade" records carry a unique `tradeID`. Storing it with a
-- UNIQUE constraint lets the fetcher de-duplicate (it skips trade IDs
-- already present) and makes the ingest idempotent.
--
-- Nullable so trades entered by other means (manual, SQL seeds) are
-- still allowed; the UNIQUE constraint ignores NULLs in PostgreSQL.
--
-- Run as: psql -U stock_user -d stocks -h <host> \
--           -f db_scripts/migrations/add_trade_ib_id.sql
-- =============================================================

ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS ib_trade_id VARCHAR(50);

ALTER TABLE trade
    ADD CONSTRAINT uq_trade_ib_trade_id UNIQUE (ib_trade_id);

COMMENT ON COLUMN trade.ib_trade_id IS 'Unique IB Flex execution id (Trade.tradeID); NULL for non-IB trades.';
