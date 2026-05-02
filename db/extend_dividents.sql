-- ============================================================
-- EXTEND DIVIDENDS TABLE
-- ============================================================
-- Adds columns for richer dividend metadata:
--   declaration_date  — when the board announced the dividend
--   record_date       — shareholders of record on this date receive payment
--   dividend_type     — 'regular', 'special', 'stock', 'liquidating'
--   source            — 'edgar', 'ib', 'manual'
--   created_at        — row insertion timestamp
--
-- EDGAR-sourced rows will have approximate ex_date (period end from XBRL)
-- and dividend_type defaulting to 'regular'. IB-sourced rows will have
-- accurate ex_date and may flag specials.
--
-- Run as: psql -U stock_user -d stocks_dev -h 10.0.0.1 \
--           -f db_scripts/migrations/004_extend_dividends.sql
-- ============================================================

ALTER TABLE dividends
    ADD COLUMN IF NOT EXISTS declaration_date DATE,
    ADD COLUMN IF NOT EXISTS record_date      DATE,
    ADD COLUMN IF NOT EXISTS dividend_type    VARCHAR(20) DEFAULT 'regular',
    ADD COLUMN IF NOT EXISTS source           VARCHAR(50) DEFAULT 'edgar',
    ADD COLUMN IF NOT EXISTS created_at       TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_dividends_declaration
    ON dividends (declaration_date);

CREATE INDEX IF NOT EXISTS idx_dividends_type
    ON dividends (stock_id, dividend_type);

CREATE INDEX IF NOT EXISTS idx_dividends_source
    ON dividends (source);
