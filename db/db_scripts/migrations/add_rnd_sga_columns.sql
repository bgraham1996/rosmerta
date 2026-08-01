-- =============================================================
-- Migration: Add R&D and SG&A expense columns to fundamentals
-- =============================================================
-- These two income-statement lines are fetched and stored by the
-- EDGAR pipeline (price_retrival/edgar_api.py XBRL_TAG_MAP + INSERTs,
-- surfaced in main.py and db_scripts/missing_fundamentals.sql) but
-- were originally added to the live DB via an ad-hoc ALTER that was
-- never captured as a schema file. Without these columns the EDGAR
-- fundamentals INSERT fails on a fresh rebuild.
--
-- Both nullable NUMERIC(18,2), matching the existing income-statement
-- columns. Companies that don't report a separate R&D or SG&A line
-- will have NULLs here.
--
-- Run as: psql -U stock_user -d stocks -h <host> \
--           -f db_scripts/migrations/add_rnd_sga_columns.sql
-- =============================================================

ALTER TABLE fundamentals
    ADD COLUMN IF NOT EXISTS research_and_development NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS sga_expense             NUMERIC(18,2);

COMMENT ON COLUMN fundamentals.research_and_development IS 'Research & development expense (income statement).';
COMMENT ON COLUMN fundamentals.sga_expense IS 'Selling, general & administrative expense (income statement).';
