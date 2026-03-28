-- =============================================================
-- Migration: Add financial-sector columns to fundamentals table
-- =============================================================
-- Run against both stocks_dev and stocks databases:
--   psql -h 10.0.0.1 -U stock_user -d stocks_dev -f migrate_financial_columns.sql
--   psql -h 10.0.0.1 -U stock_user -d stocks -f migrate_financial_columns.sql
--
-- All columns are nullable NUMERIC — only populated for
-- financial-sector companies (banks, insurers, asset managers).
-- Standard industrial/tech companies will have NULLs here.
-- =============================================================

-- Banks / Investment Banks
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS net_interest_income NUMERIC;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS noninterest_income NUMERIC;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS noninterest_expense NUMERIC;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS provision_for_credit_losses NUMERIC;

-- Insurance
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS premiums_earned NUMERIC;

-- General Financial (insurers, asset managers, banks)
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS investment_income NUMERIC;

-- Add comments for documentation
COMMENT ON COLUMN fundamentals.net_interest_income IS 'Net interest income (interest earned - interest paid). Primary revenue for banks.';
COMMENT ON COLUMN fundamentals.noninterest_income IS 'Non-interest income (fees, trading, advisory, commissions). Second revenue component for banks.';
COMMENT ON COLUMN fundamentals.noninterest_expense IS 'Non-interest expense (compensation, occupancy, tech). Primary expense line for banks.';
COMMENT ON COLUMN fundamentals.provision_for_credit_losses IS 'Provision for credit/loan losses. Key risk indicator for banks.';
COMMENT ON COLUMN fundamentals.premiums_earned IS 'Net premiums earned. Core revenue for insurance companies.';
COMMENT ON COLUMN fundamentals.investment_income IS 'Investment income (interest, dividends, gains/losses on investments). Relevant for insurers and asset managers.';
