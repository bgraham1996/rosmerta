-- ============================================================
-- SEED: Original core watchlist (50 tickers)
-- ============================================================
-- Restores the original 'core' watchlist tickers from the old
-- database (db_exports/tickers.csv + watchlist_export.csv,
-- old stock_ids 1-58, deduplicated to 50 distinct symbols).
--
-- The companion script core_expansion_seed.sql covers the other
-- 50 'core' symbols (the March 2026 expansion, old ids 101-150).
-- Run both to fully reconstruct the 100-symbol 'core' watchlist.
--
-- Name defaults to the symbol as a placeholder; the IB fetcher
-- backfills full details (name, ib_con_id, etc.) on first fetch.
--
-- Safe to re-run: uses ON CONFLICT to skip existing records.
--
-- Run as: psql -U stock_user -d stocks -h 10.0.0.1 -f original_core_seed.sql
-- ============================================================

BEGIN;

-- ── Insert minimal stock records ───────────────────────────
INSERT INTO stocks (symbol, name, exchange, currency) VALUES
    ('AAL',   'AAL',   'NASDAQ', 'USD'),
    ('AAPL',  'AAPL',  'NASDAQ', 'USD'),
    ('ABBV',  'ABBV',  'NYSE',   'USD'),
    ('ABT',   'ABT',   'NYSE',   'USD'),
    ('ADSK',  'ADSK',  'NASDAQ', 'USD'),
    ('AMD',   'AMD',   'NASDAQ', 'USD'),
    ('AMGN',  'AMGN',  'NASDAQ', 'USD'),
    ('AMZN',  'AMZN',  'NASDAQ', 'USD'),
    ('AZN',   'AZN',   'NASDAQ', 'USD'),
    ('BAC',   'BAC',   'NYSE',   'USD'),
    ('BIIB',  'BIIB',  'NASDAQ', 'USD'),
    ('BMY',   'BMY',   'NYSE',   'USD'),
    ('COST',  'COST',  'NASDAQ', 'USD'),
    ('CVS',   'CVS',   'NYSE',   'USD'),
    ('DD',    'DD',    'NYSE',   'USD'),
    ('DE',    'DE',    'NYSE',   'USD'),
    ('DIS',   'DIS',   'NYSE',   'USD'),
    ('DOMO',  'DOMO',  'NASDAQ', 'USD'),
    ('DUK',   'DUK',   'NYSE',   'USD'),
    ('DXCM',  'DXCM',  'NASDAQ', 'USD'),
    ('F',     'F',     'NYSE',   'USD'),
    ('GILD',  'GILD',  'NASDAQ', 'USD'),
    ('GOOGL', 'GOOGL', 'NASDAQ', 'USD'),
    ('GSK',   'GSK',   'NYSE',   'USD'),
    ('IBM',   'IBM',   'NYSE',   'USD'),
    ('INTC',  'INTC',  'NASDAQ', 'USD'),
    ('JNJ',   'JNJ',   'NYSE',   'USD'),
    ('KO',    'KO',    'NYSE',   'USD'),
    ('LLY',   'LLY',   'NYSE',   'USD'),
    ('MA',    'MA',    'NYSE',   'USD'),
    ('MDT',   'MDT',   'NYSE',   'USD'),
    ('META',  'META',  'NASDAQ', 'USD'),
    ('MMM',   'MMM',   'NYSE',   'USD'),
    ('MRK',   'MRK',   'NYSE',   'USD'),
    ('MRNA',  'MRNA',  'NASDAQ', 'USD'),
    ('MSFT',  'MSFT',  'NASDAQ', 'USD'),
    ('NVDA',  'NVDA',  'NASDAQ', 'USD'),
    ('NVO',   'NVO',   'NYSE',   'USD'),
    ('ORCL',  'ORCL',  'NYSE',   'USD'),
    ('PEP',   'PEP',   'NASDAQ', 'USD'),
    ('PFE',   'PFE',   'NYSE',   'USD'),
    ('PG',    'PG',    'NYSE',   'USD'),
    ('PYPL',  'PYPL',  'NASDAQ', 'USD'),
    ('REGN',  'REGN',  'NASDAQ', 'USD'),
    ('SMR',   'SMR',   'NYSE',   'USD'),
    ('T',     'T',     'NYSE',   'USD'),
    ('TMO',   'TMO',   'NYSE',   'USD'),
    ('V',     'V',     'NYSE',   'USD'),
    ('VTRS',  'VTRS',  'NASDAQ', 'USD'),
    ('VZ',    'VZ',    'NYSE',   'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

-- ── Assign all to the 'core' watchlist ─────────────────────
INSERT INTO watchlist_members (stock_id, list_name, source, notes)
SELECT
    s.stock_id,
    'core',
    'csv_import',
    'Original core watchlist — restored from old DB export'
FROM stocks s
WHERE s.symbol IN (
    'AAL',  'AAPL', 'ABBV', 'ABT',  'ADSK', 'AMD',  'AMGN', 'AMZN', 'AZN',  'BAC',
    'BIIB', 'BMY',  'COST', 'CVS',  'DD',   'DE',   'DIS',  'DOMO', 'DUK',  'DXCM',
    'F',    'GILD', 'GOOGL','GSK',  'IBM',  'INTC', 'JNJ',  'KO',   'LLY',  'MA',
    'MDT',  'META', 'MMM',  'MRK',  'MRNA', 'MSFT', 'NVDA', 'NVO',  'ORCL', 'PEP',
    'PFE',  'PG',   'PYPL', 'REGN', 'SMR',  'T',    'TMO',  'V',    'VTRS', 'VZ'
)
ON CONFLICT (stock_id, list_name) DO NOTHING;

COMMIT;

-- ── Verify ─────────────────────────────────────────────────
SELECT list_name, COUNT(*) AS member_count
FROM watchlist_members
WHERE list_name = 'core'
GROUP BY list_name;
