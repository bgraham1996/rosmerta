-- ============================================================
-- SEED: Core watchlist expansion (49 new tickers)
-- ============================================================
-- Adds new stock records and assigns them to the existing
-- 'core' watchlist. The IB fetcher will backfill full stock
-- details (name, ib_con_id, etc.) on first fetch.
--
-- Safe to re-run: uses ON CONFLICT to skip existing records.
--
-- Run as: psql -U stock_user -d stocks -h 10.0.0.1 -f seed_core_expansion.sql
-- ============================================================

BEGIN;

-- ── Insert minimal stock records ───────────────────────────
-- Name defaults to symbol as placeholder; IB fetcher backfills.
-- European-listed tickers (LONN, ABBN, SIE, GLEN, VIE, IPN)
-- replaced with US-listed alternatives.

INSERT INTO stocks (symbol, name, exchange, currency) VALUES
    -- Financials
    ('IBKR',  'IBKR',  'NASDAQ', 'USD'),
    ('MS',    'MS',    'NYSE',   'USD'),
    -- Semiconductors / Tech
    ('TSM',   'TSM',   'NYSE',   'USD'),
    ('ASML',  'ASML',  'NASDAQ', 'USD'),
    ('MU',    'MU',    'NASDAQ', 'USD'),
    ('SNOW',  'SNOW',  'NYSE',   'USD'),
    -- Pharma / Biotech
    ('NVS',   'NVS',   'NYSE',   'USD'),
    ('INCY',  'INCY',  'NASDAQ', 'USD'),
    ('RHHBY', 'RHHBY', 'OTC',    'USD'),
    ('BNTX',  'BNTX',  'NASDAQ', 'USD'),
    ('APGE',  'APGE',  'NASDAQ', 'USD'),
    ('RPRX',  'RPRX',  'NASDAQ', 'USD'),
    ('ZTS',   'ZTS',   'NYSE',   'USD'),
    ('EXEL',  'EXEL',  'NASDAQ', 'USD'),
    ('JAZZ',  'JAZZ',  'NASDAQ', 'USD'),
    -- Life sciences tools / Healthcare
    ('DHR',   'DHR',   'NYSE',   'USD'),
    ('ILMN',  'ILMN',  'NASDAQ', 'USD'),
    ('A',     'A',     'NYSE',   'USD'),
    ('CRL',   'CRL',   'NYSE',   'USD'),
    ('GEHC',  'GEHC',  'NASDAQ', 'USD'),
    ('TECH',  'TECH',  'NASDAQ', 'USD'),
    ('ALC',   'ALC',   'NYSE',   'USD'),
    ('BDX',   'BDX',   'NYSE',   'USD'),
    ('TWST',  'TWST',  'NASDAQ', 'USD'),
    -- Consumer
    ('SBUX',  'SBUX',  'NASDAQ', 'USD'),
    -- Industrials
    ('CAT',   'CAT',   'NYSE',   'USD'),
    ('FDX',   'FDX',   'NYSE',   'USD'),
    ('UPS',   'UPS',   'NYSE',   'USD'),
    ('GEV',   'GEV',   'NYSE',   'USD'),
    ('NUE',   'NUE',   'NYSE',   'USD'),
    ('PLD',   'PLD',   'NYSE',   'USD'),
    -- Chemicals / Agriculture
    ('DOW',   'DOW',   'NYSE',   'USD'),
    ('CTVA',  'CTVA',  'NYSE',   'USD'),
    ('FMC',   'FMC',   'NYSE',   'USD'),
    ('SMG',   'SMG',   'NYSE',   'USD'),
    ('ALB',   'ALB',   'NYSE',   'USD'),
    ('SQM',   'SQM',   'NYSE',   'USD'),
    -- Energy / Utilities
    ('TTE',   'TTE',   'NYSE',   'USD'),
    ('VST',   'VST',   'NYSE',   'USD'),
    ('SO',    'SO',    'NYSE',   'USD'),
    ('CEG',   'CEG',   'NASDAQ', 'USD'),
    -- Mining / Materials
    ('FCX',   'FCX',   'NYSE',   'USD'),
    -- Water / Environment
    ('XYL',   'XYL',   'NYSE',   'USD'),
    ('ARE',   'ARE',   'NYSE',   'USD'),
    -- US replacements for European tickers
    ('ABB',   'ABB',   'NYSE',   'USD'),   -- replaces ABBN (SIX)
    ('HON',   'HON',   'NASDAQ', 'USD'),   -- replaces SIE (XETRA)
    ('AWK',   'AWK',   'NYSE',   'USD'),   -- replaces VIE (Euronext)
    ('TECK',  'TECK',  'NYSE',   'USD'),   -- replaces GLEN (LSE)
    ('WM',    'WM',    'NYSE',   'USD'),   -- replaces VIE (Euronext, partial)
    ('RGEN',  'RGEN',  'NASDAQ', 'USD')    -- replaces LONN (SIX) / IPN (Euronext)
ON CONFLICT (symbol, exchange) DO NOTHING;

-- ── Assign all to the 'core' watchlist ─────────────────────
INSERT INTO watchlist_members (stock_id, list_name, source, notes)
SELECT
    s.stock_id,
    'core',
    'manual',
    'Core watchlist expansion — March 2026'
FROM stocks s
WHERE s.symbol IN (
    'IBKR', 'MS',
    'TSM',  'ASML', 'MU',   'SNOW',
    'NVS',  'INCY', 'RHHBY','BNTX', 'APGE', 'RPRX', 'ZTS', 'EXEL', 'JAZZ',
    'DHR',  'ILMN', 'A',    'CRL',  'GEHC', 'TECH', 'ALC', 'BDX',  'TWST',
    'SBUX',
    'CAT',  'FDX',  'UPS',  'GEV',  'NUE',  'PLD',
    'DOW',  'CTVA', 'FMC',  'SMG',  'ALB',  'SQM',
    'TTE',  'VST',  'SO',   'CEG',
    'FCX',
    'XYL',  'ARE',
    'ABB',  'HON',  'AWK',  'TECK', 'WM',   'RGEN'
)
ON CONFLICT (stock_id, list_name) DO NOTHING;

COMMIT;

-- ── Verify ─────────────────────────────────────────────────
SELECT list_name, COUNT(*) AS member_count
FROM watchlist_members
WHERE list_name = 'core'
GROUP BY list_name;
