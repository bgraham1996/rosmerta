-- ============================================================
-- Seed script: dev_test watchlist
-- ============================================================
-- Run against stocks_dev to set up a small test watchlist.
-- After running this, use `fetch price` for each ticker to
-- populate full stock records from IB, then test bulk fetch.
--
-- Usage:
--   psql -h 10.0.0.1 -U stock_user -d stocks_dev -f seed_dev_watchlist.sql
-- ============================================================

-- Pharma
INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('PFE', 'Pfizer Inc', 'NYSE', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('LLY', 'Eli Lilly and Company', 'NYSE', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

-- Tech
INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('AAPL', 'Apple Inc', 'NASDAQ', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('MSFT', 'Microsoft Corporation', 'NASDAQ', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

-- Financial
INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('JPM', 'JPMorgan Chase & Co', 'NYSE', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

INSERT INTO stocks (symbol, name, exchange, currency)
VALUES ('GS', 'The Goldman Sachs Group Inc', 'NYSE', 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

-- Create dev_test watchlist
INSERT INTO watchlist_members (stock_id, list_name)
SELECT stock_id, 'dev_test'
FROM stocks
WHERE symbol IN ('PFE', 'LLY', 'AAPL', 'MSFT', 'JPM', 'GS')
ON CONFLICT DO NOTHING;

-- Verify
SELECT s.symbol, s.name, wm.list_name
FROM watchlist_members wm
JOIN stocks s ON s.stock_id = wm.stock_id
WHERE wm.list_name = 'dev_test'
ORDER BY s.symbol;
