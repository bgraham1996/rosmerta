-- ============================================================
-- WATCHLISTS: Named lists of stocks for bulk operations
-- ============================================================
-- Supports curated lists (e.g. 'pharma_top20', 'nasdaq_30')
-- and ad hoc imports. A stock can belong to multiple lists.
--
-- Usage examples:
--   SELECT s.symbol, s.exchange
--   FROM watchlist_members wm
--   JOIN stocks s ON s.stock_id = wm.stock_id
--   WHERE wm.list_name = 'pharma_top20';
--
-- Run as: psql -U stock_user -d stocks -h 10.0.0.1 -f add_watchlists.sql
-- ============================================================

CREATE TABLE watchlist_members (
    watchlist_member_id  SERIAL PRIMARY KEY,
    stock_id             INTEGER NOT NULL REFERENCES stocks(stock_id),
    list_name            VARCHAR(100) NOT NULL,
    source               VARCHAR(20) DEFAULT 'manual',  -- 'manual', 'csv_import', 'programmatic'
    added_at             TIMESTAMPTZ DEFAULT NOW(),
    notes                TEXT,

    CONSTRAINT uq_watchlist_member UNIQUE (stock_id, list_name)
);

CREATE INDEX idx_watchlist_list_name ON watchlist_members (list_name);
CREATE INDEX idx_watchlist_stock ON watchlist_members (stock_id);
CREATE INDEX idx_watchlist_source ON watchlist_members (source);

-- Handy view: see watchlist contents with stock details
CREATE VIEW v_watchlists AS
SELECT
    wm.list_name,
    s.symbol,
    s.name,
    s.exchange,
    s.currency,
    wm.source,
    wm.added_at,
    wm.notes
FROM watchlist_members wm
JOIN stocks s ON s.stock_id = wm.stock_id
WHERE s.is_active = TRUE
ORDER BY wm.list_name, s.symbol;
