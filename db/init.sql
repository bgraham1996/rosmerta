

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- `stocks` holds equities AND futures under the "contracts-as-stocks" model
-- (migrations dedupe_stocks_symbol_exchange.sql, add_futures_contract_support.sql,
-- add_contfut_sec_type.sql, reconciled here so a fresh init matches a migrated DB):
--   * sec_type distinguishes the row kind — STK (equity) | FUTROOT (futures root,
--     an organizing parent) | FUT (a specific contract, e.g. ESZ26) | CONTFUT (a
--     derived continuous series, e.g. ES.c1).
--   * parent_stock_id is a self-FK: a FUT/CONTFUT row points at its FUTROOT row;
--     equities and roots stay NULL.
--   * expiry/multiplier describe a contract; NULL for equities and roots.
-- UNIQUE is on symbol alone (not symbol+exchange): every contract carries a
-- distinct normalised symbol (ESZ26 != ESU26 != ES), and futures registration
-- upserts via ON CONFLICT (symbol).
CREATE TABLE stocks (
  stock_id SERIAL PRIMARY KEY,
  symbol VARCHAR(20) NOT NULL,
  name VARCHAR(255) NOT NULL,
  exchange VARCHAR(50) NOT NULL,
  currency VARCHAR(3) NOT NULL,
  ib_con_id INTEGER,
  country VARCHAR(3),
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  parent_stock_id INTEGER REFERENCES stocks(stock_id),
  sec_type VARCHAR(10) NOT NULL DEFAULT 'STK',
  expiry DATE,
  multiplier NUMERIC(20,4),

  CONSTRAINT uq_stocks_symbol UNIQUE (symbol),
  CONSTRAINT ck_stocks_sec_type CHECK (sec_type IN ('STK', 'FUTROOT', 'FUT', 'CONTFUT'))
);

CREATE INDEX idx_stocks_symbol on stocks (symbol);
CREATE INDEX idx_stocks_exchange on stocks (exchange);
CREATE INDEX idx_stocks_country on stocks (country);
CREATE INDEX idx_stocks_parent_stock_id on stocks (parent_stock_id);

COMMENT ON COLUMN stocks.parent_stock_id IS
    'Self-FK: a FUT/CONTFUT row points at its FUTROOT row. NULL for equities and roots.';
COMMENT ON COLUMN stocks.sec_type IS
    'Row kind: STK (equity), FUTROOT (futures root/organizing parent), FUT (specific futures contract), CONTFUT (derived continuous series).';
COMMENT ON COLUMN stocks.expiry IS
    'Futures contract expiry (IB lastTradeDateOrContractMonth) as a DATE. NULL for STK/FUTROOT.';
COMMENT ON COLUMN stocks.multiplier IS
    'Futures contract multiplier (e.g. ES=50). NULL for STK/FUTROOT.';

-- OHLCV candles at an explicit timeframe. Generalised from the original
-- price_hourly table (migration add_bar_size_support.sql): bar_size is part
-- of the row identity, so a 1-minute and an hourly bar at the same instant
-- coexist instead of colliding. bar_size is the IB barSizeSetting string
-- ('1 hour', '1 min', '5 mins', '15 mins', ...); '1 hour' is the default so
-- legacy hourly inserts need no change. PK puts bar_size before timestamp so
-- the PK index directly serves the dominant size-scoped range read
-- (read_bars(stock_id, bar_size, start, end)).
CREATE TABLE price_bars (
  stock_id INTEGER NOT NULL REFERENCES stocks(stock_id),
  timestamp TIMESTAMPTZ NOT NULL,
  open NUMERIC(12,4) NOT NULL,
  high NUMERIC(12,4) NOT NULL,
  low  NUMERIC(12,4) NOT NULL,
  close  NUMERIC(12,4) NOT NULL,
  volume BIGINT NOT NULL,
  bar_size VARCHAR(16) NOT NULL DEFAULT '1 hour',

  PRIMARY KEY (stock_id, bar_size, timestamp)
);

CREATE INDEX idx_price_bars_ts ON price_bars (timestamp);

COMMENT ON TABLE price_bars IS
    'OHLCV candles at an explicit timeframe. One row per (stock_id, bar_size, timestamp). bar_size is the IB barSizeSetting string.';
COMMENT ON COLUMN price_bars.bar_size IS
    'Timeframe as an IB barSizeSetting string: ''1 hour'' (default/legacy), ''1 min'', ''5 mins'', ''15 mins'', ...';

-- Read back-compat: a view that looks exactly like the old price_hourly table
-- (hourly rows, no bar_size column). Writes go to price_bars directly
-- (INSERT ... ON CONFLICT is unsupported through a view).
CREATE VIEW price_hourly AS
    SELECT stock_id, timestamp, open, high, low, close, volume
    FROM price_bars
    WHERE bar_size = '1 hour';

COMMENT ON VIEW price_hourly IS
    'Read back-compat view: hourly rows of price_bars. Writes go to price_bars directly (ON CONFLICT is unsupported through a view).';

CREATE TABLE dividends (
  dividend_id SERIAL PRIMARY KEY, 
  stock_id INTEGER NOT NULL REFERENCES stocks(stock_id),
  ex_date DATE NOT NULL,
  pay_date DATE,
  amount NUMERIC(10,4) NOT NULL,
  currency VARCHAR(3) NOT NULL,
  frequency VARCHAR(20),

  CONSTRAINT uq_dividend UNIQUE (stock_id, ex_date, amount)
);

CREATE INDEX idx_dividend_stock ON dividends (stock_id);
CREATE INDEX idx_dividends_ex_date ON dividends (ex_date);

CREATE TABLE fundamentals (
    fundamental_id  SERIAL PRIMARY KEY,
    stock_id        INTEGER NOT NULL REFERENCES stocks(stock_id),
    period_end      DATE NOT NULL,              -- end of fiscal period
    period_type     VARCHAR(10) NOT NULL,       -- 'Q1','Q2','Q3','Q4','FY'
    fiscal_year     INTEGER NOT NULL,

    -- Income statement
    revenue             NUMERIC(18,2),
    cost_of_revenue     NUMERIC(18,2),
    gross_profit        NUMERIC(18,2),
    operating_expenses  NUMERIC(18,2),
    operating_income    NUMERIC(18,2),
    net_income          NUMERIC(18,2),
    eps_basic           NUMERIC(10,4),
    eps_diluted         NUMERIC(10,4),

    -- Balance sheet
    total_assets        NUMERIC(18,2),
    total_liabilities   NUMERIC(18,2),
    total_equity        NUMERIC(18,2),
    cash_and_equivalents NUMERIC(18,2),
    total_debt          NUMERIC(18,2),

    -- Cash flow
    operating_cash_flow NUMERIC(18,2),
    capex               NUMERIC(18,2),
    free_cash_flow      NUMERIC(18,2),

    -- Ratios (pre-calculated for fast screening)
    pe_ratio            NUMERIC(10,2),
    debt_to_equity      NUMERIC(10,4),
    gross_margin        NUMERIC(8,4),           -- as decimal, e.g. 0.45 = 45%
    operating_margin    NUMERIC(8,4),
    net_margin          NUMERIC(8,4),
    roe                 NUMERIC(8,4),           -- return on equity

    -- Shares
    shares_outstanding  BIGINT,

    -- Metadata
    source              VARCHAR(50) DEFAULT 'ib', -- 'ib', 'sec', 'manual' etc.
    created_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_fundamental UNIQUE (stock_id, period_end, period_type)
);

CREATE INDEX idx_fundamentals_stock ON fundamentals (stock_id);
CREATE INDEX idx_fundamentals_period ON fundamentals (period_end);

CREATE TABLE stock_tags (
    tag_id          SERIAL PRIMARY KEY,
    stock_id        INTEGER NOT NULL REFERENCES stocks(stock_id),
    category        VARCHAR(50) NOT NULL,       -- 'sector', 'therapy', 'product', 'theme'
    tag             VARCHAR(100) NOT NULL,      -- the keyword itself
    valid_from      DATE NOT NULL,              -- when this tag became relevant
    valid_to        DATE,                       -- NULL = still active
    notes           TEXT,                       -- optional context, e.g. "entered Phase III"

    CONSTRAINT uq_stock_tag UNIQUE (stock_id, category, tag, valid_from)
);

CREATE INDEX idx_stock_tags_stock ON stock_tags (stock_id);
CREATE INDEX idx_stock_tags_tag ON stock_tags (tag);
CREATE INDEX idx_stock_tags_category ON stock_tags (category);
CREATE INDEX idx_stock_tags_active ON stock_tags (stock_id, valid_to)
    WHERE valid_to IS NULL;

-- ============================================================
-- DATA FETCH LOG: Track what data has been fetched
-- ============================================================
-- Useful for knowing where gaps are and avoiding duplicate fetches
CREATE TABLE fetch_log (
    log_id          SERIAL PRIMARY KEY,
    stock_id        INTEGER NOT NULL REFERENCES stocks(stock_id),
    data_type       VARCHAR(20) NOT NULL,       -- 'price_hourly', 'fundamentals', 'dividends'
    fetch_start     TIMESTAMPTZ NOT NULL,
    fetch_end       TIMESTAMPTZ NOT NULL,
    bars_fetched    INTEGER,
    status          VARCHAR(20) DEFAULT 'success', -- 'success', 'partial', 'error'
    error_message   TEXT,
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_fetch_log_stock ON fetch_log (stock_id, data_type);

-- ============================================================
-- HELPER: auto-update updated_at on stocks table
-- ============================================================
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stocks_modtime
    BEFORE UPDATE ON stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- ============================================================
-- USEFUL VIEWS
-- ============================================================

-- Active tags for any stock
CREATE VIEW v_active_tags AS
SELECT
    s.symbol,
    s.exchange,
    st.category,
    st.tag,
    st.valid_from,
    st.notes
FROM stock_tags st
JOIN stocks s ON s.stock_id = st.stock_id
WHERE st.valid_to IS NULL
ORDER BY s.symbol, st.category, st.tag;

-- Latest fundamentals per stock
CREATE VIEW v_latest_fundamentals AS
SELECT DISTINCT ON (f.stock_id)
    s.symbol,
    s.exchange,
    f.*
FROM fundamentals f
JOIN stocks s ON s.stock_id = f.stock_id
ORDER BY f.stock_id, f.period_end DESC;






