-- =============================================================
-- Returns: per-stock return series at four timeframes
-- =============================================================
-- Builds one "series" view per timeframe on top of v_price_daily
-- (db/scripts/returns_base.sql — must be applied first):
--
--   v_returns_daily      close-to-close per trading day
--   v_returns_weekly     week ending (Mon-Sun bucket; last close ~ Friday)
--   v_returns_monthly    calendar month (last close = last trading day)
--   v_returns_bimonthly  calendar bi-month, anchored to January
--                        (Jan-Feb, Mar-Apr, ... , Nov-Dec)
--
-- Each row is one period per stock:
--   symbol | period_end | close | ret
-- where `ret` is the SIMPLE return close / prev_close - 1 (a fraction,
-- e.g. 0.043 = +4.3%), matching Asset.get_growth's pct_change(). The
-- first period per stock has ret = NULL (no prior close), same as
-- pandas pct_change().dropna().
--
-- Weekly/monthly/bi-monthly reuse the "last close in the bucket" rule
-- via DISTINCT ON (... ORDER BY ... day DESC), exactly as v_price_daily
-- picks the day's last close — so this is resampling by close = last,
-- consistent with utils/bars.py OHLCV_AGG.
--
-- Query examples:
--   SELECT * FROM v_returns_weekly WHERE symbol = 'NVDA' ORDER BY period_end;
--   -- per-stock aggregates are a simple GROUP BY on any series view:
--   SELECT symbol, avg(ret), stddev_samp(ret)
--   FROM v_returns_monthly WHERE ret IS NOT NULL GROUP BY symbol;
--
-- Run:
--   PGPASSWORD="$DB_PASS" psql -v ON_ERROR_STOP=1 \
--     -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
--     -f db/scripts/returns_by_timeframe.sql
-- =============================================================

-- Daily -------------------------------------------------------
CREATE OR REPLACE VIEW v_returns_daily AS
SELECT
    stock_id,
    symbol,
    day AS period_end,
    close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY day), 0) - 1 AS ret
FROM v_price_daily;

-- Weekly (week ending; Postgres week = Mon-Sun, last close ~ Friday) --
CREATE OR REPLACE VIEW v_returns_weekly AS
WITH periodic AS (
    SELECT DISTINCT ON (stock_id, date_trunc('week', day))
        stock_id,
        symbol,
        day AS period_end,   -- actual last trading day in the week
        close
    FROM v_price_daily
    ORDER BY stock_id, date_trunc('week', day), day DESC
)
SELECT
    stock_id,
    symbol,
    period_end,
    close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY period_end), 0) - 1 AS ret
FROM periodic;

-- Monthly -----------------------------------------------------
CREATE OR REPLACE VIEW v_returns_monthly AS
WITH periodic AS (
    SELECT DISTINCT ON (stock_id, date_trunc('month', day))
        stock_id,
        symbol,
        day AS period_end,
        close
    FROM v_price_daily
    ORDER BY stock_id, date_trunc('month', day), day DESC
)
SELECT
    stock_id,
    symbol,
    period_end,
    close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY period_end), 0) - 1 AS ret
FROM periodic;

-- Bi-monthly (calendar 2-month blocks anchored to January) ----
-- Bucket key = first month of each block: month -> ((m-1)/2)*2 + 1
-- (Jan/Feb -> Jan, Mar/Apr -> Mar, ... , Nov/Dec -> Nov).
CREATE OR REPLACE VIEW v_returns_bimonthly AS
WITH periodic AS (
    SELECT DISTINCT ON (
            stock_id,
            make_date(extract(year FROM day)::int,
                      ((extract(month FROM day)::int - 1) / 2) * 2 + 1,
                      1))
        stock_id,
        symbol,
        day AS period_end,
        close
    FROM v_price_daily
    ORDER BY
        stock_id,
        make_date(extract(year FROM day)::int,
                  ((extract(month FROM day)::int - 1) / 2) * 2 + 1,
                  1),
        day DESC
)
SELECT
    stock_id,
    symbol,
    period_end,
    close,
    close / NULLIF(LAG(close) OVER (PARTITION BY stock_id ORDER BY period_end), 0) - 1 AS ret
FROM periodic;
