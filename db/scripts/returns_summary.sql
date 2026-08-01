-- =============================================================
-- Returns: pooled distribution summary across all stocks
-- =============================================================
-- Depends on returns_base.sql + returns_by_timeframe.sql (apply first).
--
-- Creates two views:
--
--   v_returns_all      the four per-stock series stacked into one long
--                      table with a `timeframe` label and `periods_per_year`
--                      annualization factor. NULL first-period returns are
--                      dropped here (WHERE ret IS NOT NULL). Handy primitive:
--                        SELECT * FROM v_returns_all WHERE symbol='NVDA';
--
--   v_returns_summary  ONE row per timeframe: the pooled (cross-sectional,
--                      all stocks together) distribution of returns. This is
--                      the headline "average returns and spread" table.
--                      Returns are reported in PERCENT (ret * 100).
--
-- Column notes for v_returns_summary:
--   mean_pct / median_pct   central tendency (mean vs median shows skew)
--   std_pct                 spread (sample stddev of period returns)
--   p5_pct / p95_pct        5th / 95th percentile (typical extreme range)
--   min_pct / max_pct       worst / best single-period move observed
--   pct_positive            share of periods with a positive return
--   ann_mean_pct            mean * periods_per_year (linear annualization)
--   ann_vol_pct             std  * sqrt(periods_per_year) (vol scales w/ sqrt-t)
--
-- Per-stock summaries are just a GROUP BY on v_returns_all, e.g.:
--   SELECT symbol, timeframe, count(*), avg(ret)*100
--   FROM v_returns_all GROUP BY symbol, timeframe;
--
-- Run:
--   PGPASSWORD="$DB_PASS" psql -v ON_ERROR_STOP=1 \
--     -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
--     -f db/scripts/returns_summary.sql
-- =============================================================

CREATE OR REPLACE VIEW v_returns_all AS
              SELECT 'daily'::text     AS timeframe, 1 AS ord, 252 AS periods_per_year, r.*
                FROM v_returns_daily     r WHERE r.ret IS NOT NULL
    UNION ALL SELECT 'weekly',           2, 52,  r.* FROM v_returns_weekly    r WHERE r.ret IS NOT NULL
    UNION ALL SELECT 'monthly',          3, 12,  r.* FROM v_returns_monthly   r WHERE r.ret IS NOT NULL
    UNION ALL SELECT 'bimonthly',        4, 6,   r.* FROM v_returns_bimonthly r WHERE r.ret IS NOT NULL;

COMMENT ON VIEW v_returns_all IS
    'The four v_returns_* series stacked long, labelled by timeframe with an '
    'annualization factor; NULL first-period returns dropped.';

CREATE OR REPLACE VIEW v_returns_summary AS
SELECT
    timeframe,
    count(*)                                                                    AS n_obs,
    count(DISTINCT symbol)                                                       AS n_stocks,
    round(avg(ret)::numeric * 100, 4)                                           AS mean_pct,
    round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY ret::float8)::numeric * 100, 4) AS median_pct,
    round(stddev_samp(ret)::numeric * 100, 4)                                   AS std_pct,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY ret::float8)::numeric * 100, 2) AS p5_pct,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ret::float8)::numeric * 100, 2) AS p95_pct,
    round(min(ret)::numeric * 100, 2)                                           AS min_pct,
    round(max(ret)::numeric * 100, 2)                                           AS max_pct,
    round((count(*) FILTER (WHERE ret > 0)::numeric / count(*)) * 100, 1)       AS pct_positive,
    round(avg(ret)::numeric * max(periods_per_year) * 100, 1)                   AS ann_mean_pct,
    round(stddev_samp(ret)::numeric * sqrt(max(periods_per_year))::numeric * 100, 1) AS ann_vol_pct
FROM v_returns_all
GROUP BY timeframe, ord
ORDER BY ord;

COMMENT ON VIEW v_returns_summary IS
    'Pooled distribution of price returns per timeframe (mean/median/std/'
    'percentiles/%positive + annualized), across all stocks. Returns in percent.';
