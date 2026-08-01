-- =============================================================
-- Returns: base daily-close primitive
-- =============================================================
-- Collapses the raw hourly bars in `price_hourly` down to ONE close
-- per stock per trading day: the last (chronologically latest) close
-- observed on that UTC calendar day. This is the shared foundation the
-- weekly / monthly / bi-monthly resamples build on, and it mirrors the
-- Python analysis layer:
--
--   * Timeframe resampling in utils/bars.py (OHLCV "close" -> "last").
--   * Asset.get_growth in data_models.py, which defines a return as a
--     simple close-to-close percent change (NOT log returns).
--
-- Dates are taken in UTC to match how data_models.py treats timestamps
-- (US market closes ~20:00-21:00 UTC, so the UTC calendar day equals the
-- trading day). No dividend adjustment — these are price returns only,
-- consistent with get_growth using `close` alone.
--
-- Run (reads DB_* from .env, same pattern as db/rebuild.sh):
--   PGPASSWORD="$DB_PASS" psql -v ON_ERROR_STOP=1 \
--     -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
--     -f db/scripts/returns_base.sql
-- =============================================================

CREATE OR REPLACE VIEW v_price_daily AS
SELECT DISTINCT ON (ph.stock_id, (ph.timestamp AT TIME ZONE 'UTC')::date)
    ph.stock_id,
    s.symbol,
    (ph.timestamp AT TIME ZONE 'UTC')::date AS day,
    ph.close
FROM price_hourly ph
JOIN stocks s ON s.stock_id = ph.stock_id
ORDER BY
    ph.stock_id,
    (ph.timestamp AT TIME ZONE 'UTC')::date,
    ph.timestamp DESC;   -- DISTINCT ON keeps this first row => the day's last close

COMMENT ON VIEW v_price_daily IS
    'One close per stock per UTC trading day (day''s last hourly close). '
    'Shared base for the v_returns_* views.';
