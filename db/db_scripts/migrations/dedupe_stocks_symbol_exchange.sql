-- =============================================================
-- Migration: De-duplicate the stocks table (symbol/exchange fork)
-- =============================================================
-- The stocks table ended up with two parallel seedings of the same
-- ~98 tickers, because uq_symbol_exchange enforces uniqueness on
-- (symbol, exchange) -- NOT on symbol alone:
--
--   * Old seeding  (created 2026-06-27): exchange = real listing venue
--                  (NYSE / NASDAQ / OTC). These rows have NO price bars,
--                  but every watchlist_members row points at them.
--   * New seeding  (created 2026-06-30): exchange = 'SMART' (IB routing).
--                  ALL price_hourly bars and fetch_log rows hang off
--                  these, but no watchlist references them.
--
-- The two halves never meet: Asset()/Market() resolve tickers, land on
-- the empty old rows, and read zero bars (max() on an empty close ->
-- ValueError). See data_models.py Asset.__init__ / get_stats.
--
-- The IB fetcher (price_retrival/ib_api.py _get_or_create_stock) looks up
-- by (symbol, exchange) and IB always reports 'SMART', so the SMART rows
-- are what every future fetch will hit. This migration therefore makes
-- the SMART rows canonical: it repoints watchlist_members onto them,
-- deletes the empty old duplicates, and replaces uq_symbol_exchange with
-- a stricter UNIQUE(symbol) so the table can't fork again.
--
-- Untouched by design: the two singleton symbols with no SMART twin,
-- AAL (NASDAQ) and ABB (NYSE) -- they have no price data anywhere and
-- stay as-is (still need a fetch before they'll analyse).
--
-- Safety: runs in a single transaction with post-condition assertions
-- that RAISE EXCEPTION (auto-rollback) if dedup or repointing is
-- incomplete, and only deletes old rows that (a) have a SMART twin and
-- (b) hold zero price bars. Review, then run interactively:
--
--   psql -U stock_user -d stocks -h <host> \
--        -f db_scripts/migrations/dedupe_stocks_symbol_exchange.sql
-- =============================================================

BEGIN;

-- -------------------------------------------------------------
-- Step 1. Repoint watchlist memberships from each old (non-SMART)
--         row onto its SMART twin (matched by symbol). The NOT EXISTS
--         guard skips any membership whose target SMART row is already
--         in that list, so the (stock_id, list_name) unique constraint
--         cannot be violated mid-update.
-- -------------------------------------------------------------
UPDATE watchlist_members wm
SET stock_id = s_new.stock_id
FROM stocks s_old
JOIN stocks s_new
  ON s_new.symbol = s_old.symbol
 AND s_new.exchange = 'SMART'
WHERE wm.stock_id = s_old.stock_id
  AND s_old.exchange <> 'SMART'
  AND NOT EXISTS (
        SELECT 1 FROM watchlist_members wm2
        WHERE wm2.list_name = wm.list_name
          AND wm2.stock_id  = s_new.stock_id
  );

-- -------------------------------------------------------------
-- Step 2. Remove any leftover memberships still pointing at an old row
--         that has a SMART twin (i.e. rows skipped in Step 1 because the
--         SMART membership already existed -- they are now redundant).
-- -------------------------------------------------------------
DELETE FROM watchlist_members wm
USING stocks s_old
WHERE wm.stock_id = s_old.stock_id
  AND s_old.exchange <> 'SMART'
  AND EXISTS (
        SELECT 1 FROM stocks s_new
        WHERE s_new.symbol = s_old.symbol
          AND s_new.exchange = 'SMART'
  );

-- -------------------------------------------------------------
-- Step 3. Delete the empty old duplicate stock rows: any non-SMART row
--         that has a SMART twin AND holds no price bars. Singletons
--         (AAL, ABB) have no SMART twin, so they are not matched.
--         (If an old row were still referenced by any other FK table,
--         the delete would error and roll the whole migration back.)
-- -------------------------------------------------------------
DELETE FROM stocks s_old
WHERE s_old.exchange <> 'SMART'
  AND EXISTS (
        SELECT 1 FROM stocks s_new
        WHERE s_new.symbol = s_old.symbol
          AND s_new.exchange = 'SMART'
  )
  AND NOT EXISTS (
        SELECT 1 FROM price_hourly p WHERE p.stock_id = s_old.stock_id
  );

-- -------------------------------------------------------------
-- Step 4. Post-condition assertions. Any failure raises and rolls back.
-- -------------------------------------------------------------
DO $$
DECLARE
    dup_symbols   int;
    stranded_refs int;
BEGIN
    -- 4a. No symbol may appear more than once anymore.
    SELECT count(*) INTO dup_symbols FROM (
        SELECT symbol FROM stocks GROUP BY symbol HAVING count(*) > 1
    ) d;
    IF dup_symbols > 0 THEN
        RAISE EXCEPTION 'Dedup incomplete: % symbol(s) still duplicated', dup_symbols;
    END IF;

    -- 4b. No watchlist membership may still point at an old row that had
    --     a SMART twin available (proves repointing succeeded).
    SELECT count(*) INTO stranded_refs
    FROM watchlist_members wm
    JOIN stocks s ON s.stock_id = wm.stock_id
    WHERE s.exchange <> 'SMART'
      AND EXISTS (
            SELECT 1 FROM stocks s2
            WHERE s2.symbol = s.symbol AND s2.exchange = 'SMART'
      );
    IF stranded_refs > 0 THEN
        RAISE EXCEPTION 'Repoint incomplete: % watchlist row(s) still on an old duplicate', stranded_refs;
    END IF;

    RAISE NOTICE 'Dedup OK: one row per symbol, watchlists repointed to SMART.';
END $$;

-- -------------------------------------------------------------
-- Step 5. Tighten the constraint so the table cannot fork again.
--         uq_symbol_exchange (symbol, exchange) is what allowed the
--         NYSE/SMART pair; replace it with UNIQUE(symbol).
--
--         NOTE: after this, ib_api._get_or_create_stock inserting a
--         second exchange for an existing symbol will raise instead of
--         silently duplicating. IB uses 'SMART' consistently so this is
--         the intended guard, but consider switching that lookup to
--         symbol-only if IB ever reports a different exchange.
-- -------------------------------------------------------------
ALTER TABLE stocks DROP CONSTRAINT IF EXISTS uq_symbol_exchange;
ALTER TABLE stocks ADD  CONSTRAINT uq_stocks_symbol UNIQUE (symbol);

COMMIT;

-- =============================================================
-- Rollback note: if you review the psql output and want to abort BEFORE
-- committing, run this file with `psql --single-transaction` and issue
-- ROLLBACK, or wrap the run in `BEGIN; \i <file>` minus the COMMIT above.
-- After COMMIT the deletes are permanent -- take a dump first if unsure:
--   pg_dump -U stock_user -d stocks -h <host> -t stocks -t watchlist_members \
--     > stocks_watchlist_backup.sql
-- =============================================================
