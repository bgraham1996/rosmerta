---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🔴
Bucket: core-components
Cycle: "3"
DependsOn: 
StartDate: 2026-10-28
EndDate: 
tags: 
---
# Summary
The pivotal decision for [[Configurable bar size (sub-hourly retrieval)]]: **how multiple timeframes live in the DB.** Today the timeframe is implicit in the table name — `price_hourly` assumes exactly one row per `(stock_id, timestamp)` at hourly granularity — so 1-minute bars have nowhere correct to go. Decide the storage shape before touching the fetchers.

# The problem
- `price_hourly` PK/upsert is `(stock_id, timestamp)` (see `ohlcv_store._UPSERT_SQL`, `ON CONFLICT (stock_id, timestamp)`). A 1-minute and an hourly bar for the same stock at the same timestamp would **collide** on that key — the timeframe must become part of the identity.
- `ohlcv_store.upsert_hourly_bars` / `read_hourly_bars`, `data_models.Asset`/`Market` SELECTs, and `_log_fetch(data_type='price_hourly')` all hard-reference the hourly table/semantics.

# Options
- **(a) `bar_size` column on a generalised bars table (recommended).** Rename/extend to `price_bars(stock_id, timestamp, bar_size, o, h, l, c, v)` with PK `(stock_id, timestamp, bar_size)`. Timeframe is explicit (DB-as-source-of-truth); one write/read path serves all sizes. Cost: a migration that moves existing `price_hourly` rows in as `bar_size='1 hour'` (or a view named `price_hourly` for back-compat).
- **(b) Separate `price_minute` table.** Minimal blast radius (hourly code untouched), but duplicates the schema + write/read path per timeframe and doesn't scale to 5/15-min cleanly.
- **(c) Keep `price_hourly`, add `price_minute`, generalise `ohlcv_store`.** A pragmatic middle ground if only 1-min + 1-hour are ever wanted.

**Leaning (a):** one explicit, vendor-agnostic bars table. It fits the "timeframe explicit, resample on read" principle and avoids a table-per-timeframe sprawl. Confirm before building.

# Development Plan
- [x] **Decide** between (a)/(b)/(c) above (this is the deliverable of this item). → **(a)** chosen (2026-10-28): one generalised `price_bars` table + a `price_hourly` back-compat view.
- [x] Migration under `db/db_scripts/migrations/` implementing the choice; keep existing hourly rows (back-compat, likely a `price_hourly` view). Additive + idempotent, matching the futures-migration style. → `add_bar_size_support.sql`: renames `price_hourly`→`price_bars`, adds `bar_size` (default `'1 hour'`, backfills existing rows), swaps PK to `(stock_id, bar_size, timestamp)`, creates the `price_hourly` view. Idempotent (guarded rename); documented rollback. **Dry-run against prod (1.08M rows) applied + rolled back clean** — all rows preserved/backfilled, view intact, coexisting `'1 min'` bar proven. **NOT yet applied to prod**: must land with the code (current `main` fetchers write `price_hourly` directly and would break against the view) — apply at merge time.
- [x] Generalise `ohlcv_store`: `upsert_bars(conn, stock_id, candles, bar_size='1 hour')` / `read_bars(conn, stock_id, bar_size='1 hour', …)`; keep thin `*_hourly` wrappers for back-compat. All existing write callers (ib_api, continuous) go through the wrappers unchanged.
- [ ] Thread `bar_size` through `_log_fetch` (`data_type` reflects the size). → **deferred to [[Bar-size aware IB retrieval]]** (the fetch decides the size; `_log_fetch` lives in `ib_api` alongside it).
- [x] Reflect in `db/init.sql` **or** explicitly defer to [[Reconcile DB schema - init migrations and scripts]] (note which). → **deferred** to the reconcile item, consistent with the futures migration (which also left `init.sql` untouched). The migration is the source of truth for now.
- [x] Extend the `FakeDB` price model in `tests/conftest.py` to key on `(stock_id, timestamp, bar_size)`. → done; new `test_bar_sizes_coexist_at_same_instant` proves two sizes share an instant. Suite green (105 passed).

# Dependencies
##### Depends on
- Nothing hard.
##### Requires
- Live dev DB (localhost/rosmerta__prod) to apply/verify the migration.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `price_retrival/ohlcv_store.py`, `db/init.sql`, `db/db_scripts/migrations/`, `tests/conftest.py`. Related: [[Reconcile DB schema - init migrations and scripts]].
##### Prerequisite for
- [[Bar-size aware IB retrieval]], [[Bar size in the analysis layer]].
