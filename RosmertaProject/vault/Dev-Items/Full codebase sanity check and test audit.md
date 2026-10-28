---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🔴
Bucket: tech-debt
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
A full-codebase sanity check and test audit **after the futures work and the configurable-bar-size / 1-minute epic landed** (PR #6, merged 2026-10-28, and the earlier futures PRs). Those changes touched the central price schema (`price_hourly` → `price_bars` + a back-compat `price_hourly` view), the fetchers, the analysis layer, the CLI, and the refresh workflows — several modules were **not** changed and now lean on the back-compat view, so the goal is to confirm the whole system still hangs together end-to-end against the migrated prod DB, and to close obvious test-coverage gaps.

# Context — what just changed (verify against this)
- **Schema migration applied to prod** (`db/db_scripts/migrations/add_bar_size_support.sql`): `price_hourly` is now a **VIEW** over `price_bars` (PK `(stock_id, bar_size, timestamp)`); all legacy rows backfilled to `bar_size='1 hour'`. `db/init.sql` was **not** updated (deferred — see [[Reconcile DB schema - init migrations and scripts]]).
- **Futures** ("contracts-as-stocks": `FUTROOT`/`FUT`/`CONTFUT`, continuous series) — see [[Futures IB retrieval]], [[Futures OHLCV storage and retrieval]], [[Futures contract aggregation and continuous series]].
- **Configurable bar size / 1-minute** — [[Bar-size storage model]], [[Bar-size aware IB retrieval]], [[Bar size in the analysis layer]], [[Bar size in CLI and workflows]] (parent [[Configurable bar size (sub-hourly retrieval)]]).

# The risk surface (grounded in the code)
The migration generalised the **write/read** path (`ohlcv_store.upsert_bars`/`read_bars`) and `data_models` now reads `price_bars` directly, but several consumers still `SELECT ... FROM price_hourly` and now hit the **view** (hourly-only). These were not touched and must be confirmed still correct:
- `dashboards/views/watchlist_overview.py` (two `FROM price_hourly` reads).
- `.claude/skills/weekly-price-report/generate_report.py` (`from price_hourly ph`).
- `db/scripts/returns_base.sql`, `db/db_scripts/basic_price_export.sql`, `db/db_scripts/small_export.sql`.
- Root live-DB scripts `market_test.py`, `indicators_and_asset_test.py`, `dev_test/check_pfe_gaps.py` (read `price_hourly`).

# Development Plan
- [ ] **Automated suite**: `uv run pytest` — confirm green (135 tests as of the merge) and review for gaps; the fetchers (`ib_api`) only have helper-level unit tests, and the live-DB scripts are excluded from collection by design.
- [ ] **Back-compat view consumers**: exercise each `price_hourly` reader above against the migrated DB and confirm correct output (they should see the hourly subset via the view). Decide per-case whether any should move to `price_bars` + an explicit `bar_size`.
- [ ] **Live-DB scripts**: run `market_test.py` / `indicators_and_asset_test.py` / `dev_test/check_pfe_gaps.py` against prod; confirm no breakage from the rename/view.
- [ ] **Dashboard**: launch `uv run main.py dashboard` and confirm every view renders against the migrated schema.
- [ ] **CLI end-to-end** on the real DB: `fetch price` (hourly + `--bar-size '1 min'`), `bulk price`, `futures register`/`list`/`fetch`/`continuous`, `portfolio` — each should write/read `price_bars` correctly. Clean up any test rows afterwards.
- [ ] **1-minute round-trip**: fetch a small 1-min set for one equity and one futures contract → confirm stored under `bar_size='1 min'` → read back via `Asset(timeframe='1min')`; confirm hourly analysis is unaffected.
- [ ] **Futures pipeline**: `update_futures.main()` (hourly) end-to-end — register → incremental fetch → continuous `ES.c1`; confirm against the migrated DB. Clean up test data.
- [ ] **Workflows**: `update_core.main()` (hourly) and a bounded `update_core.main(bar_size='1 min')` dry pass — confirm per-bar-size incremental + bounded backfill behave and pacing holds.
- [ ] **Schema reconciliation**: confirm `db/init.sql` vs applied migrations (bar_size + futures) and either update it or explicitly hand off to [[Reconcile DB schema - init migrations and scripts]].
- [ ] **Coverage additions**: where the audit finds a gap (esp. around the view consumers and the fetch→store→read path), add targeted tests so regressions are caught.

# Known deferred follow-ons (track, don't necessarily fix here)
- Finest-base auto-derivation + session-aware daily boundaries for futures ([[Cross currency and timezone support]]).
- Sub-hourly continuous futures series (the `update_futures` continuous rebuild is hourly-only).
- A dedicated IB historical-data pacing governor (only needed for large multi-year 1-minute backfills).
- `db/init.sql` not yet reflecting the futures + bar_size migrations ([[Reconcile DB schema - init migrations and scripts]]).

# Dependencies
##### Depends on
- The merged work above (all on `main` as of 2026-10-28).
##### Requires
- Live prod DB (`localhost/rosmerta__prod`, migrated) + IB Gateway for the end-to-end checks; `uv run pytest` for the DB-free suite.
# Links
##### Relevant Docs
- Parent epic: [[Configurable bar size (sub-hourly retrieval)]]. `tests/`, `data_models.py`, `price_retrival/`, `dashboards/`, `workflows/refresh_data/`, `db/`. Related: [[Reconcile DB schema - init migrations and scripts]].
