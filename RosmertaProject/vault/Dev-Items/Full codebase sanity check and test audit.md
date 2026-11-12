---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: In Progress
Priority: 🔴
Bucket: tech-debt
Cycle: "3"
DependsOn: 
StartDate: 2026-11-12
EndDate: 
tags: 
---
> [!info] Restored onto `main` 2026-11-11 from draft PR #7 (the item had only ever existed on that branch); scheduled into [[Cycle 3]] as the first item. Note it already records the bar-size migration as **applied to prod**. `init.sql` has since been reconciled for `price_bars` (PR #9) — the schema-reconciliation step here now hands off to [[Reconcile DB schema - init migrations and scripts]].

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

# Findings — pass 1 (2026-11-12, DB-side; IB Gateway was down)

> [!warning] Headline: the code mostly holds together, but **prod data is stale or missing.** Equity hourly prices stop at **2026-07-17** (all 99 stocks; last bulk run 2026-07-20). **`fundamentals` and `dividends` are empty (0 rows)**, apparently lost in the move to the local DB. Trades stop at 2026-07-24 (21 rows; `position` is empty). Spawned: [[Refresh stale equity prices and trades]] and [[Restore fundamentals and dividends in prod]].

**Code bug found and fixed (this PR):** `Market` over the real `core` list crashed in `get_panel`. Members with no bars in the window (AAL, ABB) returned an empty price frame with an *object*-typed `timestamp`, and the merge raised object-vs-datetime64 for the whole market. Fix: `Asset.get_prices` now normalises `timestamp` to tz-aware UTC. Regression test `test_market_get_panel_with_member_without_bars` fails without the fix. Verified on prod: a 3,906 × 101 panel, with AAL/ABB as all-NaN columns.

| Check | Result |
|---|---|
| Automated suite | ✅ 143 → **144** passing (+1 regression test). Coverage **47%**: analysis/indicators/futures/storage 82–95%; `edgar_api`, `dividends_api`, `trades_api`, `portfolio`, `update_futures` **0%**; `bulk_fetch` 15%; `main.py` (CLI) 25%; `ib_api` 32% → [[CI and Fetcher Test Coverage]]. |
| `price_hourly` view | ✅ Definition is `price_bars WHERE bar_size='1 hour'`; row parity exact (1,085,054 = 1,085,054). Only STK rows (no futures leak today). |
| View consumers | ✅ All run clean (read-only session): `watchlist_overview` SQL, weekly-report skill query, `returns_base.sql` body, both export SELECTs. ⚠️ **Latent:** the weekly report (no `stock_list`) and `returns_base.sql` read *every* hourly row, so they'd pull in FUT/CONTFUT the first time `update_futures` runs at its default `'1 hour'`. Add a `sec_type = 'STK'` filter before then. Note: `v_price_daily`/`v_returns_*` aren't deployed in prod. |
| Watchlist overview | ⚠️ Runs, but `net_income`/`period_end` are NULL for all 100 (fundamentals empty), and ABB is NULL on price (never fetched). |
| Live-DB scripts | `indicators_and_asset_test.py` ✅. `market_test.py` ⚠️ runs but is empty: it targets a `dividends` watchlist that doesn't exist in prod (only `core`); the same logic on `core` hit the bug above, now fixed. `dev_test/check_pfe_gaps.py` ⚠️ fails as `uv run dev_test/check_pfe_gaps.py` (`No module named 'db_config'`) but works with `PYTHONPATH=.`; output is just weekends/holidays. |
| Dashboard | ✅ App builds; all 4 callbacks run against prod. Deep-dive: PFE hourly/daily ✅, `es.c1` minute ✅, unknown ticker and empty range give clean messages. ⚠️ ESZ26 at `15min` is empty (only `1 min` stored; intraday frames don't resample from a finer base, a known deferred item). Any recent window shows "no data" for equities (stale prices). |
| Schema vs `init.sql` | Exact drift for [[Reconcile DB schema - init migrations and scripts]]: `init.sql` lacks `trade`/`transaction`/`transfer`/`position`, `watchlist_members`, `v_watchlists`; 5 `dividends` columns; 8 `fundamentals` columns. FK name on `price_bars` differs (cosmetic). Futures + `price_bars` parts match. |
| `fetch_log` | ⚠️ Says ESZ26 had 2,086 `price_hourly` bars fetched on 2026-11-02, but 0 FUT hourly bars exist; they were likely removed by a manual cleanup without the log row. Minor. |
| `.gitignore` | ⚠️ CLAUDE.md says `*.png` is ignored, but `.gitignore` line 11 is `.png` (a typo for `*.png`, matching only a file literally named `.png`), so `indicators_and_asset_test.py`'s `<TICKER>_price_levels.png` shows up as untracked. `.coverage` isn't ignored either. One-line fix. |
| IB-dependent checks | ⏸ **Blocked**: CLI end-to-end, 1-min round-trip, `update_futures`, `update_core` passes. Need IB Gateway on `127.0.0.1:4001`. |

# Development Plan
- [x] **Automated suite** — 144 passing; coverage per module recorded above.
- [x] **Back-compat view consumers** — all correct today; latent futures-leak risk noted above.
- [x] **Live-DB scripts** — run; script-level issues noted above (not code regressions).
- [x] **Dashboard** — all callbacks exercised against prod.
- [ ] **CLI end-to-end** — ⏸ needs IB Gateway.
- [ ] **1-minute round-trip** — ⏸ needs IB Gateway.
- [ ] **Futures pipeline** (`update_futures`) — ⏸ needs IB Gateway.
- [ ] **Workflows** (`update_core`) — ⏸ needs IB Gateway (and is also the fix for stale prices).
- [x] **Schema reconciliation** — exact drift handed to [[Reconcile DB schema - init migrations and scripts]].
- [x] **Coverage additions** — regression test for the `Market` crash; fetcher coverage → [[CI and Fetcher Test Coverage]].

# Original plan (kept for reference)
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
