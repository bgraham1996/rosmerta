---
FileType: 🚥 Dev Item
Type: Feature
Status: Done
Priority: 🔴
Bucket: core-components
Cycle: "1"
DependsOn: Futures schema - contracts as stocks
StartDate: 2026-09-23
EndDate: 2026-09-23
tags: 
---
> [!success] Done — 2026-09-23. Built source-agnostic (no IB dependency), on branch `worktree-futures-schema-migration` (unpushed). New `price_retrival/ohlcv_store.py` (vendor-neutral `upsert_hourly_bars` / `read_hourly_bars` over a psycopg2 conn + `(ts,o,h,l,c,v)` tuples; same `ON CONFLICT (stock_id, timestamp)` upsert; batch de-dup last-wins) and `price_retrival/futures.py` (idempotent root/contract registration + `get_child_contracts` root→children navigation ordered by expiry). `ib_api._save_bars_to_db` refactored to delegate to `ohlcv_store` — IB and non-IB now share one write path. Verified end-to-end against the live DB (register `ES` + `ESZ26`/`ESH27`, upsert synthetic candles, navigate, ranged read) then cleaned up — DB left untouched. 16 new tests in `tests/test_futures.py` against a write/nav-extended FakeDB; full suite green.

# Summary
The actual near-term goal: **functionality that gives us OHLCV candles for a futures contract**, stored in and read back from the DB. Deliberately **source-agnostic** — build and test it with manual / synthetic candles first; the IB retrieval (front-month, rolls, sessions) is a separate deferred item ([[Futures IB retrieval]]).

# Development Plan
- [x] **Store:** `ohlcv_store.upsert_hourly_bars(conn, stock_id, candles)` upserts into `price_hourly` (same `ON CONFLICT` as equities), decoupled from IB. `ib_api._save_bars_to_db` now delegates here.
- [x] **Read (per contract):** `ohlcv_store.read_hourly_bars` by `stock_id` (+ `futures.resolve_stock_id` for symbol → id); returns rows in `Asset`'s column order.
- [x] **Read (per root):** `futures.get_child_contracts(conn, root_symbol)` — parent→children navigation ordered by expiry. Front-month / roll / aggregation still **deferred**.
- [x] Synthetic contract + candles exercised the path end-to-end without IB (live-DB run, then cleaned up).
- [x] Tests against the FakeDB (`tests/test_futures.py`): determinism/last-wins, upsert idempotency + ON-CONFLICT overwrite, root→contracts enumeration.

# Dependencies
##### Depends on
- [[Futures schema - contracts as stocks]] (needs `parent_stock_id` + contract rows).
##### Requires
- `price_hourly` (unchanged).
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `db/init.sql` (`price_hourly`), `price_retrival/ib_api.py` (`_save_bars_to_db` as the pattern to generalise).
##### Prerequisite for
- [[Futures IB retrieval]], [[Futures in Asset and Market]].
