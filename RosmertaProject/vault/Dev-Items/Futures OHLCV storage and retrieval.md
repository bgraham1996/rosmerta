---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: core-components
Cycle: "1"
DependsOn: Futures schema - contracts as stocks
StartDate: 
EndDate: 
tags: 
---
# Summary
The actual near-term goal: **functionality that gives us OHLCV candles for a futures contract**, stored in and read back from the DB. Deliberately **source-agnostic** — build and test it with manual / synthetic candles first; the IB retrieval (front-month, rolls, sessions) is a separate deferred item ([[Futures IB retrieval]]).

# Development Plan
- [ ] **Store:** upsert OHLCV candles for a given contract into `price_hourly` keyed by the contract's `stock_id` (same shape/`ON CONFLICT` as equities; decouple from the IB-specific `IBStockDataFetcher._save_bars_to_db` so a non-IB source can write too).
- [ ] **Read (per contract):** fetch a contract's candles by symbol (`ESH26`) or `stock_id` — this is largely the existing `price_hourly` read path.
- [ ] **Read (per root):** given a root (`ES`), list its **current / recent** child contracts (via `parent_stock_id`) and fetch a chosen contract's candles. This parent→children navigation is the only model functionality needed now; front-month selection / roll / cross-contract aggregation are **deferred** (need overlapping-contract aggregation logic first).
- [ ] Seed a synthetic contract + candles to exercise the path end-to-end without IB.
- [ ] Tests against the FakeDB in `tests/` (determinism, upsert idempotency, root→contracts enumeration).

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
