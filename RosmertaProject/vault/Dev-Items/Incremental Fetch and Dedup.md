---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🔴
Bucket: data-quality
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
Milestone **M2** — flagged in the project log as the *highest-leverage* work to do first. Today the fetchers re-request overlapping data every run. Before fetching, query the DB for what's already there and only pull forward of it: for prices, `start_date = max(timestamp) + 1h` per ticker; for fundamentals, skip any `(stock_id, period_end, period_type)` already present. The `fetch_log` table was designed for exactly this — make sure it's written *and* read. Also dedupe ticker lists with a `set()` (fixes the repeated PFE double-pull).

# Development Plan
- [ ] Read latest `timestamp` per `stock_id` from `price_hourly`; set IB `start_date` accordingly in `ib_api.py` / `bulk_fetch.py`.
- [ ] Read existing `(stock_id, period_end, period_type)` from `fundamentals`; skip those in `edgar_api.py`.
- [ ] Actually write + read `fetch_log` (last-success per ticker/kind) and use it in the `bulk` freshness path.
- [ ] Dedupe watchlist ticker lists with `set()` in `get_watchlist_tickers`.
- [ ] Tests against the FakeDB: "already have up to T → only requests > T".

# Dependencies
##### Depends on
- Nothing — pure enhancement to existing fetchers.
##### Requires
- `fetch_log` schema (exists in `db/init.sql`).
# Links
##### Relevant Docs
- `price_retrival/{ib_api,edgar_api,bulk_fetch}.py`, `db/init.sql` (`fetch_log`), Project Log M2.
##### Prerequisite for
- [[Data Refresh Workflows]] (daily/backfill/repair rely on incremental logic).
