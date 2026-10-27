---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: ingestion
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title; edit freely. (Originally added with the old template; migrated to the new schema and given Cycle 2, since live streaming can't be V1.)

# Summary
Today the system only pulls **historical** hourly bars in batch from IB (`IBStockDataFetcher.get_hourly_bars`). Add a **live/real-time data stream** so the latest prices update continuously rather than only on a manual fetch — feeding a live dashboard and, later, intraday signals. `ib-insync` supports this via `reqRealTimeBars` (5-second bars), `reqMktData` (ticks), or `reqHistoricalData(keepUpToDate=True)`; `nest-asyncio` is already a dependency, so the async model is partly in place.

# Development Plan
- [ ] Decide the mechanism: real-time 5s bars vs tick data vs `keepUpToDate` historical — and what timeframe(s) we actually need.
- [ ] Decide storage: append into `price_hourly` (aggregate up), a separate `price_live` table, or in-memory only for the dashboard.
- [ ] Handle the async/threading model (ib-insync event loop) and reconnection/heartbeat.
- [ ] Surface a live view in the Dash dashboard (auto-refresh).
- [ ] Reconcile with the batch fetchers so live + historical don't double-write.

# Dependencies
##### Depends on
- Nothing hard; interacts with the fetch layer.
##### Requires
- IB Gateway with live/delayed market-data permissions (`127.0.0.1:4001`); `ib-insync`, `nest-asyncio`.
# Links
##### Relevant Docs
- `price_retrival/ib_api.py` (`IBStockDataFetcher`), `dashboards/`, `db/init.sql` (`price_hourly`).
##### Prerequisite for
- Intraday signals; a real-time dashboard. Natural fit for the [[Redis and Workflow Task Manager]] streaming vision.
