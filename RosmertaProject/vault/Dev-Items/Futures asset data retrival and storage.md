---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: core-components
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title (kept your 🔴 / `core-components`). Edit freely. Migrated to the new schema + Cycle 2.

# Summary
Extend the pipeline beyond equities to **futures**: retrieve and store futures price data (a long-standing roadmap item — "futures-data workflow", "APIs by cadence: futures daily"). Today only equities exist (`stocks` + `price_hourly`). Futures bring contract expiries, rolls, and continuous series, so they need their own modelling rather than being forced into the equity tables.

# Development Plan
- [ ] Model futures: contract (symbol, expiry, exchange, multiplier) + a continuous/rolled series; decide new table(s) vs extending the schema.
- [ ] IB retrieval via `ib-insync` `Future` / `ContFuture`; handle roll logic and the front-month convention.
- [ ] Timeframe/session handling (futures trade nearly 24h — differs from equity sessions in `utils/bars`).
- [ ] Decide how `Asset`/`Market` consume futures (shared path vs parallel class).

# Dependencies
##### Depends on
- Nothing hard; sits alongside the equity fetchers.
##### Requires
- IB futures market-data permissions; `ib-insync`.
# Links
##### Relevant Docs
- `price_retrival/ib_api.py`, `db/init.sql` (`stocks`/`price_hourly`), `utils/bars.py`, Roadmap → futures-data workflow.
##### Prerequisite for
- Any futures-based analysis / stat-arb across asset classes.
