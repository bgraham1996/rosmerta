---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: portfolio
Cycle: "1"
DependsOn: Flex trades ingest (done)
StartDate: 
EndDate: 
tags: 
---

# Feature Summary
Now that executed trades land in `transaction` + `trade` (via `fetch trades`), build the **portfolio analytics layer**: realized/unrealized P&L, open positions, cost basis, holding periods, and per-trade outcomes. This is the payoff of the IB Flex work — turning raw executions into the position/performance picture the `position` table was designed for. Trader is **long-only** (buy then sell, never short), so position matching can assume that.

# Development Plan
- [ ] **Design first** → `project/design/portfolio-analytics.md`: trades → positions matching (FIFO vs average cost), long-only assumptions, P&L definitions.
- [ ] Populate the `position` table from `trade` rows (open/close matching; `asset_status` holding/closed).
- [ ] A read-only `Portfolio`/`Positions` analysis class mirroring `data_models.Asset`/`Market` (reads DB, computes, lazy-caches).
- [ ] Metrics: realized P&L per closed position, current holdings + unrealized P&L (latest price from `price_hourly`), holding-period distribution, win rate, avg gain/loss.
- [ ] Surface via a Dash view (`dashboards/views/`) and/or expand the `portfolio` CLI command.
- [ ] Tests against the FakeDB in `tests/`.

# Dependencies
##### Depends on
- IB Flex trades ingest ✅ (21 trades in prod).
- Position-matching design decision — needs `project/design/portfolio-analytics.md`.
##### Requires
- Latest-price lookup from `price_hourly` for unrealized P&L.
# Links
##### Relevant Docs
- `project/design/portfolio-analytics.md` (to write), `db/add_positions.sql`, `price_retrival/trades_api.py`.
##### Prerequisite for
- Any "how am I doing" performance dashboard / reporting.
