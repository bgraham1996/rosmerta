---
FileType: 🚥 Dev Item
Type: Feature
Status: Planned
Priority: 🟡
Bucket: portfolio
Cycle: "3"
DependsOn: Flex Trade Timezone Fix, Refresh stale equity prices and trades
StartDate: 
EndDate: 
tags: 
---
> [!info] 2026-11-18: design revised against prod data → `project/design/portfolio-analytics.md` (ready for review). Trades are current (39, to 2026-11-04, UTC). **Found:** 4 sells (KO, PFE, ADSK, DOMO) have no buy in the data because the Flex query only covers 2026, so opening positions are missing (design Decision 3). OKLO is held but has no price bars. The Flex query has no commission field.

> [!info] Carried forward — headline [[V1001]] scope that didn't ship; scheduled into [[Cycle 3]] / [[V1002]]. Design: `project/design/portfolio-analytics.md`.


# Feature Summary
Now that executed trades land in `transaction` + `trade` (via `fetch trades`), build the **portfolio analytics layer**: realized/unrealized P&L, open positions, cost basis, holding periods, and per-trade outcomes. This is the payoff of the IB Flex work — turning raw executions into the position/performance picture the `position` table was designed for. Trader is **long-only** (buy then sell, never short), so position matching can assume that.

# Development Plan
- [x] **Design first** → `project/design/portfolio-analytics.md`: FIFO, derive-in-memory, opening positions, gross-then-net fees, price source. Decisions 3 and 4 need portal changes from you.
- [ ] Populate the `position` table from `trade` rows (open/close matching; `asset_status` holding/closed).
- [ ] A read-only `Portfolio`/`Positions` analysis class mirroring `data_models.Asset`/`Market` (reads DB, computes, lazy-caches).
- [ ] Metrics: realized P&L per closed position, current holdings + unrealized P&L (latest `1 hour` close from `price_bars`), holding-period distribution, win rate, avg gain/loss.
- [ ] Surface via a Dash view (`dashboards/views/`) and/or expand the `portfolio` CLI command.
- [ ] Tests against the FakeDB in `tests/`.

# Dependencies
##### Depends on
- IB Flex trades ingest ✅ (39 trades in prod, UTC).
- Design ✅ (`project/design/portfolio-analytics.md`); confirm its decisions 1–5.
##### Requires
- Latest-price lookup from `price_bars` (`ohlcv_store.read_bars`) for unrealized P&L.
# Links
##### Relevant Docs
- `project/design/portfolio-analytics.md`, `db/add_positions.sql`, `price_retrival/trades_api.py`.
##### Prerequisite for
- Any "how am I doing" performance dashboard / reporting.
