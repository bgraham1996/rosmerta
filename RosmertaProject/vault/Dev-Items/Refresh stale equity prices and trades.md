---
FileType: 🚥 Dev Item
Type: Chore
Status: Planned
Priority: 🔴
Bucket: data-quality
Cycle: "3"
DependsOn:
StartDate:
EndDate:
tags:
---
> [!warning] Found by [[Full codebase sanity check and test audit]] on 2026-11-12; pulled into [[Cycle 3]] the same day.

# Summary
Prod equity data hasn't been refreshed since July. All **99** priced stocks' hourly bars stop at **2026-07-17** (last bulk run 2026-07-20, `fetch_log`). IB Flex trades stop at **2026-07-24** (21 rows; `position` is empty). Every hourly analysis, the weekly report, and the dashboard's recent windows run on four-month-old data. `ABB` is in `core` but has **no bars at all**.

# Development Plan
- [ ] Start IB Gateway; run `workflows/refresh_data/update_core.py` (hourly, `core`). It's incremental per stock from the last stored bar. Expect ~4 months × 99 stocks; watch IB pacing.
- [ ] Investigate `ABB` (never fetched; likely an ADR/exchange-qualification issue) and fix or drop it from `core`.
- [ ] Re-run the IB Flex trades ingest to bring `trade`/`transaction` current (prerequisite for [[Trades and Portfolio Analytics]]).
- [ ] Decide on a cadence (cron/launchd) so this doesn't silently go stale again; [[Data Refresh Workflows]] / [[Health and Init Command]] could surface last-bar age.

# Dependencies
##### Depends on
- IB Gateway running at `127.0.0.1:4001`; a valid Flex token.
##### Requires
- Nothing in code; operational.

# Links
##### Relevant Docs
- `workflows/refresh_data/update_core.py`, `price_retrival/trades_api.py`.
##### Prerequisite for
- [[Trades and Portfolio Analytics]], the weekly price report, any recent-window analysis.
