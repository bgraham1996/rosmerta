---
FileType: 🚥 Dev Item
Type: Chore
Status: In Progress
Priority: 🔴
Bucket: data-quality
Cycle: "3"
DependsOn:
StartDate: 2026-11-12
EndDate:
tags:
---
> [!warning] Found by [[Full codebase sanity check and test audit]] on 2026-11-12; pulled into [[Cycle 3]] the same day.

> [!info] Progress 2026-11-12 (IB Gateway up).
> - **Prices refreshed:** `update_core` (hourly, `core`) → **97 updated, 3 empty, 0 errors; 55,838 bars written.** 97 stocks now end 2026-11-12. The 3 empties (ABB, APGE, DOMO) are unknown to IB ("No security definition"). APGE and DOMO are also missing from SEC's ticker map, which points to delisting or a rename.
> - **Trades: blocked by a bug.** The Flex ingest downloaded **84 trades but stored 0**: the stock lookup matched symbol+exchange (`NYSE` from Flex vs our `SMART` row), so the INSERT hit `uq_stocks_symbol` on ABT and the whole batch rolled back (no partial writes; checked). Fixed in **PR #21**. Re-run `fetch trades` after it merges.
> - ⚠️ IB **Warning 2174** on every equity request: `endDateTime` is sent without an explicit time zone, and IB says implied time zones "will be removed in the next API release". → [[IB API explicit time zone on historical requests]].

# Summary
Prod equity data hasn't been refreshed since July. All **99** priced stocks' hourly bars stop at **2026-07-17** (last bulk run 2026-07-20, `fetch_log`). IB Flex trades stop at **2026-07-24** (21 rows; `position` is empty). Every hourly analysis, the weekly report, and the dashboard's recent windows run on four-month-old data. `ABB` is in `core` but has **no bars at all**.

# Development Plan
- [x] Start IB Gateway; run `workflows/refresh_data/update_core.py` (hourly, `core`). → 97/100 current to 2026-11-12, 55,838 bars, no pacing errors.
- [ ] Investigate `ABB` (never fetched; IB has no `ABB` on SMART/USD) and `APGE`/`DOMO` (now unknown to both IB and SEC). Fix the symbols or drop them from `core`.
- [ ] Re-run the IB Flex trades ingest to bring `trade`/`transaction` current (prerequisite for [[Trades and Portfolio Analytics]]). → Blocked on PR #21 (84 trades waiting).
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
