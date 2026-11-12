---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Planned
Priority: 🔴
Bucket: data-quality
Cycle: "3"
DependsOn: 
StartDate:
EndDate:
tags:
---
> [!warning] Found while restoring dividends on 2026-11-12 ([[Restore fundamentals and dividends in prod]]). **User decision (2026-11-12): load dividends from IB, not EDGAR.**

# Summary (Tech Debt)
The EDGAR dividends path records the wrong thing. For each 10-Q/10-K it stores the "dividends declared per share" figure as one payment dated at the fiscal period end. For PFE that leaves one Q2 year-to-date row per year since 2023 ($0.86 at end of June, against the real $0.43 × 4 = $1.72), with no ex-, pay or declaration dates. IB's dividend data has real per-payment dates and amounts, so IB becomes the source of truth.

# Development Plan
- [ ] Add a forced-IB path: `get_dividends(..., source='ib')` / `fetch|bulk dividends --source ib`. Today routing sends any ticker with a CIK to EDGAR, using IB only as a fallback.
- [ ] Decide what to do with the EDGAR path: drop it, or keep it only as an annual-total cross-check. Don't store YTD figures as payments.
- [ ] **Bug:** `fetch dividends --no-db` crashes. `EdgarDividendsFetcher.__enter__` calls `psycopg2.connect(**None)` when there's no `db_config`.
- [ ] **Bug:** `_get_or_create_stock` (dividends) looks up `symbol AND exchange`, but `stocks` is `UNIQUE(symbol)` since the dedupe migration. A ticker stored under another exchange misses the lookup, then the insert fails. Look up by symbol alone.
- [ ] Remove the 56 PFE `source='edgar'` pilot rows from prod (confirm first), then `bulk dividends --list core --source ib` with IB Gateway up. Check PFE 2025 = 4 × $0.43.
- [ ] Unit tests with a fake IB dividend response, and for the routing.

# Dependencies
##### Depends on
- IB Gateway at `127.0.0.1:4001` for the load.
##### Requires
- Nothing else.

# Links
##### Relevant Docs
- `price_retrival/dividends_api.py` (`get_dividends`, `EdgarDividendsFetcher`, `IBDividendsFetcher`, `_get_or_create_stock`), `main.py` `fetch|bulk dividends`.
##### Prerequisite for
- [[Restore fundamentals and dividends in prod]].
