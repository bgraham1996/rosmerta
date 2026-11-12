---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🔴
Bucket: data-quality
Cycle:
DependsOn: 
StartDate:
EndDate:
tags:
---
> [!warning] Found while restoring dividends on 2026-11-12 ([[Restore fundamentals and dividends in prod]]).
>
> **Parked 2026-11-12 (user decision).** The plan was to load from IB, but a live probe showed IB refuses fundamentals for this account: `reqFundamentalData(PFE, 'ReportsFinSummary')` → **error 10358 "Fundamentals data is not allowed."** `IBDividendsFetcher` relies on that call alone, so it can't return dividend history, and the existing "IB fallback" for ADRs has never worked either. Options when this is picked up again:
> 1. **Fix the EDGAR extraction:** turn YTD figures into per-quarter declared amounts (Q2 YTD − Q1, …), dated by fiscal period end, not real ex/pay dates. No new dependency.
> 2. **IB snapshot tick** (generic tick 456): trailing-12-month total plus next ex-date and amount. Current data only, no history.
> 3. **A third-party history source** for real ex/pay dates (adds a dependency, maybe a cost).

# Summary (Tech Debt)
The EDGAR dividends path records the wrong thing. For each 10-Q/10-K it stores the "dividends declared per share" figure as one payment dated at the fiscal period end. For PFE that leaves one Q2 year-to-date row per year since 2023 ($0.86 at end of June, against the real $0.43 × 4 = $1.72), with no ex-, pay or declaration dates. IB's fundamentals feed would have given real per-payment dates and amounts, but it isn't available to this account (see above).

# Development Plan
- [ ] Choose a source (options above). ~~Add a forced-IB path~~: not viable, IB returns error 10358 for fundamentals.
- [ ] Make the IB path fail loudly: surface error 10358 instead of quietly returning `[]`, or remove the dead fallback.
- [ ] Decide what to do with the EDGAR path: drop it, or keep it only as an annual-total cross-check. Don't store YTD figures as payments.
- [ ] **Bug:** `fetch dividends --no-db` crashes. `EdgarDividendsFetcher.__enter__` calls `psycopg2.connect(**None)` when there's no `db_config`.
- [ ] **Bug:** `_get_or_create_stock` (dividends) looks up `symbol AND exchange`, but `stocks` is `UNIQUE(symbol)` since the dedupe migration. A ticker stored under another exchange misses the lookup, then the insert fails. Look up by symbol alone.
- [ ] Remove the 56 PFE `source='edgar'` pilot rows from prod (confirm first), then load `core` from the chosen source. Check PFE 2025 = 4 × $0.43.
- [ ] Unit tests for the chosen extraction and for the routing.

# Dependencies
##### Depends on
- A decision on the data source.
##### Requires
- Nothing else.

# Links
##### Relevant Docs
- `price_retrival/dividends_api.py` (`get_dividends`, `EdgarDividendsFetcher`, `IBDividendsFetcher`, `_get_or_create_stock`), `main.py` `fetch|bulk dividends`.
##### Prerequisite for
- [[Restore fundamentals and dividends in prod]].
