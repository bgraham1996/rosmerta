---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🔴
Bucket: data-quality
Cycle: "3"
DependsOn: 
StartDate: 2026-11-18
EndDate: 2026-11-18
tags:
---
> [!success] **Source chosen 2026-11-18 (user decision): IB only.** A re-probe with the gateway up found a 4th option: IB's daily `ADJUSTED_LAST` bars divided by `TRADES` bars step up on each ex-date, and the step gives the cash amount. Checked on PFE, KO, MRK, ABBV, AAPL, NVO, O, BNTX and AMZN over 10 years: ex-dates match, amounts within ±$0.0001, splits don't trip it, non-payers give nothing. Limits: no pay/record/declaration dates, specials flagged by a size heuristic only, and pre-split/spin-off amounts are in today's share terms. Fundamentals are still refused (10358); tick 456 works on delayed data (next ex-date + amount) but isn't used yet. Free APIs (Massive, Tiingo, …) and Databento were compared and set aside for now.

> [!warning] Found while restoring dividends on 2026-11-12 ([[Restore fundamentals and dividends in prod]]).
>
> **Parked 2026-11-12 (user decision).** The plan was to load from IB, but a live probe showed IB refuses fundamentals for this account: `reqFundamentalData(PFE, 'ReportsFinSummary')` → **error 10358 "Fundamentals data is not allowed."** `IBDividendsFetcher` relies on that call alone, so it can't return dividend history, and the existing "IB fallback" for ADRs has never worked either. Options when this is picked up again:
> 1. **Fix the EDGAR extraction:** turn YTD figures into per-quarter declared amounts (Q2 YTD − Q1, …), dated by fiscal period end, not real ex/pay dates. No new dependency.
> 2. **IB snapshot tick** (generic tick 456): trailing-12-month total plus next ex-date and amount. Current data only, no history.
> 3. **A third-party history source** for real ex/pay dates (adds a dependency, maybe a cost).

# Summary (Tech Debt)
The EDGAR dividends path records the wrong thing. For each 10-Q/10-K it stores the "dividends declared per share" figure as one payment dated at the fiscal period end. For PFE that leaves one Q2 year-to-date row per year since 2023 ($0.86 at end of June, against the real $0.43 × 4 = $1.72), with no ex-, pay or declaration dates. IB's fundamentals feed would have given real per-payment dates and amounts, but it isn't available to this account (see above).

# Development Plan
- [x] Choose a source → **IB, derived from adjusted prices** (`source='ib_adjusted'`), 2026-11-18.
- [x] Remove the dead `reqFundamentalData` path; IB now raises when it has no bars for a ticker (no silent `[]`).
- [x] EDGAR path: kept as cross-check only, never routed to or stored.
- [x] **Bug:** `fetch dividends --no-db` crash: fixed (no DB connection unless storing).
- [x] **Bug:** symbol+exchange lookup: already fixed by PR #21.
- [x] Re-fetches replace the ticker's earlier `ib_adjusted` rows in the window (amounts can move by $0.0001, and `uq_dividend` includes `amount`).
- [x] Unit tests (`tests/test_dividends_ib_adjusted.py`, 14 tests).
- [x] Merge the PR → #33 `9885b6a` (2026-11-18).
- [x] Removed the 56 PFE `source='edgar'` rows (backup `db_exports/dividends_pfe_edgar_backup_2026-11-18.csv`), then `bulk dividends --list core` → 2,677 `ib_adjusted` rows for 82 of 97 `core` stocks (2016-11-22 → 2026-11-16); the other 15 pay no dividend. PFE 2025 = 4 × $0.43, KO 2025 = $2.04.
- [x] Special flag: compare each payment with its neighbours, not the 10-year median (the first rule flagged IBKR/NVDA dividend growth and SQM's variable payouts). Now flagged: COST ×3, F 2023, SMG 2020, TECK ×4, VST 2016.

Follow-ups (not in scope): tick 456 for the next announced ex-date; Flex Cash Transactions for dividends received (portfolio Decision 6).

# Dependencies
##### Depends on
- A decision on the data source ✅ (IB only).
##### Requires
- Nothing else.

# Links
##### Relevant Docs
- `price_retrival/dividends_api.py` (`get_dividends`, `EdgarDividendsFetcher`, `IBDividendsFetcher`, `_get_or_create_stock`), `main.py` `fetch|bulk dividends`.
##### Prerequisite for
- [[Restore fundamentals and dividends in prod]].
- [[Trades and Portfolio Analytics]] (dividend income and total return).
