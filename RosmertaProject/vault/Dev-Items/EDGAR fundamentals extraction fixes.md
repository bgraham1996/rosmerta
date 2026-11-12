---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Planned
Priority: 🟡
Bucket: data-quality
Cycle: "3"
DependsOn: 
StartDate:
EndDate:
tags:
---
> [!warning] Found while restoring fundamentals on 2026-11-12 ([[Restore fundamentals and dividends in prod]]).

# Summary (Tech Debt)
The `bulk fundamentals --list core` restore left four US filers wrong or missing, and the bulk summary hides one of the failures.

# Development Plan
- [ ] **GEHC: derived Q4 EPS overflow.** `_derive_q4_records` builds Q4 = FY − (Q1+Q2+Q3). GEHC spun off in Jan 2023, so FY2022 exists without its pre-spin quarterlies, and the derived Q4 2022 got `eps_basic = eps_diluted = 5,550,000`. That overflows `NUMERIC(10,4)`, and **the whole GEHC save rolls back** (0 rows stored). Fix: only derive Q4 when all three quarters exist (and sanity-bound per-share values); store per-record so one bad row can't drop the ticker.
- [ ] **Bulk tally miscount.** `bulk fundamentals` reported GEHC as ✓ / "0 failed" although `_save_fundamentals_to_db` failed (`fetch_log` has `status='error'`, "Failed to save to database"). The `FetchResult` should be `error` when the save fails.
- [ ] **AMZN: quarters labelled FY.** All 75 AMZN rows have `period_type='FY'`, including periods ending 2026-09-30; only 19 have revenue. Fiscal-quarter detection (`_detect_fy_end_month` / `_determine_fiscal_quarter`) misclassifies AMZN's facts. Compare with AAPL (correct).
- [ ] **APGE, DOMO: "not found in SEC company list".** These are US filers missing from the ticker→CIK map (renamed, delisted or acquired?). Verify on EDGAR; fix the symbol/CIK, or drop them from `core`.
- [ ] Regression tests with captured company-facts fixtures (spin-off FY-only year; AMZN-shaped facts).
- [ ] Re-run the four tickers into prod and confirm.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- EDGAR access (no IB).

# Links
##### Relevant Docs
- `price_retrival/edgar_api.py`, `price_retrival/bulk_fetch.py`, job log of the 2026-11-12 run.
##### Prerequisite for
- [[Restore fundamentals and dividends in prod]] (completeness), [[Fundamentals Gap Detection]].
