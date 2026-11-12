---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🟡
Bucket: data-quality
Cycle: "3"
DependsOn: 
StartDate: 2026-11-12
EndDate: 2026-11-12
tags:
---
> [!warning] Found while restoring fundamentals on 2026-11-12 ([[Restore fundamentals and dividends in prod]]).

> [!success] Done 2026-11-12: code merged in **PR #23** (`2e98396`). Applying it to prod is tracked separately in [[Clean up and re-fetch corrected fundamentals]].

> [!info] Code fixed 2026-11-12 (PR #23). **Regression check:** old vs new extraction across all 84 `core` tickers with fundamentals (no DB). **77 unchanged**; the 7 that change are all corrections:
> - AMZN, EXEL and VST get their real quarters back (10-Q TTM rows were labelled FY).
> - ADSK, BIIB and MRK each had an absurd stored Q4 EPS (271,365.64; 1,111.98; −170,512.82), now 0.26, 1.06 and −0.17.
> - SBUX FY revenue now comes from the 10-K.
>
> Stock-split years (AAPL, NVDA, GOOGL…) are unchanged. **Still to do in prod (after merge):** re-fetch the 7 changed tickers + GEHC, and delete 63 stale `FY` rows that the upsert can't remove (AMZN 56, EXEL 6, VST 1; exact list in the PR). The delete needs explicit OK first.

# Summary (Tech Debt)
The `bulk fundamentals --list core` restore left four US filers wrong or missing, and the bulk summary hides one of the failures.

# Development Plan
- [x] **GEHC: derived Q4 EPS overflow.** *Corrected cause:* GEHC did have Q1–Q3. The FY2022 10-K's cover-page `shares_outstanding` is **100** (pre-spin-off), so Q4 EPS = $555M / 100 = 5,550,000, which overflowed `NUMERIC(10,4)` and rolled back the whole save. **Fix:** cross-check shares against the FY-implied count (FY NI / FY EPS); if it's more than 1000× off, use the implied count (GEHC Q4 2022 → 1.22). The 1000× tolerance leaves stock-split years alone. A final guard NULLs any EPS the column can't hold. Per-record saving not done: the guard removes the overflow, and per-ticker atomicity is kept.
- [x] **Bulk tally miscount.** Fixed: `get_fundamentals` records `last_save_ok`; `bulk fundamentals` reports a rolled-back save as `error`, and `fetch fundamentals` no longer prints "✓ Saved" when it wasn't.
- [x] **AMZN: quarters labelled FY.** *Cause:* AMZN reports trailing-twelve-month figures in its 10-Qs; >300-day duration → classified FY at every quarter end, and `get_fundamentals` then drops QR rows sharing a date with an FY row, so every quarter vanished. **Fix:** a >300-day figure is FY only from a `10-K*`; 10-Q TTM figures are skipped like YTD ones. AMZN: 75 FY-only → 92 (19 FY + quarters). Same bug in EXEL and VST.
- [ ] **APGE, DOMO: "not found in SEC company list".** Checked 2026-11-12: absent from a freshly downloaded SEC map (10,405 tickers, no name match either) and unknown to IB. Almost certainly acquired/delisted. **Not a code issue**; needs a decision to drop them from `core` (or re-point to a successor ticker).
- [x] Regression tests: `tests/test_edgar_extraction.py` (8 tests; 6 fail on the old code). Suite 156 → 164.
- [x] ~~Re-run into prod after merge~~ → moved to [[Clean up and re-fetch corrected fundamentals]].

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
