---
FileType: 🚥 Dev Item
Type: Chore
Status: Planned
Priority: 🟡
Bucket: data-quality
Cycle: "3"
DependsOn: EDGAR fundamentals extraction fixes
StartDate:
EndDate:
tags:
---
# Summary
Apply the merged [[EDGAR fundamentals extraction fixes]] (PR #23, `2e98396`) to prod. The code is fixed, but prod still holds the old extraction:
- **63 stale `FY` rows** at quarter ends: trailing-twelve-month figures from 10-Qs, mislabelled as fiscal years (AMZN 56, EXEL 6, VST 1).
- **Absurd derived Q4 EPS:** ADSK Q4 2011 = 271,365.64, BIIB Q4 2009 = 1,111.98, MRK Q4 2010 = −170,512.82.
- **SBUX FY2012/FY2013 revenue** taken from a non-10-K figure.
- **GEHC missing entirely:** its save rolled back on the EPS overflow.

The fetch **upserts** on `(stock_id, period_end, period_type)`, so re-fetching corrects existing rows and adds missing ones but **cannot remove** the stale FY rows. They need an explicit delete first; otherwise AMZN ends up with both an `FY` and a `Q1`/`Q2`/`Q3` row on the same date.

# Development Plan
- [ ] Back up the affected rows first: `\copy (SELECT f.* FROM fundamentals f JOIN stocks s USING (stock_id) WHERE s.symbol IN ('AMZN','EXEL','VST','ADSK','BIIB','MRK','SBUX','GEHC')) TO 'db_exports/fundamentals_pre_cleanup_<date>.csv' CSV HEADER` (`db_exports/` is gitignored).
- [ ] **Delete the 63 stale rows** in one transaction, matching each by `symbol` + `period_end` + `period_type='FY'`; check the deleted count is exactly 63 before committing:
  - **AMZN** (56): 2008-06-30, 2008-09-30, 2009-03-31, 2009-06-30, 2009-09-30, 2010-03-31, 2010-06-30, 2010-09-30, 2011-03-31, 2011-06-30, 2011-09-30, 2012-03-31, 2012-06-30, 2012-09-30, 2013-03-31, 2013-06-30, 2013-09-30, 2014-03-31, 2014-06-30, 2014-09-30, 2015-03-31, 2015-06-30, 2015-09-30, 2016-03-31, 2016-06-30, 2016-09-30, 2017-03-31, 2017-06-30, 2017-09-30, 2018-03-31, 2018-06-30, 2018-09-30, 2019-03-31, 2019-06-30, 2019-09-30, 2020-03-31, 2020-06-30, 2020-09-30, 2021-03-31, 2021-06-30, 2021-09-30, 2022-03-31, 2022-06-30, 2022-09-30, 2023-03-31, 2023-06-30, 2023-09-30, 2024-03-31, 2024-06-30, 2024-09-30, 2025-03-31, 2025-06-30, 2025-09-30, 2026-03-31, 2026-06-30, 2026-09-30
  - **EXEL** (6): 2015-03-31, 2015-06-30, 2015-09-30, 2016-03-31, 2016-06-30, 2016-09-30
  - **VST** (1): 2018-09-30
- [ ] **Re-fetch** with the fixed code: `bulk fundamentals` can't take an ad-hoc list, so run `uv run main.py fetch fundamentals <T>` for GEHC, AMZN, EXEL, VST, ADSK, BIIB, MRK, SBUX. Each should end "✓ Saved", which is now only printed on a real save.
- [ ] **Verify:** AMZN has FY + Q1–Q4 rows (≈92) and no FY row at a non-December quarter end; GEHC Q4 2022 EPS = 1.22; ADSK/BIIB/MRK Q4 EPS ≈ 0.26 / 1.06 / −0.17; SBUX FY2013 revenue = 14,892.2M; `fetch_log` has 8 `success` rows.
- [ ] Watchlist overview still shows net income for those tickers.

# Dependencies
##### Depends on
- [[EDGAR fundamentals extraction fixes]] (merged).
##### Requires
- **Explicit OK for the delete** (prod data). EDGAR access only, no IB.

# Links
##### Relevant Docs
- PR #23 (full diff of old vs new extraction); `price_retrival/edgar_api.py`.
##### Prerequisite for
- [[Restore fundamentals and dividends in prod]] (fundamentals completeness), [[Fundamentals Gap Detection]], [[Basic Valuation Model]].
