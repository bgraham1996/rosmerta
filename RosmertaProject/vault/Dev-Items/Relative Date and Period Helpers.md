---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟢
Bucket: analysis
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
Date handling is hand-rolled and error-prone across the codebase. `Asset`/`Market` take `'%Y-%m-%d %H:%M:%S'` UTC strings, and the workflow scripts fumble the maths — `macro_stats/markets_and_assets.py` has `start = td(now-365)` (a `timedelta` of a datetime minus an int — nonsense). You already stubbed the intent in `data-models/time-periods.py` (Week/Day/Month/Quarter/Year). Build small helpers so callers can express windows cleanly ("last 5 years", "YTD", "last N trading days") and get correctly-formatted UTC bounds.

# Development Plan
- [ ] Helpers returning the `(start_str, end_str)` bounds `Asset`/`Market` expect: `last_days(n)`, `last_years(n)`, `ytd()`, `between(a, b)`.
- [ ] Consider accepting `datetime`/`date` directly in `Asset`/`Market` (not just strings) to remove the formatting footgun.
- [ ] Decide whether to build out the Week/Day/Month/Quarter/Year period objects from `data-models/time-periods.py`, or keep it to the lightweight bound-helpers first.
- [ ] Unit tests (pure logic, no DB).

# Dependencies
##### Depends on
- Overlaps with [[Finish Directory Reorg]] (which flags the broken `data-models/time-periods.py`).
##### Requires
- Stdlib `datetime` (+ pandas offsets if trading-day aware).
# Links
##### Relevant Docs
- `data_models.py` (date parsing in `Asset`/`Market`), `data-models/time-periods.py` (stub intent), `workflows/macro_stats/markets_and_assets.py` (the `td(now-365)` bug).
##### Prerequisite for
- Ergonomic workflows (relative windows without hand-formatting date strings).
