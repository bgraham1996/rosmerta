---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🔴
Bucket: ingestion
Cycle:
DependsOn:
StartDate: 2026-11-12
EndDate: 2026-11-12
tags:
---
> [!warning] Found 2026-11-12 during the prod price refresh ([[Refresh stale equity prices and trades]]).

> [!success] Fixed 2026-11-12 (PR "fix(ib_api): explicit UTC endDateTime on equity requests"). The equity loop now parses bounds to tz-aware UTC (`_as_utc_bound`), sends `endDateTime` as an aware datetime (ib_insync formats it `YYYYMMDD HH:MM:SS UTC`), and paginates with `_to_utc(bars[0].date) - step` instead of stripping tzinfo. **Live check vs IB (read-only):** old code raised Warning 2174 and new code raises none. AAPL hourly 2–12 Nov (60 bars) and 1-min 11 Nov (390 bars, crossing a chunk boundary) were **identical instant-for-instant** before and after. 8 new tests; suite 148 → 156.

# Summary (Tech Debt)
IB returns **Warning 2174** on every equity historical-data request: *"You submitted request with date-time attributes without explicit time zone. Please switch to use yyyymmdd-hh:mm:ss in UTC or use instrument time zone… Implied time zone functionality will be removed in the next API release."* The equity loop in `IBStockDataFetcher.get_hourly_bars` passes a **naive** `current_end` (`bars[0].date.replace(tzinfo=None) - step`). It works today, but **equity fetching will break** once IB ships the next API release. The futures path already uses tz-aware UTC (`_to_utc`).

# Development Plan
- [x] Send `endDateTime` as tz-aware UTC in the equity loop, as the futures path does.
- [x] Confirm Warning 2174 is gone on a live run, and that bar timestamps/boundaries are unchanged. → 1 → 0 warnings; AAPL hourly + 1-min identical.
- [x] Unit test: the `endDateTime` passed to a fake `reqHistoricalData` is tz-aware. → `tests/test_ib_explicit_timezone.py` (8 tests).

# Dependencies
##### Depends on
- Nothing.
##### Requires
- IB Gateway for the live check.

# Links
##### Relevant Docs
- `price_retrival/ib_api.py` (`get_hourly_bars` loop, `_to_utc`).
##### Prerequisite for
- Keeping [[Refresh stale equity prices and trades]] / `update_core` working past the next IB API release.
