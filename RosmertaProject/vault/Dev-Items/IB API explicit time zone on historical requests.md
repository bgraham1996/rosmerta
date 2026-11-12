---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🔴
Bucket: ingestion
Cycle:
DependsOn:
StartDate:
EndDate:
tags:
---
> [!warning] Found 2026-11-12 during the prod price refresh ([[Refresh stale equity prices and trades]]).

# Summary (Tech Debt)
IB returns **Warning 2174** on every equity historical-data request: *"You submitted request with date-time attributes without explicit time zone. Please switch to use yyyymmdd-hh:mm:ss in UTC or use instrument time zone… Implied time zone functionality will be removed in the next API release."* The equity loop in `IBStockDataFetcher.get_hourly_bars` passes a **naive** `current_end` (`bars[0].date.replace(tzinfo=None) - step`). It works today, but **equity fetching will break** once IB ships the next API release. The futures path already uses tz-aware UTC (`_to_utc`).

# Development Plan
- [ ] Send `endDateTime` as tz-aware UTC (or the explicit `yyyymmdd-hh:mm:ss` UTC string) in the equity loop, as the futures path does.
- [ ] Confirm Warning 2174 is gone on a live run, and that bar timestamps/boundaries are unchanged (compare a week of AAPL hourly before/after).
- [ ] Unit test: the `endDateTime` passed to a fake `reqHistoricalData` is tz-aware.

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
