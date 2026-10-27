---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: analysis
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
The 2026-04-24 log asked for **goals, not just features** — concrete numbers to look at per stock and across the book: 52-week high / low / average, average weekly and monthly movement, YTD price range, dividend stats, and a fundamental-to-price metric. Build this as a stats layer on `data_models.Asset` / `Market` and surface it in the Dash dashboard. This is the analytical payoff of having the data, and the home for the started `workflows/macro_stats/` work (e.g. `running_20d_returns.py`).

# Development Plan
- [ ] Per-asset stats on `Asset`: 52wk high/low/avg, YTD range, avg weekly/monthly move, realised vol.
- [ ] Dividend stats: yield, growth, payout cadence (from the `dividends` table).
- [ ] A fundamental-to-price metric (e.g. P/E, P/S) joining latest fundamentals to latest price.
- [ ] Cross-sectional versions on `Market` (percentile ranks across the watchlist).
- [ ] Fold in / formalise `workflows/macro_stats/` (running 20d returns, etc.).
- [ ] Dashboard view(s) to see them; FakeDB tests for the computations.

# Dependencies
##### Depends on
- Cleaner inputs help: [[Incremental Fetch and Dedup]], [[Dedupe Stocks and Watchlist Rebuild]].
##### Requires
- `price_hourly`, `dividends`, `v_latest_fundamentals`.
# Links
##### Relevant Docs
- `data_models.py` (`Asset`/`Market`), `utils/bars.py`, `workflows/macro_stats/running_20d_returns.py`, `dashboards/views/`, Project Log 2026-04-24.
##### Prerequisite for
- [[Basic Valuation Model]] (shares the stats/rank plumbing).
