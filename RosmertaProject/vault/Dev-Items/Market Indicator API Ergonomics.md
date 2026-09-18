---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
The real workflow scripts under `workflows/` expose friction in the `Market`/`Indicator` API. In `macro_stats/stats.py` the author hand-writes **20** near-identical `Indicator('days_offset_gain', days=N)` + `add_indicators(...)` pairs; and both `stats.py` and `basic_feature_generation/daily_ema_fan.py` call `add_indicators` with the **wrong argument order / shape** (`add_indicators(indicator, conn)` and `add_indicators(conn, 'ema', [9])` vs the real `add_indicators(self, conn, indicator, source='close')`). When even the author calls it wrong, the API needs smoothing. Also: `Indicator(name, **params)` doesn't validate params, and `days_offset_gain`'s param is `days_ahead` (not `days`) — passing `days=5` fails only later at `compute`.

# Development Plan
- [ ] Add a batch helper: apply an indicator across a list/range of params in one call, e.g. `market.add_indicator_family('ema', 'window', [9,20,50,100,200])` and `add_indicator_range('days_offset_gain', 'days_ahead', range(5,101,5))`.
- [ ] Reconcile `add_indicators` signature / arg order so the common call is natural; update the workflow scripts to match.
- [ ] Fail fast: validate `Indicator(name, **params)` against the registered function's signature at construction (clear error on unknown/misspelled params) instead of deferring to `compute`.
- [ ] FakeDB tests for the batch helpers.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- `data_models.Market`, `indicators.Indicator`.
# Links
##### Relevant Docs
- `data_models.py` (`add_indicators`, `get_indicator_panel`), `indicators.py` (`Indicator`), `workflows/macro_stats/stats.py`, `workflows/basic_feature_generation/daily_ema_fan.py`.
##### Prerequisite for
- [[Feature Generation Workflow]] and [[Goals-Oriented Stats Layer]] (both apply many indicators across the market).
