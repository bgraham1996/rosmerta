---
tags:
DevStatus: Done
DependsOn: 
DevBucket: analysis
ReviewCount:
FileType: 🚥 Dev Item
StartDate:
EndDate: 2026-08-01
Priority: 🟡
Dev Status:
Cycle: "1"
ProjectCodes:
Requirements:
Completed: true
---

# Feature Summary
Forward-looking gain: for each bar, the return N trading days ahead — the raw material for "how often is price within ±X% N days after entry". **Done (2026-08-01).**

Implemented as:
- `@register("days_offset_gain")` in `indicators.py` — bar-count lookahead (`days_ahead * bars_per_day`), `mode` pct/abs, sorts by timestamp then shifts.
- `Asset.get_offset_growth()` + `Market.add_indicators()` in `data_models.py`.
- Tests in `tests/test_indicators.py` and `tests/test_data_models.py` (suite green, 42 passed).

Design note: uses **bar-count** lookahead (8 bars/day hourly, 1 daily, 1/5 weekly), not calendar-day — deliberate choice.
