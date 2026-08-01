---
tags:
DevStatus: 
DependsOn: 
DevBucket: docs
ReviewCount:
FileType: 🚥 Dev Item
StartDate:
EndDate: 
Priority: 🟢
Dev Status:
Cycle: "1"
ProjectCodes:
Requirements:
Completed: false
---

# Feature Summary
Write a **user-facing guide** for indicators: how the `@register` registry works, how to add one, and how to attach/read them on an `Asset` (`add_indicator`/`get_indicator`) and across a `Market` (`add_indicators`). The docstrings are good, but there's no single "how to" doc.

# Development Plan
- [ ] Short doc (repo `project/design/` or vault) covering: registry model, writing a `@register` fn, params & caching (`Indicator.key`), source columns (single series vs OHLCV like `obv`/`vwap`), consuming via `Asset`.
- [ ] Worked example: add a custom indicator end-to-end.
- [ ] Cross-link the built-ins (sma/ema/rsi/bollinger/obv/vwap/days_offset_gain).

# Dependencies
##### Depends on
- Nothing (nice to include MACD once it lands).
# Links
##### Relevant Docs
- `indicators.py`, CLAUDE.md → indicators section.
