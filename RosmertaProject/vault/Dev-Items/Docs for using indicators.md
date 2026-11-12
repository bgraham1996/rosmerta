---
FileType: 🚥 Dev Item
Type: Chore
Status: In Progress
Priority: 🟢
Bucket: docs
Cycle:
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!info] Largely delivered 2026-11-12 by PR #16 (`12d6f06`): `docs/indicators.md` covers the registry, the `Indicator` wrapper, the `source=` contract, recipes, how to add one, and the full catalog. **Remaining:** detailed sections for `ema_arb`, `rsi_ema_cross` and `trading_days_offset_gain` (catalog rows only so far).

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
