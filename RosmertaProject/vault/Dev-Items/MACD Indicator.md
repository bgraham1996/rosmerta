---
FileType: 🚥 Dev Item
Type: Feature
Status: Done
Priority: 🟡
Bucket: indicators
Cycle: "1"
DependsOn: 
StartDate: 
EndDate: 2026-09-20
tags: 
---

# Feature Summary
Add a **MACD** (Moving Average Convergence Divergence) indicator to the registry in `indicators.py`, alongside `sma/ema/rsi/bollinger/obv/vwap`. MACD line = EMA(fast) − EMA(slow); signal = EMA(MACD, signal); histogram = MACD − signal. Defaults 12/26/9 on `close`.

> [!success] Done (2026-09-20)
> Ported into `main` (commit `167e64e`) as `@register("macd")` returning `macd`/`signal`/`histogram`, with a unit test in `tests/test_indicators.py`. Landed alongside `atr`, `stochastic`, `adx` from the same original branch. Registry now 14 indicators; suite green (70 passed).

# Development Plan
- [ ] Write a `@register("macd")` function returning a DataFrame with `macd`, `signal`, `histogram` columns (reuse the existing `ema` logic).
- [ ] Params: `fast=12`, `slow=26`, `signal=9`, `column='close'`.
- [ ] Add a DB-free unit test in `tests/test_indicators.py`, mirroring the `ema`/`bollinger` tests (assert macd = ema(fast) − ema(slow), shape, registry membership).
- [ ] Confirm it flows through `Asset.add_indicator` / `get_indicator` with no extra wiring.

# Dependencies
##### Depends on
- Nothing — `ema` already exists to build on.
##### Requires
- pandas (already a dep).
# Links
##### Relevant Docs
- `indicators.py` (`@register` registry), `tests/test_indicators.py`, CLAUDE.md → indicators.
##### Prerequisite for
- Dashboard indicator overlays; MACD-based signals in the `asset_stats` view.
