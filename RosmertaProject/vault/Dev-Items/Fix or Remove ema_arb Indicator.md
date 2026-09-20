---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🟡
Bucket: indicators
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 2026-09-20
tags: 
---
# Summary
`indicators.py` registers a broken WIP indicator, `ema_arb` (lines ~86–90): it calls `prices.ewn(...)` — a typo for `.ewm` (no such pandas method → `AttributeError`), defaults `s_window=0` (invalid EMA span), and just `return True`. Because it's `@register("ema_arb")`, it's a live registry entry that crashes the moment anything computes it. Either finish it (presumably an EMA-crossover / arb signal) or remove it so the registry only contains working indicators.

# Development Plan
- [ ] Decide intent: a real EMA-spread/crossover indicator, or delete the stub.
- [ ] If keeping: fix `.ewn`→`.ewm`, give sane defaults (e.g. `s_window=9, l_window=20`), return the spread/signal Series (not `True`), and add a unit test in `tests/test_indicators.py`.
- [ ] If dropping: remove the function + registration.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Nothing.
# Links
##### Relevant Docs
- `indicators.py` (`ema_arb`, the `@register` registry), `tests/test_indicators.py`.
##### Prerequisite for
- A clean, all-working indicator registry (nothing that errors on compute).

> [!success] Done (2026-09-20)
> Kept and fixed. `ema_arb(prices, s_window=9, l_window=20)` now returns the short−long EMA spread (`.ewm`, `adjust=False`); no more `.ewn` typo / `return True`. Covered by `tests/test_indicators.py`. Landed on `main` in the fix-ema-arb-workflow consolidation (commit `bedeb39`).
