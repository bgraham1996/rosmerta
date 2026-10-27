---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟢
Bucket: indicators
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
`rsi` (`indicators.py:69`) computes `rs = avg_gain / avg_loss` with no guard: over a window with no down-moves, `avg_loss` is 0, so `rs` is `inf` and RSI collapses to exactly 100 (and `0/0` gives NaN). The formula converges to the right value in the limit, but relying on IEEE inf/NaN division is fragile and noisy. Make the edge cases explicit.

# Development Plan
- [ ] Define behaviour: `avg_loss == 0 & avg_gain > 0` → RSI 100; `avg_gain == 0 & avg_loss == 0` → NaN (or 50, decide) during warm-up.
- [ ] Implement with an explicit guard / `np.where` rather than raw division.
- [ ] Add a `tests/test_indicators.py` case for an all-up (and flat) series.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Nothing.
# Links
##### Relevant Docs
- `indicators.py` (`rsi`), `tests/test_indicators.py`.
##### Prerequisite for
- Nothing (robustness of an existing indicator).
