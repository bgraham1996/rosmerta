---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟢
Bucket: analysis
Cycle:
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!info] Checked 2026-11-11 — **still open.** `64703ef` fixed cache *invalidation* (`clear_price_cache` cascades, `clear_growth` added) but `Asset.get_growth` (now `data_models.py:134`) still recomputes on every call with no `_growth_cache` guard.

# Summary
`Asset.get_growth` (`data_models.py:116`) recomputes the growth frame on **every** call and overwrites `_growth_cache`, instead of returning the cache when it exists — unlike every other `get_*` method on `Asset`, which short-circuits on a populated cache. The result is correct but the caching is dead weight (and inconsistent with the documented lazy-cache pattern), so repeated calls on a large frame do needless work.

# Development Plan
- [ ] Add the standard `if self._growth_cache is None:` guard so a second call returns the cache.
- [ ] Confirm `clear_growth()` / `clear_price_cache()` still invalidate correctly.
- [ ] Extend the existing `data_models` FakeDB test to assert the cache is reused (identity).

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Nothing.
# Links
##### Relevant Docs
- `data_models.py` (`get_growth`, `clear_growth`), `tests/test_data_models.py`.
##### Prerequisite for
- Nothing (pure cleanup).
