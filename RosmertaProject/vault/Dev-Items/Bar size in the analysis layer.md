---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "3"
DependsOn: Bar-size storage model
StartDate: 
EndDate: 
tags: 
---
# Summary
Let the analysis layer read a **sub-hourly base** timeframe and resample from it. Today `data_models.Asset`/`Market` read `price_hourly` as the base and `utils/bars.py` only resamples **upward** (hourly → daily/weekly/monthly). To use 1-minute data we need the base timeframe to be selectable and the resampling rules to cover sub-hourly steps.

# What has to change (grounded in the code)
- **`data_models.Asset`** reads `price_hourly` (its `SELECT ... FROM price_hourly WHERE stock_id = %s AND timestamp …`) and treats hourly as the base; `timeframe` (`daily`/`weekly`/`monthly`) drives `utils/bars.resample_ohlcv` *up* from hourly.
- **`utils/bars.py`** `RESAMPLE_RULES` only defines daily/weekly/monthly — no 1-min → 5-min/15-min/hourly rules.

# Development Plan
- [ ] Add a **base bar size** to `Asset`/`Market` (which `bar_size` rows to read), defaulting to `'1 hour'` for back-compat; read via the generalised `ohlcv_store.read_bars` / updated `data_models` SELECT (filter on `bar_size`).
- [ ] Extend `RESAMPLE_RULES` / `resample_ohlcv` with sub-hourly steps (e.g. `1 min → 5 min / 15 min / 1 hour`), reusing the existing `OHLCV_AGG` aggregation.
- [ ] Decide the **resample-vs-fetch** policy: prefer reading the finest stored base and resampling up on demand, rather than storing every size. Document it.
- [ ] Session awareness for sub-hourly futures bars overlaps [[Cross currency and timezone support]] (~24h sessions vs equity RTH) — keep consistent.
- [ ] Unit tests against the `FakeDB`: base-timeframe selection + a 1-min → hourly resample equals the directly-fetched hourly series (determinism).

# Dependencies
##### Depends on
- [[Bar-size storage model]] (a `bar_size` to select on) and [[Bar-size aware IB retrieval]] (data to read).
##### Requires
- `data_models.py`, `utils/bars.py`, `tests/conftest.py`.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `data_models.py`, `utils/bars.py`. Related: [[Cross currency and timezone support]], [[Futures in Asset and Market]].
