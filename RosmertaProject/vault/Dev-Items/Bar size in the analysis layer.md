---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🟡
Bucket: analysis
Cycle: "3"
DependsOn: Bar-size storage model
StartDate: 2026-10-28
EndDate: 
tags: 
---
# Summary
Let `data_models.Asset`/`Market` read any stored `bar_size`, not just hourly. Today they read `price_hourly` as the implicit base and `utils/bars.py` only resamples **upward** (hourly → daily/weekly/monthly). With `price_bars` now holding `1 min … 1 hour` (see [[Bar-size storage model]], [[Bar-size aware IB retrieval]]), the analysis layer needs to select which granularity it reads.

# Decision (2026-10-28)
**Single expanded `timeframe` + "read-what-you-ask".** Keep the existing single `timeframe` parameter and just widen its vocabulary; do **not** add a second `base_bar_size` param. The caller names the output granularity they want and the layer resolves it:

- **Intraday / hourly tokens read the matching stored `bar_size` directly (no resample):**
  `timeframe` token → stored `bar_size` →
  `'minute'`/`'1min'` → `'1 min'`, `'5min'` → `'5 mins'`, `'15min'` → `'15 mins'`, `'30min'` → `'30 mins'`, `'hourly'` → `'1 hour'`.
- **Calendar frames resample from the hourly base, exactly as today:** `'daily'`→`B`, `'weekly'`→`W-FRI`, `'monthly'`→`BME` (read `'1 hour'`, then `resample_ohlcv`). Back-compat: existing `hourly`/`daily`/`weekly`/`monthly` callers are unchanged.

**Why this shrinks the item:** read-what-you-ask means we do **not** resample *between* sub-hourly sizes (ask for 5-min → read stored 5-min rows, don't derive from 1-min). So **no new sub-hourly `RESAMPLE_RULES` are needed** — `utils/bars.py` is essentially untouched; the work is almost entirely in `data_models`.

**Explicitly deferred:** auto-discovering the *finest* stored base and deriving coarser sizes from it ("you asked for hourly, I found 1-min, so I'll roll it up"). Not stored for a requested size ⇒ you fetch it first ([[Bar-size aware IB retrieval]]); the layer does not silently substitute.

# What has to change (grounded in the code)
- **`data_models.Asset.get_prices`** (`data_models.py:85`) hard-codes `SELECT ... FROM price_hourly WHERE stock_id = %s AND timestamp …`. Replace with a `bar_size`-filtered read (via `ohlcv_store.read_bars`), where `bar_size` is resolved from `timeframe` by the mapping above. For calendar frames, read `'1 hour'` then resample (unchanged path at `data_models.py:93-99`).
- **`Asset.__init__` / `Market.__init__`** keep the single `timeframe` arg; `Market` already passes it straight through to each `Asset` (`data_models.py:404`).
- **`Asset.get_offset_growth`** (`data_models.py:323`) hard-codes `bars_per_day = {'hourly': 8, 'daily': 1, 'weekly': 1/5}`. This is the main landmine: it must grow entries for the intraday tokens — and the per-day count differs equity-RTH vs futures-~23h, so pair it with the session note below (or guard sub-hourly offset-growth behind a clear "needs session length" error until the tz item lands).
- **`utils/bars.py`** `VALID_TIMEFRAMES` must accept the new tokens; `RESAMPLE_RULES`/`resample_ohlcv`/`OHLCV_AGG` need **no** change under read-what-you-ask. Add the small `timeframe → bar_size` mapping here (home of timeframe conventions) or in `data_models`.

# Development Plan
- [x] Add the `timeframe → bar_size` mapping + widen `VALID_TIMEFRAMES`; validate unknown tokens loudly. → `utils/bars.py` `TIMEFRAME_BAR_SIZE` (1min/minute→'1 min', 5min→'5 mins', 15min→'15 mins', 30min→'30 mins', hourly→'1 hour'); `VALID_TIMEFRAMES = TIMEFRAME_BAR_SIZE ∪ RESAMPLE_RULES`; `Asset.__init__` now rejects an unknown timeframe.
- [x] Rework `Asset.get_prices` to read the resolved `bar_size` via `read_bars`; keep the daily/weekly/monthly resample-from-hourly path intact. → intraday/hourly tokens `read_bars(bar_size=…)` directly; calendar tokens read `'1 hour'` then `resample_ohlcv`. Also fixes a latent bug: `monthly` was declared valid but `get_prices` used to raise for it — it now resamples correctly.
- [x] Update `get_offset_growth`'s `bars_per_day` for the intraday tokens. → added 1min/minute=480, 5min=96, 15min=32, 30min=16 (consistent with the long-standing hourly=8 / ~8h-day basis); docstring flags the equity-RTH assumption and defers session-correct futures counts to the tz item. `monthly` intentionally still absent ("not supported" for day-based offset-growth, as before).
- [x] Unit tests against `FakeDB`: base selection (`5min` vs `hourly` isolation), unstored-size→empty (no silent substitution), invalid-timeframe rejection, and a daily resample-from-hourly regression. **128 passed.**

# Scope / non-goals
- **Session-aware daily boundaries for futures** (~23h sessions vs equity RTH / calendar midnight) overlap [[Cross currency and timezone support]]. This item scopes daily/weekly/monthly to the existing equity-style (UTC/business-day) behaviour; session-correct daily rollups are left to the tz item rather than half-solved here.
- No finest-base auto-derivation (deferred, see Decision).

# Dependencies
##### Depends on
- [[Bar-size storage model]] (a `bar_size` to select on) and [[Bar-size aware IB retrieval]] (data to read). Both land in PR #6 — this item builds on the same branch (reading `price_bars` can't ship before the migration).
##### Requires
- `data_models.py`, `utils/bars.py`, `tests/conftest.py`.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `data_models.py`, `utils/bars.py`. Related: [[Cross currency and timezone support]], [[Futures in Asset and Market]].
