---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🔴
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size storage model
StartDate: 2026-10-28
EndDate: 
tags: 
---
# Summary
Make the IB fetchers **bar-size aware** so they can retrieve 1-minute (and 5/15-minute) bars, not just hourly. Replace the hard-coded `barSizeSetting='1 hour'`, make the chunk duration depend on the bar size (IB's per-size limits), and respect IB historical-data pacing (which 1-minute requests hit far faster).

# What has to change (grounded in the code)
- **`get_hourly_bars`** (equities, `ib_api.py`) and **`get_futures_hourly_bars`** (futures) both literal-pass `barSizeSetting='1 hour'` and chunk with `durationStr = f"{min(30, …)} D"`. `get_front_month_hourly_bars` and `bulk_fetch` delegate to these.

# Development Plan
- [x] Add a `bar_size` parameter (default `'1 hour'` for back-compat) to `get_hourly_bars`, `get_futures_hourly_bars`, `get_front_month_hourly_bars`, and the `bulk_fetch` drivers; pass it to `barSizeSetting`. → done; `SUPPORTED_BAR_SIZES = ('1 min','5 mins','15 mins','30 mins','1 hour')`, validated up front (unsupported size → clear error, no silent IB rejection).
- [x] **Bar-size-aware chunking.** → `_CHUNK_DAYS` map (max days/request): `1 min`→1, `5 mins`→5, `15 mins`→10, `30 mins`→20, `1 hour`→30 (today's behaviour). **Also fixed a latent gap bug**: the loop stepped the window back a fixed `1 hour`; for sub-hourly that skips 59 min at each chunk boundary. Now steps back exactly one bar (`_BAR_INTERVAL`); de-dup makes any overlap harmless.
- [~] **Respect IB pacing.** Kept the `ib.sleep(1)` courtesy between chunks; each chunk uses a distinct `endDateTime` so the ~15s identical-request rule isn't tripped. A full request-rate governor was **not** built — the live check showed no pacing errors at this volume; revisit if bulk sub-hourly over long ranges hits limits.
- [x] **Write with the bar size.** `_save_bars_to_db(…, bar_size)` and both fetch paths call `ohlcv_store.upsert_bars(…, bar_size=…)`; `_log_fetch` uses `_fetch_log_type` (`1 hour`→`price_hourly` back-compat, else `price_<size>`).
- [x] Keep `whatToShow`/`useRTH` as-is (futures `useRTH=False`, equities `useRTH=True`); UTC conversion (`_to_utc`) unchanged.
- [x] Reality-check against the live gateway. → **AAPL 1-min: 390 bars = one RTH session, modal/min gap 1 min. ESZ26 1-min: 1,380 bars across a full 23h day (22:00Z→20:59Z) with NO boundary gaps (confirms the step-back fix). AAPL 1-hour regression: 28 bars, gap 1h.** Fetched `save_to_db=False` (migration not yet on prod); storage into `price_bars` at the right `bar_size` is covered by the rolled-back migration dry-run + unit coexistence test.

# Open questions
- **1-minute history depth from IB** — ~~validate the retention window~~ → 1-min TRADES confirmed available for recent/live contracts (equity + live ES). Depth for **expired** futures (needed for continuous back-history at 1-min) still unvalidated — revisit when building sub-hourly continuous series.
- Whether to **auto-resample** a requested coarse size from a stored finer base vs always fetching the requested size from IB → **deferred to [[Bar size in the analysis layer]]** (retrieval fetches the requested size directly; resample-on-read belongs in the analysis layer).

# Status note
Code + unit tests (19 new, 124 total green) + live reality-check complete. CLI/workflow wiring is the separate item [[Bar size in CLI and workflows]]; the fetchers and the `bulk_fetch` driver now take `bar_size` but the click commands don't yet expose it.

# Dependencies
##### Depends on
- [[Bar-size storage model]] (needs a place to put sub-hourly bars).
##### Requires
- `ib-insync`, IB Gateway, 1-minute market-data permissions.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `price_retrival/ib_api.py`, `price_retrival/bulk_fetch.py`, `price_retrival/ohlcv_store.py`.
##### Prerequisite for
- [[Bar size in CLI and workflows]].
