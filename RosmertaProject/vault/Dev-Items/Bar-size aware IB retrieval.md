---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size storage model
StartDate: 
EndDate: 
tags: 
---
# Summary
Make the IB fetchers **bar-size aware** so they can retrieve 1-minute (and 5/15-minute) bars, not just hourly. Replace the hard-coded `barSizeSetting='1 hour'`, make the chunk duration depend on the bar size (IB's per-size limits), and respect IB historical-data pacing (which 1-minute requests hit far faster).

# What has to change (grounded in the code)
- **`get_hourly_bars`** (equities, `ib_api.py`) and **`get_futures_hourly_bars`** (futures) both literal-pass `barSizeSetting='1 hour'` and chunk with `durationStr = f"{min(30, …)} D"`. `get_front_month_hourly_bars` and `bulk_fetch` delegate to these.

# Development Plan
- [ ] Add a `bar_size` parameter (default `'1 hour'` for back-compat) to `get_hourly_bars`, `get_futures_hourly_bars`, `get_front_month_hourly_bars`, and the `bulk_fetch` drivers; pass it to `barSizeSetting`.
- [ ] **Bar-size-aware chunking.** Replace the fixed `min(30, …) D` with a per-size max duration, e.g. a small map: `'1 min' → '1 D'` (IB caps ~1–2 days/req for 1-min), `'5 mins'/'15 mins' → a few days`, `'1 hour' → '30 D'` (today's behaviour). The chunk loop steps back by the actual bars returned (already does), just with a smaller window.
- [ ] **Respect IB pacing.** 1-minute pulls many more requests, so honour IB's historical-data limits: ≤ ~6 simultaneous/≈60 requests per 10 min, no identical request within ~15s, a courtesy sleep between chunks (already `ib.sleep(1)` — may need tuning). Surface clear errors on pacing violations rather than silent gaps.
- [ ] **Write with the bar size.** Pass `bar_size` to the generalised `ohlcv_store.upsert_bars` and to `_log_fetch` (depends on [[Bar-size storage model]]).
- [ ] Keep `whatToShow`/`useRTH` as-is (futures `useRTH=False`, equities `useRTH=True`); UTC conversion (`_to_utc`) is unchanged.
- [ ] Reality-check against the live gateway: a short 1-minute fetch for an equity and a futures contract, confirming bars store under the right `bar_size` and pacing holds.

# Open questions
- **1-minute history depth from IB** (how far back TRADES 1-min is available, esp. for expired futures) — validate the retention window.
- Whether to **auto-resample** a requested coarse size from a stored finer base vs always fetching the requested size from IB (prefer: fetch the base once, resample up — see [[Bar size in the analysis layer]]).

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
