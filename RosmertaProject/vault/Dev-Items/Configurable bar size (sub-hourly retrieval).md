---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: ingestion
Cycle: "3"
DependsOn: 
StartDate: 2026-10-27
EndDate: 
tags: 
---
> [!note] Epic — decomposed 2026-10-27. Umbrella for making bar size configurable (primarily to enable **1-minute** retrieval). The actionable slices are broken out into their own items (see Phasing / Links). No code yet — this is the plan.

# Summary
Retrieval is **hard-coded to 1-hour bars** for both equities and futures; there is no way to request 1-minute (or any other) bar size. This epic adds a configurable **bar size** end to end — fetch → store → analyse → CLI — with 1-minute as the driving use case.

# Current state (grounds the plan)
Verified on `main` (2026-10-27):
- **Hard-coded bar size.** `IBStockDataFetcher.get_hourly_bars` (equities, `price_retrival/ib_api.py:266`) and `get_futures_hourly_bars` (futures, `ib_api.py:414`) both pass a literal `barSizeSetting='1 hour'`. No method takes a `bar_size`/`timeframe` argument.
- **Chunking is tuned for hourly.** Both loops request `durationStr = f"{min(30, …)} D"` — up to **30 days per request** (`ib_api.py:253`, `:404`). That is fine for hourly (IB allows ~a year of hourly per request) but **far exceeds IB's limit for 1-minute** bars (~1–2 days per request), so a 30-day request with `1 min` bars is rejected.
- **Storage is hourly-specific.** Everything writes to the **`price_hourly`** table via `ohlcv_store.upsert_hourly_bars`; there is no bar-size column, so the timeframe is implicit in the table name. `_log_fetch` also records `data_type='price_hourly'`.
- **Design intent = "fetch hourly, resample up."** `utils/bars.py` (`RESAMPLE_RULES`) only resamples **hourly → daily/weekly/monthly**; there is no sub-hourly path. `data_models.Asset`/`Market` read `price_hourly` and resample upward.

# Phasing
**Decide first (prerequisite):**
- [[Bar-size storage model]] — how multiple timeframes live in the DB (the pivotal decision; everything else depends on it).

**Then build:**
- [[Bar-size aware IB retrieval]] — `bar_size` param + bar-size-aware chunk durations + IB pacing, in the equity + futures fetchers.
- [[Bar size in the analysis layer]] — a sub-hourly **base** timeframe + resampling (1 min → 5/15 min / hourly) in `data_models` + `utils/bars`.
- [[Bar size in CLI and workflows]] — surface `--bar-size` on `fetch price` / `futures fetch` / `bulk price` / the refresh workflows.

# Decisions needed (resolve in the sub-items)
- **Which bar sizes to support?** Proposed first set: `1 min`, `5 mins`, `15 mins`, `1 hour` (IB's native sizes; we can resample between them). 1-minute is the only *new base* that needs fetching — the rest can be resampled.
- **Storage shape** — a `bar_size` column on a generalised bars table vs a separate `price_minute` table. See [[Bar-size storage model]].
- **Back-compat** — existing code/tests assume hourly; every new param defaults to `'1 hour'` so nothing changes until asked.

# Design principle
Keep the **DB the source of truth and the timeframe explicit** (not implicit in a table name). Store the bar size we actually fetched; resample on read for coarser views. Ties into [[Reconcile DB schema - init migrations and scripts]].

# Dependencies
##### Depends on
- Nothing hard (the storage decision gates the build slices).
##### Requires
- IB Gateway (1-minute market-data works on the paper account for the instruments we fetch).
# Links
##### Relevant Docs
- `price_retrival/ib_api.py`, `price_retrival/ohlcv_store.py`, `utils/bars.py`, `data_models.py`, `db/init.sql`. Futures reference: `project/design/futures.md`.
##### Prerequisite for
- Any intraday / sub-hourly analysis (finer indicators, intraday backtests, execution studies).
