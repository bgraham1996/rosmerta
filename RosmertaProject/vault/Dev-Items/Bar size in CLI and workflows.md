---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🟡
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size aware IB retrieval
StartDate: 2026-10-28
EndDate: 
tags: 
---
# Summary
Surface the new bar size to users: add a `--bar-size` option to the relevant CLI commands and wire it through the refresh workflows, so 1-minute (or 5/15-minute) retrieval is reachable without touching code.

# Development Plan
- [x] **CLI** (`main.py`): `--bar-size` on `fetch price`, `futures fetch` (both the `--expiry` and front-month paths), and `bulk price` (threaded via `_run_bulk_fetch` → `bulk_fetch_prices`). Default `'1 hour'`. Validated with `click.Choice(BAR_SIZES)` — an unknown value is rejected before any IB call. `BAR_SIZES` is a literal in `main.py` (importing `ib_api.SUPPORTED_BAR_SIZES` would pull ib_insync at CLI startup); a test asserts the two stay in sync. Rich titles/messages now name the bar size. Live-smoke: `fetch price AAPL --bar-size '1 min' --no-db` → 390 one-min bars.
- [x] **Workflows:** `BAR_SIZE` constant + `main(..., bar_size=...)` on `update_core` and `update_futures`, threaded into the fetch calls. **The incremental "last stored bar" lookup is now per-bar-size** (`get_last_price_dates`/`_last_bars` query `price_bars WHERE bar_size=…`), so a 1-min refresh extends from the last 1-min bar, not the last hourly bar. Sub-hourly first-backfill is **bounded** (`_default_start_for` / `_lookback_for`) instead of `DEFAULT_START='2020-01-01'`, and the inter-ticker/contract delay is larger for sub-hourly — both to respect IB's ~60-requests/10-min pacing. `update_futures` **skips the continuous rebuild** for non-hourly (sub-hourly continuous is a separate deferred feature; raw contract bars are still stored).
- [x] Keep every default `'1 hour'` so existing invocations are unchanged.
- [x] Update `project/design/futures.md` + `README.md` with `--bar-size` usage.

# Pacing note (answers "do we need harder chunking / a wait?")
- **Chunking** is already per-size in the retrieval layer (`_CHUNK_DAYS`); the workflow needs none — but it *does* need the bounded sub-hourly lookback + per-size incremental above, else a fresh 1-min ticker emits thousands of requests.
- **Pacing**: the existing per-chunk `ib.sleep(1)` + a bar-size-aware inter-ticker delay, plus per-size incremental (steady-state fetches only the gap), keep request counts under IB's soft limit for normal refreshes. A full token-bucket governor is **deferred** — only needed for large multi-year 1-min backfills.

# Tests
`tests/test_cli_bar_size.py` (choice validation, option presence, CLI↔fetcher set parity) and two `_default_start_for` cases in `tests/test_update_core_workflow.py`. Suite green (135 passed).

# Dependencies
##### Depends on
- [[Bar-size aware IB retrieval]] (the fetchers must accept `bar_size` first).
##### Requires
- `main.py`, `workflows/refresh_data/update_core.py`, `workflows/refresh_data/update_futures.py`.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `main.py`, `workflows/refresh_data/`, `project/design/futures.md`.
