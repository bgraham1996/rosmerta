---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🟡
Bucket: data-quality
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema + Cycle 2.

# Summary
The analysis layer treats every date as UTC and prices as a single implicit currency. For ADRs / foreign tickers and any non-USD instrument that breaks down: cross-sectional comparisons mix currencies, and exchange-local sessions/timezones matter for bar alignment. Add explicit **currency** handling (metadata + FX conversion) and **timezone** awareness.

> [!warning] Concrete bug found 2026-10-27 (during [[Futures IB retrieval]]). `price_hourly.timestamp` is **`TIMESTAMPTZ`** and the dev DB session is **`Europe/London`**. The **equity** fetcher (`ib_api.get_hourly_bars`) drops tzinfo with `.replace(tzinfo=None)` and inserts a *naive* value, which Postgres reinterprets in the session tz — so equity bar instants are shifted by the London offset (0h in winter, 1h under BST). `data_models.Asset` queries with UTC-aware bounds, so range filters are silently off by that offset. Futures (`get_futures_hourly_bars`) now store **tz-aware UTC** correctly; the equity path still needs the same fix (plus a decision on migrating existing rows). Verify with `SELECT timestamp AT TIME ZONE 'UTC'` vs the true bar time.

# Development Plan
- [ ] **Fix the equity ingest tz bug (above):** store tz-aware UTC in `get_hourly_bars` like the futures path does; decide whether/how to re-timestamp existing `price_hourly` equity rows.
- [ ] Use/populate `stocks.currency`; add an FX-rate source (IB) and convert to a base currency for cross-sectional work.
- [ ] Make timezone explicit end-to-end (store/tag UTC, convert to exchange-local for session-aware resampling in `utils/bars`).
- [ ] Decide where conversion happens (ingest vs analysis) so raw data stays faithful.
- [ ] Tests for a non-USD ticker and a non-US-session instrument.

# Dependencies
##### Depends on
- Nothing hard.
##### Requires
- An FX-rate source; `stocks.currency` metadata.
# Links
##### Relevant Docs
- `data_models.py` (UTC date handling), `utils/bars.py` (resampling/sessions), `db/init.sql` (`stocks.currency`).
##### Prerequisite for
- Correct multi-market analysis; a precursor to [[Futures asset data retrival and storage]] (nearly-24h sessions).
