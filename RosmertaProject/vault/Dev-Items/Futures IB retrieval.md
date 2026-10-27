---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🟡
Bucket: ingestion
Cycle: "2"
DependsOn: Futures OHLCV storage and retrieval
StartDate: 2026-10-27
EndDate: 
tags: 
---
# Summary
Wire **IB** as a real source for the futures OHLCV storage built in [[Futures OHLCV storage and retrieval]]. Storage/retrieval was deliberately built source-agnostic first ([[Futures OHLCV storage and retrieval]] — Done, merged to main); this item connects IB as the first real producer that feeds candles through `ohlcv_store.upsert_hourly_bars` and registers contracts via `price_retrival/futures.py`.

> [!note] Started 2026-10-27 — investigation only. Live read-only probe of IB Gateway (paper, `127.0.0.1:4001`, serverVersion 176) done against `ES` to pin down how IB represents and serves futures. No code written yet; findings below drive the build.

# IB investigation findings (2026-10-27, verified against live gateway)
Probed with `ib-insync` 0.9.86 using a dedicated `clientId` (read-only: `reqContractDetails` + `reqHistoricalData`, no orders).

- **Market-data permission is present.** Historical `TRADES` hourly bars for the front ES contract returned successfully (real OHLC + volume), so the paper account can pull CME futures history. No entitlement blocker for `ES` at least.
- **Term-structure enumeration works and is the right entry point.** `reqContractDetails(Future('ES', exchange='CME', includeExpired=False))` returned **21 live contracts** (quarterly H/M/U/Z out to 2031). Each `ContractDetails.contract` carries everything we need to populate a `stocks` contract row:
  - `symbol='ES'` → the **root**.
  - `lastTradeDateOrContractMonth='20261218'` (YYYYMMDD) → our `expiry` (DATE).
  - `localSymbol='ESZ6'` → IB's vendor symbol (**single-digit year**).
  - `conId=515416632` → our `ib_con_id`.
  - `multiplier='50'` (string) → our `multiplier`.
  - `tradingClass='ES'`, `currency='USD'`, `exchange='CME'`.
  - `ContractDetails.realExpirationDate` also available (equals `lastTradeDate` for ES).
- **Normalised symbol must be _constructed_, never copied from `localSymbol`.** IB's `localSymbol` is `ESZ6` — the single-digit year is ambiguous across decades (`ESZ6` could be 2026 / 2016 / 2036). Our canonical symbol is `ESZ26`. Build it from `root + <month code> + <2-digit year>` where the year/month come from `lastTradeDateOrContractMonth` (the full, unambiguous date), not from `localSymbol`. Month codes (CME): `F G H J K M N Q U V X Z` = Jan…Dec (ES observed as H/M/U/Z = Mar/Jun/Sep/Dec). This is exactly the "DB stores the normalised id, vendor id maps in" rule — `conId` lands in `ib_con_id`, the normalised symbol is the canonical key. See [[Futures schema - contracts as stocks]].
- **A bare `Future('ES', exchange='CME')` is ambiguous and raises on `qualifyContracts`.** You must either pass `lastTradeDateOrContractMonth`, or (preferred) enumerate via `reqContractDetails` and select the specific contract by `conId`. The enumerate-then-select path is cleaner and gives us the whole term structure for registration in one call.
- **Historical request shape (confirmed):** `reqHistoricalData(contract, endDateTime=..., durationStr='30 D', barSizeSetting='1 hour', whatToShow='TRADES', useRTH=False, formatDate=1)`. `useRTH=False` yields the full ~23h session (67 bars over a 3-day sample incl. a weekend; equity path uses `useRTH=True`). The existing 30-day chunking loop in `IBStockDataFetcher.get_hourly_bars` ports directly — only the contract construction, `exchange`, and `useRTH` differ.
- **Timezone caveat (decision needed — see below).** Futures bars return **tz-aware in exchange-local time** (`-05:00`, CME/Chicago). The equity path drops tzinfo (`.replace(tzinfo=None)`) and the DB convention in `data_models` treats stored naive timestamps as **UTC**. Storing Central-local naive timestamps would silently mislabel them. Must normalise to UTC (or a single documented tz) before `upsert_hourly_bars`.
- **Continuous series exists in IB:** `ContFuture('ES','CME')` qualifies to the current front month (`secType='CONTFUT'`, same `conId` as the front contract). A candidate producer for a future `CONTFUT` child row — but roll/aggregation is deferred (see [[Futures asset data retrival and storage]]).

# Development Plan
**Phase A — single named contract (the first real end-to-end path):**
- [ ] Add a futures fetch path alongside `get_hourly_bars` (either a `get_futures_hourly_bars` method or a `sec_type`-aware branch) that builds `Future(root, lastTradeDateOrContractMonth, exchange=..., currency=...)` instead of `Stock`, qualifies it, and fetches with `useRTH=False`, `whatToShow='TRADES'`, `barSizeSetting='1 hour'`, reusing the 30-day chunking loop.
- [ ] Normalise bar timestamps to UTC before handing `(ts,o,h,l,c,v)` tuples to `ohlcv_store.upsert_hourly_bars` (do **not** reuse the equity `.replace(tzinfo=None)` drop — that assumes exchange tz == UTC).
- [ ] Register the contract via `futures.register_contract(...)` with the **constructed** normalised symbol (`ESZ26`), `expiry`, `multiplier`, and `ib_con_id=conId`; write candles against the returned `stock_id`.

**Phase B — term-structure registration:**
- [ ] `reqContractDetails(Future(root, exchange=..., includeExpired=False))` → register the root (`futures.get_or_create_root`) and all live child contracts in one pass (idempotent `ON CONFLICT (symbol)`), each carrying its `conId`/`expiry`/`multiplier`.
- [ ] A normalisation helper: IB `ContractDetails` → `(normalised_symbol, expiry, multiplier, ib_con_id)`. Unit-testable **without IB** by feeding it captured contract dicts (fixtures) — keep it a pure function so it tests against the FakeDB-style suite.

**Phase C — selection / roll (coordinate with deferred items):**
- [ ] Front-month selection (nearest unexpired `expiry`) for "fetch the active contract".
- [ ] Continuous series via `ContFuture` and/or our own roll logic — **deferred**, needs overlapping-contract aggregation first (owned by [[Futures asset data retrival and storage]] / [[Futures in Asset and Market]]).

**Phase D — CLI / bulk wiring (optional, after A–B):**
- [ ] Expose through `main.py` (lazy-import + Rich, matching `fetch`/`bulk`), e.g. `fetch future <ROOT> [--expiry ...]` and a term-structure registration command. Decide whether futures roots join the watchlist/bulk model or stay a separate command.

# Decisions needed
- **Timezone storage convention for futures candles.** Options: (a) convert all futures bars to UTC before store (consistent with the `data_models` "stored == UTC" assumption — **recommended**); (b) store exchange-local and record tz in schema (bigger change). Overlaps with [[Cross currency and timezone support]] — resolve there if that item is active.
- **Exchange per root.** `ES`→CME; later roots (CL→NYMEX, ZN/ZB→CBOT, GC→COMEX). Store the real exchange on each `stocks` row (already captured by `register_contract(exchange=...)`), never `SMART`.
- **Registration granularity.** Register the full live term structure (21 rows for ES) vs only contracts we actually fetch. Leaning: register what we fetch (Phase A) and offer full enumeration as an explicit action (Phase B), to avoid 20+ idle root-children per product.

# Open questions / deferred
- **Front-month roll rule** (expiry-based vs volume/OI-based) — deferred; needs the aggregation logic from [[Futures asset data retrival and storage]].
- **Expired-contract backfill** (`includeExpired=True`) for history beyond the current front — not needed for the first cut.
- **Non-equity-index products** (energy, rates, metals) may have different `multiplier`/session/tz quirks; validate the normalisation helper against a second root (e.g. `CL`) before generalising.

# Dependencies
##### Depends on
- [[Futures OHLCV storage and retrieval]] (Done — `ohlcv_store` + `futures.py` write path), [[Futures schema - contracts as stocks]] (Done — migration applied to `rosmerta__prod`).
##### Requires
- `ib-insync` (0.9.86), IB Gateway (paper `127.0.0.1:4001`), futures market-data permissions (confirmed present for `ES`).
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `price_retrival/ib_api.py` (equity pattern to generalise), `price_retrival/ohlcv_store.py` (`upsert_hourly_bars` — the write path), `price_retrival/futures.py` (`get_or_create_root` / `register_contract`).
- Related: [[Futures in Asset and Market]], [[Cross currency and timezone support]].
##### Prerequisite for
- [[Futures in Asset and Market]] (needs real contracts + candles in the DB).
