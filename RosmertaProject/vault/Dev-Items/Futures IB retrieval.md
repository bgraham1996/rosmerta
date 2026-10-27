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
**Phase A — single named contract (the first real end-to-end path):** *(built 2026-10-27; unit-tested; live gateway verify pending)*
- [x] Add a futures fetch path alongside `get_hourly_bars` — `IBStockDataFetcher.get_futures_hourly_bars(root, expiry, exchange, ...)`: builds `Future(root, lastTradeDateOrContractMonth, exchange, currency)` instead of `Stock`, qualifies it (bare root rejected as ambiguous), fetches with `useRTH=False`, `whatToShow='TRADES'`, `barSizeSetting='1 hour'`, reusing the 30-day chunking loop.
- [x] **UTC decision made (user-confirmed): convert to UTC before storing.** `_to_utc` converts each tz-aware exchange-local bar to **tz-aware UTC** before `ohlcv_store.upsert_hourly_bars`. Must stay tz-aware: `price_hourly.timestamp` is `TIMESTAMPTZ`, so a naive insert is reinterpreted in the DB session tz (`Europe/London`) and shifts the instant (see "Decisions made").
- [x] Register the contract via `futures.register_contract(...)` with the **constructed** normalised symbol (`normalise_future_symbol`, e.g. `ESZ26`), `expiry`, `multiplier`, and `ib_con_id=conId`; candles written against the returned `stock_id`.
- [x] Pure, IB-free symbol normalisation helper `futures.normalise_future_symbol(root, last_trade_date)` with unit tests (YYYYMMDD/YYYYMM/date inputs, quarterly month codes, bad-input rejection).
- [x] **Live end-to-end verification against IB Gateway (2026-10-27).** `get_futures_hourly_bars('ES', '20261218', 'CME', ...)` fetched 92 real hourly bars, registered `ES`→`ESZ26` (conId 515416632, expiry 2026-12-18, mult 50), stored them, and the stored **true UTC instant matches the bar's real time** (e.g. 22:00Z). Test rows cleaned up after. (First attempt was blocked by a gateway API-handshake timeout; resolved after the user updated IB Gateway.)

**Phase B — term-structure registration:** *(built + live-verified 2026-10-27)*
- [x] `IBStockDataFetcher.register_futures_term_structure(root, exchange, ...)`: `reqContractDetails(Future(root, exchange=..., includeExpired=False))` → registers the root (`futures.get_or_create_root`) and all live child contracts in one pass (idempotent `ON CONFLICT (symbol)`), each carrying its `conId`/`expiry`/`multiplier`, via `futures.register_term_structure`. No candles fetched here — registration only; pull bars per contract with `get_futures_hourly_bars`.
- [x] Pure, IB-free mapping helper `futures.normalise_ib_future(contract)` (accepts an ib-insync `Contract` **or** a dict) → `{root, symbol, expiry, multiplier, ib_con_id, exchange, currency}`, plus `futures.register_term_structure(conn, root, contracts)` (batch, expiry-ordered, idempotent). Unit-tested against captured contract fixtures + FakeDB (6 tests).
- [x] **Live verify (2026-10-27):** enumerated + registered the full **ES term structure — 21 contracts** (ESZ26…ESZ31) in expiry order, each with correct normalised symbol / expiry / multiplier / `conId` / `parent_stock_id=ES`; then cleaned up (table back to 102 STK).

**Phase C — selection / roll (coordinate with deferred items):** *(front-month built + live-verified 2026-10-27)*
- [x] **Front-month selection.** `futures.get_front_contract(conn, root, as_of=None)` — nearest `expiry >= as_of` (default today UTC) from the registered chain; expired-only chain → `None`. Pure/DB, unit-tested (nearest pick, roll past expired, on-expiry inclusive, string `as_of`, all-expired/unknown-root → None).
- [x] **"Fetch the active contract" one-call path.** `IBStockDataFetcher.get_front_month_hourly_bars(root, exchange, ...)`: registers the term structure (Phase B), selects the front contract, then fetches its bars (Phase A). Live-verified: picked `ESZ26` (expiry 2026-12-18) as front and fetched 92 UTC bars into it; other contracts registered with 0 bars as intended.
- [x] Continuous series via our own roll logic — **built 2026-10-27** in [[Futures contract aggregation and continuous series]] (volume roll + ratio back-adjustment → `CONTFUT` child `ES.c1`; live-verified across the real Sep-2026 ES roll). We build our own series rather than adopting IB's `ContFuture` (keeps the DB vendor-agnostic). Also added `get_futures_hourly_bars(..., include_expired=True)` to backfill expired contracts for the series.

**Phase D — CLI wiring:** *(built + live-verified 2026-10-27)*
- [x] Exposed through `main.py` as a `futures` command group (lazy-import + Rich, matching `fetch`/`bulk`): `futures register <ROOT> [--exchange CME] [--include-expired]` (enumerate + register the chain), `futures list <ROOT>` (DB-only, marks the front month), `futures fetch <ROOT> [--expiry YYYYMMDD] [-s -e] [--no-db]` (front-month by default; `--no-db` requires `--expiry`). Kept futures as a **separate command group** rather than folding into the equity watchlist/bulk model. Live-verified all three against the gateway + DB (register 21 ES contracts → list → fetch front-month ESZ26 92 bars), then cleaned up.

# Decisions made
- **Timezone storage convention — RESOLVED (2026-10-27): store tz-aware UTC.** Verified against the live DB: `price_hourly.timestamp` is **`TIMESTAMPTZ`** and the DB session is **`Europe/London`**, and `data_models.Asset` queries it with **UTC-aware bounds** (`data_models.py` `.replace(tzinfo=timezone.utc)`) — i.e. the analysis layer compares *true instants*. So futures bars are converted to **tz-aware UTC** (`IBStockDataFetcher._to_utc`) and passed through to the upsert tz-aware. A naive value is a trap here: Postgres reinterprets it in the session tz and shifts the instant (a 22:00Z bar stored naive landed at 21:00Z under BST). Confirmed fix: stored true UTC instant now equals the bar's real time.
  - **Checked the equity path — it is correct (no bug).** `get_hourly_bars` / `_save_bars_to_db` stores `row.date` **tz-aware** (exchange-local), so `TIMESTAMPTZ` records the right instant; its `.replace(tzinfo=None)` is only in the pagination step-back, not storage. Verified against live data: AAPL's stored bars read back as true UTC instants clustering at ~13:30–20:00 UTC = US RTH. (My Phase-A-v1 futures code was the only thing that dropped tzinfo — that was the real bug, now fixed to match the equity path's tz-aware behaviour.)
- **Exchange per root.** `ES`→CME; later roots (CL→NYMEX, ZN/ZB→CBOT, GC→COMEX). Store the real exchange on each `stocks` row (already captured by `register_contract(exchange=...)`), never `SMART`.
- **Registration granularity.** Register the full live term structure (21 rows for ES) vs only contracts we actually fetch. Leaning: register what we fetch (Phase A) and offer full enumeration as an explicit action (Phase B), to avoid 20+ idle root-children per product.

# Open questions / deferred
- **Front-month roll rule** (expiry-based vs volume/OI-based) — deferred; designed in [[Futures contract aggregation and continuous series]]. (Note: `get_front_contract` already does simple nearest-unexpired selection; a *roll* across contracts for a continuous series is the harder, separate problem.)
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
