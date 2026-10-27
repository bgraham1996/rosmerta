# Design: Futures Support

_Status: built · 2026-10-27 · owner: bg_

Extends the pipeline beyond equities to **futures**: download OHLCV for futures contracts,
store them under the existing `stocks`/`price_hourly` schema, and splice overlapping contracts
into a single **continuous series** for analysis. Built across Cycle-2 (dev items
`[[Futures IB retrieval]]`, `[[Futures contract aggregation and continuous series]]`, parent
`[[Futures asset data retrival and storage]]`).

This note is the end-to-end reference: the data model, downloading, handling multiple contracts,
building continuous data, and the refresh workflow. `Asset`/`Market` consumption of futures is
**not** covered here — that's still open (`[[Futures in Asset and Market]]`).

## Overview

```
IB Gateway ──► fetch per contract ──► price_hourly (per contract, UTC)
   │                                        │
   └─ reqContractDetails ──► register ──► stocks (FUTROOT root + FUT contracts)
                                            │
                       volume roll + ratio adjust ──► CONTFUT child (ES.c1)
```

Four moving parts, each with its own section below:

1. **Data model** — contracts live in `stocks` as "contracts-as-stocks"; candles are ordinary
   `price_hourly` rows.
2. **Download** — pull hourly bars for a single contract from IB and store them (UTC).
3. **Multiple contracts** — register a root's whole term structure; pick the front month.
4. **Continuous data** — roll across overlapping contracts and back-adjust into one series.

Plus a **workflow** that runs 1–4 for a root in one shot, and a **CLI** surface.

## Data model — contracts-as-stocks

A futures root and each of its contracts are rows in `stocks`; a contract's candles are just
`price_hourly` rows keyed by its `stock_id`. No new price table. See
`[[Futures schema - contracts as stocks]]`.

| Row kind    | `sec_type` | Example   | `parent_stock_id` | Notes                                    |
|-------------|------------|-----------|-------------------|------------------------------------------|
| Equity      | `STK`      | `AAPL`    | NULL              | unchanged                                |
| Futures root| `FUTROOT`  | `ES`      | NULL              | organizing parent; not fetched directly  |
| Contract    | `FUT`      | `ESZ26`   | → root            | `expiry`, `multiplier`, `ib_con_id` set  |
| Continuous  | `CONTFUT`  | `ES.c1`   | → root            | derived, back-adjusted series            |

Columns added to `stocks` (migration `db/db_scripts/migrations/add_futures_contract_support.sql`):
`parent_stock_id` (self-FK), `sec_type`, `expiry` (DATE), `multiplier` (NUMERIC). `CONTFUT` was
added to the `sec_type` check constraint later (`add_contfut_sec_type.sql`). Both migrations are
applied to the live dev DB; `init.sql` is not yet reconciled (`[[Reconcile DB schema - init migrations and scripts]]`).

**Canonical symbol is normalised, never IB's.** We store `ESZ26` (root + CME month-code +
2-digit year), built from IB's `lastTradeDateOrContractMonth`. IB's `localSymbol` (`ESZ6`) has a
single-digit, decade-ambiguous year and is **not** used as the key; IB's `conId` maps into
`ib_con_id`. This is the project's "DB is the source of truth, vendor ids map in" rule.

## 1. Download a contract — `price_retrival/ib_api.py`

`IBStockDataFetcher.get_futures_hourly_bars(root, expiry, exchange, ...)`:

- Qualifies `Future(root, expiry, exchange)` — a **bare root is ambiguous** and raises, so the
  expiry (`YYYYMMDD`/`YYYYMM`) is required to pin a single `conId`.
- Fetches `1 hour` `TRADES` bars with **`useRTH=False`** (futures trade ~23h), reusing the 30-day
  chunking loop from the equity path. `include_expired=True` backfills already-expired contracts
  (needed for continuous history).
- **Stores tz-aware UTC.** IB returns futures bars tz-aware in exchange-local time (CME = Central).
  `_to_utc` converts to UTC and keeps tzinfo, because `price_hourly.timestamp` is `TIMESTAMPTZ` and
  the DB session is `Europe/London` — a *naive* insert would be reinterpreted in the session tz and
  land at the wrong instant. `data_models.Asset` queries this column with UTC-aware bounds, so true
  instants are what matters. (The equity path already stores tz-aware exchange-local, so it is
  correct too; see `[[Futures IB retrieval]]` → Decisions.)
- Registers the contract (normalised symbol, expiry, multiplier, `ib_con_id`) and writes candles
  through the shared `ohlcv_store.upsert_hourly_bars` (same `ON CONFLICT` write path as equities).

## 2. Handling multiple contracts — `price_retrival/futures.py`

A root has many overlapping contracts. Two jobs: **register the chain**, and **pick the front**.

**Register the term structure.** `IBStockDataFetcher.register_futures_term_structure(root, exchange)`
enumerates the chain with `reqContractDetails(Future(root, exchange=...))` — one call returns every
contract with its `conId`/`expiry`/`multiplier` — and registers the root + all contracts
idempotently. Pure helpers do the mapping and persistence (unit-tested without IB):

- `normalise_ib_future(contract)` — IB `Contract` *or* a dict → `{root, symbol, expiry, multiplier,
  ib_con_id, exchange, currency}`.
- `register_term_structure(conn, root, contracts)` — batch register, expiry-ordered, idempotent.
- `get_child_contracts(conn, root)` — root → children navigation (`FUT` only, expiry-ordered).

**Front-month selection.** `get_front_contract(conn, root, as_of=None)` returns the contract with
the nearest `expiry >= as_of` (default today, UTC). `get_front_month_hourly_bars(root, exchange)` is
the one-call path: register the chain → select the front → fetch its bars.

> Front-month selection is *not* the same as a roll. Selecting the active contract to fetch is
> simple (nearest unexpired). Rolling **across** contracts to build a gap-free history is the harder
> problem solved next.

## 3. Continuous data — `price_retrival/continuous.py`

Adjacent contracts' price histories overlap, and they trade at different absolute prices
(contango/backwardation). To get one series we solve two sub-problems (design + rationale in
`[[Futures contract aggregation and continuous series]]`):

**(a) Roll schedule — _when_ to switch: volume roll.** Switch from the front contract to the next on
the first day the next contract's **daily volume** exceeds the front's (we already store per-bar
TRADES volume). Falls back to the front's expiry if no crossover is seen in the data.

**(b) Price adjustment — _how_ to splice: ratio back-adjustment.** The newest/front contract is the
**unadjusted anchor**; each earlier contract's OHLC is scaled by the cumulative price **ratio** at
the roll points, so **percentage returns are continuous** across every join. Volume stays raw (it's
the active contract's real volume).

The result is stored as a **`CONTFUT` child row** (`<root>.c1`) — its candles are ordinary
`price_hourly` rows, so everything downstream (indicators, resampling) works unchanged. **Raw
`FUT` contracts remain the source of truth; the continuous series is a derived, rebuildable
artefact.** We build our own series rather than adopting IB's `ContFuture`, keeping the DB
vendor-agnostic.

API:

- `build_continuous(contracts)` — pure; `[{symbol, expiry, candles}] → {candles, rolls, factors}`.
- `build_and_store_continuous(conn, root)` — reads the root's `FUT` contracts (front + priors),
  builds the series, and upserts the `CONTFUT` child. Returns a summary (`symbol, bars, rolls,
  factors, contracts`).

**Worked example (verified, Sep-2026 ES roll):** `ESU26` (expired) + `ESZ26` → `ES.c1`. Volume roll
at `2026-09-14` (ESZ26's volume overtook ESU26 ~4 days before ESU26's expiry); ratio `1.00879`
(ESZ26 ~0.88% above ESU26, contango); the step across the roll drops from the ~0.88% raw gap to
**0.042%** (ordinary bar-to-bar drift); the anchor (newest) close is left unadjusted.

## The workflow — `workflows/refresh_data/update_futures.py`

One entry point that runs the whole pipeline per root (mirrors `update_core` for equities):

1. **Register** the term structure (`include_expired=True`, so there's history behind the front).
2. **Fetch** the front + `HISTORY_CONTRACTS` prior contracts **incrementally** — from each
   contract's last stored bar through tomorrow (UTC, `ON CONFLICT` idempotent; expired contracts
   bounded at expiry, fresh ones backfilled `LOOKBACK_DAYS`). Only the front + priors are fetched;
   later contracts stay registered-but-empty until they roll to front.
3. **Build** the `CONTFUT` continuous series.

Tunable module constants: `ROOTS`, `EXCHANGE` (`CME`), `CURRENCY`, `HISTORY_CONTRACTS` (default 4 ≈
1yr for a quarterly root), `LOOKBACK_DAYS` (220), `INCLUDE_EXPIRED`, `DELAY`. `main(roots=...)`
returns a per-root summary.

```bash
uv run workflows/refresh_data/update_futures.py          # refresh ES, rebuild ES.c1
# or, programmatically:
#   from workflows.refresh_data.update_futures import main
#   main(roots=('ES', 'NQ'))
```

## CLI reference — `main.py` (`futures` group)

```bash
uv run main.py futures register ES --exchange CME        # enumerate + register the chain
uv run main.py futures list ES                           # list contracts, mark the front (no IB)
uv run main.py futures fetch ES                          # fetch the front-month contract
uv run main.py futures fetch ES --expiry 20261218 -s 2026-10-01 -e 2026-10-20
uv run main.py futures continuous ES                     # build + store ES.c1 (prints the roll schedule)
```

`futures fetch` defaults to the front month (needs the DB; `--no-db` therefore requires `--expiry`).
`futures continuous` builds from contracts already in the DB — fetch first (or use the workflow).

## Key files

| Concern                         | File                                                        |
|---------------------------------|-------------------------------------------------------------|
| Download a contract / enumerate | `price_retrival/ib_api.py`                                  |
| Registry, navigation, front     | `price_retrival/futures.py`                                 |
| Candle write/read (shared)      | `price_retrival/ohlcv_store.py`                             |
| Continuous series               | `price_retrival/continuous.py`                             |
| Refresh workflow                | `workflows/refresh_data/update_futures.py`                 |
| CLI                             | `main.py` (`futures` group)                                |
| Schema                          | `db/db_scripts/migrations/add_futures_contract_support.sql`, `add_contfut_sec_type.sql` |
| Tests                           | `tests/test_futures.py`, `tests/test_continuous.py`        |

## Not done / deferred

- **`Asset`/`Market` consumption** of futures (single contract vs `CONTFUT` vs term structure) —
  `[[Futures in Asset and Market]]`.
- **Persist the roll schedule** to a table (currently returned/logged, recomputed each build).
- **OI-based roll** (needs open-interest storage); **second-month `.c2`** series for curve/spreads.
- **Session-aware resampling** for ~24h futures in `utils/bars` — `[[Cross currency and timezone support]]`.
- **`init.sql` reconciliation** so a fresh DB has the futures columns — `[[Reconcile DB schema - init migrations and scripts]]`.
