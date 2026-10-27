---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🔴
Bucket: core-components
Cycle: "1"
DependsOn: 
StartDate: 2026-09-20
EndDate: 
tags: 
---
> [!note] Epic — decomposed 2026-09-20. This is now the umbrella for futures support; the actionable near-term slices are broken out into their own items (see Links). Kept your 🔴 / `core-components`.

> [!success] Near-term slice complete — 2026-09-23. Both Cycle-1 children are **Done**: [[Futures schema - contracts as stocks]] (migration applied to live DB) and [[Futures OHLCV storage and retrieval]] (source-agnostic `ohlcv_store` + `futures` modules, `ib_api` delegating, 16 tests). Futures contracts can now be stored and read back, with root→children navigation, entirely without IB. Epic stays **In Progress** — the deferred Cycle-2 slices remain: [[Futures IB retrieval]] and [[Futures in Asset and Market]] (plus the still-open aggregation/roll/continuous-series questions below). Work on branch `worktree-futures-schema-migration` (unpushed).

# Summary
Extend the pipeline beyond equities to **futures**: retrieve and store futures OHLCV. Today only equities exist (`stocks` + `price_hourly`).

**Chosen model (2026-09-20): contracts-as-stocks with a self-referential parent.**
- Each specific futures contract (e.g. `ESH26`) is stored as its **own `stocks` row**.
- The root (e.g. `ES`) is also a `stocks` row.
- Contracts link to their root via a new **`stocks.parent_stock_id`** (self-FK, nullable). Equities keep `parent_stock_id = NULL`.
- **OHLCV reuses `price_hourly` unchanged** (keyed by `stock_id`) — a contract's candles are just `price_hourly` rows. No new price table.

**Why this fits:**
- The existing read path already serves a contract's candles (query `price_hourly` by its `stock_id`); the root row is a stable handle to enumerate contracts / pick a front month later.
- **No clash with `UNIQUE(symbol)`** (added by the dedupe migration): each contract carries a *distinct* symbol (`ESH26` ≠ `ESM26` ≠ `ES`). This is the key advantage over cramming every expiry under one `ES` symbol.

# Phasing
**Near-term (this focus):** DB shape + OHLCV storage/retrieval, source-agnostic — can be built and tested with manual/synthetic candles *before* IB is wired.
- [[Futures schema - contracts as stocks]] — the migration(s).
- [[Futures OHLCV storage and retrieval]] — store/serve candles per contract + per root.

**Cycle-2 (now in progress):**
- [[Futures IB retrieval]] — **In Progress (2026-10-27).** IB `Future` / `ContFuture` fetch, `useRTH=False`, roll / front-month, 30-day chunking. Live read-only gateway probe done: `reqContractDetails` enumerates the term structure (21 live ES contracts), `conId`→`ib_con_id`, normalised symbol must be constructed from `lastTradeDateOrContractMonth` (not IB's ambiguous `localSymbol`), and futures bars return tz-aware Central time (UTC-normalisation decision pending). Findings + phased plan live in that item.

**Still deferred:**
- [[Futures in Asset and Market]] — how the analysis classes consume futures (shared path vs parallel). *Not now.*

# Design principle
**The DB is the source of truth and must be API / third-party agnostic** as far as possible. Vendor identifiers and quirks (IB `localSymbol`, `conId`, routing exchanges) are mapped *in* at ingestion; the canonical schema stores normalised values. See [[Reconcile DB schema - init migrations and scripts]].

# Decisions (2026-09-20)
- **Symbol convention:** store the **normalised** symbol (`ESH26`), not IB's `localSymbol` (`ESH6`). Keep the vendor id in `ib_con_id`.
- **Row kind:** use a `sec_type` column (`STK` / `FUTROOT` / `FUT`) alongside `parent_stock_id` — M2 is in scope.
- **Root row:** an **organizing parent for now** — *not* fetchable yet. Root fetchability is deferred until we have aggregation logic for overlapping contracts (see below).

# Model functionality needed now
Just **parent → children navigation**: from a root (`ES`), get its current / recent child contracts (via `parent_stock_id`). Front-month selection, roll, and cross-contract aggregation are **not** needed for the first cut.

# Open questions / deferred
- **Overlapping-contract aggregation:** ~~prerequisite for a continuous series~~ — **built 2026-10-27** (volume roll + ratio back-adjustment), see [[Futures contract aggregation and continuous series]]. Live-verified across the real Sep-2026 ES roll.
- **Continuous series:** **built** as a `CONTFUT` child (`ES.c1`) derived from immutable raw contracts — see [[Futures contract aggregation and continuous series]]. Remaining: persist the roll schedule, OI-based roll, second-month series, and `Asset`/`Market` consumption ([[Futures in Asset and Market]]).
- **Fetchable root:** now unblocked (the aggregation logic exists) — a root can expose its `CONTFUT` series. Wiring deferred to [[Futures in Asset and Market]].

# Links
##### Relevant Docs
- `db/init.sql` (`stocks` / `price_hourly`), `db/db_scripts/migrations/`, `price_retrival/ib_api.py` (later), `utils/bars.py` (later).
##### Prerequisite for
- Any futures-based analysis / cross-asset stat-arb.
