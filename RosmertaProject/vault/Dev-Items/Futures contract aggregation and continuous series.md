---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "3"
DependsOn: Futures IB retrieval
StartDate: 
EndDate: 
tags: 
---
> [!note] Design write-up 2026-10-27. Captures the overlapping-contract aggregation problem that was flagged as the key deferred prerequisite in [[Futures asset data retrival and storage]]. No code yet — this item is the design to agree before building.

# Summary
We now store each futures contract as its own `stocks` row with its own `price_hourly` candles ([[Futures OHLCV storage and retrieval]], [[Futures IB retrieval]] Phases A–D). But a single root (`ES`) has **many contracts whose price history overlaps** — at any moment several are live and liquid. To get **one continuous series per root** (for charts, indicators, backtests, cross-asset work) we must decide **(1) when to roll** from one contract to the next and **(2) how to splice** their prices across the roll. This is the prerequisite for *both* a **fetchable root** and a **continuous series** — deferred since [[Futures asset data retrival and storage]].

# Why it's needed / what depends on it
- **Continuous series** (`CONTFUT`): a gap-free price history spanning many contracts — the thing indicators (`sma/ema/rsi/...`) and the dip-entry backtest actually want.
- **Fetchable root:** `ES` as an organizing parent can't be fetched today precisely because we lack the roll/aggregation rule to collapse overlapping contracts into one series.
- **[[Futures in Asset and Market]]:** how `Asset`/`Market` consume futures depends on whether they read a single contract, a continuous series, or the term structure.

# What we have now (grounds the design)
- **Per-contract candles** keyed by `stock_id`, stored **tz-aware UTC** (correct instants), with **real TRADES volume** per bar.
- **Full term structure** registered on demand (`futures.register_term_structure`): each contract with `expiry`, `multiplier`, `ib_con_id`, `parent_stock_id`→root; navigation via `get_child_contracts` (expiry-ordered) and `get_front_contract` (nearest unexpired).
- **ES specifics observed:** quarterly H/M/U/Z cycle, multiplier 50, ~23h session; adjacent contracts overlap for months, with liquidity migrating from front to next ~days before expiry.
- **Not stored yet:** open interest (OI); IB can supply it (`whatToShow='...'`) but it's a schema add.

# The two sub-problems
### 1. Roll schedule — *when* to switch contracts
- **Expiry/calendar roll:** switch N business days before `expiry` (or on a fixed rule, e.g. the second Thursday). Deterministic, needs no market data, fully reproducible. Good default; what most vendors publish.
- **Volume roll:** switch when the next contract's rolling volume overtakes the front's. We **already store volume**, so this is feasible without new data and tracks where liquidity actually is. Noisier; needs smoothing.
- **Open-interest roll:** switch on OI crossover (the "truest" liquidity signal). Needs OI storage first.
- **Recommendation:** implement **expiry-based roll first** (simple, reproducible), with a **volume-based roll** as an opt-in refinement using data we already have. Keep the rule a parameter.

### 2. Price adjustment — *how* to splice across a roll
Adjacent contracts trade at different absolute prices (contango/backwardation), so naive concatenation leaves a jump at every roll.
- **Unadjusted:** concatenate raw; honest per-contract prices but discontinuous — fine for within-contract returns, wrong for a continuous level or multi-year study.
- **Back-adjusted (difference / "Panama"):** shift history by the roll gap so the join is continuous. Preserves absolute point moves; distorts historical levels (can even go negative over long spans).
- **Ratio-adjusted (proportional):** scale history by the price ratio at each roll so **percentage returns are continuous**. Standard for long histories and for anything return/indicator-based. **Recommended default.**
- **Recommendation:** keep **raw per-contract candles immutable as the source of truth**; derive the continuous series by **ratio back-adjustment** at roll points, and **persist the roll schedule + adjustment factors** so the series is reproducible and auditable.

# Proposed design
- **Model the continuous series as a `CONTFUT` child row** of the root (the schema note in [[Futures asset data retrival and storage]] already anticipated this): symbol like `ES.c1` (front) — leave room for `ES.c2` (second month) for calendar spreads later. Its candles are ordinary `price_hourly` rows (adjusted), so **everything downstream — `Asset`, indicators, resampling — works unchanged**.
- **Derivation module** (e.g. `price_retrival/continuous.py` or an analysis-side builder): read the root's contracts' candles, compute the **roll schedule** (expiry- or volume-based), apply **ratio adjustment**, and upsert the `CONTFUT` candles via `ohlcv_store`. Pure/DB-testable against the FakeDB like the rest of `futures.py`.
- **Store the roll schedule** (which contract is active over each date range + the adjustment factor) in a small table or as metadata, so the continuous series can be rebuilt deterministically and inspected.
- **Raw contracts stay the source of truth;** the continuous series is a derived, rebuildable artefact. The DB-as-source-of-truth / vendor-agnostic principle holds — we do **not** adopt IB's `ContFuture` series as canonical (it's a useful cross-check only).

# Open questions
- **OI storage:** worth adding `price_hourly.open_interest` (or a sibling) to enable OI-based rolls and liquidity filters? Separate schema decision.
- **Second-month / term-structure series** (`ES.c2`…) for calendar spreads and curve analysis — model now or later?
- **Session alignment for resampling:** `utils/bars` daily/weekly rules assume US-RTH boundaries; ~23h futures sessions need their own rule (overlaps [[Cross currency and timezone support]]).
- **How `Asset`/`Market` pick** a continuous series vs a single contract vs the raw term structure — belongs to [[Futures in Asset and Market]].
- **Rebuild triggers:** when new bars land for the active contract, how/when is the `CONTFUT` series refreshed (incremental vs full rebuild)?

# Dependencies
##### Depends on
- [[Futures IB retrieval]] (Done Phases A–D — contracts + candles + term structure in the DB).
##### Requires
- `price_hourly` (candles + volume), `price_retrival.futures` (navigation), `ohlcv_store` (write path). OI-based roll additionally needs OI storage.
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `price_retrival/futures.py`, `price_retrival/ohlcv_store.py`, `data_models.py`, `utils/bars.py`.
- Related: [[Futures in Asset and Market]], [[Cross currency and timezone support]].
##### Prerequisite for
- A **fetchable root** and the **continuous series** (`CONTFUT`); futures-based indicators / backtests / cross-asset stat-arb.
