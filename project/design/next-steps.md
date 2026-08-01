# Rosmerta — Next Steps & Planning

_Last updated: 2026-08-01_

A snapshot of where the project is and what to work on next. Companion to the
Dev Items in `RosmertaProject/vault/Dev-Items/`; engineering/design docs live
here in `project/design/`.

## Where the project is

The core pipeline is working end-to-end:

- **Ingest → Postgres:** IB hourly prices (`ib_api`), SEC EDGAR fundamentals &
  dividends (`edgar_api`, `dividends_api`), and **now IB Flex executed trades**
  (`trades_api` → `transaction`/`trade`). Bulk fetchers over watchlists.
- **Analysis layer (read-only):** `Asset` / `Market` in `data_models.py`, the
  indicator registry in `indicators.py`, OHLCV resampling in `utils/bars.py`.
- **Dashboard:** Dash app with a view registry (`dashboards/`).
- **Tests:** DB-free pytest suite (`tests/`) — currently **42 passed**.

### Landed recently (Jul–Aug 2026)
- **IB Flex trades ingest unblocked & working** — token recovered, 21 stock
  trades in prod, idempotent re-runs, `fetch trades` CLI reliable after retry
  hardening. (fixes uncommitted in working tree.)
- **Forward-looking gains** — `days_offset_gain` indicator + `get_offset_growth`.
- Board reconciled: **RSI, EMA, Lookforward-gains marked done** (were already in
  code but still open on the board).

## Priority legend
🔴 high / do first · 🟡 medium · 🟢 low / nice-to-have

## Recommended sequencing

### 0. Land the WIP (do this first) — 🔴 [[Finish Directory Reorg]]
The tree is a big pile of uncommitted, half-reorganized work (the `db/` move,
the vault swap, empty stub packages, plus today's real fixes). This is the
single highest-value next move — it turns a fragile checkpoint into a solid base
and makes everything after it committable. Not a feature; bookkeeping that
unblocks the rest.

### Cycle 1 (proposed)
| Item | Type | Priority | Notes |
|------|------|----------|-------|
| [[Finish Directory Reorg]] | tech-debt | 🔴 | commit WIP, settle `db/` layout, fix stubs |
| [[Trades and Portfolio Analytics]] | feature | 🟡 | the payoff of Flex — **needs a design doc first** |
| [[MACD Indicator]] | feature | 🟡 | small, well-scoped, no new wiring |

### Backlog
| Item | Type | Priority | Notes |
|------|------|----------|-------|
| [[Flex Trade Timezone Fix]] | tech-debt | 🟢 | `trade_datetime` off by session tz (+01) |
| [[Docs for using indicators]] | docs | 🟢 | how-to for the indicator registry |
| [[Install process]] | tech-debt | 🟢 | README claims an entry point `pyproject.toml` lacks |

## Open design questions (need a `project/design/` doc before building)

1. **Portfolio analytics data model** — how do `trade` rows become `position`
   rows? Trader is **long-only**, so: FIFO or average-cost basis for realized
   P&L on partial exits? Where does unrealized P&L pull its "current price"
   (latest `price_hourly` bar)? → write `project/design/portfolio-analytics.md`.
2. **Trade timezone** — store executions in UTC or exchange-local? Depends on
   how downstream analysis wants to join trades against price bars (which are
   treated as UTC).
3. **Packaging / entry point** — commit to the `rosmerta` console script, or
   standardize on `uv run main.py` and simplify the docs.

## Notable design decisions already made
- **Offset gain uses bar-count lookahead** (8/day hourly, 1 daily, 1/5 weekly),
  not calendar days — deliberate, lives in `Asset.get_offset_growth`.
- **Fetchers persist; `data_models`/`indicators` only read.** Keep that line.
- Keep the `price_retrival` (misspelled) module path.
