---
StartDate: 2026-09-21
EndDate: 2026-09-27
Version: "[[V1001]]"
Priority: 🟡
Status: ✅ Done
Dev Status:
DevStatus: Released
Cycle: "1"
ReviewCount: "1"
---

# Cycle Objectives

First increment after the [[V1000]] release — target version [[V1001]] (1.0.0.1). Focus:

1. **Branch consolidation** — merge the stranded worktree branches into `main` (workflows, indicators, dedupe SQL). ✅ *Done — see below.*
2. **Portfolio analytics** — turn the ingested IB Flex trades into positions + P&L. ❌ *Not started → carried to [[Cycle 3]].*
3. **Finish the directory reorg** — resolve `data-models/` vs the root `data_models.py`. ❌ *Not finished → carried to [[Cycle 3]] (rescoped).*
4. **Close V1 loose ends** — install process docs, indicator usage docs, Flex-trade timezone fix. ❌ *Not started → unscheduled backlog.*

What actually happened: the cycle pivoted to **futures** — schema + storage were designed and shipped (2026-09-20 → 09-23) instead of objectives 2–4.

> [!summary] Closed 2026-11-11 (retroactive review)
> Delivered: consolidation, MACD (+ATR/Stochastic/ADX), ema_arb fix, dedupe, futures schema + OHLCV storage. Shipped in [[V1001]] (released 2026-10-31, alongside [[Cycle 2]]). Undelivered objectives carried as listed above.

# Cycle Metadata

- **Version:** [[V1001]] (1.0.0.1) — shared with [[Cycle 2]]
- **Window:** 2026-09-21 → 2026-09-27 (futures storage landed 09-23)
- **Predecessor:** [[Cycle 0]] (V1.0.0.0, released) · **Successor:** [[Cycle 2]]

# Dev Items in Cycle

## Done:
- [x] [[MACD Indicator]] — ported into `main` (`167e64e`), with `atr`/`stochastic`/`adx`.
- [x] [[Fix or Remove ema_arb Indicator]] — fixed (short−long EMA spread).
- [x] [[Dedupe Stocks and Watchlist Rebuild]] — trade-aware dedupe migration, applied & DB-verified.
- [x] [[Futures schema - contracts as stocks]] — migration applied to the live DB (2026-09-23).
- [x] [[Futures OHLCV storage and retrieval]] — source-agnostic `ohlcv_store` + `futures` modules (2026-09-23).

## Carried forward to [[Cycle 3]]:
- [ ] [[Finish Directory Reorg]] 🔴 (rescoped — most of it had already landed)
- [ ] [[Trades and Portfolio Analytics]] 🟡

## Returned to unscheduled backlog:
- [ ] [[Flex Trade Timezone Fix]] 🟢
- [ ] [[Install process]] 🟢
- [ ] [[Docs for using indicators]] 🟢

# Delivered via branch consolidation (2026-09-20)

Four stranded worktree branches were consolidated into `main` (`b8b578f → a288c73`). This closed out several items that had been sitting in backlog:

- [x] [[Fix or Remove ema_arb Indicator]] — fixed (short−long EMA spread).
- [x] [[Dedupe Stocks and Watchlist Rebuild]] — trade-aware dedupe migration, applied & DB-verified.
- [~] [[Feature Generation Workflow]] — scaffolding now real code; unified feature-set still pending.
- [~] [[Data Refresh Workflows]] — `update_core` landed; `backfill`/`repair` + CLI still pending.
- Bonus indicators now in `main`: `atr`, `stochastic`, `adx`, `rsi_ema_cross`, `trading_days_offset_gain` (registry = 14). Suite green (70 passed).

See also: [[2026-09-20 Branch consolidation and repo cleanup]].
