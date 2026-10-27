---
StartDate: 2026-09-21
EndDate: 2026-09-27
Version: "[[V1001]]"
Priority: 🟡
Status: 🟢 Active
Dev Status:
DevStatus: In Progress
Cycle: "1"
ReviewCount: "0"
---

# Cycle Objectives

First increment after the [[V1000]] release — target version [[V1001]] (1.0.0.1). Focus:

1. **Branch consolidation** — merge the stranded worktree branches into `main` (workflows, indicators, dedupe SQL). ✅ *Done — see below.*
2. **Portfolio analytics** — turn the ingested IB Flex trades into positions + P&L.
3. **Finish the directory reorg** — resolve `data-models/` vs the root `data_models.py`.
4. **Close V1 loose ends** — install process docs, indicator usage docs, Flex-trade timezone fix.

# Cycle Metadata

- **Version:** [[V1001]] (1.0.0.1)
- **Window:** 2026-09-21 → 2026-09-27
- **Predecessor:** [[Cycle 0]] (V1.0.0.0, released)

# Dev Items in Cycle

## Done:
- [x] [[MACD Indicator]] — ported into `main` (`167e64e`), with `atr`/`stochastic`/`adx`.

## Backlog items:
- [ ] [[Finish Directory Reorg]] 🔴
- [ ] [[Trades and Portfolio Analytics]] 🟡
- [ ] [[Flex Trade Timezone Fix]] 🟢
- [ ] [[Install process]] 🟢
- [ ] [[Docs for using indicators]] 🟢

# Delivered via branch consolidation (2026-09-20)

Four stranded worktree branches were consolidated into `main` (`b8b578f → a288c73`). This closed out several items that had been sitting in Cycle 2 backlog:

- [x] [[Fix or Remove ema_arb Indicator]] — fixed (short−long EMA spread).
- [x] [[Dedupe Stocks and Watchlist Rebuild]] — trade-aware dedupe migration, applied & DB-verified.
- [~] [[Feature Generation Workflow]] — scaffolding now real code; unified feature-set still pending.
- [~] [[Data Refresh Workflows]] — `update_core` landed; `backfill`/`repair` + CLI still pending.
- Bonus indicators now in `main`: `atr`, `stochastic`, `adx`, `rsi_ema_cross`, `trading_days_offset_gain` (registry = 14). Suite green (70 passed).
