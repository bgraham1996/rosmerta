---
StartDate: 2026-09-28
EndDate: 2026-10-31
Version: "[[V1001]]"
Priority: 🔴
Status: ✅ Done
Dev Status:
DevStatus: Released
Cycle: "2"
ReviewCount: "1"
---

# Cycle Objectives

> [!note] Retroactive cycle — written 2026-11-11
> No cycle note was opened at the time; the work was tracked only on the dev items. This note records what actually shipped between [[Cycle 1]] and [[Cycle 3]]. (Active work ran 2026-10-27 → 10-31, PRs #3–#10.)

1. **Futures end to end** — IB retrieval of contracts + term structure, front-month selection, our own continuous series, `futures` CLI group, refresh workflow. ✅
2. **Configurable bar size (1-minute)** — storage model, bar-size-aware IB retrieval, analysis layer, CLI + workflows. ✅
3. **Schema hygiene** — `init.sql` reconciled to the `price_bars` model. ✅ (partial — rest carried in [[Reconcile DB schema - init migrations and scripts]])

# Cycle Metadata

- **Version:** [[V1001]] (1.0.0.1) — shared with [[Cycle 1]]
- **Window:** 2026-09-28 → 2026-10-31
- **PRs:** #3 futures schema/storage · #4 futures IB retrieval + continuous + `update_futures` · #5 bar-size board items · #6 configurable bar sizes · #8 bar-size-aware continuous · #9 `init.sql` ↔ `price_bars` · #10 CLAUDE.md note
- **DB:** migrations `add_bar_size_support.sql` and `add_contfut_sec_type.sql` applied to prod
- **Tests:** 135 passing
- **Predecessor:** [[Cycle 1]] · **Successor:** [[Cycle 3]]

# Dev Items in Cycle

## Done:
- [x] [[Futures IB retrieval]] — Phases A–D + `futures register/list/fetch`, live-verified (2026-10-27).
- [x] [[Futures contract aggregation and continuous series]] — volume roll + ratio back-adjust → `ES.c1`; bar-size aware (2026-10-31).
- [x] [[Configurable bar size (sub-hourly retrieval)]] — epic, all children below (2026-10-31).
	- [x] [[Bar-size storage model]] — `price_bars` + `price_hourly` view.
	- [x] [[Bar-size aware IB retrieval]] — `bar_size` param, per-size chunking, step-back gap fix.
	- [x] [[Bar size in the analysis layer]] — `timeframe → bar_size`, intraday `Asset` reads.
	- [x] [[Bar size in CLI and workflows]] — `--bar-size` on `fetch`/`futures`/`bulk`, per-size incremental refresh.

## Partial:
- [~] [[Reconcile DB schema - init migrations and scripts]] — `price_bars` part done (PR #9); continues in [[Cycle 3]].

## Split out as follow-ups (unscheduled):
- [[Futures continuous series follow-ups]]
- [[IB historical-data pacing governor]]
- [[Futures in Asset and Market]]

## Left in flight at cycle close (draft PRs, not merged):
- #11 don't truncate IB backfill on an empty chunk
- #12 `init.sql` stocks table ↔ futures schema
- #13 futures viewable in the dashboard asset deep-dive
