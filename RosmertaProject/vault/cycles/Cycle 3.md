---
StartDate: 2026-11-11
EndDate: 2026-11-24
Version: "[[V1002]]"
Priority: 🔴
Status: 🟢 Active
Dev Status:
DevStatus: In Progress
Cycle: "3"
ReviewCount: "0"
---

# Cycle Objectives

Consolidate after two big feature cycles (futures + bar size) before building more, then land the carried-forward portfolio feature. Target version [[V1002]] (1.0.0.2).

1. **Prove the system still hangs together** after the `price_bars` migration — full sanity check + test audit.
2. **Make a fresh DB buildable** — finish reconciling `init.sql` with the live schema.
3. **Clear the oldest tech debt** — the rescoped directory reorg; incremental fetch on the remaining paths.
4. **Ship portfolio analytics** — the headline [[V1001]] objective that didn't make it (on correctly-timestamped trades).
5. **Land the in-flight Cycle 2 work** — review + merge draft PRs #11 and #13.

# Cycle Metadata

- **Version:** [[V1002]] (1.0.0.2)
- **Window:** 2026-11-11 → 2026-11-24 (two weeks)
- **Predecessor:** [[Cycle 2]] (V1.0.0.1, released 2026-10-31)

# Dev Items in Cycle

> [!info] Started 2026-11-11 — items assigned, nothing being built yet. Suggested order: land the drafts → sanity check → **data refresh + restore** → schema → reorg / incremental fetch → TZ fix → portfolio analytics.

## Land in-flight work:
- [ ] [[Reconcile DB schema - init migrations and scripts]] 🟡 — futures columns landed (PR #12); trades/positions + `watchlist_members` still missing from `init.sql`.

## Consolidate:
- [ ] [[Finish Directory Reorg]] 🔴 — `data-models/` vs `data_models.py` + the `time-periods.py` syntax error.
- [ ] [[Incremental Fetch and Dedup]] 🔴 — CLI paths, fundamentals skip, `fetch_log`, ticker dedupe.

## Data (pulled in 2026-11-12 from the sanity check):
- [ ] [[Refresh stale equity prices and trades]] 🔴 — **in progress**: prices current (97/100, 2026-11-12); trades waiting on PR #21; ABB/APGE/DOMO unresolvable.
- [ ] [[Restore fundamentals and dividends in prod]] 🔴 — **in progress**: fundamentals restored 2026-11-12 (84/100 stocks); dividends **parked** (IB has no dividend history for this account, error 10358).
  - [x] [[EDGAR fundamentals extraction fixes]] 🟡 — code merged (PR #23). APGE/DOMO likely delisted (watchlist decision).
  - [ ] [[Clean up and re-fetch corrected fundamentals]] 🟡 — back up, delete 63 stale FY rows, re-fetch 8 tickers, verify. Needs OK for the delete.

## Portfolio:
- [ ] [[Flex Trade Timezone Fix]] 🟢 — prerequisite for the analytics.
- [ ] [[Trades and Portfolio Analytics]] 🟡 — carried from [[Cycle 1]].

## Done:
- [x] [[Full codebase sanity check and test audit]] — 2026-11-12. Fixed the `Market` crash (PR #18) and the trades/dividends stock lookup (PR #21); IB- and DB-side passes done.
- [x] [[IB backfill truncation on empty chunk]] — PR #11 merged `c657a8e` (2026-11-12).
- [x] [[Futures in the dashboard]] — PR #13 merged `52623ec` (2026-11-12).

## Also landed this cycle (outside the plan):
- PR #16 (`12d6f06`): forward-looking indicators `max_forward_gain` / `max_forward_drawdown` / `bars_to_target` + `docs/indicators.md`, salvaged from an uncommitted worktree. Advances [[Docs for using indicators]].
- Git cleanup: every stale worktree/branch removed; repo is down to `main`.
- Test suite: 135 → **143** passing.
