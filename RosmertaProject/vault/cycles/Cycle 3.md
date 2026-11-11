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

> [!info] Started 2026-11-11 — items assigned, nothing being built yet. Suggested order: land the drafts → sanity check → schema → reorg / incremental fetch → TZ fix → portfolio analytics.

## Land in-flight work (built in Cycle 2, needs review + merge):
- [ ] [[IB backfill truncation on empty chunk]] 🟡 — draft PR #11.
- [ ] [[Futures in the dashboard]] 🟡 — draft PR #13.
- [ ] [[Reconcile DB schema - init migrations and scripts]] 🟡 — in progress; draft PR #12 covers the futures columns, rest of `init.sql` still to do.

## Consolidate:
- [ ] [[Full codebase sanity check and test audit]] 🔴 — may spawn fix items.
- [ ] [[Finish Directory Reorg]] 🔴 — `data-models/` vs `data_models.py` + the `time-periods.py` syntax error.
- [ ] [[Incremental Fetch and Dedup]] 🔴 — CLI paths, fundamentals skip, `fetch_log`, ticker dedupe.

## Portfolio:
- [ ] [[Flex Trade Timezone Fix]] 🟢 — prerequisite for the analytics.
- [ ] [[Trades and Portfolio Analytics]] 🟡 — carried from [[Cycle 1]].

## Done:
