---
StartDate: 2026-11-11
EndDate: 2026-11-24
Version: "[[V1002]]"
Priority: 🔴
Status: 🟢 Active
Dev Status:
DevStatus: Planned
Cycle: "3"
ReviewCount: "0"
---

# Cycle Objectives

Consolidate after two big feature cycles (futures + bar size) before building more, then land the carried-forward portfolio feature. Target version [[V1002]] (1.0.0.2).

1. **Prove the system still hangs together** after the `price_bars` migration — full sanity check + test audit.
2. **Make a fresh DB buildable** — finish reconciling `init.sql` with the live schema.
3. **Clear the oldest tech debt** — the rescoped directory reorg; incremental fetch on the remaining paths.
4. **Ship portfolio analytics** — the headline [[V1001]] objective that didn't make it.

# Cycle Metadata

- **Version:** [[V1002]] (1.0.0.2)
- **Window:** 2026-11-11 → 2026-11-24 (two weeks)
- **Predecessor:** [[Cycle 2]] (V1.0.0.1, released 2026-10-31)

# Dev Items in Cycle

## Planned:
- [ ] [[Full codebase sanity check and test audit]] 🔴 — do first; may spawn fix items.
- [ ] [[Reconcile DB schema - init migrations and scripts]] 🟡 — in progress (draft PR #12 covers the futures columns).
- [ ] [[Finish Directory Reorg]] 🔴 — now just `data-models/` vs `data_models.py` + the `time-periods.py` syntax error.
- [ ] [[Incremental Fetch and Dedup]] 🔴 — price path already incremental in the workflows; remaining: CLI paths, fundamentals skip, `fetch_log`, ticker dedupe.
- [ ] [[Trades and Portfolio Analytics]] 🟡 — carried from [[Cycle 1]].

## In flight from Cycle 2 (draft PRs — review/merge during this cycle):
- #11 fix: don't truncate IB backfill on an empty chunk
- #12 `init.sql` stocks table ↔ futures schema (part of the reconcile item)
- #13 futures viewable in the dashboard asset deep-dive

## Done:
