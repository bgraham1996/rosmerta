---
FileType: 📓 Log
Date: 2026-11-11
Span: 2026-11-11
Cycle: "[[Cycle 3]]"
tags: [planning, board]
---

# Planning cleanup — cycles, versions, dev items

The board had fallen behind the code: [[Cycle 1]] was still "Active" six weeks after its window, there were no Cycle 2/3 notes even though the October futures + bar-size work (PRs #3–#10) had shipped, and ~6 finished items still read "In Progress".

## What changed
- **Cycles:** closed [[Cycle 1]] with a retro (what shipped vs. objectives); wrote a retroactive [[Cycle 2]] for the Oct work; opened [[Cycle 3]] (2026-11-11 → 11-24, target [[V1002]]).
- **Versions/releases:** [[V1001]] marked released 2026-10-31 with its *actual* contents (futures + bar size); added release note [[V 0002]]; new [[V1002]]; `metadata.json` bumped to 1.0.0.1.
- **Dev items → Done:** [[Futures IB retrieval]], [[Futures contract aggregation and continuous series]], [[Configurable bar size (sub-hourly retrieval)]] and its four children.
- **Leftovers split out:** [[Futures continuous series follow-ups]], [[IB historical-data pacing governor]].
- **Checked & still open (notes added):** [[Asset get_growth Cache Inconsistency]], [[Market Indicator API Ergonomics]].
- **Rescoped:** [[Finish Directory Reorg]] (most had landed), [[Incremental Fetch and Dedup]] (price path done in workflows), [[Reconcile DB schema - init migrations and scripts]] (PR #9 done, #12 in draft).
- **Restored:** [[Full codebase sanity check and test audit]] from draft PR #7 onto the board.
- **`Cycle` field redefined** (template updated): the number of the cycle note an item is scheduled into / delivered in; **blank = unscheduled backlog**; epics stay blank. All unscheduled items were cleared (the old "2"/"3" labels were version guesses, not cycles).
- `project/design/next-steps.md` marked superseded — the vault is the single planning home.

## Not done (deliberately)
- Git cleanup (stale worktrees/branches, draft PRs #7/#11/#12/#13) — left for later.
