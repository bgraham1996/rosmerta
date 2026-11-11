---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: In Progress
Priority: 🟡
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size aware IB retrieval
StartDate: 2026-11-02
EndDate:
tags:
---
> [!info] Built on draft PR #11 (`worktree-futures-fetch-no-truncate`, 2026-11-02) — needs review + merge. Tracked in [[Cycle 3]].

# Summary (Tech Debt)
Both IB fetch loops (`get_hourly_bars`, `get_futures_hourly_bars`) `break` on the first empty `reqHistoricalData` chunk, **silently discarding all older bars**. Empty chunks are benign and common (exchange holidays; IB error 162 "HMDS query returned no data" mid-session for `1 D` 1-min requests). Surfaced backfilling ESZ26 1-min to July: one no-data day halted the backfill at Aug 27.

# Development Plan
- [x] On an empty chunk, step back one full chunk (`request_days`) and continue toward `start` instead of `break`ing; keep the rate-limit sleep. Applied to both loops. *(PR #11)*
- [x] Network-free test `tests/test_ib_fetch_backfill.py` with a scripted `FakeIB` (`[data, empty, data]`). *(PR #11 — 136 passed)*
- [ ] Review + merge PR #11 (rebase if needed).
- [ ] Out of scope (follow-up if it bites): recovering a day IB spuriously returns empty *and* has data (the `2 D`-duration refetch trick).

# Dependencies
##### Depends on
- [[Bar-size aware IB retrieval]] (done).
##### Requires
- Nothing further.

# Links
##### Relevant Docs
- `price_retrival/ib_api.py`, PR #11.
##### Prerequisite for
- [[Full codebase sanity check and test audit]] (1-min backfill round-trip).
