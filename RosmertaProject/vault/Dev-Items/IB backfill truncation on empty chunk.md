---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🟡
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size aware IB retrieval
StartDate: 2026-11-02
EndDate: 2026-11-12
tags:
---
> [!success] Done 2026-11-12: PR #11 merged (`c657a8e`) in [[Cycle 3]] after review and a combined test run (143 passed).
>
> **Side effect to watch:** an empty chunk now steps back instead of stopping, so a range that is empty all the way back (e.g. a recently listed ticker fetched with an early `-s`) costs one IB request per chunk. The refresh workflows bound their lookback so they're unaffected. Tracked in [[IB historical-data pacing governor]].

# Summary (Tech Debt)
Both IB fetch loops (`get_hourly_bars`, `get_futures_hourly_bars`) `break` on the first empty `reqHistoricalData` chunk, **silently discarding all older bars**. Empty chunks are benign and common (exchange holidays; IB error 162 "HMDS query returned no data" mid-session for `1 D` 1-min requests). Surfaced backfilling ESZ26 1-min to July: one no-data day halted the backfill at Aug 27.

# Development Plan
- [x] On an empty chunk, step back one full chunk (`request_days`) and continue toward `start` instead of `break`ing; keep the rate-limit sleep. Applied to both loops. *(PR #11)*
- [x] Network-free test `tests/test_ib_fetch_backfill.py` with a scripted `FakeIB` (`[data, empty, data]`). *(PR #11 — 136 passed)*
- [x] Review + merge PR #11 → `c657a8e` (2026-11-12).
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
