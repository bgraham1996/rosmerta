---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle:
DependsOn: Futures contract aggregation and continuous series
StartDate:
EndDate:
tags:
---
# Summary
Follow-ups split out of [[Futures contract aggregation and continuous series]] (Done, [[Cycle 2]]). The `CONTFUT` series (`ES.c1`, volume roll + ratio back-adjustment) works and is live-verified; these extend it.

# Development Plan
- [ ] Persist the roll schedule (roll dates + adjustment ratios) to a table instead of recomputing on every rebuild.
- [ ] Open-interest–based roll as an alternative to the volume crossover.
- [ ] Second-month (`.c2`) continuous series.
- [ ] Session-aware daily resampling for futures (~23h session, not the equity RTH day) — ties into [[Cross currency and timezone support]].
- [ ] Sub-hourly continuous series — `update_futures` currently skips the continuous rebuild for non-hourly bar sizes.

# Dependencies
##### Depends on
- [[Futures contract aggregation and continuous series]] (done).
##### Requires
- Raw contract bars in `price_bars` (incl. expired contracts via `include_expired`).

# Links
##### Relevant Docs
- `price_retrival/continuous.py`, `workflows/refresh_data/update_futures.py`, `project/design/futures.md`.
##### Prerequisite for
- [[Futures in Asset and Market]]
