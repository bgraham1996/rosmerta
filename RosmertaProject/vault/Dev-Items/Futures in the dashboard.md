---
FileType: 🚥 Dev Item
Type: Feature
Status: In Progress
Priority: 🟡
Bucket: dashboards
Cycle: "3"
DependsOn: Futures contract aggregation and continuous series
StartDate: 2026-11-02
EndDate:
tags:
---
> [!info] Built on draft PR #13 (`worktree-dashboard-futures-viewability`, 2026-11-02) — needs review + merge. Tracked in [[Cycle 3]].

# Summary
The futures data (ESZ26 1-min, the `ES.c1` continuous series) was invisible in the Dash UI. The asset deep-dive view is symbol-generic, but it hard-coded the `hourly` timeframe (futures are stored at `1 min`) and upper-cased the ticker (`es.c1` → `ES.C1`, never matching `ES.c1`).

# Development Plan
- [x] Timeframe selector in the asset deep-dive (`minute`…`monthly`) passed to `Asset(timeframe=…)`; default stays `hourly`. *(PR #13)*
- [x] Case-insensitive symbol resolution (`_resolve_symbol`, exact match preferred) + clear "Unknown ticker" message. *(PR #13)*
- [x] Live-DB verification: `es.c1`/`ESZ26` → 119,080 1-min bars; `pfe` hourly regression intact. *(PR #13)*
- [ ] Review + merge PR #13.

# Dependencies
##### Depends on
- [[Futures contract aggregation and continuous series]], [[Bar size in the analysis layer]] (done).
##### Requires
- Live DB for the visual check.

# Links
##### Relevant Docs
- `dashboards/views/` (asset deep-dive), PR #13.
##### Prerequisite for
- [[Futures in Asset and Market]] (UI side of futures consumption).
