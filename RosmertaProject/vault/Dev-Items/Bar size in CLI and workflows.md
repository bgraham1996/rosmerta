---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: ingestion
Cycle: "3"
DependsOn: Bar-size aware IB retrieval
StartDate: 
EndDate: 
tags: 
---
# Summary
Surface the new bar size to users: add a `--bar-size` option to the relevant CLI commands and wire it through the refresh workflows, so 1-minute (or 5/15-minute) retrieval is reachable without touching code.

# Development Plan
- [ ] **CLI** (`main.py`, lazy-import + Rich, matching existing commands):
  - `fetch price <TICKER> --bar-size '1 min'` (default `'1 hour'`).
  - `futures fetch <ROOT> --bar-size '1 min'` (and the front-month path).
  - `bulk price --list <WL> --bar-size '1 min'`.
  - Validate the value against the supported set (`1 min`/`5 mins`/`15 mins`/`1 hour`) with a clear error.
- [ ] **Workflows:** add a `BAR_SIZE` constant / `main(..., bar_size=...)` to `workflows/refresh_data/update_core.py` and `workflows/refresh_data/update_futures.py`; thread it into the fetch calls and (for futures) the continuous build if a sub-hourly series is wanted.
- [ ] Keep every default `'1 hour'` so existing invocations are unchanged.
- [ ] Update `project/design/futures.md` (and any equity docs) with the `--bar-size` usage.

# Dependencies
##### Depends on
- [[Bar-size aware IB retrieval]] (the fetchers must accept `bar_size` first).
##### Requires
- `main.py`, `workflows/refresh_data/update_core.py`, `workflows/refresh_data/update_futures.py`.
# Links
##### Relevant Docs
- Parent: [[Configurable bar size (sub-hourly retrieval)]]. `main.py`, `workflows/refresh_data/`, `project/design/futures.md`.
