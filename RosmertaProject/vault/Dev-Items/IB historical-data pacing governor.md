---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🟢
Bucket: ingestion
Cycle:
DependsOn: Bar-size aware IB retrieval
StartDate:
EndDate:
tags:
---
# Summary
Split out of [[Bar-size aware IB retrieval]] (Done, [[Cycle 2]]). Retrieval keeps a fixed `ib.sleep(1)` between chunks and the workflows add a larger inter-ticker delay for sub-hourly, but there is no real request-rate governor for IB's historical-data pacing (~60 requests / 10 min, no identical request within 15s). Not needed at today's volumes — only matters for large multi-year 1-minute backfills.

# Development Plan
- [ ] A shared token-bucket / sliding-window limiter in `ib_api` used by every historical request.
- [ ] Back off and retry on IB pacing-violation errors (code 162) instead of failing the ticker.
- [ ] Unit-test the limiter with a fake clock.

# Dependencies
##### Depends on
- [[Bar-size aware IB retrieval]] (done).
##### Requires
- IB Gateway for a live soak test.

# Links
##### Relevant Docs
- `price_retrival/ib_api.py` (`_CHUNK_DAYS`), `workflows/refresh_data/update_core.py` (`_lookback_for`).
##### Prerequisite for
- Large sub-hourly backfills.
