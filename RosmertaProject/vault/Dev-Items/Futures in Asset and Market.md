---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "2"
DependsOn: Futures OHLCV storage and retrieval
StartDate: 
EndDate: 
tags: 
---
# Summary
Decide and build how `data_models.Asset` / `Market` consume futures once contracts + candles exist in the DB (see [[Futures asset data retrival and storage]]). Deferred — deliberately not part of the near-term storage slice.

# Development Plan
- [ ] Decide: shared `Asset` path (a futures contract is "just another stock_id with price_hourly") vs a parallel `Future`/`FuturesMarket` class.
- [ ] Session/timeframe handling for ~24h futures in `utils/bars.py` (resampling rules differ from equity RTH).
- [ ] Root → contract navigation (front-month, term structure) at the analysis layer.
- [ ] Tests against FakeDB.

# Dependencies
##### Depends on
- [[Futures OHLCV storage and retrieval]].
##### Requires
- `data_models.py`, `utils/bars.py`.
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `data_models.py`, `utils/bars.py`.
