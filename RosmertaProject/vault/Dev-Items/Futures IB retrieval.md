---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: ingestion
Cycle: "2"
DependsOn: Futures OHLCV storage and retrieval
StartDate: 
EndDate: 
tags: 
---
# Summary
Wire **IB** as a real source for the futures OHLCV storage built in [[Futures OHLCV storage and retrieval]]. Deferred on purpose — storage/retrieval is being built source-agnostic first.

# Development Plan
- [ ] Fetch via `ib-insync` `Future(symbol, lastTradeDateOrContractMonth, exchange, ...)` (and later `ContFuture`) instead of `Stock`; real exchange (CME/NYMEX/CBOT), not `SMART`.
- [ ] `useRTH=False` (futures trade ~23h); reuse the 30-day chunking + `whatToShow='TRADES'`.
- [ ] Map an IB contract → a `stocks` contract row (`sec_type='FUT'`, `expiry`, `multiplier`, `ib_con_id`) under its root, then write candles through the generalised storage path.
- [ ] Front-month / roll convention; continuous series handling.
- [ ] Requires IB futures market-data permissions.

# Dependencies
##### Depends on
- [[Futures OHLCV storage and retrieval]], [[Futures schema - contracts as stocks]].
##### Requires
- `ib-insync`, IB Gateway, futures data permissions.
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `price_retrival/ib_api.py`.
