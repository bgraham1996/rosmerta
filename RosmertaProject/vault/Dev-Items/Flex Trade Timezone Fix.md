---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟢
Bucket: data-quality
Cycle: "1"
DependsOn: Flex trades ingest (done)
StartDate: 
EndDate: 
tags: 
---

# Summary (Tech Debt)
IB Flex `dateTime` values (e.g. `20260529;095303`) are parsed **naive** and stored into `trade.trade_datetime` (`TIMESTAMPTZ`), so PostgreSQL stamps them with the session timezone (observed **+01 BST**). Result: `trade_datetime` is offset by the local UTC offset instead of reflecting the true execution time.

# Development Plan
- [ ] Decide intended tz for stored executions (UTC vs exchange-local). IB Flex emits times in the account's configured tz — check the query's timezone setting.
- [ ] In `_parse_dt` / `_save_trades_to_db`, localize the naive datetime to the correct source tz before insert (or convert to UTC).
- [ ] Re-ingest / targeted-update the 21 existing rows if the offset matters (ingest is idempotent on `ib_trade_id`).
- [ ] Add a parse/tz unit test.

# Dependencies
##### Depends on
- IB Flex ingest ✅.
# Links
##### Relevant Docs
- `price_retrival/trades_api.py` (`_parse_dt`, `_save_trades_to_db`).
