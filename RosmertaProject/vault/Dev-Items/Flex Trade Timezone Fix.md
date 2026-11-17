---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🟢
Bucket: data-quality
Cycle: "3"
DependsOn: Flex trades ingest (done)
StartDate: 2026-11-17
EndDate: 2026-11-17
tags: 
---
> [!success] Done 2026-11-17. Flex times are **US/Eastern** wall-clock: all 39 STK fills fall 09:30:01–15:25 ET, and the report's `whenGenerated` matches IB's EST response stamp. The DB session is `Europe/London`, so stored rows were 4–5 h early (4 h in the US/UK DST-gap weeks). New `IBTradesFetcher._parse_exec_dt` localises to `FLEX_TZ = "America/New_York"` and stores **UTC**; `transaction_date` keeps the exchange-local date. Tests: `tests/test_flex_trade_timezone.py` (7), suite 168 → 175. **Prod backfill:** all 39 rows rewritten by `ib_trade_id` from the saved Flex report (old values backed up to `db_exports/trade_datetime_backup_2026-11-17.csv`, gitignored); 0 rows now outside NYSE hours.

> [!info] Scheduled into [[Cycle 3]] (2026-11-11) — do before [[Trades and Portfolio Analytics]] so P&L / holding periods are built on correct execution times.

# Summary (Tech Debt)
IB Flex `dateTime` values (e.g. `20260529;095303`) are parsed **naive** and stored into `trade.trade_datetime` (`TIMESTAMPTZ`), so PostgreSQL stamps them with the session timezone (observed **+01 BST**). Result: `trade_datetime` is offset by the local UTC offset instead of reflecting the true execution time.

# Development Plan
- [x] Decide intended tz for stored executions → **UTC** (matches `price_bars`). Source tz is US/Eastern (verified from fill times + `whenGenerated`).
- [x] In `_parse_dt` / `_save_trades_to_db`, localize the naive datetime to the correct source tz before insert (or convert to UTC).
- [x] Re-ingest / targeted-update the 21 existing rows if the offset matters (ingest is idempotent on `ib_trade_id`).
- [x] Add a parse/tz unit test.

# Dependencies
##### Depends on
- IB Flex ingest ✅.
# Links
##### Relevant Docs
- `price_retrival/trades_api.py` (`_parse_dt`, `_save_trades_to_db`).
