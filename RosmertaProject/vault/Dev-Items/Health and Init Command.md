---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: workflows
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
A single `health` (and/or `init`) command that answers "is the system ready and how current is my data?" in one shot: is PostgreSQL reachable, is the IB Gateway up at `127.0.0.1:4001`, is EDGAR responding — and, per watchlist, how stale is each ticker's price/fundamentals data. Removes the guesswork before a fetch run and gives a fast daily "am I up to date" check.

# Development Plan
- [ ] `health` — check DB connect (`db_config`), IB Gateway socket, EDGAR ping; render a Rich status table (✓/✗ + latency).
- [ ] Data-freshness section: per watchlist, last `timestamp` / `period_end` vs now, flag stale tickers.
- [ ] Optional `init` — verify schema present (tables/views from `db/init.sql`), create if missing, seed dev watchlists.
- [ ] Lazy-import heavy deps; `--list` to scope freshness to one watchlist.

# Dependencies
##### Depends on
- Nothing hard; reads `fetch_log` / latest timestamps (nicer once [[Incremental Fetch and Dedup]] lands).
##### Requires
- `db_config.get_db_config()`, IB Gateway, EDGAR.
# Links
##### Relevant Docs
- `db_config.py`, `db/init.sql`, `price_retrival/`, Architecture Vision → `init` / health-check.
##### Prerequisite for
- Trustworthy scheduled workflows (know services are up before running).
