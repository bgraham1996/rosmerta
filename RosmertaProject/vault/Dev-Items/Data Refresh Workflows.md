---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: workflows
Cycle: "2"
DependsOn: Incremental Fetch and Dedup
StartDate: 
EndDate: 
tags: 
---
# Summary
The Architecture Vision names three priority CLI workflows for keeping data current: **`daily download`** (p1 — recent bars for a ticker list), **`backfill`** (p2 — last ~2 years for a ticker list), and **`repair`** (p3 — find and fill gaps in existing series). Build these as first-class commands/workflows on top of the incremental-fetch logic, so refreshing the book is one command instead of manual per-ticker `fetch` calls. This is the concrete home for the started `workflows/refresh_data/` scaffolding.

# Development Plan
- [ ] `daily download --list <watchlist>` — incremental price + (optionally) fundamentals for the whole list.
- [ ] `backfill --list <watchlist> --years 2` — bounded historical pull respecting existing data.
- [ ] `repair --list <watchlist>` — detect gaps (missing hours/days) and refetch only those windows.
- [ ] Land them under `workflows/refresh_data/`; wire into the click CLI matching the lazy-import + Rich-output style.
- [ ] Dry-run + summary table (per ticker: fetched / skipped / repaired / error).

# Dependencies
##### Depends on
- [[Incremental Fetch and Dedup]] — these commands are thin drivers over that logic.
##### Requires
- Watchlist resolution (`bulk_fetch.get_watchlist_tickers`), IB Gateway + EDGAR.
# Links
##### Relevant Docs
- `workflows/refresh_data/` (started), `price_retrival/bulk_fetch.py`, `main.py` (CLI group), Architecture Vision → CLI priorities.
##### Prerequisite for
- Scheduled/automated refresh; reliable inputs for the stats + valuation layers.
