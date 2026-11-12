---
FileType: 🚥 Dev Item
Type: Chore
Status: Backlog
Priority: 🔴
Bucket: data-quality
Cycle:
DependsOn:
StartDate:
EndDate:
tags:
---
> [!warning] Found by [[Full codebase sanity check and test audit]] on 2026-11-12.

# Summary
In the current prod DB (`rosmerta__prod` on localhost), **`fundamentals` and `dividends` are empty (0 rows)**, and `fetch_log` has no EDGAR or dividend entries at all. The schema is there (including the later-added columns), so it looks like only prices and trades came across when the DB moved local. Result: net income is NULL for all 100 stocks in the watchlist overview, and nothing that reads fundamentals or dividends has data.

# Development Plan
- [ ] Check whether the old LAN DB (`10.0.0.1/stocks`) still has the data. If it does, decide between copying it across and re-fetching.
- [ ] Otherwise re-fetch: `bulk fundamentals --list core` (EDGAR, no IB needed; 10 req/s, `email` User-Agent in `.env`) and `bulk dividends --list core` (EDGAR with IB fallback for ADRs).
- [ ] Spot-check a few tickers against EDGAR; confirm the watchlist overview shows net income.
- [ ] Note: the `dividends` watchlist the code auto-populates (and `market_test.py` targets) doesn't exist in prod yet; a dividends run recreates it.

# Dependencies
##### Depends on
- Access to the old DB (optional); EDGAR contact email in `.env`; IB Gateway for the dividend fallback.
##### Requires
- Nothing in code; operational.

# Links
##### Relevant Docs
- `price_retrival/edgar_api.py`, `price_retrival/dividends_api.py`, `price_retrival/bulk_fetch.py`, [[Move PostgreSQL Local]].
##### Prerequisite for
- [[Basic Valuation Model]], [[Fundamentals Gap Detection]], [[Better financials coverage and processing]], the watchlist overview.
