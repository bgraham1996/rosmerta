---
FileType: 🚥 Dev Item
Type: Chore
Status: In Progress
Priority: 🔴
Bucket: data-quality
Cycle: "3"
DependsOn: Dividends fetcher fixes and IB-sourced load
StartDate: 2026-11-12
EndDate:
tags:
---
> [!warning] Found by [[Full codebase sanity check and test audit]] on 2026-11-12; pulled into [[Cycle 3]] the same day.

> [!info] Progress 2026-11-12: **fundamentals restored; dividends paused** (user decision: load dividends from IB, not EDGAR).
> - Old LAN DB `10.0.0.1` was asleep and unreachable. Since EDGAR is the original source, I re-fetched instead of waking it.
> - **Fundamentals:** `bulk fundamentals --list core` → 84 ok, 15 empty, and 1 GEHC save that actually failed (the bulk tally shows it as ✓). Result: **6,679 rows across 84 stocks**, latest period 2026-10-03; watchlist-overview net income 83/100. Spot-check: AAPL has 91 periods (FY2007–FY2026 + quarterlies), NI and revenue on every row.
> - **Empty (expected):** 13 foreign issuers filing 20-F/IFRS or not SEC-registered: ABB, ALC, ASML, AZN, BNTX, GSK, NVO, NVS, RHHBY, SQM, TECK, TSM, TTE.
> - **Wrong or missing (bugs):** GEHC, AMZN, APGE, DOMO → [[EDGAR fundamentals extraction fixes]].
> - **Dividends:** the EDGAR path stores one Q2-YTD "declared per share" row per year at the fiscal period end (PFE 2025 = $0.86 vs the real $1.72; no pay/declaration dates). Not bulk-loaded. **PFE pilot rows (56, `source='edgar'`) are still in prod**; remove them before the IB load. PFE is also now in a new `dividends` watchlist. → [[Dividends fetcher fixes and IB-sourced load]].

# Summary
In the current prod DB (`rosmerta__prod` on localhost), **`fundamentals` and `dividends` are empty (0 rows)**, and `fetch_log` has no EDGAR or dividend entries at all. The schema is there (including the later-added columns), so it looks like only prices and trades came across when the DB moved local. Result: net income is NULL for all 100 stocks in the watchlist overview, and nothing that reads fundamentals or dividends has data.

# Development Plan
- [x] Check whether the old LAN DB (`10.0.0.1/stocks`) still has the data. → Unreachable (asleep); re-fetched from EDGAR instead.
- [x] ~~Otherwise re-fetch~~ fundamentals done (84/100, see above). Dividends → IB-sourced load, pending [[Dividends fetcher fixes and IB-sourced load]] + IB Gateway. Original plan: `bulk fundamentals --list core` (EDGAR, no IB needed; 10 req/s, `email` User-Agent in `.env`) and `bulk dividends --list core` (EDGAR with IB fallback for ADRs).
- [x] Spot-check a few tickers against EDGAR; confirm the watchlist overview shows net income. → AAPL checked; overview 83/100.
- [ ] Re-run GEHC/AMZN/APGE/DOMO after [[EDGAR fundamentals extraction fixes]].
- [ ] Load dividends from IB for `core`; remove the PFE EDGAR pilot rows first.
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
