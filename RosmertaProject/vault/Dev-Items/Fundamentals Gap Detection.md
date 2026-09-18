---
FileType: 🚥 Dev Item
Type: Enhancement
Status: Backlog
Priority: 🟡
Bucket: data-quality
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
Milestone **M4** + the carried-over EDGAR threads. The fundamentals data has *lots of gaps* (noted 2026-03-19). Build a diagnostic that flags missing quarters per stock (e.g. "AMGN missing Q2/Q3 2022"), improve the XBRL tag mapping so fewer concepts fall through, and handle the **Q4-only-in-annual** reporting case (many filers report Q4 only inside the 10-K, so Q4 = annual − (Q1+Q2+Q3)). Add an IB fallback path for gaps EDGAR can't fill.

# Development Plan
- [ ] `check fundamentals [--list <watchlist>]` — report expected vs present `(period_end, period_type)` per stock; a gap-analysis SQL under `db/scripts/`.
- [ ] Extend `XBRL_TAG_MAP` in `edgar_api.py` for concepts currently missed; log unmapped tags encountered.
- [ ] Derive Q4 from annual − sum(Q1..Q3) when only the annual figure is filed.
- [ ] IB fallback for still-missing quarters (`--source ib`), mirroring the dividends fallback pattern.

# Dependencies
##### Depends on
- Nothing hard.
##### Requires
- `fundamentals` table, EDGAR + IB.
# Links
##### Relevant Docs
- `price_retrival/edgar_api.py` (`XBRL_TAG_MAP`), `db/scripts/`, Project Log M4 + 2026-03-19 entry.
##### Prerequisite for
- [[Basic Valuation Model]] (needs clean, gap-free fundamentals to score on).
