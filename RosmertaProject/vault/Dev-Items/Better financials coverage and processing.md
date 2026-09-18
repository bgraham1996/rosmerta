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
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema + Cycle 2.

# Summary
Broaden and clean the fundamentals pipeline beyond the current EDGAR pull: capture more statement line-items, widen XBRL tag coverage, compute derived metrics (margins, growth, ratios), and handle units/restatements consistently. This is the broader coverage/quality effort that **extends** [[Fundamentals Gap Detection]] (which focuses on finding the missing quarters).

# Development Plan
- [ ] Audit captured vs desired line-items across income statement / balance sheet / cash flow.
- [ ] Extend `XBRL_TAG_MAP` in `edgar_api.py`; log unmapped concepts to find gaps.
- [ ] Compute derived metrics into `fundamentals` (or a view): margins, YoY/QoQ growth, key ratios.
- [ ] Normalise units and handle restated periods so downstream analysis is consistent.

# Dependencies
##### Depends on
- Overlaps with [[Fundamentals Gap Detection]] — coordinate so they don't double up.
##### Requires
- EDGAR (+ optional IB fallback); `fundamentals` schema changes for new columns.
# Links
##### Relevant Docs
- `price_retrival/edgar_api.py` (`XBRL_TAG_MAP`), `db/init.sql` (`fundamentals`, `v_latest_fundamentals`), Project Log 2026-03-19 (EDGAR gaps).
##### Prerequisite for
- [[Basic Valuation Model]] (richer, cleaner fundamentals to score on).
