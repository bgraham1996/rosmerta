---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟢
Bucket: analysis
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
"Simple Claude tagging" from the V1.0.0 checklist — use an LLM to auto-tag stocks with qualitative labels (sector/theme, business description, narrative flags) that are hard to derive from numbers alone, stored in `stock_tags`. Post-V2 candidate: nice-to-have enrichment once the core data + analysis layers are solid.

# Development Plan
- [ ] Decide the tag taxonomy + which inputs feed the prompt (EDGAR business description, recent fundamentals, price behaviour).
- [ ] A batch tagging step that writes to `stock_tags` (respect the `v_active_tags` view).
- [ ] Keep it cheap/cached — tag on add or on demand, not every run.
- [ ] Surface tags in the dashboard (filter/group the universe by theme).

# Dependencies
##### Depends on
- Clean stock universe: [[Dedupe Stocks and Watchlist Rebuild]].
##### Requires
- An LLM API key; `stock_tags` table (exists in `db/init.sql`).
# Links
##### Relevant Docs
- `db/init.sql` (`stock_tags`, `v_active_tags`), `price_retrival/edgar_api.py` (business description source), V1.0.0 checklist.
##### Prerequisite for
- Theme-based screening; qualitative features for the valuation/ML layers.
