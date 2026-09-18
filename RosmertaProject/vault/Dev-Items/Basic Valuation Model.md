---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "2"
DependsOn: Fundamentals Gap Detection
StartDate: 
EndDate: 
tags: 
---
# Summary
Milestone **M5** — a composite, **transparent** valuation score (no ML yet), so the universe can be ranked into one sortable "attractiveness" number per stock. Inputs are deliberately simple and explainable: trailing P/E vs sector median, revenue growth QoQ/YoY, EPS trend, and price vs 200-day MA. Each is normalised to a percentile rank within the universe and combined into a single score.

# Development Plan
- [ ] Define the factor set + weights in `project/design/valuation-model.md` (design first).
- [ ] Compute each factor across the `Market` universe; percentile-rank within the universe.
- [ ] Combine into one score; expose a sortable table (CLI + dashboard view).
- [ ] Keep it inspectable — show the per-factor contributions, not just the total.
- [ ] FakeDB tests for the factor math and ranking.

# Dependencies
##### Depends on
- [[Fundamentals Gap Detection]] — needs reasonably complete fundamentals to score fairly.
- [[Goals-Oriented Stats Layer]] — reuses the percentile-rank plumbing.
##### Requires
- Sector metadata (for "vs sector median") — may need a `sector` field on `stocks`.
# Links
##### Relevant Docs
- `data_models.py` (`Market`), `v_latest_fundamentals`, `project/design/valuation-model.md` (to write), Project Log M5.
##### Prerequisite for
- Any ranked "what to buy" screen; later ML (XGBoost) as a comparison baseline.
