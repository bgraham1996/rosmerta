---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟢
Bucket: analysis
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
A reusable **feature-generation** step that turns raw price/fundamental data into a standard, per-ticker feature set (returns, volatility, indicator values, macro stats) — the shared input other analysis and any future ML consumes. This is the home for the started `workflows/basic_feature_generation/` scaffolding, and it keeps feature logic in one place rather than re-derived per consumer.

# Development Plan
- [ ] Decide the v1 feature set (multi-timeframe returns, realised vol, indicator snapshots, running 20d returns, etc.).
- [ ] Build it on `data_models.Asset`/`Market` + the indicator registry so features come from the existing computation layer.
- [ ] Persist / cache generated features (table or on-demand panel) for reuse across views and models.
- [ ] Land under `workflows/basic_feature_generation/`; FakeDB tests for determinism.

# Dependencies
##### Depends on
- [[Goals-Oriented Stats Layer]] — overlapping stats; build the shared plumbing once.
##### Requires
- `indicators.py`, `data_models.py`.
# Links
##### Relevant Docs
- `workflows/basic_feature_generation/` (started), `workflows/macro_stats/`, `indicators.py`, `data_models.py`.
##### Prerequisite for
- Basic XGBoost / ML experiments (a clean feature matrix is the precondition).
