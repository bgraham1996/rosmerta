---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟢
Bucket: analysis
Cycle: "3"
DependsOn: Feature Generation Workflow
StartDate: 
EndDate: 
tags: 
---
# Summary
"Basic XGBoost implementation" from the V1.0.0 checklist — a first ML model over the generated feature matrix (e.g. predicting forward returns / the `days_offset_gain` target), benchmarked against the transparent valuation score. Post-V2: only worth doing once feature generation and clean data exist, so it sits behind them as a Cycle-3 candidate.

# Development Plan
- [ ] Define the prediction target (forward N-day return via `days_offset_gain` / `get_offset_growth`) and evaluation (walk-forward, no look-ahead).
- [ ] Assemble the training matrix from [[Feature Generation Workflow]] outputs.
- [ ] Train/evaluate XGBoost; compare against the [[Basic Valuation Model]] baseline.
- [ ] Surface predictions cautiously (as one signal, clearly separated from the transparent score).

# Dependencies
##### Depends on
- [[Feature Generation Workflow]] — needs a clean feature matrix.
- [[Basic Valuation Model]] — the non-ML baseline to beat.
##### Requires
- `xgboost`, `scikit-learn`; enough history + tickers (targeting 500–1000).
# Links
##### Relevant Docs
- `indicators.py` (`days_offset_gain`), `data_models.py` (`get_offset_growth`), V1.0.0 checklist, [[offset-gain-bar-count-decision]].
##### Prerequisite for
- Any model-driven ranking / signal in the dashboard.
