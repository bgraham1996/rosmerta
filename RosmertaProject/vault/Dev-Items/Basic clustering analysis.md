---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema; moved to Cycle 3 (builds on the ML/feature work).

# Summary
Cluster the universe by behaviour (returns / volatility / indicator features) to surface groups of similar-moving stocks — useful for diversification, pair ideas, and seeing market structure/regimes. Exploratory, not a screener.

# Development Plan
- [ ] Build a per-stock feature vector from the [[Feature Generation Workflow]] (returns, vol, correlations, indicator snapshots).
- [ ] Standardise features; run k-means / hierarchical clustering (choose k via elbow/silhouette).
- [ ] Label clusters and visualise (dashboard view — scatter/dendrogram, cluster membership table).
- [ ] Keep it reproducible (seed) and cheap to re-run over a window.

# Dependencies
##### Depends on
- [[Feature Generation Workflow]] — needs the feature matrix.
##### Requires
- `scikit-learn` (not yet a dependency), `scipy` (already present).
# Links
##### Relevant Docs
- [[Feature Generation Workflow]], `data_models.py` (`Market` panels), `dashboards/views/`.
##### Prerequisite for
- Diversification / correlation-aware selection; regime analysis.
