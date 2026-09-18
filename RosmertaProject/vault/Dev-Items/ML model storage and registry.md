---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: analysis
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema + Cycle 2.

# Summary
A place to **persist, version, and load** trained models plus their metadata (params, feature set, training window, eval metrics), so ML experiments are reproducible and a chosen model can be reused by the dashboard / signal layer instead of retraining ad-hoc. Directly supports the planned [[XGBoost Model]].

# Development Plan
- [ ] Decide storage: model artifacts (joblib/pickle) on disk + a `models` table (or metadata JSON) for the registry.
- [ ] Registry API: `save(model, meta)`, `load(name, version)`, `list()`, `latest(name)`.
- [ ] Record with each model: feature set used, train/test window, metrics, and a version/tag.
- [ ] Gitignore the artifact dir; keep only metadata in the DB/repo.

# Dependencies
##### Depends on
- [[Feature Generation Workflow]] — models train on its feature matrix.
##### Requires
- `joblib`/`pickle`; a `models` table if DB-backed.
# Links
##### Relevant Docs
- `db/init.sql` (new `models` table), [[XGBoost Model]], [[Feature Generation Workflow]].
##### Prerequisite for
- Reproducible ML; serving model predictions in the dashboard.
