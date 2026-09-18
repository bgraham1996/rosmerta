---
FileType: 🚥 Dev Item
Type: Chore
Status: Backlog
Priority: 🟡
Bucket: tech-debt
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
`tests/README.md` treats `uv run pytest` as the CI/pre-commit gate, but the repo has **no CI** and the fetchers (`price_retrival/`) have **no tests**. Add a minimal GitHub Actions workflow that runs the DB-free/network-free pytest suite on push, and grow coverage — especially a way to exercise the fetchers without hitting live IB/EDGAR.

# Development Plan
- [ ] `.github/workflows/tests.yml` — `uv sync` + `uv run pytest` on push/PR (the suite is already DB-free/network-free via the FakeDB).
- [ ] Extend analysis-layer tests (`data_models.py`, more indicators) using the existing `FakeConnection` fixture.
- [ ] Introduce fetcher tests: mock IB (`ib-insync`) and EDGAR HTTP so `price_retrival/` logic (chunking, XBRL mapping, dividend dispatch) is covered offline.
- [ ] Wire a pre-commit hook or document the CI gate.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- GitHub Actions (repo is on GitHub); `pytest` (already configured).
# Links
##### Relevant Docs
- `tests/` (+ `tests/README.md`, `conftest.py` FakeDB), `price_retrival/`, Supporting Notes → GitHub Actions Guide.
##### Prerequisite for
- Confidence to refactor the fetchers and data layer without regressions.
