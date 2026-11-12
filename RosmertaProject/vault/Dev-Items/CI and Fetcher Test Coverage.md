---
FileType: 🚥 Dev Item
Type: Chore
Status: Backlog
Priority: 🟡
Bucket: tech-debt
Cycle:
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!info] Measured 2026-11-12 ([[Full codebase sanity check and test audit]], `pytest-cov`, 144 tests): **47% overall.** 0%: `edgar_api`, `dividends_api`, `trades_api`, `portfolio`, `update_futures`, most `workflows/`. Low: `bulk_fetch` 15%, `main.py` 25%, `ib_api` 32%, `asset_deep_dive` 29%. Strong: `indicators` 95%, `ohlcv_store` 92%, `futures` 87%, `data_models` 82%.
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
