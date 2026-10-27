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
Logging is ad-hoc: each fetcher calls `logging.basicConfig(level=INFO)` *inside* a method (`ib_api.py:52`, `edgar_api.py:230`, `trades_api.py:83`), so config depends on import/call order (only the first `basicConfig` wins) and there's no central setup — while `utils/utils/logs.py` is a broken stub. The Roadmap's "Logging library" backlog item. Add one place that configures logging (level, format, console + rotating file), have every module use `logging.getLogger(__name__)`, and drop the scattered `basicConfig` calls.

# Development Plan
- [ ] A `setup_logging()` (in `utils/`) configuring root handlers, format, and a log level from env/CLI (`--verbose`).
- [ ] Call it once at CLI startup in `main.py`; remove the in-method `basicConfig` calls in the fetchers.
- [ ] Rotating file handler (e.g. `logs/rosmerta.log`) + console; gitignore the log dir.
- [ ] Delete or replace the broken `utils/utils/logs.py` stub.

# Dependencies
##### Depends on
- Overlaps with [[Finish Directory Reorg]] (which flags the broken `utils/utils/` stubs).
##### Requires
- Stdlib `logging` only.
# Links
##### Relevant Docs
- `price_retrival/{ib_api,edgar_api,trades_api}.py` (`basicConfig`), `main.py`, `utils/utils/logs.py`, Roadmap → Logging library.
##### Prerequisite for
- Debuggable scheduled workflows (know what ran and what failed).
