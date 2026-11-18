---
FileType: 🚥 Dev Item
Type: Feature
Status: Planned
Priority: 🟡
Bucket: ingestion
Cycle: "4"
DependsOn: Headless IB
StartDate:
EndDate:
tags:
---
> [!info] Split out of [[Refresh stale equity prices and trades]] on 2026-11-18, where it was the "decide on a cadence" step. Scheduled into [[Cycle 4]] with [[Headless IB]]: the Gateway's manual login is what has kept this off the board.

# Summary
Every data refresh is a manual command today, and nothing is scheduled: no cron job and no launchd agent. Before [[Cycle 3]], prod prices stopped at 2026-07-17 and trades at 2026-07-24, and nothing noticed for four months. Run the refreshes on a timer and flag any that didn't happen.

# Development Plan
- [ ] **Scheduler:** a launchd job (macOS) on weekdays after the US close. Start with:
  - `workflows/refresh_data/update_core.py` (prices; needs the Gateway)
  - **one** `fetch trades` attempt (Flex; no Gateway). Never retry in a loop: repeated SendRequests caused the 2026-11-12 lockout. If a run fails, wait for the next one.
- [ ] Log each run to a file (per-job status, rows written, errors).
- [ ] **Staleness check:** report the age of the newest `price_bars` bar for `core`, the newest `trade`, and the latest fundamentals fetch, with a threshold. Could live in [[Health and Init Command]] or a line in the weekly report.
- [ ] Once the price job has run reliably for a while, add `bulk fundamentals --list core` (weekly) and `update_futures.py` (daily).
- [ ] Note what has to be awake: the Mac, the LAN DB box, and the headless Gateway.

# Dependencies
##### Depends on
- [[Headless IB]] (the price and futures jobs need a logged-in Gateway).
##### Requires
- launchd (or cron); the existing refresh workflows; Flex token; EDGAR.

# Links
##### Relevant Docs
- `workflows/refresh_data/update_core.py`, `workflows/refresh_data/update_futures.py`, `price_retrival/trades_api.py`, [[Data Refresh Workflows]].
##### Prerequisite for
- Reliable recent-window analysis, the weekly report, [[Trades and Portfolio Analytics]] staying current.
