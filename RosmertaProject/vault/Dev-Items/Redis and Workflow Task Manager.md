---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: workflows
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
The aspirational rearchitecture from the Architecture Vision: a **Redis-backed workflow/task manager** (the "Copernicus Core" idea) so data can be inserted/processed **per ticker, concurrently**, instead of the current serial `_run_bulk_fetch` loop. Redis for streams + cache, a shared worker/task library, service-defined tasks built from a standard baseplate (actions/statuses/params), ideally containerised. This is a large, version-spanning effort — captured here as a Cycle-3 candidate, explicitly **out of V2 scope**.

# Development Plan
- [ ] Design doc first (`project/design/workflow-manager.md`): task baseplate, statuses, redis stream/queue shape, worker pool.
- [ ] Review the prior implementation at `Developer/old_investment_dirs/rosmerta/` (`app/radio/rosmerta_messages.py`, `app/workflow_handler/rosmerta_tasks.py`, `rosmerta_workers.py`) for what to pull forward.
- [ ] Stand up Redis (containerised); a minimal worker consuming a stream.
- [ ] Port the refresh workflows onto the task manager for per-ticker concurrency.

# Dependencies
##### Depends on
- [[Data Refresh Workflows]] — build the plain workflows first, then move them onto the task manager.
##### Requires
- Redis (Docker); containerisation groundwork.
# Links
##### Relevant Docs
- Architecture Vision (target stack + workflow-manager philosophy), `Developer/old_investment_dirs/rosmerta/` (prior impl), Roadmap backlog items 1–2.
##### Prerequisite for
- Scaling to 500–1000 tickers without serial-fetch bottlenecks.
