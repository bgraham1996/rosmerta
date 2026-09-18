---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: ingestion
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema + Cycle 2.

# Summary
Run the IB **Gateway headless** (no manual desktop login) so scheduled/automated fetch — and eventually [[Live data streaming]] — can run unattended. Today the Gateway is a manual GUI app at `127.0.0.1:4001`, which blocks any cron/scheduled workflow. Use **IBC (IBController)** for auto-login + auto-restart, optionally containerised.

# Development Plan
- [ ] Evaluate IBC for automated login (and the 2FA/session-timeout handling IB enforces).
- [ ] Optionally containerise the Gateway + IBC; supervise and auto-restart on disconnect.
- [ ] Manage credentials securely (out of the repo; `.env`/secrets).
- [ ] Confirm the existing fetchers connect unchanged to the headless endpoint.

# Dependencies
##### Depends on
- Nothing hard.
##### Requires
- IBC; optionally Docker.
# Links
##### Relevant Docs
- `price_retrival/ib_api.py` (`127.0.0.1:4001`), Architecture Vision → containerisation, [[Move PostgreSQL Local]].
##### Prerequisite for
- [[Data Refresh Workflows]] on a schedule and [[Live data streaming]] running unattended.
