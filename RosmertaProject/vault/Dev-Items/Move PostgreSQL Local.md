---
FileType: 🚥 Dev Item
Type: Tech-Debt
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
Postgres currently lives on the LAN Ubuntu box (`bg@10.0.0.1`), so every DB operation depends on that server being awake and mounted — friction for the "get rosmerta set up fully locally" goal (Week 25). Move the `stocks` DB onto this machine (local Postgres, optionally containerised) so the CLI and dashboard work with nothing else powered on.

# Development Plan
- [ ] Stand up local Postgres (native or Docker); create the `stocks` DB + schema from `db/init.sql`.
- [ ] Migrate existing data (dump from `10.0.0.1` → restore locally).
- [ ] Update `.env` defaults (`DB_HOST=10.0.0.1` → local) and note the change in `CLAUDE.md` / README.
- [ ] Update / retire the `server-*` zsh aliases and the `pgcli` connect docs.
- [ ] Verify CLI + dashboard + pytest all work with the server box off.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Local Postgres install (or Docker); enough disk (DB ≈ 127 MB at 50×10yr, room to grow).
# Links
##### Relevant Docs
- `db_config.py`, `.env`, `db/init.sql`, `~/.zshrc` (`server-*` aliases), Roadmap → local infrastructure backlog.
##### Prerequisite for
- Reliable offline dev; a precondition for later containerisation.
