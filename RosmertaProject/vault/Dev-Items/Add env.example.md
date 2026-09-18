---
FileType: 🚥 Dev Item
Type: Chore
Status: Backlog
Priority: 🟢
Bucket: tech-debt
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
There's no `.env.example`, so a new setup has to reverse-engineer the required environment variables from `db_config.py` and `main.py`. `.env` is (correctly) gitignored — but that means the *shape* of the config is undocumented in-repo. Add a committed template listing every variable with placeholder values and a comment on each.

# Development Plan
- [ ] Create `.env.example`: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS` (from `db_config.get_db_config`) and `email` (SEC EDGAR User-Agent contact, from `main.py`).
- [ ] Placeholder values only — no real credentials.
- [ ] Reference it in README / CLAUDE.md setup steps.
- [ ] Optionally have `db_config` raise a clear error when a required var is missing (ties into [[Health and Init Command]]).

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Nothing.
# Links
##### Relevant Docs
- `db_config.py`, `main.py` (EDGAR `email`), README.md / CLAUDE.md (Configuration).
##### Prerequisite for
- Smoother onboarding; complements [[Move PostgreSQL Local]] (which changes the DB_* defaults).
