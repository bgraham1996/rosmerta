---
tags:
DevStatus: 
DependsOn: 
DevBucket: tech-debt
ReviewCount:
FileType: 🚥 Dev Item
StartDate:
EndDate: 
Priority: 🔴
Dev Status:
Cycle: "1"
ProjectCodes:
Requirements:
Completed: false
---

# Summary (Tech Debt)
The working tree has a **half-finished reorganization**, all uncommitted, leaving the repo in a fragile checkpoint. Land it into clean commits before starting new feature work — the messy tree is part of what made the project hard to return to.

# What's in flight (uncommitted)
- `db_scripts/` → `db/db_scripts/` + new `db/scripts/returns_*.sql` (old paths deleted, new untracked).
- `project/vault/*.md` deleted → replaced by the `RosmertaProject/` Obsidian vault.
- New stub packages: `data-models/`, `utils/utils/`, `workflows/` (some empty; `data-models/time-periods.py` has a `raise:` syntax error).
- Today's real work, also uncommitted: `trades_api.py` fixes, `days_offset_gain` + `get_offset_growth` + tests.

# Development Plan
- [ ] Commit the genuinely-done work first, in coherent chunks:
    - [ ] `trades_api.py` (transaction_date fallback + SendRequest retry hardening).
    - [ ] offset-growth feature (`indicators.py`, `data_models.py`, tests) — suite green (42 passed).
- [ ] Settle the `db/` layout and update `CLAUDE.md` (it still references `db_scripts/`).
- [ ] Flesh out or remove the empty stub packages; fix/remove broken `data-models/time-periods.py`.
- [ ] Reconcile `RosmertaProject/` vs the deleted `project/vault/` — one home for planning.
- [ ] Update `.gitignore` / `CLAUDE.md` to match the new layout.

# Dependencies
##### Depends on
- Nothing — bookkeeping, but blocks clean history for everything after.
# Links
##### Relevant Docs
- CLAUDE.md (paths need updating post-reorg).
