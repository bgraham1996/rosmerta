---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Planned
Priority: 🔴
Bucket: tech-debt
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!info] Rescoped — 2026-11-11 (scheduled into [[Cycle 3]]). Most of this has already landed: the `db/` layout (`db/db_scripts/`, `db/scripts/`), the move to the `RosmertaProject/` vault, the `trades_api.py` + offset-growth commits, and the `.gitignore` tidy-up. **What remains:** decide `data-models/` vs the root `data_models.py` (one home for the analysis layer), and fix `data-models/time-periods.py` (`raise:` syntax error at line 46). Empty stub files are intentional placeholders — leave them.


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
