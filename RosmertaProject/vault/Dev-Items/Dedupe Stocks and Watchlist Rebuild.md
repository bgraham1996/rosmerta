---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟡
Bucket: data-quality
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
The `stocks` table accumulated **duplicate rows** from the watchlist seeders (logged 2026-04-24). Left unfixed, duplicates skew every per-stock query and analysis join. Do the one-off cleanup *and* leave behind a repeatable, safe watchlist-rebuild procedure so re-seeding can't re-introduce dupes.

# Development Plan
- [ ] Export current tickers + watchlist memberships to CSV (audit before touching anything).
- [ ] Migration in `db/db_scripts/migrations/`: drop watchlists → dedupe `stocks` (keep canonical row, repoint FKs) → re-add watchlists → add a uniqueness constraint on ticker.
- [ ] Make the seeders idempotent (`ON CONFLICT DO NOTHING` on ticker) so a re-run is safe.
- [ ] Verify counts before/after; document the rebuild steps.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Live DB; back up / export first.
# Links
##### Relevant Docs
- `db/db_scripts/` (`seed_dev.sql`, `core_expansion_seed.sql`, `migrations/`), Project Log 2026-04-24 dedup plan.
##### Prerequisite for
- Correct cross-sectional stats (dupes pollute `Market` panels and counts).
