---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Done
Priority: 🟡
Bucket: data-quality
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 2026-09-20
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

> [!success] Done (2026-09-20)
> `db/db_scripts/migrations/dedupe_stocks_symbol_exchange.sql` (trade-aware: repoints `watchlist_members` **and** `trade` rows onto the SMART canonical rows, then swaps `uq_symbol_exchange` for `UNIQUE(symbol)`). Applied & verified on the dev DB: 102 stocks / 102 distinct symbols, `uq_stocks_symbol` present, 0 trades on stale rows. Idempotent `seed_stock_tags.sql` added too. Version-controlled on `main` (commit `fb70e4b`).
