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
`db/init.sql` has drifted from the live schema, and structural DDL, one-off data migrations, and operational scripts are all jumbled together. Establish a clear, **manually maintained** creation-vs-upgrade split.

**Drift already present (evidence this is needed):**
- `init.sql` defines only 6 tables — it is **missing `trade` / `transaction` / `transfer` / `position`** (live via `db/add_positions.sql`) and **`watchlist_members`** (live via `db/add_watchlists.sql`, yet `Market`/`bulk_fetch` depend on it). A fresh `init.sql` build would be broken.
- `init.sql` still declares `UNIQUE(symbol, exchange)`; the live DB has `UNIQUE(symbol)` (from the dedupe migration).

> [!info] Decisions (2026-09-20)
> - **Manual, written reconciliation** — no migration framework / `schema_migrations` table for now. Keep it a documented convention.
> - **Structure (keep everything, add a fresh-create bundle):**
>   1. **Keep all existing migration files** as the historical upgrade trail — untouched, nothing deleted.
>   2. **Keep a copy of the current `init.sql`** (archive the existing one, e.g. `init_legacy.sql`) so the old baseline is preserved.
>   3. **Add a new `init.sql` + the extra `.sql` files it needs** — the fresh-create path becomes *new init + a defined set of creation scripts* (the structural pieces that belong to creation), run in a documented order. Init stays modular rather than one monolith.

**Golden rule:** never *edit* a migration already applied to a DB you can't recreate. The new-init bundle is the fresh-create path; the migration trail remains the record + upgrade path for existing DBs. Keep them in sync.

# Development Plan
- [ ] Capture the live schema as the reference: `pg_dump --schema-only` of `rosmerta__prod`.
- [ ] Archive the current `init.sql` (e.g. `db/init_legacy.sql`) — preserved copy, not the source of truth going forward.
- [ ] Build the **new fresh-create bundle**: a new `init.sql` + the extra creation `.sql` files it needs (trades/positions from `add_positions.sql`, `watchlist_members` from `add_watchlists.sql`, dividend/fundamental column adds, the `UNIQUE(symbol)` constraint, `ib_con_id`, etc.), with a **documented run order** (a `create_db.sh` / master script or a README section).
- [ ] **Keep all migration files** in `db/db_scripts/migrations/` as history; keep `dedupe_stocks_symbol_exchange.sql` as a data migration (a fresh DB has no dupes). Move the misfiled backup dump (`backup_stocks_watchlist_*.sql`) out of `migrations/`.
- [ ] **Separate operational scripts**: seeds (`seed_dev`, `core_expansion_seed`, `original_core_seed`, `seed_stock_tags`), exports/audits (`*_export.sql`, `missing_fundamentals.sql`), analysis queries (`db/scripts/returns_*.sql`).
- [ ] Remove `db/._init.sql` (macOS junk); gitignore `._*` / `.DS_Store`.
- [ ] Write the convention down: `project/design/db-migrations.md` (or `db/README.md`) — what goes in the create bundle vs migrations vs scripts, and the fresh-create run order.

# Dependencies
##### Depends on
- Nothing hard.
##### Requires
- Live dev DB for the schema-only dump / verification.
# Links
##### Relevant Docs
- `db/init.sql`, `db/add_positions.sql`, `db/add_watchlists.sql`, `db/db_scripts/migrations/`, `db/db_scripts/`.
##### Prerequisite for
- [[Futures schema - contracts as stocks]] — land the futures schema change on the reconciled create bundle, not a drifted `init.sql`.
