---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🔴
Bucket: core-components
Cycle: "1"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
The schema change that lets futures live in the existing `stocks` table as **contracts-as-stocks under a self-referential parent** (see [[Futures asset data retrival and storage]] for rationale). Each contract (`ESH26`) is a `stocks` row; its root (`ES`) is a `stocks` row; contracts point at the root via a new `parent_stock_id`.

> [!info] Decisions (2026-09-20)
> - **M2 is in scope** (confirmed): add contract metadata, not just the parent link.
> - **Canonical symbol = the normalised symbol** (e.g. `ESH26`), **not** IB's `localSymbol` (`ESH6`). Rationale: the DB is the source of truth and must be **API / third-party agnostic** — vendor identifiers get mapped *in*, never stored as the canonical key. See [[Reconcile DB schema - init migrations and scripts]] for the broader schema hygiene this sits inside.

# Development Plan
- [ ] **M1 — parent link.** `ALTER TABLE stocks ADD COLUMN parent_stock_id INTEGER REFERENCES stocks(stock_id)` (nullable; index it). Equities stay `NULL`.
- [ ] **M2 — contract metadata.** Add `sec_type` (`STK` / `FUTROOT` / `FUT`), `expiry` (last-trade date or contract month), `multiplier`. `sec_type` distinguishes a future-root/contract from an equity; `expiry`/`multiplier` describe the contract.
- [ ] Store the **normalised** symbol in `stocks.symbol` (`ESH26`); keep any vendor id in a dedicated column (`ib_con_id` already exists) rather than the symbol.
- [ ] Reflect both migrations in `db/init.sql` (canonical) **and** as forward migrations in `db/db_scripts/migrations/` — ideally after / alongside [[Reconcile DB schema - init migrations and scripts]] so it lands on an accurate baseline.
- [ ] Verify: `ES` (root) + `ESH26` / `ESM26` (contracts) do **not** trip `UNIQUE(symbol)` (distinct symbols), and `parent_stock_id` FKs resolve.
- [ ] A small helper to register a root + its contract rows (idempotent `ON CONFLICT`).

# Dependencies
##### Depends on
- [[Reconcile DB schema - init migrations and scripts]] (preferably) — land on a reconciled `init.sql`.
##### Requires
- Live dev DB (localhost/rosmerta__prod) for applying/verifying migrations.
# Links
##### Relevant Docs
- Parent: [[Futures asset data retrival and storage]]. `db/init.sql`, `db/db_scripts/migrations/`.
##### Prerequisite for
- [[Futures OHLCV storage and retrieval]].
