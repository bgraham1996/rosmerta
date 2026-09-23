---
FileType: 🚥 Dev Item
Type: Feature
Status: Done
Priority: 🔴
Bucket: core-components
Cycle: "1"
DependsOn: 
StartDate: 2026-09-23
EndDate: 2026-09-23
tags: 
---
> [!success] Done — 2026-09-23. Migration `db/db_scripts/migrations/add_futures_contract_support.sql` (M1 + M2) written, applied and verified idempotent against the live dev DB (`rosmerta__prod`); all 102 existing equity rows backfilled to `sec_type='STK'`. Validated against IB (Gateway ping): `localSymbol ESZ6 → normalised ESZ26`, `lastTradeDateOrContractMonth '20260918' → expiry DATE`, `multiplier '50'`, `conId → ib_con_id`. Landed on branch `worktree-futures-schema-migration` (unpushed). The idempotent register-root/contract helper shipped with the sibling item as `price_retrival/futures.py`. **One box deferred by choice:** reflecting the change into `db/init.sql` is held for [[Reconcile DB schema - init migrations and scripts]] — the migration is the source of truth for now.

# Summary
The schema change that lets futures live in the existing `stocks` table as **contracts-as-stocks under a self-referential parent** (see [[Futures asset data retrival and storage]] for rationale). Each contract (`ESH26`) is a `stocks` row; its root (`ES`) is a `stocks` row; contracts point at the root via a new `parent_stock_id`.

> [!info] Decisions (2026-09-20)
> - **M2 is in scope** (confirmed): add contract metadata, not just the parent link.
> - **Canonical symbol = the normalised symbol** (e.g. `ESH26`), **not** IB's `localSymbol` (`ESH6`). Rationale: the DB is the source of truth and must be **API / third-party agnostic** — vendor identifiers get mapped *in*, never stored as the canonical key. See [[Reconcile DB schema - init migrations and scripts]] for the broader schema hygiene this sits inside.

# Development Plan
- [x] **M1 — parent link.** `ALTER TABLE stocks ADD COLUMN parent_stock_id INTEGER REFERENCES stocks(stock_id)` (nullable; indexed via `idx_stocks_parent_stock_id`). Equities stay `NULL`.
- [x] **M2 — contract metadata.** Added `sec_type` (`STK` / `FUTROOT` / `FUT`, CHECK-constrained, existing rows backfilled to `STK`), `expiry` (`DATE`), `multiplier` (`NUMERIC(20,4)`).
- [x] Store the **normalised** symbol in `stocks.symbol` (`ESH26`); vendor id kept in `ib_con_id` (see `price_retrival/futures.py`).
- [ ] ~~Reflect in `db/init.sql`~~ — **deferred by choice** to [[Reconcile DB schema - init migrations and scripts]]; the forward migration is in `db/db_scripts/migrations/` and is the source of truth for now.
- [x] Verified: `ES` (root) + `ESH26` / `ESM26` (contracts) do **not** trip `UNIQUE(symbol)`, and `parent_stock_id` FKs resolve (rolled-back live-DB smoke test).
- [x] Idempotent register-root/contract helper: `futures.get_or_create_root` / `register_contract` (`ON CONFLICT (symbol)`).

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
