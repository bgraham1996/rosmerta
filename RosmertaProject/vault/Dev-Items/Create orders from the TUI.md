---
FileType: 🚥 Dev Item
Type: Feature
Status: Backlog
Priority: 🟡
Bucket: trading
Cycle: "3"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
> [!note] Your item — drafted from the title. Edit freely. Migrated to the new schema; moved to Cycle 3 (live execution, alongside the ML/rearchitecture work).

# Summary
Add an interactive **terminal UI** to review candidates and **place orders** through IB — turning rosmerta from read-only (fetch / analyse / `portfolio`) into something that can act. Long-only per how the book is traded (buy then sell, never short). Uses `ib-insync` `placeOrder`. This is a significant step: real execution, so it should start on paper and carry hard safety guards.

# Development Plan
- [ ] Pick the TUI layer: `rich` (already a dep) for a simple screen, or `textual` for a full interactive app.
- [ ] Screens: current positions/P&L, a candidate list, and an order-entry panel (market/limit, quantity, long-only).
- [ ] Build + submit orders via IB; explicit confirmation step; paper-first (guard against the live port).
- [ ] Log every order + fill; reconcile against the `trade`/`position` tables.

# Dependencies
##### Depends on
- [[Trades and Portfolio Analytics]] — positions/P&L to show alongside order entry.
##### Requires
- IB Gateway with trading permissions; `ib-insync`, `rich`/`textual`.
# Links
##### Relevant Docs
- `price_retrival/{ib_api,portfolio,trades_api}.py`, `main.py` (CLI), `db` (`trade`/`position`).
##### Prerequisite for
- Semi-automated execution of signals from the analysis layer.
