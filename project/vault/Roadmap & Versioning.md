---
FileType: 🚥 Roadmap
Project: rosmerta
Status: 🟢 Active
Created: 2026-06-15
SourceArchive: # former vault location — these notes stayed behind on extraction
  - "Note Processing/New Notes/Rosmerta V1-0-0.md"
  - "Note Processing/New Notes/Rosmerta Basic Dev Plan.md"
  - "Note Processing/New Notes/Rosmerta Week 41 2025.md"
  - "Periodic Notes/Week 12-25 2026.md"
tags:
  - rosmerta
  - project
  - roadmap
---

# Roadmap & Versioning

Capture of release planning, backlog, and the weekly task trail from the parent
vault. See [[Parent Vault Intel]] · [[Project Log & Milestones]].

## V1.0.0 release checklist

From **Rosmerta V1-0-0** (`FileType: 🚥 Dev Item`, `DevType: 🚀 Version`,
`Priority: 🔴`, `ReviewDate: 2026-04-12`, linked to `Project Rosmerta Neo`):

- [ ] Fundamentals download — *built (EDGAR)*
- [ ] 10 years of price download — *built (IB hourly bars, chunked)*
- [ ] Basic DB helper scripts — *partial (`db_scripts/`, `db_tools/`)*
- [ ] 5 indicators — *built: `sma, ema, rsi, bollinger, obv, vwap` (6)*
- [ ] Simple fundamentals aggregation model — **open**
- [ ] Simple Claude tagging — **open**
- [ ] Basic XGBoost implementation — **open**

Meta TODOs in that note: move these to "dev items" with a dataview, add a "master
version viewer." *(Status assessments above are this capture's read of the repo,
not from the source note.)*

## Ordered backlog — Rosmerta Basic Dev Plan

(`📬 Capture`, `ReviewDate: 2026-06-28`) — "order of functionality to work on":

1. Redis instance — ideally via Docker
2. Workflow + task + messaging organiser
3. uv CLI-powered workflows: **daily download / backfill / add new ticker / repair**
4. Database data processing
5. Database upgrades
6. Data export tools for the dashboard
7. Basic dashboard — *built (Dash)*
8. Logging library

> Note the redis + task-manager items (1–2) are the **unbuilt architecture vision**
> — see [[Architecture Vision]].

## Week 41 2025 task list

From **Rosmerta Week 41 2025** (`📅 Periodic Note`):

- [ ] uv tool: daily stock-data download + DB insertion
- [ ] uv tool: futures-data workflow
- [ ] redis + threading task manager
- [ ] redis deployed in a container
- [ ] bash script to turn the system on/off
- [ ] basic DB management system (poss. containerised)
- [ ] uv tool: data export to Plotly dashboard (or CSV)
- [ ] decide route for quarterly data (manual vs free API)

## Weekly task trail (Periodic Notes 2026)

Rosmerta recurs as weekly checkboxes — the de-facto to-do system:

| Week | Item | State |
|---|---|---|
| 14 | `Rosmerta V1-0-0`; update prod + retrieve new data with helper scripts | open / in-progress |
| 16–18 | "rosmerta work" | in-progress (`[-]`) |
| 21 | "Get proper plan in place for rosmerta and drosophila" | open |
| 22 | market analysis + basic ML; dividend data implementation; "get up to date" | open |
| 23 | **formalise rosmerta development cycle & infrastructure**; organise dev process | open |
| 25 | "get rosmerta set up fully locally" | open |

`[-]` = in-progress/deferred, `[ ]` = open, `[x]` = done (Obsidian Tasks syntax).

## Local infrastructure backlog (added 2026-06-15)

New infra todos — supports the Week 25 "get rosmerta set up fully locally" goal:

- [ ] Move PostgreSQL off the LAN Ubuntu box (`bg@10.0.0.1`, ethernet-connected)
  onto this machine — local Postgres so the DB no longer depends on the server
  being awake/mounted. Update `.env` defaults (`DB_HOST=10.0.0.1` → local) and
  the `server-*` zsh aliases / docs once migrated.
- [ ] Look into further IB **Gateway** configuration — what more can be tuned
  beyond the current `127.0.0.1:4001` paper setup.
- [ ] Evaluate containerising some functionality (Docker) — ties into the redis +
  task-manager items in the [[Architecture Vision]] and the backlog above.
- [ ] Start adding more tests (added 2026-06-16). A DB-free/network-free `tests/`
  pytest suite already exists (indicator registry, OHLCV resampling, `Asset`/`Market`
  analysis via the `FakeConnection` fixture); grow coverage from there — extend the
  analysis-layer tests and find a way to exercise the fetchers (`price_retrival/`),
  which currently have none.

## Business goal (Week 23 2026)

Target: rosmerta generating **~£1000/month on ~£20k capital** (~£250/week) as a
first income stream, with the intent to build a second £1000/month source
alongside.

Related: [[Architecture Vision]] · [[Project Log & Milestones]]
