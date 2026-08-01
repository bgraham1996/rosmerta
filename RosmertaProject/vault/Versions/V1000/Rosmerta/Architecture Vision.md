---
FileType: 🏛️ Architecture
Project: rosmerta
Status: 🟡 Aspirational
Created: 2026-06-15
SourceArchive: # former vault location — these notes stayed behind on extraction
  - "Note Processing/New Notes/Rosmerta Architecture.md"
  - "Note Processing/New Notes/Roserta Architecture.md"
  - "Note Processing/New Notes/Rosmerta Extended Core.md"
tags:
  - rosmerta
  - project
  - architecture
---

# Architecture Vision

The parent vault holds an **envisioned rearchitecture** of rosmerta. It is
**aspirational** — `Status: 🟡` — and differs materially from what's built. See
the gap table at the bottom. See also [[Parent Vault Intel]].

## Target stack (Rosmerta Architecture)

- **Redis** — streams + cache
- **PostgreSQL** — backend (stored procedures / "backend functions")
- **argparse** for the CLI *(repo actually uses `click`)*
- **uv** for project management / tooling
- **JSON** for quick configs
- **threading** for concurrency + a **custom task-management architecture**
- **Plotly** (and "swift") for dashboarding; possibly **iframes in Obsidian**

### Envisioned app shape

The "rosmerta app" creates the environment, connects to all services, and builds
one large object that **reacts to its own redis stream**, composed from layered
sub-objects:

- **API layer** — all API requests (api *trading* would be a separate service)
- **CLI layer** — command mappings/tooling
- **DB layer** — all read/write + invoking stored procedures
- **Workflow-manager layer** — workflow configs + concurrent tasks
- **Router** — main business logic tying the layers together

## Data processing & tooling (Roserta Architecture)

- **APIs by cadence:** futures (daily), stock (daily), financials (weekly) + a
  calendar/info service.
- **Processing:** multi-timeframe EMAs (+ standardisation), aggregation, RSI &
  MACD, stat-arb tools with timeframe-offset management.
- **DB tools:** data export for dashboarding; backup / tests / deploy / redeploy.
- **Dashboard:** wait for Sierra Charts setup; focus on identifying levels & zones.
- **CLI commands (priorities):** `daily download` (p1, recent data for a ticker
  list), `backfill` (p2, last 2 years for a ticker list), `repair` (p3, find gaps
  in existing series); plus `init` / health-check (DB + redis up?) and
  `create data exports`.
- **Manual for now:** adding tickers, adding/updating workflows, swapping ticker
  lists.
- **Point of rosmerta:** "a relatively lightweight tool for managing asset data
  collection, processing, and analysis."

## Workflow-manager philosophy (Rosmerta Extended Core)

Built on a shared **`Copernicus Core`**. Problem: data must be inserted/processed
**per ticker, separately**. Proposed shape:

- A **shared workflow library** — worker management + redis pub/sub streams.
- **Service-defined tasks** built from a **standard task baseplate** (defined
  actions, statuses, parameters).
- **Redis services**, ideally containerised.
- Diagram: `workflow management python architecture.canvas` (Obsidian Canvas).

## Plan vs. reality

| Vision (vault) | Repo as built (`HEAD`) |
|---|---|
| Redis streams + cache | **None** |
| Custom workflow/task manager, worker pool | **None** (bulk fetchers loop serially; `_run_bulk_fetch` is the only "driver") |
| argparse CLI | **`click`** group `rosmerta` (`main.py`) |
| App reacting to its own redis stream | Plain command-per-invocation CLI |
| Stored-procedure "backend functions" | SQL schema + scripts; logic in Python |
| Containerised services / on-off bash script | Manual `uv run main.py`; IB Gateway local, Postgres on LAN box |
| Dashboard "wait for Sierra Charts" | **Dash app already built** (`dashboards/`) |

A **prior implementation** that *did* pursue the redis/worker vision exists at
`Developer/old_investment_dirs/rosmerta/` (`app/radio/rosmerta_messages.py`,
`app/workflow_handler/rosmerta_tasks.py`, `rosmerta_workers.py`). The current repo
is a leaner rewrite; these notes describe pulling that vision forward. See
[[Supporting Notes]].

Related: [[Roadmap & Versioning]] · [[Project Log & Milestones]]
