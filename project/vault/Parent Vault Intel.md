---
FileType: 🗂️ Intel Index
Project: rosmerta
Status: 🟢 Active
Created: 2026-06-15
tags:
  - rosmerta
  - project
  - intel
---

# Parent Vault Intel — Index

Rosmerta's code lives in this repo, but its **planning, roadmap, dev log, and
architecture vision live as notes in the wider Immrama Obsidian vault** (mostly
under `Note Processing/New Notes/` and `Periodic Notes/`). The repo's `project/`
scaffold was empty; these summaries pull that scattered intel into the repo so the
plan is versioned alongside the code.

This note is the index. The detailed captures:

- [[Project Log & Milestones]] — the `ROSM001` dev journal: dated decisions,
  milestones, and bugs (2026-03 → 2026-06).
- [[Roadmap & Versioning]] — V1 release checklist, backlog, weekly task trail, and
  the business goal.
- [[Architecture Vision]] — the envisioned redis/workflow-manager rearchitecture,
  and how it differs from what's actually built.
- [[Supporting Notes]] — index of operational/domain notes in the parent vault
  (DB setup, pgcli, click, GitHub Actions, prior implementation).

## How project management actually works here

| Concern | Where it lives | Notes |
|---|---|---|
| Project hub / journal | `Note Processing/New Notes/Project Rosmerta Neo.md` (`ProjectCode: ROSM001`) | Dated dev-log entries; the real source of project history. |
| Release planning | `Rosmerta V1-0-0.md` (🚥 Dev Item / 🚀 Version) | V1 checklist. |
| Backlog / ideas | `Rosmerta Basic Dev Plan.md`, architecture captures | Unordered/ordered feature lists. |
| Live task tracking | `Periodic Notes/Week NN 2026.md` | Weekly checkboxes — the actual to-do system. |
| Versioning / deploy | Git → `github.com/bgraham1996/rosmerta` (solo, straight-to-`main`, no PRs/CI) | No deployment pipeline; run manually via `uv run main.py`. |

## Two standing flags

1. **Plan vs. reality gap.** The vault architecture notes (redis streams, a custom
   workflow/task manager, layered api/cli/db/router app) are **aspirational**. The
   actual repo is `click` + PostgreSQL + Dash with **no redis and no task manager**.
   Treat the architecture notes as a roadmap, not a description of `HEAD`. The
   recurring weekly task "formalise the rosmerta development process" reflects this.
2. **Secret in the vault.** `Project Rosmerta Neo.md` contains a plaintext Postgres
   password and a `CREATE USER … PASSWORD` block. It is **not** copied into these
   repo notes on purpose (`project/` is committable to GitHub). DB credentials
   belong in `.env` (gitignored). See [[Supporting Notes]].

## Source notes (former vault location — archival)

These notes **stayed behind in the Immrama vault** when this project was extracted;
the intel above is the durable repo-side copy. The paths are recorded for
provenance only and were valid under `/Users/bengraham/Vaults/Immrama/Immrama/`:

- `Note Processing/New Notes/Project Rosmerta Neo.md`
- `Note Processing/New Notes/Rosmerta V1-0-0.md`
- `Note Processing/New Notes/Rosmerta Basic Dev Plan.md`
- `Note Processing/New Notes/Rosmerta Architecture.md`
- `Note Processing/New Notes/Roserta Architecture.md` *(filename typo, kept as-is)*
- `Note Processing/New Notes/Rosmerta Extended Core.md`
- `Note Processing/New Notes/Rosmerta Week 41 2025.md`
- `Note Processing/New Notes/Rosmerta deiology.md`
- `Note Processing/New Notes/{Dev Database Setup, PGCli Guide, New Database info,`
  ` Python click CLI Framework, GitHub Actions Guide, Algo 3, Algo3 Comms Channels}.md`
- `Periodic Notes/Week {12–25} 2026.md`
- `Developer/old_investment_dirs/rosmerta/` (prior implementation)
