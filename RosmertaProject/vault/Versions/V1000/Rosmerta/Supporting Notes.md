---
FileType: 🗃️ Reference Index
Project: rosmerta
Status: 🟢 Active
Created: 2026-06-15
tags:
  - rosmerta
  - project
  - reference
---

# Supporting Notes

Operational, domain, and reference notes in the parent vault that support rosmerta
but aren't core planning. Pointers, not full copies. See [[Parent Vault Intel]].

## Operational / database

- **`Dev Database Setup`** — `# Dev Database Setup — Rosmerta`. Provisioning
  record for the PostgreSQL `stocks` DB (Overview / What was done). Referenced from
  the dev log on 2026-03-25.
- **`New Database info`** — DB reference, linked from the architecture note.
- **`PGCli Guide`** — 361-line guide to connecting/querying the `stocks` DB with
  `pgcli` (connect string `pgcli -d stocks -U stock_user -h 10.0.0.1`).

> ⚠️ **Credentials.** `Project Rosmerta Neo.md` (parent vault) contains a plaintext
> Postgres password and a `CREATE USER … PASSWORD …; GRANT …` block for
> `stock_user`. **Deliberately not reproduced here** — `project/` is committable to
> GitHub. Real credentials belong only in `.env` (gitignored), read by
> `db_config.get_db_config()`. If you need the setup SQL in-repo, store it
> parameterised (no literal password) under `db_scripts/`.

## Domain / reference

- **`Python click CLI Framework`** — 108-line primer on `click`; matches the
  repo's actual CLI choice (vs the architecture notes' "argparse").
- **`GitHub Actions Guide`** — 448-line CI primer. Relevant because the repo has
  **no CI** yet, while `tests/README.md` says `uv run pytest` is meant as a
  CI/pre-commit gate. A minimal `pytest` workflow is the obvious first use.
- **`Algo 3`** / **`Algo3 Comms Channels`** — adjacent trading-strategy notes
  (goals/objects, comms channels) that the architecture captures reference.

## Prior implementation

- `Developer/old_investment_dirs/rosmerta/` — an **earlier, differently-architected
  version** with the redis/workflow-manager vision partly built:
  - `Rosmerta.md` (its own hub note)
  - `app/radio/rosmerta_messages.py`
  - `app/workflow_handler/rosmerta_tasks.py`
  - `app/workflow_handler/rosmerta_workers.py`
  - Useful as a reference if/when reviving the workflow-manager idea
    (see [[Architecture Vision]]).

## Etymology (trivia)

- **`Rosmerta deiology`** — Rosmerta is a Gallo-Roman goddess of fertility and
  abundance, depicted in Gaul alongside the god Mercury.

Related: [[Roadmap & Versioning]] · [[Architecture Vision]] · [[Project Log & Milestones]]
