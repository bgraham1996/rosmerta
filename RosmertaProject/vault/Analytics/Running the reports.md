---
FileType: 📊 Analytics
tags: [analytics, reports, skills, how-to]
Completed: false
---

# Running the reports

How to (re)generate the [[Price Behaviour Analytics]] dashboards. They are produced
by the **`weekly-price-report`** Claude Code skill, which lives in the repo at
`.claude/skills/weekly-price-report/`.

## Easiest: just ask Claude

In a Claude Code session started in the project, say what you want — the skill
auto-triggers:

- *"refresh the weekly report"* → weekly dashboard
- *"make the biweekly / fortnightly report"* → biweekly dashboard
- *"backtest the red-Wednesday dip"* → backtest report
- *"…for the `core` watchlist, from 2023"* → adds `--list core --start 2023-01-01`

Claude runs the script, then publishes the HTML with the **Artifact** tool and gives
you the link. To update an existing dashboard in place, ask it to refresh that report
and pass the artifact's URL.

## Manual commands

Run from the **project root** (so `.env` / `db_config.py` resolve; the LAN Postgres
must be reachable). Write output somewhere temporary — it's a generated file, not
repo content.

```bash
# Weekly behaviour dashboard
PYTHONPATH=. uv run .claude/skills/weekly-price-report/generate_report.py \
  --timeframe weekly  --out /tmp/weekly-behaviour.html

# Biweekly / fortnightly dashboard
PYTHONPATH=. uv run .claude/skills/weekly-price-report/generate_report.py \
  --timeframe biweekly --out /tmp/biweekly-behaviour.html

# Dip-entry backtest
PYTHONPATH=. uv run .claude/skills/weekly-price-report/backtest.py \
  --hold 10 --sma 50 --cost-bps 10 --out /tmp/backtest-report.html
```

### Options

**`generate_report.py`** — `--timeframe weekly|biweekly` · `--list <watchlist>` ·
`--start YYYY-MM-DD` · `--end YYYY-MM-DD` · `--out <path>`.

**`backtest.py`** — `--hold <days>` (default 10 ≈ 2 weeks) · `--sma <window>`
(uptrend filter, default 50) · `--cost-bps <n>` (default 10) · plus
`--list` / `--start` / `--end` / `--out`.

Each script prints the row counts / headline stats and the output path. Then it's
published via the Artifact tool to get a claude.ai link (see [[Price Behaviour Analytics]]
for the current URLs).

## Getting to the artifacts

- Every published report is a **private Artifact** on your account. Find them all at
  **claude.ai/code/artifacts** (newest first), or use the links table in
  [[Price Behaviour Analytics]].
- To **share** one, open it and use the page's share menu — private until you do.
- **Refreshing** a report to the same artifact keeps its URL; a brand-new run mints a
  new one.

## Under the hood (for edits)

- **Read-only** on the DB — touches only `price_hourly`, `stocks`, `watchlist_members`.
- Builds daily bars from hourly (UTC date = trading date), then weekly / fortnightly
  frames; consecutive-period returns only (weekly 5–10 days apart; fortnightly 8–18).
- HTML **templates** are separate files in `.claude/skills/weekly-price-report/templates/`
  (`weekly.html`, `biweekly.html`, `backtest.html`); computed data is injected into the
  `__DATA__` token. Edit a template to change the look or add a panel — no need to touch
  the Python.
- Full skill docs: `.claude/skills/weekly-price-report/SKILL.md`.
