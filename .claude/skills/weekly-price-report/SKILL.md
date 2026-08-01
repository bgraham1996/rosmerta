---
name: weekly-price-report
description: Generate the rosmerta price-behaviour dashboards and dip-entry backtest. Three outputs from the live Postgres — (1) the WEEKLY "Behaviour of the Book" report (Fri→Fri return distribution, movement types, intra-week path, momentum vs volatility, seasonality), (2) the BIWEEKLY/fortnightly version of the same, and (3) a BACKTEST of the "buy the red-Wednesday dip in an uptrend, hold ~2 weeks" rule (equity curve, execution edge, stops, costs). Use for any weekly/fortnightly price-behaviour report, "the weekly report", "the biweekly report", refreshing them, or backtesting the dip-entry timing (optionally for a watchlist or date range).
---

# Price-behaviour reports & dip-entry backtest

Builds self-contained HTML dashboards from the rosmerta Postgres and publishes them
as Artifacts. Returns are on a **close-to-close** basis (weekly = Fri→Fri; biweekly =
every-other-Friday) — the tradeable move, weekend gap included.

## When to use

- "make/refresh the weekly report" or "the biweekly / fortnightly report"
- "how do stocks move over a week / two weeks"
- "backtest the red-Wednesday dip" / "does the dip-entry timing hold up"
- Any of the above scoped to a **watchlist** (`--list core`) or a **date range**.

## Setup (all three)

Run from the **project root** so `db_config.py` / `.env` resolve; needs the LAN
Postgres reachable. Deps (`psycopg2`, `pandas`, `numpy`, `scipy`) are in the venv.
Write outputs to the **scratchpad**, not the repo, then publish with the **Artifact**
tool (self-contained HTML, no `<head>` needed). To refresh an already-published
artifact, re-run to the same path and republish; pass the prior artifact `url` when
updating one from another session.

## 1 & 2 — Behaviour reports (`generate_report.py`)

```bash
# weekly (default)
PYTHONPATH=. uv run .claude/skills/weekly-price-report/generate_report.py \
  --timeframe weekly  --out weekly-behaviour.html
# fortnightly
PYTHONPATH=. uv run .claude/skills/weekly-price-report/generate_report.py \
  --timeframe biweekly --out biweekly-behaviour.html
```

| flag | effect | default |
|------|--------|---------|
| `--timeframe` | `weekly` or `biweekly` | `weekly` |
| `--list <name>` | restrict to a watchlist (`watchlist_members.list_name`) | all stocks |
| `--start` / `--end` | inclusive date window `YYYY-MM-DD` | full history |
| `--out <path>` | output HTML | `<timeframe>-behaviour.html` |

Both compute: return distribution (mean/median/σ/skew/kurtosis, tails, histogram),
a movement-type taxonomy (Clean rally / selloff / Quiet / Dip & recover / Pop & fade),
the intra-period cumulative path and when the high/low prints (weekday for weekly; a
10-slot week-1/week-2 grid for biweekly), momentum-vs-volatility persistence, and
month/year seasonality. Biweekly also splits **week-1 vs week-2** strength.

## 3 — Dip-entry backtest (`backtest.py`)

```bash
PYTHONPATH=. uv run .claude/skills/weekly-price-report/backtest.py \
  --hold 10 --sma 50 --cost-bps 10 --out backtest-report.html
```

Signal: a Wednesday closing red while `close > SMA(sma)` (uptrend = bullish-thesis
proxy). Enter at that close, exit `--hold` trading days later. Report covers the
equity curve vs equal-weight buy-and-hold, the **execution edge vs a random-day entry**
in the same name, cost sensitivity, stop-loss variants, the intra-hold drawdown
distribution, and by-year returns.

| flag | effect | default |
|------|--------|---------|
| `--hold <n>` | holding period in trading days | `10` (~2 weeks) |
| `--sma <n>` | uptrend-filter SMA window | `50` |
| `--cost-bps <n>` | round-trip cost for the equity curve | `10` |
| `--list` / `--start` / `--end` / `--out` | as above | — |

## Key findings baked into the current reports (2020–2026, 99 stocks)

- Direction is ~random week to week (return autocorrelation ≈ 0) but **volatility
  clusters** strongly (range autocorrelation +0.5). σ scales ≈ √2 from weekly to
  biweekly — the random-walk signature.
- The dip-entry has a **real but modest execution edge** (+0.36%/trade vs a random
  entry, p<1e-4) — worth it as an *overlay* on names you'd buy anyway, **not** as a
  standalone system (it trails buy-and-hold, Sharpe 0.94 vs 1.14). Tight stops hurt
  (it's a mean-reversion trade). Drop the "green Monday" precondition — the red
  Wednesday is the whole signal.

## Notes & gotchas

- **Read-only.** Touches only `price_hourly`, `stocks`, `watchlist_members`.
- Equal-weight across whatever tickers are in scope — **not** an index; say so when
  presenting, and flag the bull-heavy 2020–2026 window. Gross of costs/borrow/slippage.
- **Long-only framing** — the user does not short; keep suggestions on the buy side.
- Templates live in `templates/{weekly,biweekly,backtest}.html`; data flows in via
  the `__DATA__` token. Edit those files to change design or add a panel. The two
  behaviour scripts share `load_daily`; `backtest.py` imports it from `generate_report.py`.
- "No rows"/"No trades" → the date window or `--list` name is wrong, or the DB is
  unreachable (check the LAN Postgres / `.env`).
