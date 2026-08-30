# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in the **rosmerta**
project. It is self-contained: rosmerta was extracted as a standalone copy, so the wider-workspace
context that used to live in a vault-root `CLAUDE.md` is consolidated below.

## Origin & wider context

Rosmerta originated inside the **Immrama** Obsidian vault, under a `Developer/` directory that
doubles as a [uv](https://docs.astral.sh/uv/) workspace (pinned to Python 3.13). A few things
carried over from that environment that are still worth knowing:

- **Independent uv project.** Rosmerta has its own `pyproject.toml` / `uv.lock` / `.venv` and is
  *not* a member of the parent `Developer/` uv workspace. Run all uv commands from this directory.
- **Its own git repo.** In the vault, each project under `Developer/` (rosmerta, apollo, yemo,
  etc.) was an independent git repository — there was no top-level repo. This extracted copy is
  likewise a self-standing repo.
- **Sibling-project conventions.** A sibling project, `yemo`, defined a `metadata.json`/`project/`
  scaffold pattern intended to track projects and surface them back into Obsidian via `dataview`.
  That explains the untracked `project/` + `metadata.json` scaffolding you may still see here; it
  is not part of the rosmerta CLI itself.
- **Skills.** Vault automation skills (`vault-automation`, `vault-search`) may be bundled under
  `.claude/skills/` in this copy. They are general vault helpers, not rosmerta-specific tooling.

## What this is

Rosmerta is a `click`-based **Stock Analysis & Portfolio Management CLI**. It pulls hourly price
bars and portfolio positions from **Interactive Brokers** (`ib-insync`), fundamentals and
dividends from **SEC EDGAR** (free XBRL API), stores everything in **PostgreSQL**, and exposes a
**Dash** dashboard for visualization. It is an independent uv project (own `pyproject.toml` /
`uv.lock` / `.venv`) — it is *not* a member of the parent `Developer/` uv workspace.

## Environment & running

Python 3.13+ (`.python-version`). Run everything through uv from this directory:

```bash
uv sync                          # install/sync deps
uv run main.py --help            # the CLI is a click.group() named `rosmerta`
uv run main.py dashboard         # launch the Dash app (default 127.0.0.1:8050)
uv run main.py dashboard --debug # auto-reload + dev tools
```

The README documents an `rosmerta ...` entry point via `pip install -e .`, but there is no
`[project.scripts]` in `pyproject.toml` — in practice invoke `uv run main.py <command>`.

### Configuration (`.env`, read by `db_config.py` and `main.py`)

- `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASS` — PostgreSQL connection. `get_db_config()`
  returns a psycopg2-style dict (keys: `host, port, dbname, user, password`), or `None` when
  `no_db=True`. Defaults point at a LAN Postgres (`10.0.0.1:5432/stocks`).
- `email` — required User-Agent for the SEC EDGAR API (SEC mandates a contact email; 10 req/s limit).

Interactive Brokers: a TWS/IB **Gateway** must be reachable at `127.0.0.1:4001` (paper). The
gateway runs on the Mac; the DB typically lives on a separate Ubuntu box.

## CLI surface (`main.py`)

All commands accept `--no-db` (skip persistence) where persistence applies. Command groups:

- `fetch price <TICKER> -s <date> -e <date>` — hourly OHLCV bars from IB.
- `fetch fundamentals <TICKER> [-s -e]` — financial statements from EDGAR (omit dates for all).
- `fetch dividends <TICKER> [-s -e]` — dividends via EDGAR with **IB fallback** for ADRs/foreign
  tickers (`--no-fallback` to disable).
- `portfolio` — current IB positions with P&L (no DB).
- `bulk price|fundamentals|dividends --list <WATCHLIST>` — iterate a watchlist. Supports
  `--dry-run`; `bulk fundamentals` adds freshness skipping (`--max-age`, `--force`).
- `dashboard` — launch Dash.

`main.py` imports fetcher modules lazily *inside* each command so the CLI starts fast and a
missing IB/EDGAR dependency only fails the command that needs it. `_run_bulk_fetch` is the shared
driver for all three bulk subcommands; Rich `Progress`/`Table` render all output. Keep new
commands consistent with this lazy-import + Rich-output style.

## Architecture

### `price_retrival/` — data fetchers (note: directory name is misspelled, keep it)

- `ib_api.py` — `IBStockDataFetcher` context manager; hourly bars in ≤30-day chunks, optional
  direct Postgres insert via `execute_values`.
- `edgar_api.py` — `EdgarFundamentalsFetcher`; `XBRL_TAG_MAP` maps DB columns to ordered lists of
  US-GAAP XBRL tags (companies use different tags for the same concept — first match wins).
- `dividends_api.py` — `EdgarDividendsFetcher` + `IBDividendsFetcher`, dispatched by the
  `get_dividends(...)` function: tickers with a CIK route to EDGAR, ADRs/foreign route to IB.
  Returns `(records, source)` where source ∈ `edgar`/`ib`/`ib_fallback`. Successful tickers are
  auto-added to the `dividends` watchlist.
- `bulk_fetch.py` — watchlist resolution (`get_watchlist_tickers`, `get_available_lists`),
  freshness (`get_last_fetch_times`), and `bulk_fetch_{prices,fundamentals,dividends}`. Each
  returns a list of `FetchResult` (`status` ∈ `success`/`empty`/`skipped`/`error`). **Bulk price
  fetching opens a fresh IB connection per ticker** (`_fetch_single_ticker`) for isolation.

### `data_models.py` — analysis layer (separate from fetchers)

- `Asset(conn, ticker, start_date, end_date, timeframe='hourly')` — reads price/dividend/
  fundamental data already in the DB and computes stats, growth, volume-by-price levels, and
  indicators. Heavily **lazy-cached** via `self._*_cache` fields; mutating methods set the cache
  and `clear_*` resets it. Dates are passed as `'%Y-%m-%d %H:%M:%S'` strings and treated as UTC.
- `Market(conn, start_date, end_date, stock_list='core', timeframe='hourly')` — a collection of
  `Asset`s drawn from a watchlist; builds cross-sectional panels (`get_panel(field)`) and market
  stats. Workflow: `seed_assets()` → `populate_assets()` → `get_panel()` / `get_market_stats()`.
  Indicators apply across the whole market via `add_indicators(conn, Indicator(...))` (each asset
  gets its own copy) and their cross-sectional (timestamp × symbol) result is read back with
  `get_indicator_panel(conn, name, **params)` — single-Series indicators only.

`utils/bars.py` holds the OHLCV resampling used by `Asset` for `daily`/`weekly`/`monthly`
timeframes (`RESAMPLE_RULES`, `resample_ohlcv`, `OHLCV_AGG`).

### `indicators.py` — indicator registry

Indicators register via the `@register("name")` decorator into `_REGISTRY`; `Indicator(name,
**params)` wraps a registered function with a cache and a `key` (name + sorted params). `Asset`
consumes these through `add_indicator` / `get_indicator`. Built-ins: `sma, ema, rsi, bollinger,
obv, vwap`. Add a new indicator by writing a `@register`-decorated function — no other wiring.

### `dashboards/` — Dash app

`app.py` is the app factory (`create_app`/`run`); a persistent header + dropdown swaps the active
view. Views subclass `base.View`, self-register with `@register_view("key")` (registry in
`registry.py`), and use `self.cid(name)` to namespace component IDs. **To add a view:** create
`dashboards/views/<name>.py` and add its import line to `dashboards/views/__init__.py` (importing
the package is what triggers registration). `theme.py`, `figures/` (Plotly figure builders), and
`assets/style.css` support the views.

### Database

- `db/init.sql` is the canonical schema: `stocks`, `price_hourly`, `dividends`, `fundamentals`,
  `stock_tags`, `fetch_log`, plus views `v_active_tags` / `v_latest_fundamentals`. Watchlists are
  modeled via `watchlist_members(list_name, stock_id)` joined to `stocks` (see `Market` and
  `bulk_fetch`).
- `db_scripts/` — operational SQL: `seed_dev.sql`, `core_expansion_seed.sql`, exports, audits,
  and `migrations/` (apply migrations there as the schema evolves).
- `db_exports/` — generated CSVs (gitignored).

## Tests

There are two distinct kinds of tests, kept separate on purpose:

- **`tests/` — the automated pytest suite** (`uv run pytest`). DB-free and network-free: it
  covers the indicator registry (`indicators.py`), OHLCV resampling (`utils/bars.py`), and the
  `Asset`/`Market` analysis computations (`data_models.py`). `tests/conftest.py` provides a
  `FakeConnection`/`FakeDB` that stands in for psycopg2, so the read-only analysis layer is
  tested without a live PostgreSQL. Config lives in `[tool.pytest.ini_options]` in
  `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["."]`). When you add a fetcher-free,
  pure-logic feature, add a unit test here. See `tests/README.md`.
- **Root `*_test.py` + `dev_test/` — runnable live-DB scripts, *not* pytest** (`market_test.py`,
  `indicators_and_asset_test.py`, `dev_test/check_pfe_gaps.py`). They connect to a **live
  populated PostgreSQL** and (for the asset test) write a PNG/CSV via a headless matplotlib
  (`Agg`) backend. They are **excluded from pytest collection** via `testpaths`; run them
  directly, e.g. `uv run market_test.py`, against a real DB.

The pytest suite previously pinned two behavioural bugs it surfaced; both are now fixed and the
suite asserts the correct behaviour: `Asset.get_price_levels` bins `avg_price` over the
`avg_price` min/max (so no out-of-range bars are dropped), and `Market.get_market_stats` computes
`count`/`std` from the symbol columns alone (no longer polluted by the `avg` column). See
`tests/README.md`.

## Conventions

- Keep the `price_retrival` spelling — it is the real module path.
- Fetchers persist; `data_models`/`indicators` only read and analyze. Don't blur that line.
- New CLI commands: lazy-import heavy deps inside the command body and render with Rich, matching
  the existing `fetch`/`bulk` commands.
- `.env`, `*.csv`, `*.png`, and `db_exports/` are gitignored — don't commit generated data.
