---
FileType: 🧪 Project Doc
Project: rosmerta
Status: 🟢 Active
tags:
  - rosmerta
  - testing
  - pytest
---

# Rosmerta — Testing

How the [[rosmerta]] test tooling works, how to run it, and how to read what it
prints. This is the vault-facing companion to the in-repo `tests/README.md`; the
repo file is the source of truth, this note is the orientation/explainer.

## TL;DR

```bash
cd Developer/rosmerta
uv run pytest          # run the automated suite (currently 34 tests)
```

A green run ends with `==== 34 passed in 0.4s ====`. That's the whole signal you
need day to day. Everything below explains the rest.

## Two separate kinds of "tests" — don't confuse them

Rosmerta has two things that look like tests but serve different purposes:

| | **Automated unit suite** | **Live-DB scripts** |
|---|---|---|
| Location | `tests/` | root `*_test.py`, `dev_test/` |
| Run with | `uv run pytest` | `uv run market_test.py` (etc.) |
| Needs PostgreSQL? | **No** | **Yes — live, populated DB** |
| Needs IB gateway? | No | No (but assumes DB already fetched) |
| What it's for | Catch logic regressions, fast & anywhere | Manual integration checks, diagnostics, plots |
| Collected by pytest? | Yes | **No** (excluded via `testpaths`) |

The unit suite is the one to run routinely — it's fast, deterministic, and needs
nothing but the code. The live-DB scripts (`market_test.py`,
`indicators_and_asset_test.py`, `dev_test/check_pfe_gaps.py`) are kept as
runnable diagnostics and are deliberately **not** picked up by `pytest`.

## What the unit suite covers

Three test modules under `tests/`:

- **`test_indicators.py`** — the six technical indicators in `indicators.py`
  (`sma`, `ema`, `rsi`, `bollinger`, `obv`, `vwap`) checked against small,
  hand-worked inputs, plus the `Indicator` wrapper (registry lookup,
  parameter-key identity, result caching). **Not yet covered:** the newer
  forward-looking `days_offset_gain` (added 2026-07-03) — add a
  `test_indicators.py` case with a hand-computed expected value (offset =
  `days_ahead × bars_per_day` bars; trailing bars → `NaN`).
- **`test_bars.py`** — OHLCV resampling in `utils/bars.py` (open=first,
  high=max, low=min, close=last, volume=sum; `stock_id` preservation; empty
  input; dropping empty calendar periods).
- **`test_data_models.py`** — the analysis layer (`Asset` / `Market`): price
  loading + caching, stats, growth, average price, volume-by-price levels,
  indicator integration, panels, and market stats.

### How it runs without a database

`Asset` and `Market` only ever **read** the DB, through a tiny
`conn.cursor() → execute / fetchone / fetchall` interface. `tests/conftest.py`
supplies a `FakeConnection` / `FakeDB` that implements exactly that interface and
answers each query from canned Python data. So the analysis maths is exercised
against known inputs with **no PostgreSQL involved**. Key fixtures: `db`, `conn`,
`sample_prices`, `date_bounds`, `utc`.

This is why the suite is fast and portable — and why it tests *our* logic, not
the database or the network.

## Running it — common invocations

```bash
uv run pytest                       # everything
uv run pytest -v                    # one line per test (names + PASSED/FAILED)
uv run pytest tests/test_bars.py    # just one module
uv run pytest -k indicator          # only tests whose name matches "indicator"
uv run pytest -x                    # stop at the first failure
uv run pytest -q                    # quiet (just the dots + summary)
```

Config lives in `pyproject.toml` under `[tool.pytest.ini_options]`
(`testpaths = ["tests"]`, `pythonpath = ["."]`, `addopts = "-ra"`). The
`pythonpath = ["."]` line is what lets the tests `import data_models` / `indicators`
without installing the project.

## Reading the output

A normal passing run:

```
collected 34 items

tests/test_bars.py ......                                                [ 17%]
tests/test_data_models.py .................                              [ 67%]
tests/test_indicators.py ...........                                     [100%]

============================== 34 passed in 0.38s ==============================
```

- **`collected 34 items`** — how many tests pytest found. If this number drops
  unexpectedly, a module probably failed to import (look for an `ERROR` block).
- **Each `.`** is one passing test; the `[ 17% ]` is overall progress.
- Characters other than `.`:
  - `F` — a **failure** (an `assert` was false / the wrong value came out).
  - `E` — an **error** (the test crashed before asserting, e.g. an exception or
    import problem).
  - `s` — **skipped**, `x` — expected failure (`xfail`). The suite currently has
    none of these.
- **`-ra` summary** — because of the `-ra` option, a short reason list for every
  non-passing test is printed at the end, so you don't have to scroll up.

When something fails you get a traceback plus a diff-style line, e.g.:

```
>       assert stats["count"].iloc[0] == 2
E       assert np.int64(3) == 2
tests/test_data_models.py:233: AssertionError
```

Read it bottom-up: the `E` line shows actual-vs-expected, and the
`file:line` tells you exactly where. Captured `print()` output from the code
under test appears under a `Captured stdout call` heading (e.g. the
`Asset Object ... initialised` lines).

**Exit code:** `0` when everything passes, non-zero otherwise — so
`uv run pytest` works as a CI/pre-commit gate.

## Adding a test

For any new **fetcher-free, pure-logic** behaviour (a new indicator, a new
`Asset`/`Market` computation, a `utils/` helper), add a unit test next to the
matching module in `tests/`:

1. New indicator → add a case to `test_indicators.py` with a hand-computed
   expected value.
2. New `Asset`/`Market` method → add canned rows to the `FakeDB` (or extend a
   fixture in `conftest.py`) and assert on the result.
3. Keep it DB-free. If a feature genuinely needs a live DB, it belongs as a
   root-level `*_test.py` script, not in `tests/`.

Run `uv run pytest` and confirm green before committing.

## Known bugs these tests surfaced

The suite pins (and comments) two real defects it found, rather than hiding them:

- **`Asset.get_price_levels`** builds price bins over the **close** min/max but
  bins each bar's **avg_price** — bars whose `avg_price` falls outside the close
  range are silently dropped, undercounting volume.
- **`Market.get_market_stats`** (work-in-progress) computes `count`/`std`
  *after* appending its own `avg` column, so those aggregations include `avg` —
  inflating `count` by 1 and polluting `std`.

A third, an unambiguous typo (`Market.populate_assets` calling
`Asset.get_growth(conn)` against a zero-arg signature — a guaranteed
`TypeError`), was **fixed** when the suite was added.

## See also

- `tests/README.md` — in-repo, source-of-truth version of this note.
- `CLAUDE.md` → "Tests" — the short orientation for agents working in the repo.
- [[rosmerta]] — project hub note.
