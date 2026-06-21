# tests/

Automated **unit** suite for rosmerta. Everything here is **DB-free and
network-free** — no PostgreSQL, no Interactive Brokers gateway — so it runs
anywhere and fast.

```bash
uv run pytest          # run the suite
uv run pytest -v       # verbose
```

## What's covered

- `test_indicators.py` — the indicator registry (`sma`, `ema`, `rsi`,
  `bollinger`, `obv`, `vwap`) against hand-verifiable inputs, plus the
  `Indicator` wrapper (registry lookup, key identity, compute caching).
- `test_bars.py` — OHLCV resampling in `utils/bars.py`.
- `test_data_models.py` — the analysis layer (`Asset` / `Market`) computations,
  driven by an in-memory fake connection.

## The fake DB (`conftest.py`)

`Asset`/`Market` only ever *read* the database through a small
`conn.cursor() → execute / fetchone / fetchall` interface. `conftest.py` stands
a `FakeConnection` in for psycopg2 that answers each query from canned Python
data, so the analysis logic can be tested without a live DB. Fixtures: `db`,
`conn`, `sample_prices`, `date_bounds`, `utc`.

## Not collected here

The root-level `*_test.py` files (`market_test.py`,
`indicators_and_asset_test.py`) and `dev_test/` are **runnable scripts that hit a
live, populated PostgreSQL** — they are integration/diagnostic tools, not pytest
tests, and are excluded from collection via `testpaths` in `pyproject.toml`. Run
them directly (e.g. `uv run market_test.py`) against a real DB.

## Known bugs surfaced by these tests

- `Asset.get_price_levels` builds price bins over the **close** min/max but bins
  each bar's **avg_price**, so bars whose `avg_price` falls outside the close
  range are silently dropped (undercounting volume).
- `Market.get_market_stats` (WIP) computes `count`/`std` *after* appending the
  `avg` column, so those aggregations include `avg` itself — inflating `count`
  by 1 and polluting `std`.

`Market.populate_assets` previously called `Asset.get_growth(conn)` against a
zero-arg signature (guaranteed `TypeError`); that one-line typo was fixed.
