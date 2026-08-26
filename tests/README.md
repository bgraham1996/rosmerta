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

## Fixed bugs (previously surfaced here)

These two were once pinned as "known bugs" in the assertions; both are now
fixed and the tests assert the correct behaviour:

- `Asset.get_price_levels` used to build price bins over the **close** min/max
  while binning each bar's **avg_price**, silently dropping bars whose
  `avg_price` fell outside the close range. It now bins over the `avg_price`
  min/max, so every bar is counted.
- `Market.get_market_stats` used to compute `count`/`std` *after* appending the
  `avg` column, so those aggregations included `avg` itself (inflating `count`
  by 1 and polluting `std`). It now computes all three from the symbol columns
  alone.

`Market.populate_assets` previously called `Asset.get_growth(conn)` against a
zero-arg signature (guaranteed `TypeError`); that one-line typo was fixed.
