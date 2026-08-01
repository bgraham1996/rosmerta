# Rosmerta

**Stock Analysis & Portfolio Management CLI**

Rosmerta is a comprehensive command-line tool for financial data analysis, combining real-time data from Interactive Brokers with fundamental data from SEC EDGAR filings.

## Features

- **Price Data**: Fetch hourly price bars from Interactive Brokers
- **Fundamentals**: Retrieve financial statements and metrics from SEC EDGAR
- **Dividends**: EDGAR dividends with automatic IB fallback for ADRs/foreign tickers
- **Portfolio Management**: View current IB portfolio positions with P&L analysis
- **Bulk Operations**: Process entire watchlists efficiently
- **Analysis Layer**: `Asset` / `Market` models with stats, growth, volume-by-price, and indicators
- **Dashboard**: Dash app for interactive visualization
- **Database Integration**: Store and manage data with PostgreSQL
- **Rich CLI Interface**: Beautiful tables and progress indicators

## Installation

Rosmerta is a [uv](https://docs.astral.sh/uv/) project (Python 3.13, pinned via
`.python-version`). Run every command through `uv` from the project directory.

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd rosmerta
   ```

2. **Install dependencies**
   ```bash
   uv sync          # creates .venv and installs from uv.lock
   ```

3. **Environment setup**
   Create a `.env` file (read by `db_config.py` and `main.py`):
   ```bash
   email=your-email@domain.com   # Required: SEC EDGAR User-Agent contact

   DB_HOST=10.0.0.1              # PostgreSQL connection
   DB_PORT=5432
   DB_NAME=stocks
   DB_USER=your-db-user
   DB_PASS=your-db-password
   ```

> **Note:** there is no `[project.scripts]` entry point, so the CLI is invoked
> as `uv run main.py <command>` (not a bare `rosmerta` command). The examples
> below use that form.

## Prerequisites

- **Python 3.13+** with **uv**
- **PostgreSQL** (for data storage; defaults point at a LAN host `10.0.0.1:5432/stocks`)
- **Interactive Brokers TWS/Gateway** reachable at `127.0.0.1:4001` (paper) for
  price data and portfolio

## Quick Start

```bash
uv run main.py --help            # the CLI is a click.group() named `rosmerta`
```

### Fetch Price Data
```bash
# Single ticker
uv run main.py fetch price AAPL --start 2024-01-01 --end 2024-03-31

# Skip database storage
uv run main.py fetch price AAPL --start 2024-01-01 --end 2024-03-31 --no-db
```

### Fetch Fundamental Data
```bash
# Single ticker with date range
uv run main.py fetch fundamentals PFE --start 2020-01-01 --end 2024-12-31

# All available data
uv run main.py fetch fundamentals MSFT
```

### Fetch Dividends
```bash
# EDGAR with automatic IB fallback for ADRs/foreign tickers
uv run main.py fetch dividends KO

# Disable the IB fallback
uv run main.py fetch dividends KO --no-fallback
```

### Portfolio Management
```bash
# View current positions with P&L (no DB)
uv run main.py portfolio
```

### Bulk Operations
```bash
# Fetch prices for entire watchlist
uv run main.py bulk price --list core --start 2025-01-01 --end 2025-02-01

# Fetch fundamentals with automatic freshness checking
uv run main.py bulk fundamentals --list pharma_top20

# Force refresh all data
uv run main.py bulk fundamentals --list core --force
```

### Dashboard
```bash
# Launch the Dash app (default 127.0.0.1:8050)
uv run main.py dashboard

# With auto-reload + dev tools
uv run main.py dashboard --debug
```

## Commands

### Data Fetching
- `fetch price <TICKER>` - Fetch hourly OHLCV bars from IB
- `fetch fundamentals <TICKER>` - Fetch SEC EDGAR financial statements
- `fetch dividends <TICKER>` - Fetch dividends (EDGAR, with IB fallback)

### Portfolio
- `portfolio` - Show current IB positions with P&L

### Bulk Operations
- `bulk price --list <WATCHLIST>` - Bulk price fetching
- `bulk fundamentals --list <WATCHLIST>` - Bulk fundamentals fetching
- `bulk dividends --list <WATCHLIST>` - Bulk dividends fetching

### Visualization
- `dashboard` - Launch the Dash dashboard

### Options
- `--start/-s`, `--end/-e` - Date ranges (YYYY-MM-DD)
- `--no-db` - Skip database storage
- `--dry-run` - Preview operations without execution
- `--force` - Override freshness checks
- `--exchange` - IB exchange routing (default: SMART)

## Project Structure

```
rosmerta/
  main.py              # CLI entry point (click.group named `rosmerta`)
  db_config.py         # Loads the DB connection dict from .env
  data_models.py       # Analysis layer: Asset / Market (read-only over the DB)
  indicators.py        # Technical-indicator registry (sma/ema/rsi/bollinger/obv/vwap)
  price_retrival/      # Data fetching modules (spelling is intentional)
    ib_api.py          # Interactive Brokers price bars
    edgar_api.py       # SEC EDGAR fundamentals client
    dividends_api.py   # EDGAR + IB dividends with dispatch/fallback
    portfolio.py       # IB portfolio positions
    bulk_fetch.py      # Watchlist resolution + bulk operations
  utils/bars.py        # OHLCV resampling for daily/weekly/monthly timeframes
  db/                  # Canonical schema (init.sql) + SQL helpers
  db_scripts/          # Operational SQL: seeds, exports, migrations/
  db_tools/            # Database utilities
  dashboards/          # Dash app (app.py, views/, figures/, registry, theme)
```

## Analysis Layer

Separate from the fetchers (which only write), `data_models.py` only reads and
analyzes data already in the DB:

- **`Asset`** - one ticker over a date window/timeframe. Lazily computes and
  caches prices, stats, growth, dividends, net income, indicators, and
  volume-by-price levels.
- **`Market`** - a collection of `Asset`s drawn from a watchlist. Builds
  cross-sectional panels (`get_panel`) and market-wide stats. Workflow:
  `seed_assets()` → `populate_assets()` → `get_panel()` / `get_market_stats()`.

Technical indicators live in `indicators.py`: register a new one by writing an
`@register("name")`-decorated function - no other wiring needed.

## Configuration

The application reads configuration from a `.env` file:

- `email` - Required SEC EDGAR User-Agent contact (SEC mandates it; 10 req/s limit)
- `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASS` - PostgreSQL connection,
  assembled into a psycopg2 connection dict by `db_config.get_db_config()`. Pass
  `--no-db` on commands to skip persistence entirely.

## Interactive Brokers Setup

1. **Install TWS or Gateway**
2. **Configure API access**:
   - Enable API in TWS/Gateway settings
   - Default connection: `127.0.0.1:4001`
   - Ensure paper trading mode for testing

## Database Schema

`db/init.sql` is the canonical schema. It covers:
- Price data (`price_hourly` OHLCV bars)
- Fundamental data (`fundamentals` financial statements)
- Dividends (`dividends`)
- Watchlist management (`watchlist_members` joined to `stocks`)
- Data freshness tracking (`fetch_log`)

Operational SQL (seeds, exports, schema migrations) lives in `db_scripts/`.

## Tests

The `*_test.py` files at the project root (`market_test.py`,
`indicators_and_asset_test.py`, `dev_test/check_pfe_gaps.py`) are **runnable
scripts, not pytest suites** - they connect to a live, populated PostgreSQL and
(for the asset test) write a PNG via a headless matplotlib backend. There is no
automated test runner configured; run them directly:

```bash
uv run market_test.py
```

## Development

The project uses:
- **uv** for environment and dependency management
- **Click** for the CLI framework
- **Rich** for terminal output (tables, progress)
- **ib-insync** for Interactive Brokers integration
- **psycopg2** for PostgreSQL connectivity
- **pandas / numpy** for data manipulation and analysis
- **Dash / Plotly** for the dashboard

## Error Handling

- Automatic retry logic for API failures
- Graceful handling of missing data
- Comprehensive error reporting in bulk operations
- Rate limiting compliance for external APIs

## License

[Add your license information here]

## Support

For issues and feature requests, please use the project's issue tracker.
