"""CLI-level checks for the --bar-size option (board item "Bar size in CLI and
workflows"). These use Click's CliRunner and exercise only option parsing /
validation — no IB connection is made (an invalid choice fails before the
command body runs, and --help never runs it)."""

from click.testing import CliRunner

from main import cli, BAR_SIZES


runner = CliRunner()


def test_bar_sizes_match_fetcher_supported_set():
    # Keep the CLI choice list in step with the retrieval layer.
    from price_retrival.ib_api import SUPPORTED_BAR_SIZES
    assert set(BAR_SIZES) == set(SUPPORTED_BAR_SIZES)


def test_fetch_price_rejects_invalid_bar_size():
    result = runner.invoke(
        cli, ['fetch', 'price', 'AAPL', '-s', '2026-10-20', '-e', '2026-10-24',
              '--bar-size', '2 mins'],
    )
    assert result.exit_code != 0          # Click rejects before any IB call
    assert '2 mins' in result.output or 'Invalid value' in result.output


def test_fetch_price_exposes_bar_size():
    result = runner.invoke(cli, ['fetch', 'price', '--help'])
    assert result.exit_code == 0
    assert '--bar-size' in result.output


def test_bulk_price_exposes_bar_size():
    result = runner.invoke(cli, ['bulk', 'price', '--help'])
    assert result.exit_code == 0
    assert '--bar-size' in result.output


def test_futures_fetch_exposes_bar_size():
    result = runner.invoke(cli, ['futures', 'fetch', '--help'])
    assert result.exit_code == 0
    assert '--bar-size' in result.output
