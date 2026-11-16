"""CLI checks for `futures fetch --include-expired`. No IB/DB: the fetcher class
is replaced with a recording stub, so only the option wiring is exercised."""

import pandas as pd
import pytest
from click.testing import CliRunner

import price_retrival.ib_api as ib_api
from main import cli


runner = CliRunner()


class _StubFetcher:
    calls = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_futures_hourly_bars(self, root, expiry, exchange, **kwargs):
        _StubFetcher.calls.append((root, expiry, exchange, kwargs))
        ts = pd.Timestamp('2026-03-02 15:00', tz='UTC')
        return pd.DataFrame(
            [(ts, 1.0, 2.0, 0.5, 1.5, 10)],
            columns=['date', 'open', 'high', 'low', 'close', 'volume'],
        )


@pytest.fixture
def stub(monkeypatch):
    _StubFetcher.calls = []
    monkeypatch.setattr(ib_api, 'IBStockDataFetcher', _StubFetcher)
    return _StubFetcher


def test_futures_fetch_exposes_include_expired():
    result = runner.invoke(cli, ['futures', 'fetch', '--help'])
    assert result.exit_code == 0
    assert '--include-expired' in result.output


def test_include_expired_is_passed_to_the_fetcher(stub):
    result = runner.invoke(
        cli, ['futures', 'fetch', 'es', '--expiry', '20260320',
              '--include-expired', '--no-db'],
    )
    assert result.exit_code == 0, result.output
    (root, expiry, _exchange, kwargs), = stub.calls
    assert (root, expiry) == ('ES', '20260320')
    assert kwargs['include_expired'] is True


def test_include_expired_defaults_to_false(stub):
    result = runner.invoke(
        cli, ['futures', 'fetch', 'ES', '--expiry', '20261218', '--no-db'],
    )
    assert result.exit_code == 0, result.output
    assert stub.calls[0][3]['include_expired'] is False


def test_include_expired_requires_expiry(stub):
    result = runner.invoke(cli, ['futures', 'fetch', 'ES', '--include-expired'])
    assert '--include-expired requires --expiry' in result.output
    assert stub.calls == []          # rejected before any fetcher is built
