"""The IB fetch loop must not truncate a backfill on an empty chunk.

Network-free: an empty ``reqHistoricalData`` response (an exchange holiday, or
IB answering "no data" for a window ending mid-session) used to ``break`` and
silently drop every bar older than the first such day. The loop now steps the
window back one chunk and keeps going, so older history is still retrieved.

We stand a tiny ``FakeIB`` in for the gateway and script one empty chunk between
two chunks that return data; the fix is proven by the older chunk's bars still
appearing in the result (and by the request being issued *after* the empty one).
"""

from datetime import datetime, timezone

from price_retrival.ib_api import IBStockDataFetcher


class _Bar:
    """Minimal stand-in for an ib_insync BarData row used by the fetch loop."""

    def __init__(self, date):
        self.date = date
        self.open = self.high = self.low = self.close = 1.0
        self.volume = 100


class FakeIB:
    """Scripted gateway: returns ``scripted[i]`` for the i-th historical request."""

    def __init__(self, scripted):
        self.scripted = scripted
        self.calls = 0
        self.RaiseRequestErrors = False

    def isConnected(self):
        return True

    def qualifyContracts(self, contract):
        contract.localSymbol = 'ESZ6'
        contract.conId = 1
        contract.multiplier = '50'
        contract.exchange = 'CME'
        contract.lastTradeDateOrContractMonth = '20261218'
        return [contract]

    def reqHistoricalData(self, contract, **kw):
        i = self.calls
        self.calls += 1
        return self.scripted[i] if i < len(self.scripted) else []

    def sleep(self, *_):
        pass

    def disconnect(self):
        pass


def test_futures_fetch_continues_past_empty_chunk():
    f = IBStockDataFetcher(db_config=None)
    feb = [_Bar(datetime(2026, 2, 15, tzinfo=timezone.utc))]
    jan = [_Bar(datetime(2026, 1, 15, tzinfo=timezone.utc))]
    # chunk 1 -> Feb data; chunk 2 -> EMPTY (simulated gap); chunk 3 -> Jan data.
    f.ib = FakeIB([feb, [], jan])

    df = f.get_futures_hourly_bars(
        'ES', '20261218', 'CME',
        start_date='2026-01-01', end_date='2026-03-01', save_to_db=False,
    )

    assert df is not None and not df.empty
    months = {ts.month for ts in df['date']}
    # The old break-on-empty would stop at the gap and never reach January.
    assert 1 in months and 2 in months, months
    # Proof the loop issued a request *after* the empty chunk instead of bailing.
    assert f.ib.calls >= 3
