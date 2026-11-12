"""Every historical request must send ``endDateTime`` with an explicit time zone.

IB returns Warning 2174 on the equity path ("…without explicit time zone…
Implied time zone functionality will be removed in the next API release"). The
equity loop used to send a bare ``'YYYYMMDD HH:MM:SS'`` string and paginate on a
tz-stripped bar time, relying on IB guessing the Gateway's zone. It now keeps
tz-aware UTC throughout, like the futures path. Network-free: a fake gateway
records what it was asked for and returns bars tz-aware in Europe/London (how
the real Gateway hands back equity bars).
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from ib_insync import BarData, util

from price_retrival.ib_api import IBStockDataFetcher, _as_utc_bound

LONDON = ZoneInfo("Europe/London")


def _Bar(when):
    # Real BarData: the equity path builds its frame with util.df(bars).
    return BarData(date=when, open=1.0, high=1.0, low=1.0, close=1.0, volume=100)


class RecordingIB:
    """Returns ``scripted[i]`` for the i-th request and records its kwargs."""

    def __init__(self, scripted):
        self.scripted, self.requests = scripted, []

    def isConnected(self):
        return True

    def qualifyContracts(self, contract):
        contract.exchange = "SMART"
        return [contract]

    def reqHistoricalData(self, contract, **kw):
        self.requests.append(kw)
        i = len(self.requests) - 1
        return self.scripted[i] if i < len(self.scripted) else []

    def sleep(self, *_):
        pass

    def disconnect(self):
        pass


def _fetch(scripted, **kw):
    f = IBStockDataFetcher(db_config=None)
    f.ib = RecordingIB(scripted)
    df = f.get_hourly_bars("AAPL", kw.pop("start", "2026-11-09"), kw.pop("end", "2026-11-13"),
                           save_to_db=False, **kw)
    return f.ib.requests, df


def test_every_end_datetime_is_tz_aware_and_formats_as_utc():
    # Two chunks of London-tz bars (summer and winter offsets both exercised).
    chunk1 = [_Bar(datetime(2026, 11, 10, 14, 30, tzinfo=LONDON)),
              _Bar(datetime(2026, 11, 12, 20, 0, tzinfo=LONDON))]
    chunk2 = [_Bar(datetime(2026, 10, 1, 14, 30, tzinfo=LONDON))]   # BST (+01:00)
    reqs, df = _fetch([chunk1, chunk2], start="2026-09-25", end="2026-11-13")

    assert len(reqs) >= 2
    for kw in reqs:
        end = kw["endDateTime"]
        assert isinstance(end, datetime), f"sent {end!r}: bare strings have no zone"
        assert end.tzinfo is not None
        assert util.formatIBDatetime(end).endswith(" UTC")
    assert df is not None and not df.empty


def test_first_request_ends_at_utc_midnight_of_end_date():
    reqs, _ = _fetch([[_Bar(datetime(2026, 11, 12, 20, 0, tzinfo=LONDON))]])
    assert reqs[0]["endDateTime"] == datetime(2026, 11, 13, tzinfo=timezone.utc)


def test_pagination_steps_back_one_bar_from_earliest_bar_in_utc():
    earliest = datetime(2026, 10, 1, 14, 30, tzinfo=LONDON)          # 13:30Z in BST
    reqs, _ = _fetch([[_Bar(earliest), _Bar(datetime(2026, 11, 12, 20, 0, tzinfo=LONDON))]],
                     start="2026-09-01", end="2026-11-13")
    assert reqs[1]["endDateTime"] == datetime(2026, 10, 1, 13, 30, tzinfo=timezone.utc) - timedelta(hours=1)
    # same instant as the old wall-clock pagination, now explicit
    assert reqs[1]["endDateTime"] == earliest.astimezone(timezone.utc) - timedelta(hours=1)


def test_sub_hourly_step_is_one_bar():
    earliest = datetime(2026, 11, 12, 14, 30, tzinfo=LONDON)
    reqs, _ = _fetch([[_Bar(earliest)]], start="2026-11-11", end="2026-11-13", bar_size="1 min")
    assert reqs[1]["endDateTime"] == earliest.astimezone(timezone.utc) - timedelta(minutes=1)


@pytest.mark.parametrize("value, expected", [
    ("2026-11-13", datetime(2026, 11, 13, tzinfo=timezone.utc)),
    (date(2026, 11, 13), datetime(2026, 11, 13, tzinfo=timezone.utc)),
    (datetime(2026, 11, 13, 9, 30), datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)),   # naive = UTC
    (datetime(2026, 7, 1, 12, 0, tzinfo=LONDON), datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc)),
])
def test_as_utc_bound(value, expected):
    got = _as_utc_bound(value)
    assert got == expected and got.tzinfo is not None
