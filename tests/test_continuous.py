"""
Unit tests for the futures continuous-series builder (price_retrival.continuous).

Pure / IB-free / DB-free: exercises the volume-roll schedule and ratio
back-adjustment on synthetic daily candles.
"""

from datetime import datetime, timezone

from price_retrival import continuous


def _d(y, m, d):
    return datetime(y, m, d, tzinfo=timezone.utc)


def _bars(dates_prices_vols):
    """[(date, close, vol), ...] -> flat OHLCV candles (o=h=l=c=close)."""
    return [(_d(*dp[0]), dp[1], dp[1], dp[1], dp[1], dp[2]) for dp in dates_prices_vols]


# Two contracts: A (front, expiry Sep) priced ~100; B (next, expiry Dec) priced
# ~110. B's daily volume overtakes A's on Sep-16 -> roll there; ratio 110/100.
A = {
    'symbol': 'ESU26', 'expiry': _d(2026, 9, 18).date(),
    'candles': _bars([
        ((2026, 9, 14), 100, 100),
        ((2026, 9, 15), 100, 100),
        ((2026, 9, 16), 100, 50),
        ((2026, 9, 17), 100, 20),
    ]),
}
B = {
    'symbol': 'ESZ26', 'expiry': _d(2026, 12, 18).date(),
    'candles': _bars([
        ((2026, 9, 15), 110, 10),
        ((2026, 9, 16), 110, 60),   # B vol (60) > A vol (50) -> roll here
        ((2026, 9, 17), 110, 80),
        ((2026, 9, 18), 110, 100),
        ((2026, 9, 19), 110, 100),
    ]),
}


def test_single_contract_passthrough():
    out = continuous.build_continuous([B])
    assert out['candles'] == B['candles']
    assert out['rolls'] == []
    assert out['factors'] == {'ESZ26': 1.0}


def test_empty_input():
    out = continuous.build_continuous([])
    assert out['candles'] == [] and out['rolls'] == [] and out['factors'] == {}


def test_volume_roll_date_and_ratio():
    out = continuous.build_continuous([A, B])
    assert len(out['rolls']) == 1
    roll = out['rolls'][0]
    assert roll['from'] == 'ESU26' and roll['to'] == 'ESZ26'
    # Roll on the first day B's volume beats A's: 2026-09-16.
    assert roll['roll_ts'] == _d(2026, 9, 16)
    # Ratio = B/A at the last common bar before the roll (2026-09-15) = 110/100.
    assert roll['ratio'] == 110 / 100


def test_back_adjustment_is_continuous():
    out = continuous.build_continuous([A, B])
    # Anchor (newest) unadjusted; older scaled up by 1.1 -> every close == 110.
    assert out['factors'] == {'ESU26': 110 / 100, 'ESZ26': 1.0}
    closes = [round(c[4], 6) for c in out['candles']]
    assert closes == [110.0] * len(closes)   # gap-free across the roll


def test_active_slices_partition_by_roll():
    out = continuous.build_continuous([A, B])
    ts = [c[0] for c in out['candles']]
    # A contributes only pre-roll days (14, 15); B contributes from the roll on.
    assert ts == [_d(2026, 9, 14), _d(2026, 9, 15), _d(2026, 9, 16),
                  _d(2026, 9, 17), _d(2026, 9, 18), _d(2026, 9, 19)]
    # Volume is the ACTIVE contract's real volume (unadjusted): 9/15 = A's 100,
    # 9/16 = B's 60.
    by_ts = {c[0]: c[5] for c in out['candles']}
    assert by_ts[_d(2026, 9, 15)] == 100   # A active
    assert by_ts[_d(2026, 9, 16)] == 60    # B active


def test_expiry_fallback_when_no_volume_crossover():
    # B's volume never beats A's -> roll falls back to A's expiry (2026-09-18).
    a = {
        'symbol': 'ESU26', 'expiry': _d(2026, 9, 18).date(),
        'candles': _bars([((2026, 9, 15), 100, 100), ((2026, 9, 16), 100, 100),
                          ((2026, 9, 17), 100, 100), ((2026, 9, 18), 100, 100)]),
    }
    b = {
        'symbol': 'ESZ26', 'expiry': _d(2026, 12, 18).date(),
        'candles': _bars([((2026, 9, 16), 105, 1), ((2026, 9, 17), 105, 1),
                          ((2026, 9, 18), 105, 1), ((2026, 9, 19), 105, 1)]),
    }
    out = continuous.build_continuous([a, b])
    assert out['rolls'][0]['roll_ts'] == _d(2026, 9, 18)   # A's expiry day
