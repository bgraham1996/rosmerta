"""Unit tests for the bar-size-aware retrieval helpers in ``price_retrival.ib_api``.

These cover the pure, IB-free logic that decides how a request is chunked and
labelled per bar size (board item "Bar-size aware IB retrieval"). The fetch
methods themselves hit a live IB gateway and are exercised by the live
reality-check, not here.
"""

from datetime import timedelta

import pytest

from price_retrival import ib_api


def test_supported_bar_sizes_cover_sub_hourly_through_hourly():
    assert ib_api.SUPPORTED_BAR_SIZES == ("1 min", "5 mins", "15 mins", "30 mins", "1 hour")


@pytest.mark.parametrize("bar_size,days", [
    ("1 min", 1), ("5 mins", 5), ("15 mins", 10), ("30 mins", 20), ("1 hour", 30),
])
def test_chunk_days_shrinks_for_finer_bars(bar_size, days):
    # Finer bars pack more rows per day, so fewer days per request.
    assert ib_api._chunk_days(bar_size) == days


def test_chunk_days_monotonic_non_decreasing():
    vals = [ib_api._chunk_days(b) for b in ib_api.SUPPORTED_BAR_SIZES]
    assert vals == sorted(vals)  # coarser bar size -> >= window


def test_chunk_days_rejects_unsupported_size():
    with pytest.raises(ValueError, match="Unsupported bar_size"):
        ib_api._chunk_days("2 mins")


@pytest.mark.parametrize("bar_size,delta", [
    ("1 min", timedelta(minutes=1)),
    ("5 mins", timedelta(minutes=5)),
    ("15 mins", timedelta(minutes=15)),
    ("30 mins", timedelta(minutes=30)),
    ("1 hour", timedelta(hours=1)),
])
def test_bar_interval_is_one_bar(bar_size, delta):
    # The chunk loop steps back one bar between requests; a fixed 1-hour step
    # would skip 59 minutes of 1-minute bars (the gap bug this guards against).
    assert ib_api._bar_interval(bar_size) == delta


def test_fetch_log_type_hourly_is_backcompat_label():
    # Hourly keeps the legacy label so existing fetch_log rows/queries stay valid.
    assert ib_api._fetch_log_type("1 hour") == "price_hourly"


@pytest.mark.parametrize("bar_size,label", [
    ("1 min", "price_1_min"),
    ("5 mins", "price_5_mins"),
    ("15 mins", "price_15_mins"),
    ("30 mins", "price_30_mins"),
])
def test_fetch_log_type_sub_hourly_label(bar_size, label):
    assert ib_api._fetch_log_type(bar_size) == label


def test_fetch_log_type_fits_column_width():
    # fetch_log.data_type is VARCHAR(20); every label must fit.
    for bar_size in ib_api.SUPPORTED_BAR_SIZES:
        assert len(ib_api._fetch_log_type(bar_size)) <= 20
