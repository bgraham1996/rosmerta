"""Unit tests for the update_core refresh workflow's pure date logic.

DB-free and network-free: covers only the two fetcher-free helpers that decide
*what window* to refresh — ``_tomorrow`` (the end date) and ``_start_for`` (the
per-ticker start derived from its last stored bar). The IB/DB fetch itself is
I/O and exercised live, not here.
"""

from datetime import datetime, timezone

from workflows.refresh_data import update_core as wf


def test_tomorrow_is_one_day_ahead():
    now = datetime(2026, 8, 30, 15, 0, tzinfo=timezone.utc)
    assert wf._tomorrow(now) == '2026-08-31'


def test_tomorrow_rolls_month_end():
    now = datetime(2026, 8, 31, 23, 30, tzinfo=timezone.utc)
    assert wf._tomorrow(now) == '2026-09-01'


def test_start_for_uses_last_bar_day():
    last_ts = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    # Starts on the calendar day of the last stored bar (time-of-day dropped), so
    # the final partial session is re-fetched and no session is skipped.
    assert wf._start_for(last_ts) == '2026-08-28'


def test_start_for_falls_back_when_no_history():
    assert wf._start_for(None) == wf.DEFAULT_START
    assert wf._start_for(None, default='2019-01-01') == '2019-01-01'
