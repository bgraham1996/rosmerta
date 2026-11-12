"""EDGAR fundamentals extraction fixes (2026-11-12 restore run).

Network- and DB-free. Covers:
- AMZN: trailing-twelve-month figures in 10-Qs were classified FY, which then
  hid every real quarter sharing the date. A >300-day figure is FY only from a
  10-K.
- GEHC: derived Q4 EPS used a nonsense cover-page share count (100) and hit
  5,550,000, overflowing NUMERIC(10,4) and rolling back the whole save. Now the
  FY-implied share count is used when shares_outstanding is wildly off — while
  stock-split years (shares legitimately ~4-40x from the restated implied count)
  keep the existing NI/shares behaviour.
- A failed DB save is reported as an error, not a success, by bulk fetch.
"""

import pytest

from price_retrival import bulk_fetch, edgar_api
from price_retrival.edgar_api import EdgarFundamentalsFetcher


@pytest.fixture
def fetcher():
    return EdgarFundamentalsFetcher(db_config=None, user_agent="test test@example.com")


def _fact(start, end, val, form, filed="2026-01-01"):
    return {"start": start, "end": end, "val": val, "form": form, "filed": filed}


# --- TTM classification ------------------------------------------------------


def test_ttm_figure_in_10q_is_not_classified_fy(fetcher):
    facts = [
        _fact("2025-07-01", "2025-09-30", 20, "10-Q"),      # true quarter
        _fact("2024-10-01", "2025-09-30", 76, "10-Q"),      # TTM in a 10-Q
        _fact("2025-01-01", "2025-12-31", 77, "10-K"),      # real fiscal year
    ]
    out = fetcher._deduplicate_facts(facts, is_instant=False, start_date=None, end_date=None)
    assert out == {("2025-09-30", "QR"): 20, ("2025-12-31", "FY"): 77}


def test_amzn_shaped_company_keeps_its_quarters(fetcher, monkeypatch):
    """End to end through get_fundamentals: TTM rows must not hide the quarters."""
    rev = [
        _fact("2025-01-01", "2025-03-31", 155, "10-Q"), _fact("2024-04-01", "2025-03-31", 650, "10-Q"),
        _fact("2025-04-01", "2025-06-30", 167, "10-Q"), _fact("2024-07-01", "2025-06-30", 670, "10-Q"),
        _fact("2025-07-01", "2025-09-30", 180, "10-Q"), _fact("2024-10-01", "2025-09-30", 690, "10-Q"),
        _fact("2025-01-01", "2025-12-31", 717, "10-K"),
    ]
    facts_json = {"facts": {"us-gaap": {"Revenues": {"units": {"USD": rev}}}}}
    monkeypatch.setattr(edgar_api, "XBRL_TAG_MAP", {"revenue": ["Revenues"]})
    monkeypatch.setattr(fetcher, "resolve_ticker",
                        lambda t: {"cik": 1, "cik_padded": "0000000001", "name": "Shape Co"})
    monkeypatch.setattr(fetcher, "_fetch_company_facts", lambda cik: facts_json)

    recs = fetcher.get_fundamentals("SHAPE", save_to_db=False)
    by_type = {r["period_type"]: r for r in recs}
    assert set(by_type) == {"Q1", "Q2", "Q3", "Q4", "FY"}
    assert by_type["Q3"]["revenue"] == 180
    assert by_type["FY"]["revenue"] == 717
    assert by_type["Q4"]["revenue"] == 717 - (155 + 167 + 180)   # derived


# --- Q4 EPS derivation -------------------------------------------------------


def _year(fy_shares, fy_eps=4.22, q_shares=454_000_000):
    q = lambda pt, end, ni, eps: {"period_type": pt, "period_end": end, "fiscal_year": 2022,
                                  "net_income": ni, "eps_basic": eps, "eps_diluted": eps,
                                  "shares_outstanding": q_shares}
    fy = {"period_type": "FY", "period_end": "2022-12-31", "fiscal_year": 2022,
          "net_income": 1_916_000_000, "eps_basic": fy_eps, "eps_diluted": fy_eps,
          "shares_outstanding": fy_shares}
    return [q("Q1", "2022-03-31", 389_000_000, 0.86), q("Q2", "2022-06-30", 485_000_000, 1.07),
            q("Q3", "2022-09-30", 487_000_000, 1.07), fy]


def test_gehc_nonsense_share_count_uses_fy_implied_shares(fetcher):
    (q4,) = fetcher._derive_q4_records(_year(fy_shares=100))   # GEHC pre-spin cover page
    assert q4["net_income"] == 555_000_000
    implied = 1_916_000_000 / 4.22                               # ~454M
    assert q4["eps_basic"] == pytest.approx(555_000_000 / implied, abs=1e-4)
    assert abs(q4["eps_diluted"]) < 10                           # was 5,550,000


def test_plausible_share_count_keeps_ni_over_shares(fetcher):
    (q4,) = fetcher._derive_q4_records(_year(fy_shares=460_000_000))
    assert q4["eps_basic"] == round(555_000_000 / 460_000_000, 4)


def test_stock_split_year_keeps_existing_behaviour(fetcher):
    # FY EPS restated 7:1 after a split → implied shares ~7x the as-of count.
    # That's within the sanity factor, so NI/shares is still used (no change).
    (q4,) = fetcher._derive_q4_records(_year(fy_shares=460_000_000, fy_eps=4.22 / 7))
    assert q4["eps_basic"] == round(555_000_000 / 460_000_000, 4)


def test_unstorable_eps_is_nulled_not_saved(fetcher):
    recs = _year(fy_shares=1)
    for r in recs:                      # no EPS anywhere → nothing to sanity-check against
        r["eps_basic"] = r["eps_diluted"] = None
    (q4,) = fetcher._derive_q4_records(recs)
    assert q4["eps_basic"] is None and q4["eps_diluted"] is None   # 555M/1 can't be stored


# --- save outcome reporting --------------------------------------------------


def test_last_save_ok_false_when_save_fails(fetcher, monkeypatch):
    fetcher.db_config = {"dummy": True}
    monkeypatch.setattr(fetcher, "resolve_ticker",
                        lambda t: {"cik": 1, "cik_padded": "0000000001", "name": "X"})
    monkeypatch.setattr(fetcher, "_fetch_company_facts", lambda cik: {"facts": {"us-gaap": {
        "Revenues": {"units": {"USD": [_fact("2025-01-01", "2025-12-31", 10, "10-K")]}}}}})
    monkeypatch.setattr(edgar_api, "XBRL_TAG_MAP", {"revenue": ["Revenues"]})
    monkeypatch.setattr(fetcher, "_get_or_create_stock", lambda *a: 1)
    monkeypatch.setattr(fetcher, "_save_fundamentals_to_db", lambda *a: False)
    monkeypatch.setattr(fetcher, "_log_fetch", lambda *a, **k: None)

    recs = fetcher.get_fundamentals("X", save_to_db=True)
    assert recs and fetcher.last_save_ok is False


def test_bulk_reports_failed_save_as_error(monkeypatch):
    class FailingSaveFetcher:
        def __init__(self, **kw):
            self.last_save_ok = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get_fundamentals(self, ticker, start_date=None, end_date=None, save_to_db=True):
            self.last_save_ok = False                 # save rolled back
            return [{"period_end": "2025-12-31"}]     # but records came back

    monkeypatch.setattr(edgar_api, "EdgarFundamentalsFetcher", FailingSaveFetcher)
    (res,) = bulk_fetch.bulk_fetch_fundamentals([{"symbol": "GEHC"}], db_config={}, user_agent="u",
                                                force=True)
    assert res.status == "error" and res.records == 0
