"""
Dividends Data Fetcher
======================
Retrieves per-share dividend history (ex-date + amount) for stocks from IB.

Architecture
------------
- `derive_dividends_from_bars` — pure function: derives ex-dates and cash
  amounts from IB's daily TRADES vs ADJUSTED_LAST closes (see the section
  comment below for the maths and its limits).
- `IBDividendsFetcher` — pulls both daily series from IB and derives the
  records (`source='ib_adjusted'`). IB's fundamentals feed is refused for this
  account (error 10358), so that route is gone.
- `get_dividends(...)` — public entry point: fetch from IB, optionally store,
  and auto-populate the 'dividends' watchlist.
- `EdgarDividendsFetcher` — kept for cross-checks only. EDGAR XBRL gives
  fiscal-period totals (often year-to-date) dated at the period end, not
  payments, so its output is never stored.

Dividends actually *received* (cash, withholding tax, pay dates) belong to
the IB Flex Cash Transactions section, not this module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional

import psycopg2
import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


# ============================================================
# Cross-validation tolerance bands
# ============================================================
VALIDATION_SILENT_PCT = 0.05    # <5% mismatch — silent
VALIDATION_WARN_PCT = 0.20      # 5–20% — warn; >20% — error


# ============================================================
# XBRL concepts we look for, in priority order
# ============================================================
PER_SHARE_CONCEPTS = [
    'CommonStockDividendsPerShareDeclared',
    'CommonStockDividendsPerShareCashPaid',
]
TOTAL_CASH_CONCEPTS = [
    'DividendsCommonStockCash',
    'PaymentsOfDividendsCommonStock',
    'Dividends',
]


# ============================================================
# Result type
# ============================================================
@dataclass
class DividendRecord:
    """A single dividend payment as we model it before DB insert."""
    ex_date: date
    pay_date: Optional[date]
    declaration_date: Optional[date]
    record_date: Optional[date]
    amount: Decimal
    currency: str
    frequency: Optional[str]
    dividend_type: str   # 'regular', 'special', 'stock', 'liquidating'
    source: str          # 'ib_adjusted' (stored); 'edgar' (cross-check only)

    def as_db_tuple(self, stock_id: int) -> tuple:
        """Convert to the tuple shape expected by the dividends INSERT."""
        return (
            stock_id,
            self.ex_date,
            self.pay_date,
            self.amount,
            self.currency,
            self.frequency,
            self.declaration_date,
            self.record_date,
            self.dividend_type,
            self.source,
        )


# ============================================================
# Shared DB helpers
# ============================================================
def _get_or_create_stock(conn, symbol: str, exchange: str = 'SMART',
                         currency: str = 'USD') -> Optional[int]:
    """Look up stock_id by symbol, creating a placeholder if absent.

    Matches on ``symbol`` alone: ``stocks`` is UNIQUE(symbol) since the dedupe
    migration, so a symbol+exchange lookup could miss an existing row stored
    under another exchange and the placeholder INSERT would then fail.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stock_id FROM stocks WHERE symbol = %s",
            (symbol,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            """INSERT INTO stocks (symbol, name, exchange, currency)
               VALUES (%s, %s, %s, %s) RETURNING stock_id""",
            (symbol, symbol, exchange, currency)
        )
        stock_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Created stock record: {symbol} ({exchange}) -> id {stock_id}")
        return stock_id


def _replace_derived_dividends(conn, stock_id: int,
                               records: list[DividendRecord]) -> int:
    """Store derived records, replacing earlier rows of the same source.

    Derived amounts can move by ~$0.0001 between fetches (IB re-rounds its
    adjusted closes), and ``uq_dividend`` includes ``amount``, so a plain
    upsert would leave near-duplicate rows. Instead, within one transaction,
    delete this stock's rows of the same source inside the fetched ex-date
    window and insert the fresh set. Rows from other sources are untouched.
    """
    if not records:
        return 0
    source = records[0].source
    lo = min(r.ex_date for r in records)
    hi = max(r.ex_date for r in records)
    insert_sql = """
        INSERT INTO dividends
            (stock_id, ex_date, pay_date, amount, currency, frequency,
             declaration_date, record_date, dividend_type, source)
        VALUES %s
        ON CONFLICT (stock_id, ex_date, amount) DO NOTHING
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """DELETE FROM dividends
                   WHERE stock_id = %s AND source = %s
                     AND ex_date BETWEEN %s AND %s""",
                (stock_id, source, lo, hi),
            )
            execute_values(cur, insert_sql,
                           [r.as_db_tuple(stock_id) for r in records])
        conn.commit()
        logger.info(f"Stored {len(records)} {source} dividends for stock_id={stock_id}")
        return len(records)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving dividends to DB: {e}")
        raise


def _add_to_dividends_watchlist(conn, stock_id: int) -> bool:
    """
    Add stock to the 'dividends' watchlist if not already present.
    Returns True if a new row was added, False otherwise.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO watchlist_members
                       (stock_id, list_name, source, notes)
                   VALUES (%s, 'dividends', 'programmatic',
                           'Auto-added by dividend fetcher')
                   ON CONFLICT (stock_id, list_name) DO NOTHING
                   RETURNING watchlist_member_id""",
                (stock_id,)
            )
            added = cur.fetchone() is not None
        conn.commit()
        if added:
            logger.info(f"Added stock_id={stock_id} to 'dividends' watchlist")
        return added
    except Exception as e:
        conn.rollback()
        logger.error(f"Error adding to dividends watchlist: {e}")
        return False


def _log_fetch(conn, stock_id: int, fetch_start: datetime,
               fetch_end: datetime, records: int, status: str,
               error_message: Optional[str] = None) -> None:
    """Record a fetch operation in fetch_log with data_type='dividends'."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO fetch_log
                       (stock_id, data_type, fetch_start, fetch_end,
                        bars_fetched, status, error_message)
                   VALUES (%s, 'dividends', %s, %s, %s, %s, %s)""",
                (stock_id, fetch_start, fetch_end, records, status, error_message)
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging dividend fetch: {e}")
        conn.rollback()


def _get_latest_shares_outstanding(conn, stock_id: int,
                                   as_of: date) -> Optional[int]:
    """
    Look up the most recent shares_outstanding from `fundamentals`
    on or before `as_of`. Used for cross-validation.
    """
    with conn.cursor() as cur:
        cur.execute(
            """SELECT shares_outstanding
               FROM fundamentals
               WHERE stock_id = %s
                 AND period_end <= %s
                 AND shares_outstanding IS NOT NULL
               ORDER BY period_end DESC
               LIMIT 1""",
            (stock_id, as_of)
        )
        row = cur.fetchone()
        return row[0] if row else None


# ============================================================
# Cross-validation
# ============================================================
def _cross_validate(symbol: str, ex_date: date, per_share: Decimal,
                    total_cash: Optional[Decimal],
                    shares_outstanding: Optional[int]) -> str:
    """
    Compare per_share × shares_outstanding against total_cash.

    Returns a status string: 'ok', 'warn', or 'error'. Logs at the
    appropriate level. If we don't have both total_cash and
    shares_outstanding, returns 'ok' (can't validate).
    """
    if total_cash is None or shares_outstanding is None:
        return 'ok'

    expected = Decimal(per_share) * Decimal(shares_outstanding)
    if expected == 0:
        return 'ok'

    diff_pct = abs(float((expected - Decimal(total_cash)) / expected))

    if diff_pct < VALIDATION_SILENT_PCT:
        return 'ok'
    elif diff_pct < VALIDATION_WARN_PCT:
        logger.warning(
            f"[{symbol} {ex_date}] dividend mismatch {diff_pct:.1%}: "
            f"per_share×shares={expected:,.0f} vs total_cash={total_cash:,.0f}"
        )
        return 'warn'
    else:
        logger.error(
            f"[{symbol} {ex_date}] LARGE dividend mismatch {diff_pct:.1%}: "
            f"per_share×shares={expected:,.0f} vs total_cash={total_cash:,.0f}"
        )
        return 'error'


# ============================================================
# EDGAR fetcher
# ============================================================
class EdgarDividendsFetcher:
    """
    Fetches dividend history from SEC EDGAR's companyfacts XBRL endpoint.

    Reuses the same `companyfacts` JSON as the fundamentals fetcher, so
    we benefit from the same CIK lookup logic. Rather than reimplementing
    that, we delegate to `EdgarFundamentalsFetcher.resolve_ticker()`.
    """

    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{:010d}.json"

    def __init__(self, db_config: dict, user_agent: str):
        self.db_config = db_config
        self.user_agent = user_agent
        self.db_conn = None
        self._fundamentals_fetcher = None  # lazy init for CIK resolution

    # -- Connection lifecycle ---------------------------------
    def __enter__(self):
        if self.db_config:
            self.db_conn = psycopg2.connect(**self.db_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

    # -- CIK resolution (delegated) ---------------------------
    def _get_fundamentals_fetcher(self):
        """Lazy-init the fundamentals fetcher to reuse its CIK lookup."""
        if self._fundamentals_fetcher is None:
            from price_retrival.edgar_api import EdgarFundamentalsFetcher
            self._fundamentals_fetcher = EdgarFundamentalsFetcher(
                db_config=self.db_config,
                user_agent=self.user_agent,
            )
            self._fundamentals_fetcher.__enter__()
        return self._fundamentals_fetcher

    def resolve_ticker(self, ticker: str) -> Optional[dict]:
        """Returns {'cik': int, 'name': str} or None if ticker has no CIK."""
        return self._get_fundamentals_fetcher().resolve_ticker(ticker)

    # -- XBRL fetching ----------------------------------------
    def _fetch_companyfacts(self, cik: int) -> Optional[dict]:
        """Fetch the raw companyfacts JSON for a CIK."""
        url = self.BASE_URL.format(cik)
        headers = {
            'User-Agent': self.user_agent,
            'Accept-Encoding': 'gzip, deflate',
        }
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"No companyfacts for CIK {cik:010d}")
                return None
            logger.error(f"Error fetching companyfacts for CIK {cik}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching companyfacts for CIK {cik}: {e}")
            return None

    @staticmethod
    def _extract_concept_facts(facts: dict, concept: str) -> list[dict]:
        """
        Pull facts for a given us-gaap concept. Returns list of fact dicts
        with {'start', 'end', 'val', 'filed', 'unit'} keys, or empty list.
        """
        us_gaap = facts.get('facts', {}).get('us-gaap', {})
        concept_data = us_gaap.get(concept)
        if not concept_data:
            return []

        units = concept_data.get('units', {})
        results = []

        for unit_key, fact_list in units.items():
            for fact in fact_list:
                # Per-share is in 'USD/shares'; total cash in 'USD'
                if 'val' not in fact:
                    continue
                results.append({
                    'start': fact.get('start'),
                    'end': fact.get('end'),
                    'val': fact['val'],
                    'filed': fact.get('filed'),
                    'unit': unit_key,
                })
        return results

    def _build_per_share_records(self, facts: dict) -> dict:
        """
        Build a dict keyed by `end_date` of per-share dividend amounts.
        Walks `PER_SHARE_CONCEPTS` in priority order; first hit wins per date.
        """
        result = {}
        for concept in PER_SHARE_CONCEPTS:
            for fact in self._extract_concept_facts(facts, concept):
                if not fact['end']:
                    continue
                # Only keep USD/shares units
                if 'shares' not in fact['unit'].lower():
                    continue
                end_date = datetime.strptime(fact['end'], '%Y-%m-%d').date()

                # Filter to single-quarter or single-period facts.
                # Skip cumulative YTD figures (period > 115 days).
                if fact['start']:
                    start_date = datetime.strptime(
                        fact['start'], '%Y-%m-%d'
                    ).date()
                    duration = (end_date - start_date).days
                    if duration > 115:
                        continue

                # First concept wins for a given end_date
                if end_date in result:
                    continue
                result[end_date] = {
                    'amount': Decimal(str(fact['val'])),
                    'start': fact.get('start'),
                    'filed': fact.get('filed'),
                }
        return result

    def _build_total_cash_lookup(self, facts: dict) -> dict:
        """Build a dict keyed by end_date of total cash dividend payments."""
        result = {}
        for concept in TOTAL_CASH_CONCEPTS:
            for fact in self._extract_concept_facts(facts, concept):
                if not fact['end']:
                    continue
                if 'usd' not in fact['unit'].lower() or 'shares' in fact['unit'].lower():
                    continue
                end_date = datetime.strptime(fact['end'], '%Y-%m-%d').date()

                if fact['start']:
                    start_date = datetime.strptime(
                        fact['start'], '%Y-%m-%d'
                    ).date()
                    duration = (end_date - start_date).days
                    if duration > 115:
                        continue

                if end_date in result:
                    continue
                result[end_date] = Decimal(str(fact['val']))
        return result

    def get_dividends(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[DividendRecord]:
        """
        Fetch dividend records for a ticker from EDGAR.

        Returns an empty list if the ticker has no CIK or no dividend data.
        Cross-validates per-share against total cash where both available.
        """
        info = self.resolve_ticker(ticker)
        if not info:
            return []

        facts = self._fetch_companyfacts(info['cik'])
        if not facts:
            return []

        per_share_data = self._build_per_share_records(facts)
        total_cash_lookup = self._build_total_cash_lookup(facts)

        if not per_share_data:
            logger.info(f"No dividend per-share data for {ticker} on EDGAR")
            return []

        # Apply date filtering
        start_d = (datetime.strptime(start_date, '%Y-%m-%d').date()
                   if start_date else None)
        end_d = (datetime.strptime(end_date, '%Y-%m-%d').date()
                 if end_date else None)

        # Cross-validation needs shares_outstanding from the DB (optional)
        stock_id = (_get_or_create_stock(self.db_conn, ticker)
                    if self.db_conn else None)

        records = []
        for ex_date, ps in sorted(per_share_data.items()):
            if start_d and ex_date < start_d:
                continue
            if end_d and ex_date > end_d:
                continue
            if ps['amount'] == 0:
                continue

            # Cross-validate
            total_cash = total_cash_lookup.get(ex_date)
            shares = (_get_latest_shares_outstanding(
                self.db_conn, stock_id, ex_date) if stock_id else None)
            _cross_validate(ticker, ex_date, ps['amount'], total_cash, shares)

            records.append(DividendRecord(
                ex_date=ex_date,
                pay_date=None,         # EDGAR doesn't give pay dates
                declaration_date=None, # could be enriched from filed date later
                record_date=None,
                amount=ps['amount'],
                currency='USD',
                frequency=None,
                dividend_type='regular',  # EDGAR can't reliably flag specials
                source='edgar',
            ))

        logger.info(f"Built {len(records)} EDGAR dividend records for {ticker}")
        return records

    def close(self):
        """Clean up the inner fundamentals fetcher if it was opened."""
        if self._fundamentals_fetcher is not None:
            try:
                self._fundamentals_fetcher.__exit__(None, None, None)
            except Exception:
                pass
            self._fundamentals_fetcher = None


# ============================================================
# Dividends derived from IB adjusted prices
# ============================================================
# IB refuses `reqFundamentalData` for this account (error 10358, "Fundamentals
# data is not allowed"), so dividend history can't come from ReportsFinSummary.
# It does serve daily `ADJUSTED_LAST` bars, which are TRADES bars adjusted for
# dividends (both series are already split-adjusted). The ratio
#
#     f(d) = adjusted_close(d) / raw_close(d)
#
# is flat between dividends and steps up on each ex-date. With
# step = f(d) / f(d-1), the cash dividend is
#
#     dividend = raw_close(d-1) * (1 - 1 / step)
#
# Checked against known payouts (PFE, KO, MRK, ABBV, AAPL, NVO, O, BNTX, AMZN):
# ex-dates match and amounts land within ±$0.0001. Limits: no pay, record or
# declaration dates, and amounts before a split or spin-off are in today's
# share terms (as total-return maths wants).

#: A step smaller than this is float noise in IB's adjusted closes.
MIN_STEP = 2e-5
#: Ignore implied dividends under half a cent (also noise).
MIN_AMOUNT = 0.005
#: Snap to a 3-decimal amount when within this distance (0.4299 -> 0.430).
SNAP_TOLERANCE = 0.00015
#: An amount this many times the series median is flagged 'special'.
SPECIAL_MULTIPLE = 3.0

#: Median gap (days) between ex-dates -> frequency label.
_FREQUENCY_BANDS = [
    (45, 'monthly'),
    (135, 'quarterly'),
    (270, 'semi-annual'),
    (500, 'annual'),
]


def _snap_amount(value: float) -> Decimal:
    """Round to 4 dp, snapping to 3 dp when the gap is only rounding noise."""
    three = round(value, 3)
    if abs(value - three) <= SNAP_TOLERANCE:
        return Decimal(f"{three:.3f}")
    return Decimal(f"{value:.4f}")


def _infer_frequency(ex_dates: list[date]) -> Optional[str]:
    """Label a series by the median gap between consecutive ex-dates."""
    if len(ex_dates) < 2:
        return None
    gaps = sorted((b - a).days for a, b in zip(ex_dates, ex_dates[1:]))
    median = gaps[len(gaps) // 2]
    for limit, label in _FREQUENCY_BANDS:
        if median <= limit:
            return label
    return 'irregular'


def _to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()


def derive_dividends_from_bars(raw_closes, adjusted_closes,
                               currency: str = 'USD') -> list[DividendRecord]:
    """Derive cash dividends from unadjusted vs dividend-adjusted daily closes.

    Args:
        raw_closes: iterable of ``(date, close)`` from TRADES daily bars.
        adjusted_closes: iterable of ``(date, close)`` from ADJUSTED_LAST bars.
        currency: currency of the contract (amounts are in this currency).

    Returns:
        ``DividendRecord`` list ordered by ex-date, ``source='ib_adjusted'``.
        Only dates present in both series are used.
    """
    raw = {_to_date(d): float(c) for d, c in raw_closes if c}
    adj = {_to_date(d): float(c) for d, c in adjusted_closes if c}
    days = sorted(raw.keys() & adj.keys())

    events: list[tuple[date, float]] = []
    for prev, cur in zip(days, days[1:]):
        step = (adj[cur] / raw[cur]) / (adj[prev] / raw[prev])
        if step - 1 <= MIN_STEP:
            continue
        amount = raw[prev] * (1 - 1 / step)
        if amount >= MIN_AMOUNT:
            events.append((cur, amount))

    frequency = _infer_frequency([d for d, _ in events])
    amounts = sorted(a for _, a in events)
    median = amounts[len(amounts) // 2] if amounts else 0.0

    records = []
    for ex_date, amount in events:
        special = len(events) >= 4 and amount > SPECIAL_MULTIPLE * median
        records.append(DividendRecord(
            ex_date=ex_date,
            pay_date=None,
            declaration_date=None,
            record_date=None,
            amount=_snap_amount(amount),
            currency=currency,
            frequency=frequency,
            dividend_type='special' if special else 'regular',
            source='ib_adjusted',
        ))
    return records


def _ib_duration(start_date: Optional[str], default_years: int = 10) -> str:
    """IB durationStr covering ``start_date`` to today (one extra year of slack)."""
    if not start_date:
        return f"{default_years} Y"
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    years = (date.today() - start).days // 365 + 1
    return f"{max(years, 1)} Y"


# ============================================================
# IB fetcher
# ============================================================
class IBDividendsFetcher:
    """Dividend history from IB, derived from adjusted vs unadjusted daily bars.

    Needs only historical-data access (no fundamentals or market-data
    subscription). Use as a context manager.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 4001,
                 client_id: int = 11):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = None

    # -- Connection lifecycle ---------------------------------
    def __enter__(self):
        from ib_insync import IB
        import nest_asyncio
        nest_asyncio.apply()

        self.ib = IB()
        self.ib.connect(self.host, self.port, clientId=self.client_id,
                        timeout=10, readonly=True)
        logger.info(f"Connected to IB on {self.host}:{self.port}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
        except Exception as e:
            logger.error(f"Error during IB disconnect: {e}")

    # -- Fetching ---------------------------------------------
    def _daily_closes(self, contract, duration: str, what: str) -> list:
        bars = self.ib.reqHistoricalData(
            contract, endDateTime='', durationStr=duration,
            barSizeSetting='1 day', whatToShow=what, useRTH=True,
        )
        return [(b.date, b.close) for b in bars or []]

    def get_dividends(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = 'SMART',
        currency: str = 'USD',
    ) -> list[DividendRecord]:
        """Derive dividend records for ``ticker`` (default: last 10 years).

        Returns an empty list when the stock paid no dividends in the window.

        Raises:
            RuntimeError: the contract can't be qualified or IB returned no
                daily bars, so "no dividends" can't be told apart from "no data".
        """
        from ib_insync import Stock

        qualified = self.ib.qualifyContracts(Stock(ticker, exchange, currency))
        if not qualified:
            raise RuntimeError(f"Could not qualify IB contract for {ticker}")
        contract = qualified[0]

        duration = _ib_duration(start_date)
        raw = self._daily_closes(contract, duration, 'TRADES')
        adjusted = self._daily_closes(contract, duration, 'ADJUSTED_LAST')
        if not raw or not adjusted:
            raise RuntimeError(
                f"IB returned no daily bars for {ticker} "
                f"(TRADES {len(raw)}, ADJUSTED_LAST {len(adjusted)})"
            )

        records = derive_dividends_from_bars(
            raw, adjusted, currency=contract.currency or currency)

        start_d = (datetime.strptime(start_date, '%Y-%m-%d').date()
                   if start_date else None)
        end_d = (datetime.strptime(end_date, '%Y-%m-%d').date()
                 if end_date else None)
        records = [r for r in records
                   if (not start_d or r.ex_date >= start_d)
                   and (not end_d or r.ex_date <= end_d)]

        logger.info(f"Derived {len(records)} IB dividend records for {ticker}")
        return records


# ============================================================
# Public dispatcher
# ============================================================
def get_dividends(
    ticker: str,
    db_config: Optional[dict],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exchange: str = 'SMART',
    currency: str = 'USD',
    ib_host: str = '127.0.0.1',
    ib_port: int = 4001,
    save_to_db: bool = True,
) -> tuple[list[DividendRecord], str]:
    """Fetch dividend history for ``ticker`` from IB and optionally store it.

    IB-derived history (``source='ib_adjusted'``) is the only stored source.
    EDGAR's per-share facts are fiscal-period totals, not payments, so
    ``EdgarDividendsFetcher`` is kept for cross-checks only and never routed to.

    On ``save_to_db=True`` the derived rows replace earlier ``ib_adjusted``
    rows in the same date window, the ticker joins the 'dividends' watchlist
    when it has records, and the fetch is logged to ``fetch_log``.

    Returns:
        ``(records, 'ib_adjusted')``.

    Raises:
        RuntimeError: from ``IBDividendsFetcher.get_dividends`` when IB has
            no data for the ticker.
    """
    ticker = ticker.upper()
    fetch_start = datetime.now()

    with IBDividendsFetcher(host=ib_host, port=ib_port) as ib:
        records = ib.get_dividends(
            ticker, start_date=start_date, end_date=end_date,
            exchange=exchange, currency=currency,
        )

    if save_to_db:
        conn = psycopg2.connect(**db_config)
        try:
            stock_id = _get_or_create_stock(conn, ticker, exchange, currency)
            if records:
                _replace_derived_dividends(conn, stock_id, records)
                _add_to_dividends_watchlist(conn, stock_id)
                _log_fetch(conn, stock_id, fetch_start, datetime.now(),
                           len(records), 'success')
            else:
                _log_fetch(conn, stock_id, fetch_start, datetime.now(),
                           0, 'empty', 'No dividends in IB adjusted prices')
        finally:
            conn.close()

    return records, 'ib_adjusted'


# ============================================================
# Example usage
# ============================================================
if __name__ == "__main__":
    records, source = get_dividends('PFE', db_config=None,
                                    start_date='2023-01-01', save_to_db=False)
    print(f"\nDerived {len(records)} dividend records via {source}")
    for r in records:
        print(f"  {r.ex_date}  {r.amount}  {r.frequency}  {r.dividend_type}")
