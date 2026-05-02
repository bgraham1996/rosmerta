"""
Dividends Data Fetcher
======================
Retrieves dividend history for stocks, with EDGAR as the primary source
and IB as a fallback for ADRs/foreign tickers and when EDGAR returns nothing.

Architecture
------------
- `EdgarDividendsFetcher` — parses XBRL `companyfacts` for the four standard
  dividend concepts. Cross-validates per-share × shares_outstanding against
  total cash paid, with 5%/20% tolerance bands.
- `IBDividendsFetcher` — uses `reqFundamentalData(contract, 'ReportsFinSummary')`
  and parses the resulting XML for the `<Dividends>` block.
- `get_dividends(...)` — public dispatcher. Routes by CIK existence; falls back
  to IB on empty EDGAR result. Auto-populates the 'dividends' watchlist when
  any records are returned.

Notes on EDGAR limitations
--------------------------
EDGAR XBRL gives us the period (start/end) the dividend belongs to and the
filing date — not the actual ex-date. We use the period `end` as a proxy for
`ex_date` and flag the row with `source='edgar'` so downstream code can
distinguish it from IB-sourced rows with real ex-dates.

For dividend-capture trades, prefer rows with `source='ib'`.
"""

from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
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
    source: str          # 'edgar' or 'ib'

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
    """Look up stock_id by symbol+exchange, creating a placeholder if absent."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stock_id FROM stocks WHERE symbol = %s AND exchange = %s",
            (symbol, exchange)
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


def _save_dividends_to_db(conn, stock_id: int,
                          records: list[DividendRecord]) -> int:
    """
    Upsert dividend records. Returns the number of rows successfully written.

    The unique constraint on `dividends` is (stock_id, ex_date, amount), so
    re-fetches will hit ON CONFLICT for unchanged data. We refresh metadata
    columns on conflict to allow late enrichment (e.g. an EDGAR row later
    backfilled with IB's actual ex-date logic — though we'd typically dedupe
    that case before insert).
    """
    if not records:
        return 0

    rows = [r.as_db_tuple(stock_id) for r in records]

    insert_sql = """
        INSERT INTO dividends
            (stock_id, ex_date, pay_date, amount, currency, frequency,
             declaration_date, record_date, dividend_type, source)
        VALUES %s
        ON CONFLICT (stock_id, ex_date, amount)
        DO UPDATE SET
            pay_date         = COALESCE(EXCLUDED.pay_date, dividends.pay_date),
            declaration_date = COALESCE(EXCLUDED.declaration_date, dividends.declaration_date),
            record_date      = COALESCE(EXCLUDED.record_date, dividends.record_date),
            dividend_type    = EXCLUDED.dividend_type,
            source           = EXCLUDED.source
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)
        conn.commit()
        logger.info(f"Saved {len(rows)} dividend records for stock_id={stock_id}")
        return len(rows)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving dividends to DB: {e}")
        return 0


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

        # Look up stock_id once for cross-validation queries
        stock_id = _get_or_create_stock(self.db_conn, ticker)

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
            shares = _get_latest_shares_outstanding(
                self.db_conn, stock_id, ex_date
            )
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
# IB fetcher
# ============================================================
class IBDividendsFetcher:
    """
    Fetches dividend history from IB via reqFundamentalData / ReportsFinSummary.

    Pattern mirrors IBStockDataFetcher: connection lifecycle, qualify contract,
    fetch, parse. Designed to be used as context manager.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 4001,
                 client_id: int = 11, db_config: Optional[dict] = None):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.db_config = db_config
        self.ib = None
        self.db_conn = None

    # -- Connection lifecycle ---------------------------------
    def __enter__(self):
        from ib_insync import IB
        import nest_asyncio
        nest_asyncio.apply()

        self.ib = IB()
        self.ib.connect(self.host, self.port,
                        clientId=self.client_id, timeout=10)
        logger.info(f"Connected to IB on {self.host}:{self.port}")

        if self.db_config:
            self.db_conn = psycopg2.connect(**self.db_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
        except Exception as e:
            logger.error(f"Error during IB disconnect: {e}")
        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
        except Exception as e:
            logger.error(f"Error closing DB connection: {e}")

    # -- Fetching ---------------------------------------------
    def get_dividends(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = 'SMART',
        currency: str = 'USD',
    ) -> list[DividendRecord]:
        """
        Fetch dividend records via reqFundamentalData(ReportsFinSummary).

        Returns an empty list if the contract can't be qualified or no
        dividend block is present in the XML.
        """
        from ib_insync import Stock

        contract = Stock(ticker, exchange, currency)
        qualified = self.ib.qualifyContracts(contract)
        if not qualified:
            logger.error(f"Could not qualify contract for {ticker}")
            return []
        contract = qualified[0]

        try:
            xml_str = self.ib.reqFundamentalData(contract, 'ReportsFinSummary')
        except Exception as e:
            logger.error(f"reqFundamentalData failed for {ticker}: {e}")
            return []

        if not xml_str:
            logger.info(f"No fundamental XML for {ticker} from IB")
            return []

        records = self._parse_dividends_xml(xml_str, currency=currency)

        # Date filtering
        start_d = (datetime.strptime(start_date, '%Y-%m-%d').date()
                   if start_date else None)
        end_d = (datetime.strptime(end_date, '%Y-%m-%d').date()
                 if end_date else None)

        filtered = []
        for r in records:
            if start_d and r.ex_date < start_d:
                continue
            if end_d and r.ex_date > end_d:
                continue
            filtered.append(r)

        logger.info(f"Built {len(filtered)} IB dividend records for {ticker}")
        return filtered

    @staticmethod
    def _parse_dividends_xml(xml_str: str,
                             currency: str = 'USD') -> list[DividendRecord]:
        """
        Parse the ReportsFinSummary XML for dividend entries.

        Structure (approximate):
            <ReportFinancialSummary>
              <Dividends>
                <Dividend type="..." exDate="YYYY-MM-DD" recordDate="..."
                          payDate="..." declarationDate="..." currency="...">
                    <amount>X.XX</amount>
                </Dividend>
                ...
              </Dividends>
            </ReportFinancialSummary>

        Real-world XML varies — we handle the common cases and fall back
        gracefully where elements are absent.
        """
        records: list[DividendRecord] = []
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as e:
            logger.error(f"Failed to parse IB fundamentals XML: {e}")
            return records

        # IB nests dividends in different shapes depending on report version.
        # Find any <Dividend> elements regardless of ancestor.
        for div in root.iter('Dividend'):
            try:
                ex_date_str = div.get('exDate') or div.findtext('exDate')
                if not ex_date_str:
                    continue
                ex_date = datetime.strptime(ex_date_str, '%Y-%m-%d').date()

                # Amount can be on attribute or inner text
                amount_str = (
                    div.get('value')
                    or div.findtext('amount')
                    or (div.text.strip() if div.text else None)
                )
                if not amount_str:
                    continue
                amount = Decimal(amount_str)
                if amount == 0:
                    continue

                dt_attr = (div.get('type') or '').lower()
                if 'special' in dt_attr or 'extra' in dt_attr:
                    dividend_type = 'special'
                elif 'stock' in dt_attr:
                    dividend_type = 'stock'
                elif 'liquidat' in dt_attr:
                    dividend_type = 'liquidating'
                else:
                    dividend_type = 'regular'

                pay_date_str = div.get('payDate') or div.findtext('payDate')
                rec_date_str = div.get('recordDate') or div.findtext('recordDate')
                dec_date_str = (
                    div.get('declarationDate')
                    or div.findtext('declarationDate')
                )

                records.append(DividendRecord(
                    ex_date=ex_date,
                    pay_date=(datetime.strptime(pay_date_str, '%Y-%m-%d').date()
                              if pay_date_str else None),
                    declaration_date=(datetime.strptime(dec_date_str, '%Y-%m-%d').date()
                                      if dec_date_str else None),
                    record_date=(datetime.strptime(rec_date_str, '%Y-%m-%d').date()
                                 if rec_date_str else None),
                    amount=amount,
                    currency=div.get('currency') or currency,
                    frequency=div.get('frequency'),
                    dividend_type=dividend_type,
                    source='ib',
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping malformed dividend element: {e}")
                continue

        return records


# ============================================================
# Public dispatcher
# ============================================================
def get_dividends(
    ticker: str,
    db_config: dict,
    user_agent: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exchange: str = 'SMART',
    currency: str = 'USD',
    ib_host: str = '127.0.0.1',
    ib_port: int = 4001,
    save_to_db: bool = True,
    try_ib_fallback: bool = True,
) -> tuple[list[DividendRecord], str]:
    """
    Top-level dispatcher for dividend data.

    Routing:
      1. Resolve ticker to CIK via EDGAR's company tickers list.
      2. If CIK found: try EDGAR.
      3. If EDGAR returns empty and `try_ib_fallback=True`: try IB.
      4. If no CIK: skip EDGAR, go straight to IB.

    On any successful fetch (records > 0) and `save_to_db=True`:
      - Write records to the dividends table.
      - Add the ticker to the 'dividends' watchlist if not already there.
      - Log to fetch_log.

    Returns
    -------
    (records, source_used): the records and the actual source string
        ('edgar', 'ib', 'ib_fallback', or 'none').
    """
    ticker = ticker.upper()
    fetch_start = datetime.now()

    # Open a long-lived DB connection for this fetch
    conn = psycopg2.connect(**db_config) if save_to_db else None

    try:
        # ── Step 1: try EDGAR if CIK exists ──────────────────────
        records: list[DividendRecord] = []
        source_used = 'none'

        edgar_attempted = False
        with EdgarDividendsFetcher(db_config=db_config,
                                   user_agent=user_agent) as edgar:
            info = edgar.resolve_ticker(ticker)
            if info:
                edgar_attempted = True
                logger.info(
                    f"Routing {ticker} to EDGAR (CIK {info['cik']}, "
                    f"{info['name']})"
                )
                records = edgar.get_dividends(
                    ticker, start_date=start_date, end_date=end_date
                )
                if records:
                    source_used = 'edgar'

            edgar.close()

        # ── Step 2: IB fallback ──────────────────────────────────
        if not records and (not edgar_attempted or try_ib_fallback):
            reason = "no CIK found" if not edgar_attempted else "EDGAR empty"
            logger.info(f"Routing {ticker} to IB ({reason})")
            try:
                with IBDividendsFetcher(
                    host=ib_host, port=ib_port, db_config=db_config
                ) as ib:
                    records = ib.get_dividends(
                        ticker,
                        start_date=start_date,
                        end_date=end_date,
                        exchange=exchange,
                        currency=currency,
                    )
                if records:
                    source_used = 'ib_fallback' if edgar_attempted else 'ib'
            except Exception as e:
                logger.error(f"IB fallback failed for {ticker}: {e}")

        # ── Step 3: persist if we have records ───────────────────
        if save_to_db and records and conn is not None:
            stock_id = _get_or_create_stock(conn, ticker, exchange, currency)
            if stock_id:
                _save_dividends_to_db(conn, stock_id, records)
                _add_to_dividends_watchlist(conn, stock_id)
                _log_fetch(
                    conn, stock_id, fetch_start, datetime.now(),
                    len(records), 'success'
                )
        elif save_to_db and not records and conn is not None:
            stock_id = _get_or_create_stock(conn, ticker, exchange, currency)
            if stock_id:
                _log_fetch(
                    conn, stock_id, fetch_start, datetime.now(),
                    0, 'empty', 'No dividend data from any source'
                )

        return records, source_used

    finally:
        if conn is not None and not conn.closed:
            conn.close()


# ============================================================
# Example usage
# ============================================================
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from db_config import get_db_config

    load_dotenv()
    user_agent = os.getenv('email')
    db_config = get_db_config()

    records, source = get_dividends(
        ticker='PFE',
        db_config=db_config,
        user_agent=user_agent,
        start_date='2018-01-01',
        end_date='2024-12-31',
    )

    print(f"\nFetched {len(records)} dividend records via {source}")
    for r in records[:10]:
        print(f"  {r.ex_date}  {r.amount}  {r.dividend_type}  ({r.source})")
