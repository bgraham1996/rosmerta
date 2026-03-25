"""
EDGAR Fundamentals Fetcher — SEC XBRL API with PostgreSQL Integration
=====================================================================
Fetches fundamental financial data from SEC EDGAR's free XBRL API
and stores it in the PostgreSQL database.

Mirrors the connection and DB patterns from IB_stocks.py so that
both fetchers share the same db_config dict from main.py.

SEC EDGAR API:
  - No API key required
  - Requires User-Agent header with name + email
  - Rate limit: 10 requests/second
  - Data source: XBRL companyfacts endpoint
"""

import os
import json
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date
from pathlib import Path


# =============================================================
# XBRL Tag → DB Column Mapping
# =============================================================
# SEC EDGAR uses US-GAAP XBRL taxonomy tags. Companies may use
# different tags for the same concept (e.g. Revenue has several
# variants). We try each tag in order and take the first match.

XBRL_TAG_MAP = {
    # --- Income Statement ---
    'revenue': [
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'RevenueFromContractWithCustomerIncludingAssessedTax',
        'Revenues',
        'SalesRevenueNet',
        'SalesRevenueGoodsNet',
        'SalesRevenueServicesNet',
    ],
    'cost_of_revenue': [
        'CostOfGoodsAndServicesSold',
        'CostOfGoodsSold',
        'CostOfRevenue',
        'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization',
    ],
    'gross_profit': [
        'GrossProfit',
    ],
    'operating_expenses': [
        'OperatingExpenses',
        'CostsAndExpenses',
    ],
    'operating_income': [
        'OperatingIncomeLoss',
    ],
    'net_income': [
        'NetIncomeLoss',
        'NetIncomeLossAvailableToCommonStockholdersBasic',
        'ProfitLoss',
    ],
    'eps_basic': [
        'EarningsPerShareBasic',
    ],
    'eps_diluted': [
        'EarningsPerShareDiluted',
    ],
    'research_and_development': [
        'ResearchAndDevelopmentExpense',
        'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost',
    ],
    'sga_expense': [
        'SellingGeneralAndAdministrativeExpense',
        'GeneralAndAdministrativeExpense',
    ],

    # --- Balance Sheet ---
    'total_assets': [
        'Assets',
    ],
    'total_liabilities': [
        'Liabilities',
        'LiabilitiesAndStockholdersEquity',  # fallback: subtract equity
    ],
    'total_equity': [
        'StockholdersEquity',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    ],
    'cash_and_equivalents': [
        'CashAndCashEquivalentsAtCarryingValue',
        'CashCashEquivalentsAndShortTermInvestments',
        'Cash',
    ],
    'total_debt': [
        'LongTermDebt',
        'LongTermDebtAndCapitalLeaseObligations',
        'LongTermDebtNoncurrent',
    ],

    # --- Cash Flow ---
    'operating_cash_flow': [
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
    ],
    'capex': [
        'PaymentsToAcquirePropertyPlantAndEquipment',
        'PaymentsToAcquireProductiveAssets',
    ],
    'free_cash_flow': [
        # Rarely reported directly; we compute it if missing
    ],

    # --- Shares ---
    'shares_outstanding': [
        'CommonStockSharesOutstanding',
        'EntityCommonStockSharesOutstanding',
        'WeightedAverageNumberOfShareOutstandingBasicAndDiluted',
        'WeightedAverageNumberOfDilutedSharesOutstanding',
    ],
}

# Tags that represent balance sheet (point-in-time / "instant") values
# vs income statement / cash flow (duration) values.
INSTANT_TAGS = {
    'total_assets', 'total_liabilities', 'total_equity',
    'cash_and_equivalents', 'total_debt', 'shares_outstanding',
}


class EdgarFundamentalsFetcher:
    """
    Fetches fundamental financial data from SEC EDGAR's XBRL API
    and stores it in PostgreSQL.

    Uses the same db_config pattern as IBStockDataFetcher so both
    can share configuration from a single main.py.
    """

    # SEC requires max 10 req/sec; we'll be conservative
    REQUEST_DELAY = 0.15  # seconds between requests

    # Cache directory for CIK mapping file
    CACHE_DIR = Path.home() / '.stock_cli' / 'cache'

    def __init__(self, db_config=None, user_agent=None):
        """
        Args:
            db_config:   Dict with PostgreSQL connection params, e.g.:
                         {'host': '10.0.0.1', 'port': 5432,
                          'dbname': 'stocks', 'user': 'stock_user',
                          'password': '...'}
                         If None, database features are disabled.
            user_agent:  SEC-required User-Agent string.
                         If None, reads from EDGAR_USER_AGENT env var.
                         Format: "Name email@example.com"
        """
        self.db_config = db_config
        self.db_conn = None

        # SEC User-Agent
        self.user_agent = user_agent or os.environ.get('EDGAR_USER_AGENT')
        if not self.user_agent:
            raise ValueError(
                "SEC EDGAR requires a User-Agent header. "
                "Set EDGAR_USER_AGENT env var or pass user_agent= "
                "Format: 'Your Name your@email.com'"
            )

        # HTTP session with rate limiting
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept-Encoding': 'gzip, deflate',
        })
        self._last_request_time = 0

        # CIK mapping cache
        self._ticker_to_cik = None

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    # ---------------------------------------------------------
    # Rate-limited request
    # ---------------------------------------------------------
    def _throttled_get(self, url):
        """
        GET request with rate limiting to respect SEC's 10 req/sec limit.
        """
        elapsed = time.time() - self._last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)

        self.logger.debug(f"GET {url}")
        response = self.session.get(url)
        self._last_request_time = time.time()

        response.raise_for_status()
        return response

    # ---------------------------------------------------------
    # Database Connection (matches IB_stocks.py pattern)
    # ---------------------------------------------------------
    def connect_db(self):
        """Establish PostgreSQL connection. Returns True on success."""
        if not self.db_config:
            self.logger.warning("No db_config provided — database features disabled")
            return False
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            self.db_conn.autocommit = False
            self.logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def disconnect(self):
        """Close DB connection and HTTP session."""
        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
                self.logger.info("Closed DB connection")
        except Exception as e:
            self.logger.error(f"Error closing DB connection: {e}")

        try:
            self.session.close()
        except Exception:
            pass

    # ---------------------------------------------------------
    # Context Manager (matches IB_stocks.py pattern)
    # ---------------------------------------------------------
    def __enter__(self):
        if self.db_config:
            self.connect_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    # ---------------------------------------------------------
    # Ticker → CIK Resolution
    # ---------------------------------------------------------
    def _load_ticker_map(self, force_refresh=False):
        """
        Load the SEC ticker→CIK mapping from cache or download it.
        The file at sec.gov/files/company_tickers.json is small (~1MB)
        and changes infrequently.
        """
        if self._ticker_to_cik and not force_refresh:
            return

        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = self.CACHE_DIR / 'company_tickers.json'

        # Use cache if less than 24 hours old
        if (cache_file.exists() and not force_refresh
                and (time.time() - cache_file.stat().st_mtime) < 86400):
            with open(cache_file, 'r') as f:
                data = json.load(f)
        else:
            self.logger.info("Downloading SEC company tickers mapping...")
            resp = self._throttled_get(
                'https://www.sec.gov/files/company_tickers.json'
            )
            data = resp.json()
            with open(cache_file, 'w') as f:
                json.dump(data, f)
            self.logger.info(f"Cached {len(data)} company records")

        # Build lookup: ticker (upper) -> {cik, name}
        self._ticker_to_cik = {}
        for entry in data.values():
            ticker = entry['ticker'].upper()
            self._ticker_to_cik[ticker] = {
                'cik': entry['cik_str'],
                'name': entry['title'],
            }

    def resolve_ticker(self, ticker):
        """
        Resolve a ticker symbol to CIK and company name.

        Returns:
            dict with 'cik' (int), 'cik_padded' (str, 10-digit),
            and 'name' (str). Or None if not found.
        """
        self._load_ticker_map()
        ticker = ticker.upper()
        info = self._ticker_to_cik.get(ticker)
        if not info:
            return None

        cik = int(info['cik'])
        return {
            'cik': cik,
            'cik_padded': str(cik).zfill(10),
            'name': info['name'],
        }

    # ---------------------------------------------------------
    # Stock lookup / auto-create (matches IB_stocks.py pattern)
    # ---------------------------------------------------------
    def _get_or_create_stock(self, symbol, company_name):
        """
        Look up a stock by symbol. If it doesn't exist,
        insert a minimal record with source='edgar'.
        Returns the stock_id.
        """
        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return None

        with self.db_conn.cursor() as cur:
            # First try exact match on symbol (any exchange)
            cur.execute(
                "SELECT stock_id FROM stocks WHERE symbol = %s LIMIT 1",
                (symbol,)
            )
            row = cur.fetchone()
            if row:
                return row[0]

            # Auto-create with source='edgar' flag
            cur.execute(
                """INSERT INTO stocks (symbol, name, source)
                   VALUES (%s, %s, %s) RETURNING stock_id""",
                (symbol, company_name, 'edgar')
            )
            stock_id = cur.fetchone()[0]
            self.db_conn.commit()
            self.logger.info(
                f"Created stock record from EDGAR: {symbol} "
                f"({company_name}) -> id {stock_id}"
            )
            return stock_id

    # ---------------------------------------------------------
    # Fetch Log (matches IB_stocks.py pattern)
    # ---------------------------------------------------------
    def _log_fetch(self, stock_id, data_type, fetch_start, fetch_end,
                   records_fetched, status='success', error_message=None):
        """Record a fetch operation in the fetch_log table."""
        if not self.db_conn or self.db_conn.closed:
            return

        try:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO fetch_log
                       (stock_id, data_type, fetch_start, fetch_end,
                        bars_fetched, status, error_message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (stock_id, data_type, fetch_start, fetch_end,
                     records_fetched, status, error_message)
                )
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging fetch: {e}")
            self.db_conn.rollback()

    # ---------------------------------------------------------
    # Fetch Company Facts from EDGAR
    # ---------------------------------------------------------
    def _fetch_company_facts(self, cik_padded):
        """
        Fetch the full companyfacts JSON from SEC EDGAR.

        Returns the parsed JSON dict, or None on error.
        """
        url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json'
        try:
            resp = self._throttled_get(url)
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.error(
                    f"No XBRL data found for CIK {cik_padded}. "
                    "Company may not have XBRL filings."
                )
            else:
                self.logger.error(f"HTTP error fetching company facts: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching company facts: {e}")
            return None

    # ---------------------------------------------------------
    # Extract & Deduplicate Facts
    # ---------------------------------------------------------
    def _extract_facts(self, company_facts, db_column, start_date, end_date):
        """
        For a given DB column, try each XBRL tag in priority order.
        Extract facts, deduplicate, and filter by date range.

        Returns a dict: {(period_end, period_type): value}
        where period_end is a date string and period_type is
        'Q1','Q2','Q3','Q4', or 'FY'.
        """
        xbrl_tags = XBRL_TAG_MAP.get(db_column, [])
        if not xbrl_tags:
            return {}

        is_instant = db_column in INSTANT_TAGS

        # Try us-gaap first, then dei (for shares outstanding)
        taxonomies_to_try = ['us-gaap', 'dei']

        all_facts = []
        for taxonomy in taxonomies_to_try:
            tax_facts = company_facts.get('facts', {}).get(taxonomy, {})
            for tag in xbrl_tags:
                tag_data = tax_facts.get(tag, {})
                # Units could be 'USD', 'USD/shares', 'shares', 'pure'
                for unit_key, unit_facts in tag_data.get('units', {}).items():
                    all_facts.extend(unit_facts)

                if all_facts:
                    break  # Found data for this priority tag
            if all_facts:
                break  # Found data in this taxonomy

        if not all_facts:
            return {}

        # Filter to 10-K and 10-Q filings only (most reliable)
        valid_forms = {'10-K', '10-K/A', '10-Q', '10-Q/A'}
        filtered = [f for f in all_facts if f.get('form') in valid_forms]

        # Deduplicate: for each unique period, keep the most recently
        # filed value. EDGAR returns duplicates because the same figure
        # appears in multiple filings (e.g. Q1 revenue restated in 10-K).
        period_facts = {}
        for fact in filtered:
            period_end = fact.get('end')
            if not period_end:
                # Instant facts use 'end' for the point-in-time date
                continue

            # Apply date range filter
            try:
                pe_date = datetime.strptime(period_end, '%Y-%m-%d').date()
            except (ValueError, TypeError):
                continue

            if start_date and pe_date < start_date:
                continue
            if end_date and pe_date > end_date:
                continue

            # Determine period type from form and duration
            form = fact.get('form', '')
            start = fact.get('start')  # None for instant facts

            if is_instant:
                # For balance sheet items, determine period from filing form
                period_type = 'FY' if '10-K' in form else 'QR'
            else:
                # For duration facts, calculate approximate quarter
                if start:
                    try:
                        start_dt = datetime.strptime(start, '%Y-%m-%d').date()
                        duration_days = (pe_date - start_dt).days
                    except (ValueError, TypeError):
                        duration_days = 0

                    if duration_days > 300:
                        period_type = 'FY'
                    else:
                        period_type = 'QR'  # quarterly
                else:
                    period_type = 'FY' if '10-K' in form else 'QR'

            # Deduplicate: keep the fact with the latest filing date
            key = (period_end, period_type)
            filed = fact.get('filed', '')
            existing = period_facts.get(key)
            if not existing or filed > existing.get('filed', ''):
                period_facts[key] = {
                    'value': fact.get('val'),
                    'filed': filed,
                    'form': form,
                    'period_end': period_end,
                    'period_type': period_type,
                }

        # Return clean mapping
        return {
            key: entry['value']
            for key, entry in period_facts.items()
            if entry['value'] is not None
        }

    def _determine_fiscal_quarter(self, period_end_str, period_type):
        """
        Determine the fiscal quarter label (Q1-Q4 or FY) from the
        period end date. Uses calendar quarter as approximation.

        Returns: ('Q1'|'Q2'|'Q3'|'Q4'|'FY', fiscal_year)
        """
        try:
            pe = datetime.strptime(period_end_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None, None

        fiscal_year = pe.year

        if period_type == 'FY':
            return 'FY', fiscal_year

        # Map calendar month to approximate quarter
        month = pe.month
        if month <= 3:
            return 'Q1', fiscal_year
        elif month <= 6:
            return 'Q2', fiscal_year
        elif month <= 9:
            return 'Q3', fiscal_year
        else:
            return 'Q4', fiscal_year

    # ---------------------------------------------------------
    # Main Fetch Method
    # ---------------------------------------------------------
    def get_fundamentals(self, ticker, start_date=None, end_date=None,
                         save_to_db=True):
        """
        Fetch fundamental data for a stock from SEC EDGAR.

        Args:
            ticker:      Stock ticker symbol (e.g. 'AAPL', 'PFE').
            start_date:  Start date as 'YYYY-MM-DD' string or date.
                         If None, fetches all available data.
            end_date:    End date as 'YYYY-MM-DD' string or date.
                         If None, fetches up to present.
            save_to_db:  If True and db_config was provided, save
                         directly to PostgreSQL.

        Returns:
            List of dicts (one per period), or None on error.
        """
        # Parse dates
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        # Resolve ticker → CIK
        company_info = self.resolve_ticker(ticker)
        if not company_info:
            self.logger.error(
                f"Ticker '{ticker}' not found in SEC company list. "
                "Check the symbol or try the full company name."
            )
            return None

        self.logger.info(
            f"Resolved {ticker} → CIK {company_info['cik']} "
            f"({company_info['name']})"
        )

        # Fetch company facts
        facts_json = self._fetch_company_facts(company_info['cik_padded'])
        if not facts_json:
            return None

        # Extract each field
        extracted = {}
        for db_column in XBRL_TAG_MAP:
            extracted[db_column] = self._extract_facts(
                facts_json, db_column, start_date, end_date
            )

        # Collect all unique (period_end, period_type) keys across all fields
        all_periods = set()
        for field_data in extracted.values():
            all_periods.update(field_data.keys())

        if not all_periods:
            self.logger.warning(f"No fundamental data found for {ticker} in date range")
            return None

        # Build records: one dict per period
        records = []
        for period_end_str, raw_period_type in sorted(all_periods):
            period_type, fiscal_year = self._determine_fiscal_quarter(
                period_end_str, raw_period_type
            )
            if not period_type:
                continue

            record = {
                'period_end': period_end_str,
                'period_type': period_type,
                'fiscal_year': fiscal_year,
            }

            # Fill in each field
            for db_column in XBRL_TAG_MAP:
                key = (period_end_str, raw_period_type)
                record[db_column] = extracted[db_column].get(key)

            # Compute free_cash_flow if not directly available
            if record.get('free_cash_flow') is None:
                ocf = record.get('operating_cash_flow')
                capex = record.get('capex')
                if ocf is not None and capex is not None:
                    record['free_cash_flow'] = ocf - capex

            # Compute ratios
            record.update(self._compute_ratios(record))

            records.append(record)

        self.logger.info(
            f"Extracted {len(records)} periods for {ticker} "
            f"({company_info['name']})"
        )

        # Save to DB
        if save_to_db and self.db_config:
            stock_id = self._get_or_create_stock(
                ticker.upper(), company_info['name']
            )
            if stock_id:
                saved = self._save_fundamentals_to_db(stock_id, records)
                if saved:
                    self._log_fetch(
                        stock_id, 'fundamentals',
                        start_date or date(2000, 1, 1),
                        end_date or date.today(),
                        len(records), 'success'
                    )
                else:
                    self._log_fetch(
                        stock_id, 'fundamentals',
                        start_date or date(2000, 1, 1),
                        end_date or date.today(),
                        0, 'error', 'Failed to save to database'
                    )

        return records

    # ---------------------------------------------------------
    # Compute Ratios
    # ---------------------------------------------------------
    def _compute_ratios(self, record):
        """
        Compute derived ratios from raw fundamentals.
        Returns a dict of ratio values (or None if inputs missing).
        """
        ratios = {}

        revenue = record.get('revenue')
        gross_profit = record.get('gross_profit')
        operating_income = record.get('operating_income')
        net_income = record.get('net_income')
        total_equity = record.get('total_equity')
        total_debt = record.get('total_debt')
        eps_diluted = record.get('eps_diluted')

        # Gross margin
        if revenue and gross_profit and revenue != 0:
            ratios['gross_margin'] = round(gross_profit / revenue, 4)
        else:
            ratios['gross_margin'] = None

        # Operating margin
        if revenue and operating_income and revenue != 0:
            ratios['operating_margin'] = round(operating_income / revenue, 4)
        else:
            ratios['operating_margin'] = None

        # Net margin
        if revenue and net_income and revenue != 0:
            ratios['net_margin'] = round(net_income / revenue, 4)
        else:
            ratios['net_margin'] = None

        # Return on equity
        if net_income and total_equity and total_equity != 0:
            ratios['roe'] = round(net_income / total_equity, 4)
        else:
            ratios['roe'] = None

        # Debt to equity
        if total_debt and total_equity and total_equity != 0:
            ratios['debt_to_equity'] = round(total_debt / total_equity, 4)
        else:
            ratios['debt_to_equity'] = None

        # P/E ratio — requires a current stock price, which we don't
        # have in this context. Leave as None; can be computed later
        # by joining with price data.
        ratios['pe_ratio'] = None

        return ratios

    # ---------------------------------------------------------
    # Save to Database
    # ---------------------------------------------------------
    def _save_fundamentals_to_db(self, stock_id, records):
        """
        Upsert fundamental records into the fundamentals table.
        Uses ON CONFLICT to handle re-fetches gracefully.
        """
        if not self.db_conn or self.db_conn.closed:
            if not self.connect_db():
                return False

        columns = [
            'stock_id', 'period_end', 'period_type', 'fiscal_year',
            'revenue', 'cost_of_revenue', 'gross_profit',
            'operating_expenses', 'operating_income', 'net_income',
            'eps_basic', 'eps_diluted',
            'research_and_development', 'sga_expense',
            'total_assets', 'total_liabilities', 'total_equity',
            'cash_and_equivalents', 'total_debt',
            'operating_cash_flow', 'capex', 'free_cash_flow',
            'pe_ratio', 'debt_to_equity', 'gross_margin',
            'operating_margin', 'net_margin', 'roe',
            'shares_outstanding',
            'source',
        ]

        # Build tuples for execute_values
        rows = []
        for r in records:
            row = (
                stock_id,
                r['period_end'],
                r['period_type'],
                r['fiscal_year'],
                r.get('revenue'),
                r.get('cost_of_revenue'),
                r.get('gross_profit'),
                r.get('operating_expenses'),
                r.get('operating_income'),
                r.get('net_income'),
                r.get('eps_basic'),
                r.get('eps_diluted'),
                r.get('research_and_development'),
                r.get('sga_expense'),
                r.get('total_assets'),
                r.get('total_liabilities'),
                r.get('total_equity'),
                r.get('cash_and_equivalents'),
                r.get('total_debt'),
                r.get('operating_cash_flow'),
                r.get('capex'),
                r.get('free_cash_flow'),
                r.get('pe_ratio'),
                r.get('debt_to_equity'),
                r.get('gross_margin'),
                r.get('operating_margin'),
                r.get('net_margin'),
                r.get('roe'),
                r.get('shares_outstanding'),
                'edgar',
            )
            rows.append(row)

        # Build the upsert SQL
        col_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # On conflict, update all data columns (not the key columns)
        update_cols = [c for c in columns
                       if c not in ('stock_id', 'period_end', 'period_type')]
        update_clause = ', '.join(
            f"{c} = EXCLUDED.{c}" for c in update_cols
        )

        insert_sql = f"""
            INSERT INTO fundamentals ({col_names})
            VALUES %s
            ON CONFLICT (stock_id, period_end, period_type)
            DO UPDATE SET {update_clause}
        """

        try:
            with self.db_conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
            self.db_conn.commit()
            self.logger.info(
                f"Saved {len(rows)} fundamental records for stock_id={stock_id}"
            )
            return True
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"Error saving fundamentals to DB: {e}")
            return False


# =============================================================
# Convenience function
# =============================================================
def fetch_fundamentals(ticker, start_date=None, end_date=None,
                       db_config=None, user_agent=None):
    """
    Quick function to fetch fundamentals without managing the class.

    Args:
        ticker:      Stock ticker symbol
        start_date:  'YYYY-MM-DD' or None
        end_date:    'YYYY-MM-DD' or None
        db_config:   PostgreSQL connection dict, or None to skip DB
        user_agent:  SEC User-Agent string, or reads EDGAR_USER_AGENT env

    Returns:
        List of dicts or None
    """
    with EdgarFundamentalsFetcher(
        db_config=db_config, user_agent=user_agent
    ) as fetcher:
        return fetcher.get_fundamentals(
            ticker, start_date, end_date
        )


# =============================================================
# Example usage
# =============================================================
if __name__ == "__main__":
    # PostgreSQL on the Ubuntu server
    from db_config import get_db_config
    DB_CONFIG = get_db_config()

    # SEC requires User-Agent — set via env:
    #   export EDGAR_USER_AGENT="Your Name your@email.com"

    with EdgarFundamentalsFetcher(db_config=DB_CONFIG) as fetcher:
        records = fetcher.get_fundamentals(
            ticker='PFE',
            start_date='2020-01-01',
            end_date='2024-12-31',
        )

        if records:
            print(f"\nFetched {len(records)} periods")
            for r in records[:3]:
                print(f"\n  {r['period_type']} ending {r['period_end']}:")
                print(f"    Revenue:  {r.get('revenue'):>15,}" if r.get('revenue') else "    Revenue:  N/A")
                print(f"    R&D:      {r.get('research_and_development'):>15,}" if r.get('research_and_development') else "    R&D:      N/A")
                print(f"    Net Inc:  {r.get('net_income'):>15,}" if r.get('net_income') else "    Net Inc:  N/A")
