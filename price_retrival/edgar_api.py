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

        # Collect facts from ALL matching tags, not just the first one.
        # Companies sometimes switch XBRL tags mid-history (e.g. LLY
        # switched from ResearchAndDevelopmentExpense to
        # ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost
        # in Q3 2023). We merge data from all tags, with earlier tags
        # in the priority list winning when there's overlap for a period.
        merged_result = {}

        for taxonomy in taxonomies_to_try:
            tax_facts = company_facts.get('facts', {}).get(taxonomy, {})
            for tag in xbrl_tags:
                tag_data = tax_facts.get(tag, {})
                tag_facts = []
                for unit_key, unit_facts in tag_data.get('units', {}).items():
                    tag_facts.extend(unit_facts)

                if not tag_facts:
                    continue

                # Process this tag's facts through the standard pipeline
                tag_result = self._deduplicate_facts(
                    tag_facts, is_instant, start_date, end_date
                )

                # Merge: only add periods not already covered by a
                # higher-priority tag
                for key, value in tag_result.items():
                    if key not in merged_result:
                        merged_result[key] = value

        return merged_result

    def _deduplicate_facts(self, facts, is_instant, start_date, end_date):
        """
        Filter, classify, and deduplicate a list of XBRL facts.

        Returns a dict: {(period_end_str, period_type): value}
        """
        # Filter to 10-K and 10-Q filings only (most reliable)
        valid_forms = {'10-K', '10-K/A', '10-Q', '10-Q/A'}
        filtered = [f for f in facts if f.get('form') in valid_forms]

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
                # For duration facts, classify by period length.
                # EDGAR returns three kinds of duration facts:
                #   ~60-115 days  → true single quarter (Q1/Q2/Q3)
                #   ~150-290 days → cumulative YTD (Q1+Q2, Q1+Q2+Q3, etc.)
                #   ~300-380 days → full fiscal year (FY)
                # We only want single-quarter and FY; skip YTD cumulative.
                # Duration facts MUST have a start date; skip if missing
                # (these are metadata/instant entries that don't represent
                # actual financial period data).
                if not start:
                    continue

                try:
                    start_dt = datetime.strptime(start, '%Y-%m-%d').date()
                    duration_days = (pe_date - start_dt).days
                except (ValueError, TypeError):
                    continue

                if duration_days > 300:
                    period_type = 'FY'
                elif duration_days <= 115:
                    period_type = 'QR'  # true single quarter
                else:
                    # Cumulative YTD (e.g. 6-month or 9-month) — skip
                    continue

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

    def _detect_fy_end_month(self, all_periods):
        """
        Detect the company's fiscal year end month from the FY records.

        Looks at all periods classified as FY and finds the most common
        ending month. Returns 12 (December) as default if no FY records
        are found.
        """
        fy_months = []
        for period_end_str, period_type in all_periods:
            if period_type == 'FY':
                try:
                    pe = datetime.strptime(period_end_str, '%Y-%m-%d').date()
                    fy_months.append(pe.month)
                except (ValueError, TypeError):
                    continue

        if not fy_months:
            return 12  # default to calendar year

        # Return the most common FY end month
        from collections import Counter
        return Counter(fy_months).most_common(1)[0][0]

    def _determine_fiscal_quarter(self, period_end_str, period_type,
                                   fy_end_month=12):
        """
        Determine the fiscal quarter label and fiscal year from the
        period end date, relative to the company's fiscal year end month.

        For a company with FY ending in month M:
          Q1 ends ~3 months after FY end  (M+3)
          Q2 ends ~6 months after FY end  (M+6)
          Q3 ends ~9 months after FY end  (M+9)
          Q4 ends at FY end               (M)

        Examples:
          Calendar FY (Dec): Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec
          Apple FY (Sep):    Q1=Dec, Q2=Mar, Q3=Jun, Q4=Sep

        Returns: ('Q1'|'Q2'|'Q3'|'Q4'|'FY', fiscal_year)
        """
        try:
            pe = datetime.strptime(period_end_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None, None

        if period_type == 'FY':
            return 'FY', pe.year

        month = pe.month

        # Calculate how many months past the FY end this period falls.
        # months_past_fy_end == 0 means same month as FY end (i.e. Q4).
        months_past_fy_end = (month - fy_end_month) % 12

        # Map to fiscal quarter:
        #   1-3 months after FY end → Q1
        #   4-6 months after FY end → Q2
        #   7-9 months after FY end → Q3
        #   10-12 (or 0) months after FY end → Q4
        if months_past_fy_end == 0:
            # Same month as FY end — this is Q4
            # But if it's also tagged as QR (not FY), it's a true Q4
            quarter = 'Q4'
        elif months_past_fy_end <= 3:
            quarter = 'Q1'
        elif months_past_fy_end <= 6:
            quarter = 'Q2'
        elif months_past_fy_end <= 9:
            quarter = 'Q3'
        else:
            quarter = 'Q4'

        # Determine fiscal year: for non-calendar FY companies,
        # quarters after the FY end month belong to the NEXT fiscal year.
        # e.g. Apple FY ends Sep 2024, so Oct-Dec 2024 is FY2025 Q1.
        if fy_end_month != 12 and month > fy_end_month:
            fiscal_year = pe.year + 1
        else:
            fiscal_year = pe.year

        return quarter, fiscal_year

    # ---------------------------------------------------------
    # Q4 Derivation
    # ---------------------------------------------------------
    # Fields where Q4 = FY - (Q1 + Q2 + Q3)
    DURATION_FIELDS = [
        'revenue', 'cost_of_revenue', 'gross_profit',
        'operating_expenses', 'operating_income', 'net_income',
        'research_and_development', 'sga_expense',
        'operating_cash_flow', 'capex', 'free_cash_flow',
    ]

    # Fields where Q4 value = FY value (point-in-time snapshots)
    INSTANT_FIELDS = [
        'total_assets', 'total_liabilities', 'total_equity',
        'cash_and_equivalents', 'total_debt', 'shares_outstanding',
    ]

    def _derive_q4_records(self, records):
        """
        Derive Q4 records from FY - (Q1 + Q2 + Q3) for each fiscal year.

        Many companies don't file a separate 10-Q for Q4 — they go
        straight to the 10-K. This means quarterly XBRL data has Q1-Q3
        but Q4 is missing. We can derive it by subtraction.

        For duration fields (income statement, cash flow):
            Q4 = FY - (Q1 + Q2 + Q3)
        For instant fields (balance sheet):
            Q4 = FY snapshot (same point in time)
        For EPS:
            Derived from Q4 net income / Q4 shares outstanding
        """
        # Group records by fiscal year
        by_year = {}
        for r in records:
            fy = r['fiscal_year']
            pt = r['period_type']
            by_year.setdefault(fy, {})[pt] = r

        derived = []
        for fy, periods in by_year.items():
            # Only derive if we have FY and Q4 is missing or empty
            if 'FY' not in periods:
                continue

            if 'Q4' in periods:
                # Check if the existing Q4 has any duration data populated.
                # If it's all None (e.g. only balance sheet instant facts
                # from a 10-K with the same period_end), treat it as empty
                # and derive the duration fields.
                existing_q4 = periods['Q4']
                has_duration_data = any(
                    existing_q4.get(f) is not None
                    for f in self.DURATION_FIELDS
                )
                if has_duration_data:
                    continue  # Q4 already has real data, skip derivation

            # Need at least Q1-Q3 to derive Q4
            quarters_present = [q for q in ('Q1', 'Q2', 'Q3') if q in periods]
            if len(quarters_present) < 3:
                self.logger.debug(
                    f"FY {fy}: cannot derive Q4 — only have "
                    f"{quarters_present} (need Q1, Q2, Q3)"
                )
                continue

            fy_rec = periods['FY']
            q1, q2, q3 = periods['Q1'], periods['Q2'], periods['Q3']

            q4 = {
                'period_end': fy_rec['period_end'],  # Q4 ends same day as FY
                'period_type': 'Q4',
                'fiscal_year': fy,
            }

            # Duration fields: Q4 = FY - (Q1 + Q2 + Q3)
            for field in self.DURATION_FIELDS:
                fy_val = fy_rec.get(field)
                q1_val = q1.get(field)
                q2_val = q2.get(field)
                q3_val = q3.get(field)

                if all(v is not None for v in (fy_val, q1_val, q2_val, q3_val)):
                    q4[field] = fy_val - (q1_val + q2_val + q3_val)
                else:
                    q4[field] = None

            # Instant fields: copy from FY (same point-in-time snapshot)
            for field in self.INSTANT_FIELDS:
                q4[field] = fy_rec.get(field)

            # EPS: derive from Q4 net income / shares rather than
            # subtracting per-share figures (which don't subtract cleanly
            # due to share count changes across quarters)
            q4_ni = q4.get('net_income')
            q4_shares = q4.get('shares_outstanding')
            if q4_ni is not None and q4_shares and q4_shares != 0:
                q4['eps_basic'] = round(q4_ni / q4_shares, 4)
                q4['eps_diluted'] = round(q4_ni / q4_shares, 4)
            else:
                # Fall back to subtraction if we can't compute from shares
                for eps_field in ('eps_basic', 'eps_diluted'):
                    fy_val = fy_rec.get(eps_field)
                    q1_val = q1.get(eps_field)
                    q2_val = q2.get(eps_field)
                    q3_val = q3.get(eps_field)
                    if all(v is not None for v in (fy_val, q1_val, q2_val, q3_val)):
                        q4[eps_field] = round(
                            fy_val - (q1_val + q2_val + q3_val), 4
                        )
                    else:
                        q4[eps_field] = None

            # Compute ratios on the derived Q4 record
            q4.update(self._compute_ratios(q4))

            self.logger.info(
                f"Derived Q4 {fy} from FY - (Q1+Q2+Q3) "
                f"(period_end: {q4['period_end']})"
            )
            derived.append(q4)

        return derived

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

        # Collect periods that have data from duration fields (income
        # statement, cash flow). These are reliable because duration
        # facts require a start date, which we enforce in
        # _deduplicate_facts — so filing-date ghost entries can't
        # sneak through. We use this to validate quarterly periods.
        duration_columns = [
            col for col in XBRL_TAG_MAP if col not in INSTANT_TAGS
        ]
        periods_with_duration_data = {
            key for col in duration_columns
            for key in extracted[col].keys()
        }

        # Filter: only keep periods that have at least one duration field
        # populated. Duration facts require a start date (enforced in
        # _deduplicate_facts), so filing-date ghost entries can't sneak
        # through. This eliminates all ghost records regardless of whether
        # they're QR or FY classified.
        all_periods = {
            (pe, pt) for pe, pt in all_periods
            if (pe, pt) in periods_with_duration_data
        }

        # Also filter out QR records that share period_end with an FY
        # record — these are balance-sheet-only artefacts from 10-K filings.
        fy_period_ends = {
            pe for pe, pt in all_periods if pt == 'FY'
        }
        all_periods = {
            (pe, pt) for pe, pt in all_periods
            if not (pt == 'QR' and pe in fy_period_ends)
        }

        if not all_periods:
            self.logger.warning(f"No fundamental data found for {ticker} in date range")
            return None

        # Detect the company's fiscal year end month
        fy_end_month = self._detect_fy_end_month(all_periods)
        if fy_end_month != 12:
            self.logger.info(
                f"{ticker} has non-calendar fiscal year "
                f"(FY ends in month {fy_end_month})"
            )

        # Build records: one dict per period
        records = []
        data_fields = list(XBRL_TAG_MAP.keys())  # all mappable fields

        for period_end_str, raw_period_type in sorted(all_periods):
            period_type, fiscal_year = self._determine_fiscal_quarter(
                period_end_str, raw_period_type, fy_end_month
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

            # Skip records where every data field is None — these are
            # ghost entries from filing-date instant facts or other
            # metadata that don't represent real financial periods.
            if not any(record.get(f) is not None for f in data_fields):
                continue

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

        # Derive Q4 records where missing (FY - Q1 - Q2 - Q3)
        derived_q4 = self._derive_q4_records(records)
        if derived_q4:
            # Remove any empty Q4 records that the derivation is replacing
            derived_keys = {(r['fiscal_year'], r['period_type']) for r in derived_q4}
            records = [
                r for r in records
                if (r['fiscal_year'], r['period_type']) not in derived_keys
            ]
            records.extend(derived_q4)
            # Re-sort so Q4 sits in the right chronological position
            records.sort(key=lambda r: (r['period_end'], r['period_type']))
            self.logger.info(
                f"Derived {len(derived_q4)} Q4 record(s) for {ticker}"
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
