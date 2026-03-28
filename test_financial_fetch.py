"""
Test: validate financial-sector XBRL fetcher against JPM, BRK, GS, BLK.

Run from the rosmerta project root:
    uv run test_financial_fetch.py

Tests without saving to DB (save_to_db=False) so it's safe to run
before or after migration. Prints a summary table showing which
fields are populated for each company.
"""

from price_retrival.edgar_api import EdgarFundamentalsFetcher

# Financial sector fields we expect to see populated
FINANCIAL_FIELDS = [
    'net_interest_income', 'noninterest_income', 'noninterest_expense',
    'provision_for_credit_losses', 'premiums_earned', 'investment_income',
]

# Standard fields to check coverage
STANDARD_FIELDS = [
    'revenue', 'cost_of_revenue', 'gross_profit',
    'operating_expenses', 'operating_income', 'net_income',
    'eps_basic', 'eps_diluted',
    'total_assets', 'total_equity', 'total_debt',
    'operating_cash_flow', 'shares_outstanding',
]

# Ratio fields
RATIO_FIELDS = ['gross_margin', 'operating_margin', 'net_margin', 'roe']

TICKERS = ['JPM', 'BRK-B', 'GS', 'BLK']

DATE_START = '2023-01-01'
DATE_END = '2025-12-31'


def test_ticker(fetcher, ticker):
    """Fetch and summarise results for one ticker."""
    print(f"\n{'='*70}")
    print(f"  {ticker}")
    print(f"{'='*70}")

    records = fetcher.get_fundamentals(
        ticker, start_date=DATE_START, end_date=DATE_END,
        save_to_db=False,
    )

    if not records:
        print(f"  !! No records returned for {ticker}")
        # Try alternate BRK tickers
        if ticker.startswith('BRK'):
            for alt in ['BRK.B', 'BRK-A', 'BRK.A']:
                print(f"  Trying alternate ticker: {alt}")
                records = fetcher.get_fundamentals(
                    alt, start_date=DATE_START, end_date=DATE_END,
                    save_to_db=False,
                )
                if records:
                    ticker = alt
                    break
        if not records:
            return

    # Count period types
    types = {}
    for r in records:
        pt = r['period_type']
        types[pt] = types.get(pt, 0) + 1
    print(f"  Periods: {types}")

    # Show the most recent quarterly record
    quarterly = [r for r in records if r['period_type'] in ('Q1', 'Q2', 'Q3', 'Q4')]
    latest = quarterly[-1] if quarterly else (records[-1] if records else None)
    if not latest:
        print("  No quarterly records found")
        return

    print(f"\n  Latest quarter: {latest['period_type']} FY{latest['fiscal_year']} "
          f"(ending {latest['period_end']})")

    # Standard fields
    print(f"\n  Standard fields:")
    for field in STANDARD_FIELDS:
        val = latest.get(field)
        if val is not None:
            if abs(val) >= 1_000_000:
                print(f"    {field:<30} {val:>18,.0f}")
            else:
                print(f"    {field:<30} {val:>18,.4f}")
        else:
            print(f"    {field:<30} {'NULL':>18}")

    # Financial sector fields
    print(f"\n  Financial sector fields:")
    any_financial = False
    for field in FINANCIAL_FIELDS:
        val = latest.get(field)
        if val is not None:
            any_financial = True
            print(f"    {field:<30} {val:>18,.0f}")
        else:
            print(f"    {field:<30} {'NULL':>18}")

    if not any_financial:
        print(f"    (none populated — expected for non-financial companies)")

    # Ratios
    print(f"\n  Computed ratios:")
    for field in RATIO_FIELDS:
        val = latest.get(field)
        if val is not None:
            print(f"    {field:<30} {val:>18.4f}")
        else:
            print(f"    {field:<30} {'NULL':>18}")

    # Q4 derivation check
    q4s = [r for r in records if r['period_type'] == 'Q4']
    if q4s:
        print(f"\n  Q4 derivation: {len(q4s)} Q4 record(s) present")
    else:
        print(f"\n  Q4 derivation: no Q4 records (check if FY + Q1-Q3 exist)")


def main():
    print("Financial Sector EDGAR Fetcher — Validation Test")
    print("=" * 70)
    print(f"Date range: {DATE_START} → {DATE_END}")
    print(f"Tickers: {', '.join(TICKERS)}")

    # No DB needed for this test
    fetcher = EdgarFundamentalsFetcher(db_config=None)

    for ticker in TICKERS:
        try:
            test_ticker(fetcher, ticker)
        except Exception as e:
            print(f"\n  !! ERROR for {ticker}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*70}")
    print("Validation complete.")
    print("\nExpected results:")
    print("  JPM: revenue=NULL, net_interest_income + noninterest_income populated")
    print("  GS:  revenue=NULL, net_interest_income + noninterest_income populated")
    print("  BRK: revenue populated, premiums/investment_income may populate")
    print("  BLK: revenue populated, investment_income populated")


if __name__ == '__main__':
    main()
