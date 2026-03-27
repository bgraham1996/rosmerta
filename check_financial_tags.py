"""
Diagnostic: inspect which XBRL tags financial sector companies use.
Targets: JPM (bank), BRK (insurer/conglomerate), GS (investment bank), BLK (asset manager)

Run from the rosmerta project directory:
    python check_financial_tags.py

This dumps all us-gaap tags each company uses that relate to:
  - Revenue / income (interest, non-interest, premiums, fees)
  - Expenses (provisions, claims, compensation)
  - Standard fields we already map (to see what's populated vs blank)

Output is a text report showing tag name, fact count, date range,
and filing forms — so we can see exactly which tags to map.
"""

import requests
import json
import sys
from collections import Counter

# CIK numbers for our target companies
COMPANIES = {
    'JPM':  {'cik': '0000019617',  'name': 'JPMorgan Chase'},
    'BRK':  {'cik': '0001067983',  'name': 'Berkshire Hathaway'},
    'GS':   {'cik': '0000886982',  'name': 'Goldman Sachs'},
    'BLK':  {'cik': '0001364742',  'name': 'BlackRock'},
}

# Keywords to search for in tag names (case-insensitive)
# These cover the financial-sector-specific concepts we care about
KEYWORDS = [
    # Revenue / Income
    'interest', 'revenue', 'sales', 'premium', 'fee',
    'noninterest', 'commission', 'trading', 'advisory',
    'investmentincome', 'investment',
    # Expenses
    'provision', 'creditloss', 'loanloss', 'allowance',
    'compensation', 'benefit', 'claim', 'policyholder',
    'costofrevenue', 'costofgoods',
    # Standard fields (check coverage)
    'operatingincome', 'operatingexpense',
    'netincome', 'profitloss',
    'earningspershare',
    'assets', 'liabilities', 'equity',
    'cashandcash', 'debt',
    'operatingactivities', 'cashprovided',
    'capex', 'paymentstoacquire',
    'sharesoutstanding', 'weightedaverage',
    # Gross profit (likely absent for banks)
    'grossprofit',
    # SGA / R&D (likely absent for banks)
    'sellinggeneral', 'researchanddevelopment',
    # Other financial-sector concepts
    'bookvalue', 'tangiblebookvalue',
    'returnon', 'netinterest',
    'assetmanagement', 'assetsundermanagement',
]

# Tags we currently map (to check if they have data)
CURRENT_MAPPED_TAGS = [
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'RevenueFromContractWithCustomerIncludingAssessedTax',
    'Revenues',
    'SalesRevenueNet',
    'CostOfGoodsAndServicesSold',
    'CostOfRevenue',
    'GrossProfit',
    'OperatingExpenses',
    'CostsAndExpenses',
    'OperatingIncomeLoss',
    'NetIncomeLoss',
    'NetIncomeLossAvailableToCommonStockholdersBasic',
    'ProfitLoss',
    'EarningsPerShareBasic',
    'EarningsPerShareDiluted',
    'ResearchAndDevelopmentExpense',
    'SellingGeneralAndAdministrativeExpense',
    'Assets',
    'Liabilities',
    'StockholdersEquity',
    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    'CashAndCashEquivalentsAtCarryingValue',
    'LongTermDebt',
    'NetCashProvidedByUsedInOperatingActivities',
    'PaymentsToAcquirePropertyPlantAndEquipment',
    'CommonStockSharesOutstanding',
    'EntityCommonStockSharesOutstanding',
    'WeightedAverageNumberOfDilutedSharesOutstanding',
]

USER_AGENT = 'Rosmerta Research research@example.com'

import os
ua = os.environ.get('EDGAR_USER_AGENT', USER_AGENT)

def fetch_company_facts(cik):
    """Fetch companyfacts JSON from SEC EDGAR."""
    url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
    headers = {
        'User-Agent': ua,
        'Accept-Encoding': 'gzip, deflate',
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def summarize_tag(tag_data, tag_name):
    """Summarize a single tag's facts: count, date range, forms."""
    all_facts = []
    for unit_key, facts in tag_data.get('units', {}).items():
        all_facts.extend(facts)

    if not all_facts:
        return None

    # Filter to 10-K/10-Q only
    valid = [f for f in all_facts if f.get('form') in {'10-K', '10-K/A', '10-Q', '10-Q/A'}]
    if not valid:
        return None

    dates = [f.get('end', '') for f in valid if f.get('end')]
    forms = Counter(f.get('form', '') for f in valid)

    return {
        'tag': tag_name,
        'total_facts': len(valid),
        'min_date': min(dates) if dates else '?',
        'max_date': max(dates) if dates else '?',
        'forms': dict(forms),
        'label': tag_data.get('label', ''),
    }


def analyze_company(ticker, info):
    """Analyze a single company's XBRL tags."""
    print(f"\n{'='*80}")
    print(f"  {ticker} — {info['name']} (CIK {info['cik']})")
    print(f"{'='*80}")

    try:
        data = fetch_company_facts(info['cik'])
    except Exception as e:
        print(f"  ERROR fetching data: {e}")
        return

    gaap = data.get('facts', {}).get('us-gaap', {})
    dei = data.get('facts', {}).get('dei', {})

    # --- Section 1: Check current mapped tags ---
    print(f"\n  --- CURRENT MAPPED TAGS (do they have data?) ---")
    for tag in CURRENT_MAPPED_TAGS:
        tag_data = gaap.get(tag) or dei.get(tag)
        if tag_data:
            summary = summarize_tag(tag_data, tag)
            if summary:
                print(f"  ✓ {tag:<60} {summary['total_facts']:>4} facts  "
                      f"{summary['min_date']} → {summary['max_date']}  "
                      f"{summary['forms']}")
            else:
                print(f"  ✗ {tag:<60} (has data but not in 10-K/10-Q)")
        else:
            print(f"  ✗ {tag:<60} NOT FOUND")

    # --- Section 2: Find financial-sector tags by keyword ---
    print(f"\n  --- FINANCIAL SECTOR TAGS (keyword matches) ---")
    found_tags = {}
    for tag_name, tag_data in sorted(gaap.items()):
        tag_lower = tag_name.lower()
        for kw in KEYWORDS:
            if kw.lower() in tag_lower:
                summary = summarize_tag(tag_data, tag_name)
                if summary and summary['total_facts'] >= 3:  # skip noise
                    found_tags[tag_name] = summary
                break

    # Also check dei namespace
    for tag_name, tag_data in sorted(dei.items()):
        tag_lower = tag_name.lower()
        for kw in KEYWORDS:
            if kw.lower() in tag_lower:
                summary = summarize_tag(tag_data, tag_name)
                if summary and summary['total_facts'] >= 3:
                    found_tags[tag_name] = summary
                break

    # Print sorted by fact count (most used first)
    for tag_name in sorted(found_tags, key=lambda t: found_tags[t]['total_facts'], reverse=True):
        s = found_tags[tag_name]
        label_short = s['label'][:50] if s['label'] else ''
        print(f"  {tag_name:<60} {s['total_facts']:>4} facts  "
              f"{s['min_date']} → {s['max_date']}  "
              f"{label_short}")

    print(f"\n  Total financial-sector tags found: {len(found_tags)}")

    import time
    time.sleep(0.2)  # respect SEC rate limit between companies


if __name__ == '__main__':
    print("Financial Sector XBRL Tag Diagnostic")
    print("=" * 80)
    print(f"Analyzing: {', '.join(COMPANIES.keys())}")
    print(f"User-Agent: {ua}")

    # Allow filtering to a single ticker via command line
    if len(sys.argv) > 1:
        tickers = [t.upper() for t in sys.argv[1:]]
        companies = {t: COMPANIES[t] for t in tickers if t in COMPANIES}
    else:
        companies = COMPANIES

    for ticker, info in companies.items():
        analyze_company(ticker, info)

    print(f"\n{'='*80}")
    print("Done. Use this output to plan new XBRL_TAG_MAP entries.")
