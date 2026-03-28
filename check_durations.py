"""
Diagnostic: check what duration lengths JPM uses for financial-sector tags.
This will tell us if the 115-day ceiling is filtering out the data.

Run from project root:
    uv run check_durations.py
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

USER_AGENT = os.environ.get('EDGAR_USER_AGENT', 'Rosmerta Research research@example.com')

# JPM CIK
CIK = '0000019617'

# Tags we want to inspect
TAGS_TO_CHECK = [
    'InterestIncomeExpenseNet',
    'NoninterestIncome',
    'NoninterestExpense',
    'ProvisionForLoanLeaseAndOtherLosses',
    # Also check a tag we know works (NetIncomeLoss)
    'NetIncomeLoss',
]

def main():
    url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json'
    headers = {'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate'}

    print(f"Fetching JPM companyfacts...")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    gaap = data.get('facts', {}).get('us-gaap', {})

    for tag in TAGS_TO_CHECK:
        print(f"\n{'='*60}")
        print(f"  {tag}")
        print(f"{'='*60}")

        tag_data = gaap.get(tag, {})
        if not tag_data:
            print("  NOT FOUND in us-gaap")
            continue

        all_facts = []
        for unit_key, facts in tag_data.get('units', {}).items():
            all_facts.extend(facts)

        # Filter to 10-K/10-Q
        valid_forms = {'10-K', '10-K/A', '10-Q', '10-Q/A'}
        valid = [f for f in all_facts if f.get('form') in valid_forms]

        # Only look at recent facts (2023+)
        recent = []
        for f in valid:
            end = f.get('end', '')
            if end >= '2023-01-01':
                recent.append(f)

        print(f"  Total 10-K/10-Q facts: {len(valid)}, recent (2023+): {len(recent)}")

        # Show duration distribution
        durations = {}
        for f in recent:
            start = f.get('start')
            end = f.get('end')
            form = f.get('form', '')

            if start and end:
                try:
                    s = datetime.strptime(start, '%Y-%m-%d').date()
                    e = datetime.strptime(end, '%Y-%m-%d').date()
                    days = (e - s).days
                    bucket = f"{days}d"
                except:
                    bucket = "parse_error"
            elif end and not start:
                bucket = "instant (no start)"
            else:
                bucket = "no_end"

            key = (bucket, form)
            if key not in durations:
                durations[key] = []
            durations[key].append(f"{start or '?'} → {end}")

        for (bucket, form), examples in sorted(durations.items()):
            print(f"  {bucket:<25} {form:<8} {len(examples)} facts")
            # Show first 3 examples
            for ex in examples[:3]:
                print(f"    {ex}")

if __name__ == '__main__':
    main()
