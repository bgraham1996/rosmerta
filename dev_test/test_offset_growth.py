"""Live-DB check for ``Asset.get_offset_growth`` / the ``days_offset_gain``
indicator.

Runnable script, **not pytest** — needs a populated PostgreSQL (see CLAUDE.md).
For each offset it prints the distribution of forward gains plus the kind of
hit-rate stat the indicator exists for ("how often is price within ±0.2%
N days after investment").
"""

from db_config import get_db_config
from data_models import Asset
import psycopg2 as db

ticker = 'PFE'
db_config = get_db_config()

connection = db.connect(**db_config)
connection.autocommit = False

start_date = '2024-01-01 00:00:00'
end_date = '2026-06-03 00:00:00'

with connection as conn:
    pfe = Asset(conn, ticker, start_date, end_date)
    pfe.get_prices(conn)

    for days in (5, 10, 15, 20):
        growth = pfe.get_offset_growth(conn, days=days)
        gains = growth[f'gain_{days}d'].dropna()
        print(f"\n=== {days}-day offset gain — {len(gains)} of {len(growth)} bars ===")
        print(gains.describe())
        print(f"share positive:     {(gains > 0).mean():.1%}")
        print(f"share within ±0.2%: {(gains.abs() <= 0.002).mean():.1%}")
