
from db_config import *
from data_models import *
from indicators import *
import psycopg2 as db
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

ticker = 'PFE'

db_config = get_db_config()

connection = db.connect(**db_config)
connection.autocommit = False

start_date = '2024-01-01 00:00:00'
end_date = '2026-03-01 00:00:00'

with connection as conn:
    pfe = Asset(conn, ticker, start_date, end_date)

print(pfe.asset_metadata())

with connection as conn:
    prices = pfe.get_prices(conn)

print(pfe._prices_cache.head(5))

# next need to test indicator implementaion

with connection as conn:
    sma50 = Indicator('sma', window=50)
    pfe.add_indicator(sma50, conn)
    print(sma50._result.tail(5))


with connection as conn:
    pfe.add_indicator(Indicator('obv'), conn, source=['close', 'volume'])
    

with connection as conn:
    pfe.get_price_levels(split = 100)
print(pfe._levels_cache.head(5))
levels = pfe._levels_cache
pfe._levels_cache.to_csv(f'{ticker}_levels.csv', index = False, float_format='%.4f')

fig, axes = plt.subplots(1, 3, figsize=(12, 8), sharey=True)

axes[0].barh(levels['price'], levels['volume'], height=levels['price'].diff().mean())
axes[0].set_xlabel('Volume')
axes[0].set_ylabel('Price')
axes[0].set_title('Volume by price')

axes[1].barh(levels['price'], levels['time'], height=levels['price'].diff().mean())
axes[1].set_xlabel('Bars')
axes[1].set_title('Time at price')

axes[2].barh(levels['price'], levels['density'], height=levels['price'].diff().mean())
axes[2].set_xlabel('Density')
axes[2].set_title('Volume × time')

fig.suptitle(f'{pfe.ticker} price levels — {pfe.start_date[:10]} to {pfe.end_date[:10]}')
fig.tight_layout()
fig.savefig(f'{ticker}_price_levels.png', dpi=120)
plt.close(fig)

print("End of File")
