

\copy (SELECT s.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume FROM price_hourly p JOIN stocks s on s.stock_id = p.stock_id WHERE s.symbol = 'ADSK') TO 'db_exports/small.csv' WITH CSV HEADER


