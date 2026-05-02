




\copy (SELECT s.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume FROM price_hourly p JOIN stocks s on s.stock_id = p.stock_id) TO 'db_exports/basic.csv' WITH CSV HEADER
