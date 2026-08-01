\copy (select stock_id, symbol from stocks) TO 'db_exports/tickers.csv' WITH CSV HEADER
