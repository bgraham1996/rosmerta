

\copy (select s.symbol, w.watchlist_member_id, w.stock_id, w.list_name from watchlist_members w join stocks s on s.stock_id = w.stock_id) TO 'db_exports/watchlist_export.csv' WITH CSV HEADER
