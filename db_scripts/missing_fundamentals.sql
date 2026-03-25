

SELECT
    s.symbol AS ticker,
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(f.revenue) AS revenue_nulls,
    COUNT(*) - COUNT(f.cost_of_revenue) AS cost_of_revenue_nulls,
    COUNT(*) - COUNT(f.gross_profit) AS gross_profit_nulls,
    COUNT(*) - COUNT(f.operating_expenses) AS operating_expenses_nulls,
    COUNT(*) - COUNT(f.operating_income) AS operating_income_nulls,
    COUNT(*) - COUNT(f.net_income) AS net_income_nulls,
    COUNT(*) - COUNT(f.eps_basic) AS eps_basic_nulls,
    COUNT(*) - COUNT(f.eps_diluted) AS eps_diluted_nulls,
    COUNT(*) - COUNT(f.research_and_development) AS rd_nulls,
    COUNT(*) - COUNT(f.sga_expense) AS sga_nulls,
    COUNT(*) - COUNT(f.total_assets) AS total_assets_nulls,
    COUNT(*) - COUNT(f.total_liabilities) AS total_liabilities_nulls,
    COUNT(*) - COUNT(f.total_equity) AS total_equity_nulls,
    COUNT(*) - COUNT(f.cash_and_equivalents) AS cash_nulls,
    COUNT(*) - COUNT(f.total_debt) AS total_debt_nulls,
    COUNT(*) - COUNT(f.operating_cash_flow) AS op_cashflow_nulls,
    COUNT(*) - COUNT(f.capex) AS capex_nulls,
    COUNT(*) - COUNT(f.free_cash_flow) AS fcf_nulls,
    COUNT(*) - COUNT(f.pe_ratio) AS pe_ratio_nulls,
    COUNT(*) - COUNT(f.debt_to_equity) AS dte_nulls,
    COUNT(*) - COUNT(f.gross_margin) AS gross_margin_nulls,
    COUNT(*) - COUNT(f.operating_margin) AS op_margin_nulls,
    COUNT(*) - COUNT(f.net_margin) AS net_margin_nulls,
    COUNT(*) - COUNT(f.roe) AS roe_nulls,
    COUNT(*) - COUNT(f.shares_outstanding) AS shares_nulls
FROM fundamentals f
JOIN stocks s ON s.stock_id = f.stock_id


GROUP BY s.symbol
ORDER BY s.symbol;
