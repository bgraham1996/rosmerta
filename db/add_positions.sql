CREATE TABLE IF NOT EXISTS transaction(
  transaction_id SERIAL PRIMARY KEY,
  transaction_type VARCHAR(10) NOT NULL, -- allowed transaction types: -buy, -sell, deposit cash, -exchange currency, -withdraw cash, -dividend payments
  transaction_date DATE
);


CREATE TABLE IF NOT EXISTS trade(
  trade_id SERIAL PRIMARY KEY,
  transaction_id INT NOT NULL REFERENCES transaction(transaction_id),
  trade_type VARCHAR(4),
  stock_id INT REFERENCES stocks(stock_id),
  stock_quantity INT,
  stock_price NUMERIC(12,4),
  trade_datetime TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS transfer(
  transfer_id SERIAL PRIMARY KEY,
  transaction_id INT NOT NULL REFERENCES transaction(transaction_id),
  transfer_type VARCHAR(5), -- allowed types: -in, -out, -dividend payment
  transfer_datetime TIMESTAMPTZ,
  cash_amount NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS position(
  position_id SERIAL PRIMARY KEY,
  stock_id INT NOT NULL REFERENCES stocks(stock_id),
  trade_open_id INT NOT NULL REFERENCES trade(trade_id),
  trade_close_id INT REFERENCES trade(trade_id),
  asset_status VARCHAR(20), -- allowed types: --holding, --shorted --closed hold, --closed shot
  position_open_dt TIMESTAMPTZ,
  position_close_dt TIMESTAMPTZ,
  realized_pl NUMERIC(12,4)
);
