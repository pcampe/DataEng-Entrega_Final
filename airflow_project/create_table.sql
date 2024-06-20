CREATE TABLE combined_data (
    id SERIAL PRIMARY KEY,
    fetch_time TIMESTAMP,
    stock_date DATE,
    stock_symbol VARCHAR(10),
    stock_price DECIMAL,
    city VARCHAR(50),
    temp_c DECIMAL,
    condition VARCHAR(50)
);
