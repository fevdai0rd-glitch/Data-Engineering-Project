-- Create the analytics schema with an explicit Database Primary Key
CREATE TABLE IF NOT EXISTS fact_bitcoin_rates (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    extraction_time TIMESTAMP NOT NULL,
    currency VARCHAR(10) NOT NULL,
    price_usd DOUBLE PRECISION NOT NULL,
    price_gbp DOUBLE PRECISION NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    CONSTRAINT unique_extraction_time UNIQUE (extraction_time) -- ◄─ Guards against duplicate times
);
