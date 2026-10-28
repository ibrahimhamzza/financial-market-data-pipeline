-- Financial Market Data Pipeline — PostgreSQL schema
-- Run once: psql -U postgres -d financial_db -f database/schema.sql

CREATE TABLE IF NOT EXISTS raw_market_data (
    id            SERIAL PRIMARY KEY,
    symbol        VARCHAR(10) NOT NULL,
    trade_date    DATE NOT NULL,
    open_price    NUMERIC(12, 4) NOT NULL,
    high_price    NUMERIC(12, 4) NOT NULL,
    low_price     NUMERIC(12, 4) NOT NULL,
    close_price   NUMERIC(12, 4) NOT NULL,
    volume        BIGINT NOT NULL CHECK (volume >= 0),
    loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_symbol_date UNIQUE (symbol, trade_date),
    CONSTRAINT chk_raw_high_low CHECK (high_price >= low_price)
);

CREATE TABLE IF NOT EXISTS transformed_market_data (
    id                 SERIAL PRIMARY KEY,
    symbol             VARCHAR(10) NOT NULL,
    trade_date         DATE NOT NULL,
    close_price        NUMERIC(12, 4) NOT NULL,
    daily_return       NUMERIC(10, 6),
    ma_7               NUMERIC(12, 4),
    ma_20              NUMERIC(12, 4),
    ma_50              NUMERIC(12, 4),
    avg_volume_7d      NUMERIC(20, 2),
    volume_ratio       NUMERIC(10, 4),
    cumulative_return  NUMERIC(10, 6),
    volatility_20d     NUMERIC(10, 6),
    loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transformed_symbol_date UNIQUE (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL PRIMARY KEY,
    run_date            TIMESTAMP NOT NULL DEFAULT NOW(),
    status              VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'running')),
    symbols_processed   INTEGER,
    rows_inserted       INTEGER,
    duration_seconds    NUMERIC(12, 3),
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_date
    ON raw_market_data (symbol, trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_date_close
    ON raw_market_data (symbol, trade_date, close_price);

CREATE INDEX IF NOT EXISTS idx_transformed_symbol_date
    ON transformed_market_data (symbol, trade_date);

CREATE INDEX IF NOT EXISTS idx_transformed_symbol_date_close
    ON transformed_market_data (symbol, trade_date, close_price);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date
    ON pipeline_runs (run_date DESC);
