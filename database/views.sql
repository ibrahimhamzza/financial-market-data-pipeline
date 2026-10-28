-- Reusable analyst views. Run after schema.sql.

CREATE OR REPLACE VIEW v_latest_prices AS
SELECT DISTINCT ON (symbol)
    symbol,
    trade_date,
    close_price,
    daily_return,
    ma_7,
    ma_20,
    ma_50,
    volume_ratio,
    volatility_20d
FROM transformed_market_data
ORDER BY symbol, trade_date DESC;

CREATE OR REPLACE VIEW v_top_gainers AS
SELECT
    symbol,
    trade_date,
    close_price,
    daily_return,
    RANK() OVER (PARTITION BY trade_date ORDER BY daily_return DESC NULLS LAST) AS gainer_rank
FROM transformed_market_data
WHERE daily_return IS NOT NULL;

CREATE OR REPLACE VIEW v_volume_spikes AS
SELECT
    symbol,
    trade_date,
    close_price,
    avg_volume_7d,
    volume_ratio
FROM transformed_market_data
WHERE volume_ratio IS NOT NULL
  AND volume_ratio >= 1.5
ORDER BY trade_date DESC, volume_ratio DESC;
