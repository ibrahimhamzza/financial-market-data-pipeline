-- Analytical queries for the evaluation (window functions + CTEs).
-- Run against financial_db after transformed_market_data is populated.

-- 1. Top 3 best-performing days per symbol in the last 90 days
WITH ranked AS (
    SELECT
        symbol,
        trade_date,
        daily_return,
        close_price,
        RANK() OVER (PARTITION BY symbol ORDER BY daily_return DESC NULLS LAST) AS return_rank
    FROM transformed_market_data
    WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
)
SELECT
    symbol,
    trade_date,
    ROUND(daily_return * 100, 2) AS return_pct,
    close_price
FROM ranked
WHERE return_rank <= 3
ORDER BY symbol, return_rank;


-- 2. Previous-day close and day-over-day return using LAG
SELECT
    symbol,
    trade_date,
    close_price,
    LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close,
    ROUND(
        (
            close_price
            - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date)
        )
        / NULLIF(LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date), 0)
        * 100,
        2
    ) AS sql_daily_return_pct
FROM transformed_market_data
ORDER BY symbol, trade_date;


-- 3. Next-session close (LEAD) to flag reversals after a large drop
WITH with_neighbors AS (
    SELECT
        symbol,
        trade_date,
        daily_return,
        LEAD(daily_return) OVER (PARTITION BY symbol ORDER BY trade_date) AS next_return
    FROM transformed_market_data
)
SELECT
    symbol,
    trade_date,
    ROUND(daily_return * 100, 2) AS drop_pct,
    ROUND(next_return * 100, 2) AS next_day_pct
FROM with_neighbors
WHERE daily_return <= -0.03
ORDER BY trade_date DESC, daily_return;


-- 4. 20-day average close vs today's close (AVG OVER)
SELECT
    symbol,
    trade_date,
    close_price,
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS ma_20_sql,
    close_price - AVG(close_price) OVER (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS distance_from_ma20
FROM transformed_market_data
ORDER BY symbol, trade_date DESC;


-- 5. Rank symbols by this month's cumulative performance (CTE + RANK)
WITH month_bounds AS (
    SELECT
        symbol,
        MIN(trade_date) AS first_day,
        MAX(trade_date) AS last_day
    FROM transformed_market_data
    WHERE trade_date >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY symbol
),
month_prices AS (
    SELECT
        m.symbol,
        first_row.close_price AS start_close,
        last_row.close_price AS end_close
    FROM month_bounds m
    JOIN transformed_market_data first_row
      ON first_row.symbol = m.symbol AND first_row.trade_date = m.first_day
    JOIN transformed_market_data last_row
      ON last_row.symbol = m.symbol AND last_row.trade_date = m.last_day
)
SELECT
    symbol,
    start_close,
    end_close,
    ROUND((end_close - start_close) / NULLIF(start_close, 0) * 100, 2) AS month_return_pct,
    RANK() OVER (
        ORDER BY (end_close - start_close) / NULLIF(start_close, 0) DESC
    ) AS month_rank
FROM month_prices
ORDER BY month_rank;


-- 6. ROW_NUMBER of each bar inside its symbol history (sanity / pagination)
SELECT
    symbol,
    trade_date,
    close_price,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS bar_number
FROM raw_market_data
ORDER BY symbol, trade_date;
