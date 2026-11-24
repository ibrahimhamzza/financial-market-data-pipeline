-- =====================================================================
-- Analytical queries (PostgreSQL) — window functions + CTEs
-- Run against financial_db after transformed_market_data is populated:
--   psql -U postgres -d financial_db -f database/analytics.sql
--
-- "Today" in Q1/Q5 means the LATEST date present in the data, not
-- CURRENT_DATE, so results are never empty on weekends, holidays, the 1st of
-- a month, or when the API data is a few days stale.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q1. Top 3 best-performing days per symbol in the last 90 days
-- Business meaning: which sessions produced the biggest one-day gains, so
-- analysts can look for the news/events behind them.
-- Uses: CTE + RANK() OVER (PARTITION BY symbol)
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT
        symbol,
        trade_date,
        daily_return,
        close_price,
        RANK() OVER (
            PARTITION BY symbol
            ORDER BY daily_return DESC NULLS LAST
        ) AS return_rank
    FROM transformed_market_data
    WHERE trade_date >= (SELECT MAX(trade_date) FROM transformed_market_data) - INTERVAL '90 days'
)
SELECT
    symbol,
    trade_date,
    ROUND(daily_return * 100, 2) AS return_pct,
    close_price,
    return_rank
FROM ranked
WHERE return_rank <= 3
ORDER BY symbol, return_rank, trade_date;


-- ---------------------------------------------------------------------
-- Q2. Day-over-day change recomputed in SQL with LAG
-- Business meaning: independent cross-check of the Python daily_return, plus
-- the previous close next to today's close for quick eyeballing.
-- Uses: CTE + LAG() OVER (PARTITION BY symbol ORDER BY trade_date)
-- ---------------------------------------------------------------------
WITH with_prev AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close
    FROM transformed_market_data
)
SELECT
    symbol,
    trade_date,
    close_price,
    prev_close,
    ROUND((close_price - prev_close) / NULLIF(prev_close, 0) * 100, 2) AS sql_daily_return_pct
FROM with_prev
ORDER BY symbol, trade_date;


-- ---------------------------------------------------------------------
-- Q3. What happened the day AFTER a big drop (>= 3 %)?
-- Business meaning: tests the "dip then bounce" idea — do large one-day drops
-- tend to reverse the next session or keep falling?
-- Uses: CTE + LEAD() OVER (PARTITION BY symbol ORDER BY trade_date)
-- ---------------------------------------------------------------------
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
    ROUND(next_return * 100, 2)  AS next_day_pct,
    CASE
        WHEN next_return IS NULL THEN 'no next session yet'
        WHEN next_return > 0     THEN 'rebound'
        ELSE 'continued down'
    END AS next_day_outcome
FROM with_neighbors
WHERE daily_return <= -0.03
ORDER BY trade_date DESC, daily_return;


-- ---------------------------------------------------------------------
-- Q4. Close vs its own 20-day average (trend / mean-reversion signal)
-- Business meaning: a close far ABOVE its 20-day average = stretched uptrend;
-- far BELOW = oversold. Only complete 20-bar windows are shown, because an
-- average over 3 or 7 bars (the first rows of a symbol) is not a 20-day MA.
-- Uses: AVG() OVER with ROWS frame, COUNT() OVER, named WINDOW clause
-- ---------------------------------------------------------------------
WITH ma AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        COUNT(*)         OVER w AS bars_in_window,
        AVG(close_price) OVER w AS ma_20_sql
    FROM transformed_market_data
    WINDOW w AS (
        PARTITION BY symbol
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
)
SELECT
    symbol,
    trade_date,
    close_price,
    ROUND(ma_20_sql, 4)                                         AS ma_20_sql,
    ROUND(close_price - ma_20_sql, 4)                           AS distance_from_ma20,
    ROUND((close_price - ma_20_sql) / NULLIF(ma_20_sql, 0) * 100, 2) AS distance_pct
FROM ma
WHERE bars_in_window = 20
ORDER BY symbol, trade_date DESC;


-- ---------------------------------------------------------------------
-- Q5. Rank symbols by performance in the latest month of data
-- Business meaning: month-to-date leaderboard (first close of the month vs the
-- most recent close), the kind of table a portfolio review opens with.
-- Uses: multi-step CTEs + RANK() OVER (ORDER BY ...)
-- ---------------------------------------------------------------------
WITH latest_month AS (
    SELECT DATE_TRUNC('month', MAX(trade_date))::date AS month_start
    FROM transformed_market_data
),
month_bounds AS (
    SELECT
        t.symbol,
        MIN(t.trade_date) AS first_day,
        MAX(t.trade_date) AS last_day
    FROM transformed_market_data t
    CROSS JOIN latest_month lm
    WHERE t.trade_date >= lm.month_start
    GROUP BY t.symbol
),
month_prices AS (
    SELECT
        b.symbol,
        f.close_price AS start_close,
        l.close_price AS end_close
    FROM month_bounds b
    JOIN transformed_market_data f ON f.symbol = b.symbol AND f.trade_date = b.first_day
    JOIN transformed_market_data l ON l.symbol = b.symbol AND l.trade_date = b.last_day
)
SELECT
    symbol,
    start_close,
    end_close,
    ROUND((end_close - start_close) / NULLIF(start_close, 0) * 100, 2) AS month_return_pct,
    RANK() OVER (ORDER BY (end_close - start_close) / NULLIF(start_close, 0) DESC) AS month_rank
FROM month_prices
ORDER BY month_rank;


-- ---------------------------------------------------------------------
-- Q6. Sequence number of every bar inside its symbol's history
-- Business meaning: data-completeness check / pagination helper — the highest
-- bar_number per symbol is how many trading days we hold for it.
-- Uses: ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)
-- ---------------------------------------------------------------------
SELECT
    symbol,
    trade_date,
    close_price,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date) AS bar_number
FROM raw_market_data
ORDER BY symbol, trade_date;


-- ---------------------------------------------------------------------
-- Q7. Pipeline run health (ties the analytics to the pipeline_runs audit log)
-- Business meaning: is the nightly job healthy? Shows each run's duration,
-- rows loaded versus the average of the last 5 runs, and whether the previous
-- run failed. An incremental run that loads 0 rows is normal on weekends.
-- Uses: LAG() and AVG() OVER on pipeline_runs
-- ---------------------------------------------------------------------
SELECT
    id,
    start_time,
    end_time,
    status,
    rows_inserted,
    ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::numeric, 2) AS duration_s,
    ROUND(AVG(rows_inserted) OVER (
        ORDER BY start_time
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 1) AS avg_rows_last_5_runs,
    LAG(status) OVER (ORDER BY start_time) AS previous_run_status
FROM pipeline_runs
ORDER BY start_time DESC;
