WITH numbered AS (
    SELECT
    ticker,
    date,
    close,
    close > LAG(close) OVER tick_dt AS is_up,
    ROW_NUMBER() OVER tick_dt AS rn_all
    FROM ohlcv
    WINDOW tick_dt AS (PARTITION BY ticker ORDER BY date)
), 
up_days AS (
    SELECT
    ticker,
    date,
    rn_all,
    rn_all - ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as grp
    FROM numbered
    WHERE is_up
),
streaks AS (
    SELECT
    ticker,
    COUNT(*) AS streak_len,
    MIN(date) as start_dt,
    MAX(date) AS end_dt
    FROM up_days
    GROUP BY ticker, grp   
),
best AS (
    SELECT
    s.*,
    RANK() OVER (PARTITION BY ticker ORDER BY streak_len DESC) AS r
    FROM streaks s 
)
SELECT ticker, streak_len, start_dt, end_dt
FROM best
WHERE r = 1 
ORDER BY ticker, start_dt;