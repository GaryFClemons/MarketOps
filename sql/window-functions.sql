SELECT 
    ticker, 
    date, 
    close,
    CASE WHEN count(close) OVER w20 = 20
    THEN AVG(close) OVER w20
    END AS ma_20,
    (close / LAG(close) OVER w_tick) - 1 as ret_1d 
FROM ohlcv 
WINDOW 
    w_tick AS (PARTITION BY ticker ORDER BY date),
    w20 AS (PARTITION BY ticker ORDER BY date
             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
ORDER BY ticker, date;


