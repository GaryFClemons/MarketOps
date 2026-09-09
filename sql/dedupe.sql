SELECT date, ticker, count(*) as n
FROM ohlcv 
GROUP BY date, ticker
HAVING count(*) > 1;

WITH ranked AS (
    SELECT t.*,
    ROW_NUMBER() OVER 
    (PARTITION BY date, ticker 
    ORDER BY volume DESC) AS RN
    FROM ohlcv t
)
SELECT date, ticker, open, high, low, close, volume, trade_count
FROM ranked
WHERE RN = 1;