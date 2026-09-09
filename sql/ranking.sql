WITH ranked AS (
    SELECT 
    date,
    ticker,
    vwap * volume AS dollar_volume,
    RANK() OVER (PARTITION by date ORDER BY vwap * volume DESC) AS dv_rank
    FROM ohlcv 
    WHERE vwap IS NOT NULL
)
SELECT date, ticker, dollar_volume, dv_rank
FROM ranked
WHERE dv_rank <= 5
ORDER BY date, dv_rank;