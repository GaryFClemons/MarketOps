--one row per ticker per session
WITH daily_returns AS (
    SELECT 
    ticker,
    date,
    (close / LAG(close) OVER (PARTITION BY ticker ORDER BY date)) -1 AS ret_1d
    FROM ohlcv
),
--one row per ticker
ticker_vol AS (
    SELECT
    ticker,
    COUNT(ret_1d) AS n_obs,
    STDDEV_SAMP(ret_1d) * SQRT(252) AS ann_vol
    FROM daily_returns
    WHERE ret_1d IS NOT NULL
    GROUP BY ticker
),
--one row, all tickers
market_baseline AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ann_vol) AS median_vol
    FROM ticker_vol
)
--one row per ticker
SELECT
v.ticker,
v.n_obs,
v.ann_vol,
b.median_vol,
v.ann_vol / b.median_vol AS vol_ratio,
RANK() OVER (ORDER BY v.ann_vol DESC) AS vol_rank
FROM ticker_vol v
CROSS JOIN market_baseline b
ORDER BY vol_rank, v.ticker;






