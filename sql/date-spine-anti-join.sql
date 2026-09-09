WITH bounds AS (
    SELECT DATE '2026-06-04' AS start_dt,
    DATE '2026-09-04' AS end_dt
),
CALENDAR AS (
    SELECT d::DATE AS dt 
    FROM bounds,
    generate_series(start_dt, end_dt, INTERVAL '1 day') AS g(d)
    WHERE EXTRACT(ISODOW FROM d) <= 5
)
SELECT c.dt
FROM calendar c 
WHERE NOT EXISTS (
    select 1
    FROM ohlcv o
    WHERE o.dt = c.dt
)
ORDER BY c.dt;