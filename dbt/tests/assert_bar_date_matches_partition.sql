-- The raw zone's core invariant, as a dbt test: every row in dt=D is dated D.
-- The 2026-08-30 incident was a partition that violated this while every other
-- check passed. Returns the offending rows; any row fails the test.

select session_date, bar_date, ticker
from {{ ref('stg_ohlcv') }}
where bar_date <> session_date
