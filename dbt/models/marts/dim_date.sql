-- Dimension: one row per calendar date in the loaded range.
--
-- is_trading_day comes from the data, not a market calendar: a date is a
-- trading day if the vendor returned bars for it. That's the same decision the
-- DAG made (ROADMAP decision log 2026-08-30 and 2026-09-05), carried into the
-- warehouse.

with bounds as (
    select min(session_date) as start_dt, max(session_date) as end_dt
    from {{ ref('stg_ohlcv') }}
),

spine as (
    select cast(d as date) as calendar_date
    from bounds, generate_series(start_dt, end_dt, interval '1 day') as g(d)
),

sessions as (
    select distinct session_date from {{ ref('stg_ohlcv') }}
)

select
    s.calendar_date,
    cast(extract(isodow from s.calendar_date) as integer) as iso_day_of_week,
    extract(isodow from s.calendar_date) <= 5              as is_weekday,
    t.session_date is not null                              as is_trading_day
    -- TODO(owner, later): a weekday with no partition is either a holiday or a
    -- missing load, and this table can't tell which. Add is_market_holiday from
    -- evidence that the vendor answered "closed" (the skipped DagRun), so
    -- assert_no_missing_sessions can exclude holidays and become an error.
from spine s
left join sessions t on t.session_date = s.calendar_date
