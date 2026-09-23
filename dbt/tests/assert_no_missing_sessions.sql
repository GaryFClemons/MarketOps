-- Weekdays inside the loaded range with no partition: sql/date-spine-anti-join.sql
-- promoted from a drill to a test, so an absent partition goes from
-- "detectable if someone runs the query" to "detected on every dbt build".
--
-- Warn, not error, for now: market holidays (2026-06-19, 2026-07-03,
-- 2026-09-07) are correctly absent and will appear here.
-- TODO(owner, later): exclude holidays once dim_date can tell a holiday from a
-- gap, then raise severity to error.
{{ config(severity='warn') }}

with bounds as (
    select min(session_date) as start_dt, max(session_date) as end_dt
    from {{ ref('stg_ohlcv') }}
),

weekdays as (
    select cast(d as date) as calendar_date
    from bounds, generate_series(start_dt, end_dt, interval '1 day') as g(d)
    where extract(isodow from d) <= 5
)

select w.calendar_date
from weekdays w
where not exists (
    select 1 from {{ ref('stg_ohlcv') }} s where s.session_date = w.calendar_date
)
