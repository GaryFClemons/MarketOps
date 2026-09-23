-- Fact: one row per (session_date, ticker). The grain comes first; every
-- measure below is defined at it.
--
-- Incremental, because history is append-mostly and a full rebuild rereads
-- every partition. The risk of incremental is late or corrected data landing
-- behind the high-water mark: a re-derived partition for a past session would
-- be silently skipped by a naive `where session_date > max(session_date)`.
-- Hence a lookback window, rebuilt on every run with delete+insert on the key.
{{
    config(
        materialized='incremental',
        unique_key=['session_date', 'ticker'],
        incremental_strategy='delete+insert',
    )
}}

with bars as (
    select *
    from {{ ref('stg_ohlcv') }}
    {% if is_incremental() %}
    -- TODO(owner, later): size the lookback to how far back a partition can be
    -- re-derived in practice (a backfill), in sessions rather than calendar
    -- days. Seven calendar days is a placeholder.
    where session_date >= (select max(session_date) - interval 7 day from {{ this }})
    {% endif %}
)

select
    session_date,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    trade_count,
    -- TODO(owner, later): close / lag(close) over (partition by ticker order by
    -- session_date) - 1. The trap: inside an incremental run the first session
    -- of the window has no previous row in scope, so LAG returns null and the
    -- return is wrong. Select one extra prior session for context, compute LAG,
    -- then keep only the window's rows.
    cast(null as double) as daily_return
from bars
