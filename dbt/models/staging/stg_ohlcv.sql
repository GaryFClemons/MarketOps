-- Staging: the raw zone, typed. One row per (session_date, ticker).
--
-- Only types and names change here. Cleaning, derived measures and joins
-- belong downstream, so a bug in a mart is fixed by re-deriving from raw,
-- never by re-fetching from a vendor that may no longer serve that history.

select
    cast(dt as date)            as session_date,   -- the partition key: the contract readers prune on
    cast("date" as date)        as bar_date,       -- kept only to test the partition invariant
    ticker,
    cast(open as double)        as open,
    cast(high as double)        as high,
    cast(low as double)         as low,
    cast(close as double)       as close,
    cast(volume as double)      as volume,         -- Polygon volume is fractional; an int cast would truncate
    cast(vwap as double)        as vwap,
    cast(trade_count as double) as trade_count     -- float: some tickers omit it in the vendor frame
from {{ source('raw', 'ohlcv') }}
