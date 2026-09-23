-- Dimension: one row per version of each ticker's membership (SCD Type 2).
-- Natural key: ticker. A version is current when valid_to is null.

select
    ticker,
    is_active,
    dbt_valid_from          as valid_from,
    dbt_valid_to            as valid_to,
    dbt_valid_to is null    as is_current
from {{ ref('ticker_universe_snapshot') }}
-- TODO(owner, later): the first snapshot run stamps every row valid_from = the
-- run time, not the date the ticker actually joined the universe. Decide
-- whether to backdate the initial versions to the first session each ticker
-- appears in fct_daily_bars.
