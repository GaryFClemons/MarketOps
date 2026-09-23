# dbt — the warehouse layer

Staging → marts over the raw zone, in a local DuckDB file. Lower priority than the AI layer: the structure and the design decisions are in place, and the unfinished parts are marked `TODO(owner, later)`.

| Layer | Model | Grain | State |
|---|---|---|---|
| staging | `stg_ohlcv` | (session_date, ticker) | Written |
| marts | `fct_daily_bars` | (session_date, ticker) | Incremental skeleton; `daily_return` and lookback sizing TODO |
| marts | `dim_date` | calendar date | Written; holiday vs gap TODO |
| marts | `dim_ticker` | ticker membership version (SCD2) | Written over the snapshot; backdating TODO |
| snapshot | `ticker_universe_snapshot` | ticker version | Written |
| seed | `ticker_universe` | ticker | 73 active + ANSS inactive |

Tests: `unique_combination` (a local generic test, so no packages), not-null, relationships, accepted values, plus two singular tests:

- `assert_bar_date_matches_partition`: the raw-zone invariant (every row in `dt=D` is dated D).
- `assert_no_missing_sessions`: the date-spine anti-join from `sql/`, as a warn-level test until holidays can be excluded.

The staging model, `dim_date`, and both singular tests were checked by running their SQL, with the Jinja rendered by hand, directly in DuckDB against the real raw zone. `dbt build` itself has not been run yet.

## Run

From the repo root (relative paths resolve from there):

```bash
pip install dbt-duckdb
mkdir -p data/warehouse
dbt build --project-dir dbt --profiles-dir dbt
```

Data model and grain decisions: [docs/data_model.md](../docs/data_model.md).
