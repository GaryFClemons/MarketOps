# Query cost: partition pruning, measured

The open Days 5–6 item: make one query measurably faster, and write down why partition layout dominates query cost. This page is the experiment protocol and the results table. **No results are recorded yet.**

## The claim to test

The raw zone is Hive-partitioned (`data/raw/ohlcv/dt=YYYY-MM-DD/ohlcv.parquet`). A filter on `dt` is resolved from directory names, so DuckDB never opens files outside the range. The same filter on the `date` column inside the file is logically identical (the invariant guarantees `date = dt`), but the engine can't know that, so it must open every file and check.

## Protocol

From the repo root, in DuckDB:

1. `sql/00_setup.sql` creates the `ohlcv` view.
2. Pick one session-range query, for example the average close per ticker over one week.
3. Write it twice: once filtering on `dt`, once filtering on `date`. Keep everything else identical.
4. `EXPLAIN ANALYZE` each. Record the files or row groups scanned, the rows read, and the total time from the profile.
5. Run each 5 times after one warm-up run; record the median. The first run pays for the OS file cache.
6. Repeat with a one-session range and with the full range. Pruning should matter most for narrow ranges and not at all for the full range.

## Results

| Query | Filter column | Range | Files scanned | Rows read | Median ms |
|---|---|---|---|---|---|
| | `dt` | 1 week | | | |
| | `date` | 1 week | | | |
| | `dt` | 1 session | | | |
| | `date` | 1 session | | | |
| | `dt` | all | | | |
| | `date` | all | | | |

## Why layout dominates cost

TODO(owner): write this from the numbers above. It should cover: skipping I/O beats making I/O faster; columnar files mean unread columns cost nothing; small files have a per-file overhead (75 partitions of ~73 rows each is tiny, so the effect may be small here, and saying so honestly is part of the answer); and at warehouse scale, partition and cluster choice is what decides the bill.
