CREATE OR REPLACE VIEW ohlcv AS
SELECT * FROM read_parquet('data/raw/ohlcv/dt=*/ohlcv.parquet', hive_partitioning = 1);