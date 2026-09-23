"""Pure data-quality checks, extracted from dags/ingest_ohlcv_daily.py.

Design: every check is a pure function that returns a *verdict or a value* and
never imports Airflow. The DAG is the adapter that turns a verdict into an
Airflow exception. Three payoffs:

1. The retry taxonomy — which failures retry and which fail fast — becomes
   testable on a laptop in milliseconds, instead of only observable in a live run.
2. The same check runs in three places without drift: ``validate_partition`` in
   the DAG, ``scripts/audit_partitions.py``, and the triage agent's
   ``check_partition`` tool.
3. A check can't accidentally depend on Airflow context (``now()``, the metadata
   DB), which is the property that made the 2026-08-30 incident possible.

Wiring into the DAG (owner does this)
-------------------------------------

=============================  ========================================  ================================
Function                       Replaces in ``fetch_ohlcv`` / validate     Verdict -> exception at the DAG
=============================  ========================================  ================================
``classify_status``            the ``if response_code in [401, 403]``    FAIL_FAST -> AirflowFailException
                               / ``500 <= code < 600`` / ``!= 200``      RETRY -> requests.HTTPError
                               chain                                     THROTTLED -> in-task wait loop
``throttle_wait_seconds``      the ``Retry-After`` parsing line inside   (value, not a verdict)
                               the 429 loop
``classify_payload``           ``if not results`` / ``len < 8000``       MARKET_CLOSED -> AirflowSkipException
                                                                         TRUNCATED -> AirflowFailException
``find_date_mismatches``       the ``mismatched = ...`` block            non-empty -> AirflowFailException
``assess_completeness``        the ``missing = set(tickers) - ...``      should_fail -> ValueError (retryable)
                               block                                     missing -> log.warning
``validate_partition_frame``   the body of ``validate_partition``        problems -> AirflowFailException
                                                                         (or ValueError, per current DAG)
``partition_date_from_path``   ``Path(path).parent.name.removeprefix``   ValueError on a bad path
=============================  ========================================  ================================

Import ``OHLCV_COLUMNS`` from here once wired, so the schema lives in one place.
"""
