"""market_ops — the Python half of market-ops-platform.

The Airflow DAGs in ``dags/`` own scheduling and I/O against the vendor. This
package owns everything that should be a plain, testable function instead:

    quality/    pure data-quality checks, extracted from the ingestion DAG
    alerts/     deadline-miss and task-failure callbacks -> RunSignal files
    retrieval/  corpus loading, chunking, BM25 + dense + hybrid search
    agent/      the triage agent: RunSignal in, IncidentBrief out, audited
    evals/      golden sets, retrieval metrics, brief-quality scoring

Data flows left to right across the two layers:

    DAG run fails or misses its deadline
      -> alerts/ writes a RunSignal (data/ops/signals/*.json)
      -> agent/ reads it, calls read-only tools, retrieves runbook context
      -> emits an IncidentBrief + one audit line per step (data/ops/audit/)
      -> evals/ measures whether those briefs and retrievals are any good

Scaffolding status is tracked in code: anything unfinished calls
``market_ops._scaffold.todo()``. See docs/build-order.md for the order to
finish them in.
"""
