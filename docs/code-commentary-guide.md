# RetailCo — Code Commentary Guide

This package adds documentation to **every Python module and function** in the available Stage 4A project snapshot. The intent is to explain purpose, contracts, risks and recovery reasoning while preserving existing behavior. It is not a functional rewrite or a connection to live SQL Server CDC.

## Read the code in this order

1. `src/extract_orders.py` — full SQL Server snapshot boundary, connection and landing contract.
2. `src/orders_io.py` — snapshot selection and safe local-file replacement.
3. `src/bronze_orders.py` — landed snapshot -> Bronze.
4. `src/silver_orders.py` — order validation, business rules and customer integrity checks.
5. `src/orders_runs.py` — persistent operational state and file hashes.
6. `src/run_orders_pipeline.py` — stage orchestration, retry and recovery.
7. `src/orders_cdc.py` — offline CDC reducer for order INSERT/UPDATE/DELETE events.
8. `src/resolve_customer_identity.py` and `src/silver_customer.py` — customer matching and current-state merge.
9. `src/quarantine_customer_updates.py` — unresolved identity records.
10. `src/gold_customer.py` and `src/gold_customer_order_summary.py` — consumption views and metrics.

## Things the code does **not** guarantee yet

- Snapshot ingestion is not SQL Server CDC capture. `orders_cdc.py` is currently an offline event reducer.
- SQLite run-state changes and Parquet writes are not part of one transaction.
- `silver_customer.py` does not preserve Type 2 address history.
- Timestamp/file-arrival heuristics do not ensure a complete incremental feed.
- Existing order summaries still need finance sign-off and source-to-target reconciliation.

## Documentation convention

- **Top-level module docstrings** explain the file's role and its boundaries.
- **Function docstrings** explain its purpose and behavior.
- **Inline engineering notes** flag safety assumptions and architectural limitations.
- Existing comments and test assertions are preserved.
- Ordinary assignments/obvious syntax are intentionally *not* narrated line-by-line, because redundant commentary can mislead future maintainers.

## Installing this change

Copy `src/` and `tests/` Python files (and this `docs/` guide) into your existing Git checkout, reviewing the `git diff` before commit. This archive includes no local databases or production data. Run `python -m pytest tests -v`, then commit and push. Do not replace your `.git`, `.env` or local `data/` folders.

## Provenance

As direct GitHub access was unavailable, this was assembled from the latest provided Stage 3C full project archive and Stage 4A commented CDC archive. If GitHub has commits beyond those archives, compare and merge instead of overwriting newer work.
