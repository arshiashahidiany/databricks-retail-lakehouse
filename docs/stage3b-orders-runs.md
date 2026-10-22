# Stage 3B: Orders pipeline run tracking

Run from the project root with `python src/run_orders_pipeline.py`.
A random run identifier is printed and registered in the local SQLite database
`data/operational/orders_runs.sqlite`. The runner pins the output file of each
stage as the next stage's input. It records status, file paths, row counts and
errors. To retry a failed run, execute
`python src/run_orders_pipeline.py --run-id <the-same-run-id>`.
A successful run with the same ID exits without doing work.

Inspect runs using Python's sqlite3 library or a SQLite VS Code extension.

## Important limitations
- **Full snapshots, not CDC.** This change does not deliver incremental extraction.
- **Single-process development prototype.** Do not run multiple pipeline instances
  concurrently: SQLite and Silver publication are not coordinated locks.
- The published `data/silver/orders/orders.parquet` is a *current-state output*,
  so successful runs can overwrite it. A run log does not make it versioned.
- A file disappearing after a stage succeeds can lead to an unsafe downstream
  retry; the pipeline will fail instead of silently using a newer source file.
  Strong recovery guarantees require immutable storage, checksums, a staging
  area, and coordinated commit/publication.
- Avoid putting the SQLite run database under Git; it is local operational state.
- Run `python -m pytest tests/test_orders_io.py tests/test_orders_runs.py -v`.
