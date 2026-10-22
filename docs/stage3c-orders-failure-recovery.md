# Stage 3C — Orders failure and recovery validation

## What changed

- `orders_runs.py`: adds `output_sha256` to each stage record and upgrades existing Stage 3B SQLite schemas in place. Hashes are SHA-256 of the **actual published file bytes**.
- `run_orders_pipeline.py`: verifies the recorded output hash before reusing any completed stage in a resumed run; also verifies all outputs before short-circuiting an already-successful run.
- `tests/test_orders_recovery.py`: fault-injection tests for Silver failures, safe resume, changed outputs, SQLite schema upgrade and simulated interrupted writes.

## Why

File existence alone does not prove an output belongs to a prior run. In particular, Silver publishes to `data/silver/orders/orders.parquet`, a **shared output path**. A later run can replace it. Prior to this change, an older run could incorrectly treat that different file as its previously completed output. The pipeline now **fails closed** if the file no longer matches the checksum recorded for the run.

## Running tests

From the project root:

```powershell
python -m pip install pytest pandas pyarrow
python -m pytest tests/test_orders_io.py tests/test_orders_runs.py tests/test_orders_recovery.py -v
```

Then run a **new** Orders execution:

```powershell
python src/run_orders_pipeline.py
```

**Important Stage 3B upgrade:** previous stage records have no output SHA-256; they are not considered verifiable for reuse. Start a fresh run after installing Stage 3C. The existing SQLite file is migrated automatically; do not delete it. Do not try to resume an earlier Stage 3B run with missing hashes.

## Verified locally (automated simulation)

1. A simulated Silver failure marks the run FAILED while retaining successful extract/Bronze records.
2. A retry using the same run ID reuses extract/Bronze outputs without calling their functions again.
3. A third attempt on a succeeded run performs no work when hashes still match.
4. An overwritten Bronze output causes the resume attempt to abort rather than ingest wrong data.
5. An overwritten Silver output causes a previously succeeded run to refuse an unsafe replay.
6. A simulated interrupted temporary-file write preserves the old published file.
7. The SQLite schema migrates from Stage 3B without deleting history.

## Known limitations / engineering debt

- The runner has **no concurrency lock**. Do not run two instances simultaneously.
- SQLite state updates and Parquet publication are **not one atomic transaction**; hashes help detect stale outputs but do not make the whole pipeline transactional.
- `data/silver/orders/orders.parquet` is a mutable shared output. Immutable, run-scoped Silver outputs and a publish pointer/manifest would improve recovery further.
- There is no CDC, watermark or source snapshot isolation. This remains a full-snapshot prototype.
- Automated tests use small simulated files and mocked Parquet reading where appropriate. They do not establish SQL Server integration, source consistency, financial reconciliation, or production readiness.
- Older Stage 3B completed runs cannot be proven against SHA-256 because they were not originally hashed.

## Next engineering stage

Implement durable incremental extraction with explicit source-change semantics, persistent watermarks and tests for inserts, updates, deletes and replay; do not simply apply timestamp filtering without examining source guarantees.
