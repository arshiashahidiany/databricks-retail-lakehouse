# Stage 3A — Orders snapshot reliability

## Current contract
The SQL Server Orders extraction is a **full snapshot** (`SELECT ... FROM Orders`). Silver is the current-state snapshot, not an append-only history or CDC target. A changed order status appears on the next successful full extract and Silver rebuild.

## Changes
- Use microsecond timestamps in landing filenames to avoid collisions within one second.
- Select snapshots by timestamped filename instead of filesystem modification time.
- Write Parquet to a temporary file and atomically replace the destination (on the same filesystem).
- Fail Silver publication if source OrderID is missing or duplicated; never silently publish ambiguous keys.
- Add tests for selection and atomic writes.

## Remaining limitations (deliberately not solved yet)
- There is no CDC, watermark, transactional multi-file commit, or ingestion manifest.
- Selecting the latest Bronze snapshot does not prove it corresponds to the latest landing snapshot. Add an explicit run manifest next.
- An extraction is not a database-consistent snapshot merely because it uses one SQL query; consistency depends on SQL Server isolation and configuration.
- File-level atomic replace is not an end-to-end transaction, and does not guarantee durability after a hardware failure.
- Full-table reads are not suitable for a large production OLTP source without DBA-approved snapshot/replica strategy.
- Existing customer/JSON paths have not yet been hardened.
- Order totals are currently converted to floating point; financial-grade decimal precision is future work.

## Test
`python -m pytest tests/test_orders_io.py -q`

## Next
Introduce an explicit ingestion run manifest that binds extraction → Bronze → Silver, with status, input identity, row counts, and restart semantics.
