# Stage 4A — Offline Orders CDC reducer

## Purpose
Keep the **Stage 3 snapshot pipeline unchanged**. Test a deterministic model for inserting, updating and deleting Orders from change events before any SQL Server CDC extraction is introduced.

## Contract
Events must contain: `OrderID`, `operation` (`I`, `U`, `D`), `change_lsn`, `seqval`, and `CustomerID`, `OrderDate`, `OrderStatus`, `TotalAmount` (populated for inserts/updates). Positions are normalized 20-character hexadecimal strings. These are **simulated CDC event fields**, not a raw SQL Server CDC output schema. Actual SQL Server CDC operation codes (1 delete, 2 insert, 3 update-before, 4 update-after), capture instance and LSN mapping require an adapter in Stage 4B.

## Engineering decisions
- Persist **last position per OrderID**, not just one global watermark. A late or duplicate delivery is ignored only if the target has already applied an event at the same or later position.
- Preserve delete **tombstones** in state to prevent replayed old inserts resurrecting deleted rows. Downstream active view filters tombstones.
- Reject conflicting payloads at the same position and invalid ordering fields. Avoid silent lost updates.
- This reducer uses pandas intentionally for a small offline correctness exercise, before a transactional Delta MERGE implementation.

## Run tests
From repo root: `python -m pytest tests/test_orders_cdc.py -v`.
No SQL Server, network, or Parquet dependency is needed for these eight unit tests.

## Not production ready
- No SQL Server CDC connector, initial consistent snapshot/LSN handoff, or global offsets.
- SQL Server CDC may contain multiple events at a shared LSN/seqval; a deterministic disambiguator may be required. For this exercise, events are uniquely ordered *per OrderID* using LSN+seqval.
- State output publication and source-offset commit are **not** one transaction. Stage 4B must address this with transactional Delta commits or provably safe replay.
- Schema evolution, data-type precision, source consistency, CDC retention gap and privacy deletions are not solved.
- Existing snapshots cannot be seeded into this CDC reducer without assigning a **matching source snapshot boundary**. Do not combine a random full extract with subsequent CDC records.

## Next engineering task
Verify SQL Server has CDC enabled for Orders; inspect `sys.tables.is_tracked_by_cdc`, `cdc.change_tables`, `sys.fn_cdc_get_min_lsn` and `sys.fn_cdc_get_max_lsn`. Design initial snapshot-to-CDC LSN handoff and capture-instance mapping before production ingestion.
