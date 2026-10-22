"""Apply SQL Server-style CDC events to an Orders *current-state* table.

Stage 4A is an offline exercise, not a live SQL Server CDC connector.

Expected event fields:
    OrderID, operation (I/U/D), change_lsn, seqval, and the Order fields.

Positions are 20-character hexadecimal strings; the pair (change_lsn, seqval)
orders events for a given order. In real SQL Server CDC, the mapping from
__$operation and __$start_lsn / __$seqval must be handled by the extractor.

Why retain the latest CDC position AND delete tombstones?
    If a delayed old update arrives after a delete, we must not resurrect the
    deleted order. Keeping both the tombstone and its position prevents that.

This module calculates the next state. It does NOT guarantee that publishing
state and committing a CDC offset happen atomically; that is later work.
"""

# CDC CONTRACT: Ordering, duplicate handling and deletion markers matter
# independently of whether changes arrive by file, batch query or stream.

from pathlib import Path

import pandas as pd

# Fields representing an Order's current business values. Insert/update
# events must contain complete after-images for these fields.
FIELDS = ["CustomerID", "OrderDate", "OrderStatus", "TotalAmount"]
KEYS = ["OrderID", "change_lsn", "seqval"]
LSN_LENGTH = 20


def _validate_position(value, name):
    """Normalise an LSN/sequence to uppercase for consistent comparison.

    Fixed-length hexadecimal strings sort lexicographically in the same order
    as their unsigned numeric values. Variable-length strings would not.
    """
    if not isinstance(value, str) or len(value) != LSN_LENGTH:
        raise ValueError(f"{name} must be a 20-character hexadecimal string")
    try:
        int(value, 16)
    except ValueError as e:
        raise ValueError(f"{name} must be hexadecimal") from e
    return value.upper()


def apply_changes(state: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Return Orders state after applying events, without side effects.

    Invariants:
    - One state row per OrderID (including deleted Orders).
    - Replaying an event already applied never changes the state.
    - An older event cannot overwrite a later event for the same OrderID.
    - Conflicting deliveries at the same CDC position are rejected.

    This function does not persist state, write audit records or advance an
    extraction watermark; its only responsibility is change application.
    """
    needed = set(KEYS + ["operation"] + FIELDS)
    if not needed.issubset(events.columns):
        raise ValueError(f"Missing CDC event columns: {sorted(needed - set(events.columns))}")

    state_columns = ["OrderID", *FIELDS, "is_deleted", "change_lsn", "seqval"]

    # Build an OrderID -> last-known-state map. An empty state represents the
    # first ingestion; any existing state must already obey our key contract.
    if state.empty:
        existing = {}
    else:
        if not set(state_columns).issubset(state.columns):
            raise ValueError("State missing required columns")
        if state.OrderID.isna().any() or state.OrderID.duplicated().any():
            raise ValueError("State has invalid OrderID keys")
        existing = {int(r["OrderID"]): r.to_dict() for _, r in state.iterrows()}
        for r in existing.values():
            r["change_lsn"] = _validate_position(r["change_lsn"], "state.change_lsn")
            r["seqval"] = _validate_position(r["seqval"], "state.seqval")

    # Validate and standardise incoming events BEFORE applying any of them.
    # We accept only after-image I/U rows and D rows; e.g. SQL Server CDC
    # update-before images must be removed/mapped by the upstream extractor.
    normalized = []
    for _, row in events.iterrows():
        if pd.isna(row["OrderID"]) or isinstance(row["OrderID"], bool):
            raise ValueError("CDC OrderID cannot be null or boolean")
        key = int(row["OrderID"])
        if key != row["OrderID"]:
            raise ValueError("CDC OrderID must be integral")
        op = str(row["operation"]).upper()
        if op not in {"I", "U", "D"}:
            raise ValueError(f"Unsupported CDC operation {op}; pre-filter before-images")
        r = row.to_dict()
        r.update(
            OrderID=key,
            operation=op,
            change_lsn=_validate_position(row["change_lsn"], "change_lsn"),
            seqval=_validate_position(row["seqval"], "seqval"),
        )
        normalized.append(r)

    # A source connector may redeliver the same event after a retry. Drop
    # identical redeliveries; a different payload at the same position is a
    # data-contract violation rather than a legitimate subsequent change.
    seen = {}
    for r in normalized:
        identity = (r["OrderID"], r["change_lsn"], r["seqval"])
        if identity in seen:
            prev = seen[identity]
            if r["operation"] != prev["operation"] or any(
                not _same(r[c], prev[c]) for c in FIELDS if r["operation"] != "D"
            ):
                raise ValueError(f"Conflicting CDC events for {identity}")
        else:
            seen[identity] = r

    # Apply in source position order, not delivery/file order. A previously
    # processed position (<= current one) is skipped, which makes retries
    # idempotent and handles late redelivery of older events.
    for r in sorted(seen.values(), key=lambda r: (r["change_lsn"], r["seqval"], r["OrderID"])):
        key = r["OrderID"]
        previous = existing.get(key)
        position = (r["change_lsn"], r["seqval"])
        if previous is not None and position <= (previous["change_lsn"], previous["seqval"]):
            continue

        if r["operation"] == "D":
            # We do NOT remove a deleted Order from persisted state. Keeping
            # the tombstone and its latest position prevents a stale INSERT
            # or UPDATE from bringing that Order back.
            values = {f: previous.get(f) if previous else None for f in FIELDS}
        else:
            # This reducer requires full after-images, not sparse patches.
            # Otherwise an UPDATE could accidentally blank unchanged fields.
            if any(pd.isna(r[f]) for f in FIELDS):
                raise ValueError(f"Incomplete CDC after-image for OrderID={key}")
            values = {f: r[f] for f in FIELDS}

        existing[key] = {
            "OrderID": key,
            **values,
            "is_deleted": r["operation"] == "D",
            "change_lsn": position[0],
            "seqval": position[1],
        }

    # Consistent column and row ordering makes tests and repeated executions
    # deterministic (even when the event batch arrives in a different order).
    return pd.DataFrame([existing[k] for k in sorted(existing)], columns=state_columns)


def _same(a, b):
    """Treat two missing values as equivalent for duplicate-event checks."""
    if pd.isna(a) and pd.isna(b):
        return True
    return a == b


def active_orders(state: pd.DataFrame) -> pd.DataFrame:
    """Expose only live Orders while retaining tombstones in stored state."""
    return state.loc[~state["is_deleted"]].drop(
        columns=["is_deleted", "change_lsn", "seqval"]
    ).copy()


def run_demo(events_file: Path, state_file: Path, output_file: Path):
    """Demonstrate applying CDC CSV events and publishing the next state.

    Important: --state is an EXISTING CSV input, whereas --output is a new
    Parquet file. This is not yet a complete persistent CDC restart workflow.
    """
    events = pd.read_csv(events_file, dtype={"change_lsn": "string", "seqval": "string"})
    state = (
        pd.read_csv(state_file, dtype={"change_lsn": "string", "seqval": "string"})
        if state_file.exists()
        else pd.DataFrame()
    )
    next_state = apply_changes(state, events)

    # A safe per-file replacement prevents an interrupted Parquet write from
    # overwriting a valid prior file. It does NOT atomically commit a CDC offset
    # together with the data: Stage 4B+ must implement that guarantee.
    from orders_io import atomic_parquet_write

    atomic_parquet_write(next_state, output_file)
    print(
        f"Applied {len(events)} events; state rows: {len(next_state)}; "
        f"active: {len(active_orders(next_state))}"
    )
    print(f"Published state: {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Offline CDC reducer exercise")
    parser.add_argument("--events", type=Path, required=True, help="CDC events CSV")
    parser.add_argument("--state", type=Path, required=True, help="Existing CDC state CSV (or missing)")
    parser.add_argument("--output", type=Path, required=True, help="New state Parquet")
    args = parser.parse_args()
    run_demo(args.events, args.state, args.output)
