"""Apply CDC-style events to the current Orders state.

This is a local reducer, not a live SQL Server CDC connector.
It keeps the last seen position and delete tombstones so late events cannot revive a deleted order.
"""

# Event ordering, deduping, and delete markers still matter whether the input is a file, a batch, or a stream.

from pathlib import Path

import pandas as pd

# These are the current business fields for an order.
# Insert and update events must include a full after-image for all of them.
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

    # Build an OrderID -> last-known-state map.
    # An empty state means first load; any existing state must already match the key contract.
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

    # Validate and normalize incoming events before applying them.
    # We only accept full I/U after-images and D rows.
    # Update-before images should be removed or mapped upstream.
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

    # Retries can deliver the same event twice.
    # Ignore identical duplicates; a different payload at the same position is a contract violation.
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

    # Apply rows in source order, not file order.
    # If a position is already processed, skip it. This keeps retries idempotent.
    for r in sorted(seen.values(), key=lambda r: (r["change_lsn"], r["seqval"], r["OrderID"])):
        key = r["OrderID"]
        previous = existing.get(key)
        position = (r["change_lsn"], r["seqval"])
        if previous is not None and position <= (previous["change_lsn"], previous["seqval"]):
            continue

        if r["operation"] == "D":
            # Keep the deleted order in state as a tombstone.
            # That stops a stale insert or update from resurrecting it.
            values = {f: previous.get(f) if previous else None for f in FIELDS}
        else:
            # This reducer needs a full after-image, not a partial patch.
            # Otherwise an update can blank fields that were not meant to change.
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

    # Keep the output order stable so tests and repeated runs are deterministic.
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

    # Replace the output file safely so a partial write does not wipe a valid file.
    # This still does not atomically commit the CDC offset with the state change.
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
