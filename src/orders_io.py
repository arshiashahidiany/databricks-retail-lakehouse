"""Shared file helpers for the Orders snapshot pipeline."""

# Safe file writes protect one artifact at a time.
# They do not make the data write and SQLite state update a single transaction.
from pathlib import Path
import os
import uuid
import pandas as pd


def latest_snapshot(directory: Path, prefix: str) -> Path:
    """Select the newest timestamped snapshot by filename, not filesystem mtime."""
    files = list(directory.glob(f"{prefix}_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No {prefix} snapshots in {directory}")
    return max(files, key=lambda f: f.name)


def atomic_parquet_write(frame: pd.DataFrame, target: Path) -> None:
    """Write beside target, then atomically replace on the same filesystem."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
