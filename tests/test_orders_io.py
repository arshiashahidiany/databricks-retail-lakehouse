"""Automated regression tests for RetailCo ingestion and recovery. Tests use isolated fixtures where possible instead of a live SQL Server database."""

import sys
from pathlib import Path
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from orders_io import latest_snapshot, atomic_parquet_write


def test_latest_snapshot_uses_snapshot_id_not_mtime(tmp_path):
    """Verify snapshot ID ordering remains correct even when filesystem timestamps disagree."""
    old = tmp_path / "orders_20260825_010000_000000.parquet"
    new = tmp_path / "orders_20260825_020000_000000.parquet"
    old.touch()
    new.touch()
    import os
    os.utime(old, (9999999999, 9999999999))
    assert latest_snapshot(tmp_path, "orders") == new


def test_atomic_write_replaces_output(tmp_path, monkeypatch):
    """Ensure a completed atomic write publishes the new output."""
    target = tmp_path / "orders.parquet"
    def fake_to_parquet(self, path, index=False):
        """Support this module's processing or test workflow; inspect callers for the expected input and output contract."""
        Path(path).write_text(",".join(map(str, self["OrderID"].tolist())))
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    atomic_parquet_write(pd.DataFrame({"OrderID": [1]}), target)
    atomic_parquet_write(pd.DataFrame({"OrderID": [1, 2]}), target)
    assert target.read_text() == "1,2"
    assert len(list(tmp_path.iterdir())) == 1


def test_latest_snapshot_missing_raises(tmp_path):
    """Fail loudly when no source snapshot exists."""
    with pytest.raises(FileNotFoundError):
        latest_snapshot(tmp_path, "orders")
