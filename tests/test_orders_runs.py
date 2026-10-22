from pathlib import Path
import sys
import sqlite3
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from run_orders_pipeline import run_pipeline


def test_pinned_snapshot_and_resume(tmp_path, monkeypatch):
    # Exercise runner semantics without requiring optional Parquet dependencies.
    monkeypatch.setattr(pd, "read_parquet", pd.read_pickle)
    db = tmp_path / "runs.sqlite"
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    c = tmp_path / "c.parquet"
    calls = {"extract": 0, "bronze": 0, "silver": 0}
    fail = {"once": True}

    def extract():
        calls["extract"] += 1
        pd.DataFrame({"OrderID": [1]}).to_pickle(a)
        return a

    def bronze(input_path):
        calls["bronze"] += 1
        assert input_path == a
        pd.read_parquet(input_path).to_pickle(b)
        return b

    def silver(input_path):
        calls["silver"] += 1
        assert input_path == b
        if fail["once"]:
            fail["once"] = False
            raise RuntimeError("transient failure")
        pd.read_parquet(input_path).to_pickle(c)
        return c

    with pytest.raises(RuntimeError):
        run_pipeline("run-001", db, extract, bronze, silver)
    assert calls == {"extract": 1, "bronze": 1, "silver": 1}
    assert run_pipeline("run-001", db, extract, bronze, silver) == "run-001"
    assert calls == {"extract": 1, "bronze": 1, "silver": 2}
    assert run_pipeline("run-001", db, extract, bronze, silver) == "run-001"
    assert calls == {"extract": 1, "bronze": 1, "silver": 2}
    with sqlite3.connect(db) as con:
        assert con.execute("SELECT status FROM orders_runs").fetchone()[0] == "SUCCEEDED"
        assert con.execute("SELECT COUNT(*) FROM orders_stages WHERE status='SUCCEEDED'").fetchone()[0] == 3
