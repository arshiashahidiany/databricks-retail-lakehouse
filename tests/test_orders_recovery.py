"""Fault-injection tests: no SQL Server or Parquet driver required."""
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from run_orders_pipeline import run_pipeline
from orders_io import atomic_parquet_write
from orders_runs import connect


def simulated_pipeline(tmp_path, monkeypatch):
    monkeypatch.setattr(pd, 'read_parquet', pd.read_pickle)
    events = {'extract': 0, 'bronze': 0, 'silver': 0}
    landing, bronze, silver = [tmp_path / f'{x}.parquet' for x in ('landing', 'bronze', 'silver')]
    failure = {'silver': False}

    def extract():
        events['extract'] += 1
        pd.DataFrame({'OrderID': [1, 2]}).to_pickle(landing)
        return landing

    def to_bronze(source):
        events['bronze'] += 1
        assert source == landing
        pd.read_pickle(source).to_pickle(bronze)
        return bronze

    def to_silver(source):
        events['silver'] += 1
        assert source == bronze
        if failure['silver']:
            raise RuntimeError('simulated Silver outage')
        pd.read_pickle(source).to_pickle(silver)
        return silver

    return (extract, to_bronze, to_silver), events, failure, (landing, bronze, silver)


def test_failure_then_resume_skips_completed_stages(tmp_path, monkeypatch):
    funcs, calls, failure, paths = simulated_pipeline(tmp_path, monkeypatch)
    db = tmp_path / 'tracking.sqlite'
    failure['silver'] = True
    with pytest.raises(RuntimeError, match='simulated Silver outage'):
        run_pipeline('recover-1', db, *funcs)
    assert calls == {'extract': 1, 'bronze': 1, 'silver': 1}
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status FROM orders_runs WHERE run_id='recover-1'").fetchone()[0] == 'FAILED'
    failure['silver'] = False
    run_pipeline('recover-1', db, *funcs)
    assert calls == {'extract': 1, 'bronze': 1, 'silver': 2}
    run_pipeline('recover-1', db, *funcs)
    assert calls == {'extract': 1, 'bronze': 1, 'silver': 2}
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT COUNT(*) FROM orders_stages WHERE output_sha256 IS NOT NULL').fetchone()[0] == 3


def test_resume_rejects_tampered_bronze(tmp_path, monkeypatch):
    funcs, calls, failure, paths = simulated_pipeline(tmp_path, monkeypatch)
    db = tmp_path / 'tracking.sqlite'
    failure['silver'] = True
    with pytest.raises(RuntimeError):
        run_pipeline('tamper-1', db, *funcs)
    paths[1].write_bytes(b'overwritten by another process')
    failure['silver'] = False
    with pytest.raises(RuntimeError, match='bronze output changed'):
        run_pipeline('tamper-1', db, *funcs)
    assert calls == {'extract': 1, 'bronze': 1, 'silver': 1}


def test_completed_run_rejects_replaced_silver(tmp_path, monkeypatch):
    funcs, calls, failure, paths = simulated_pipeline(tmp_path, monkeypatch)
    db = tmp_path / 'tracking.sqlite'
    run_pipeline('done-1', db, *funcs)
    paths[2].write_bytes(b'corrupt')
    with pytest.raises(RuntimeError, match='modified silver output'):
        run_pipeline('done-1', db, *funcs)


def test_db_schema_upgrades_existing_stage3b_database(tmp_path):
    path = tmp_path / 'legacy.sqlite'
    with sqlite3.connect(path) as con:
        con.execute('''CREATE TABLE orders_stages (
            run_id TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL,
            input_path TEXT, output_path TEXT, row_count INTEGER,
            updated_at TEXT NOT NULL, error TEXT,
            PRIMARY KEY (run_id, stage))''')
    with connect(path) as conn:
        columns = {row[1] for row in conn.execute('PRAGMA table_info(orders_stages)')}
    assert 'output_sha256' in columns


def test_failed_atomic_write_preserves_previous_snapshot(tmp_path, monkeypatch):
    target = tmp_path / 'orders.parquet'
    target.write_bytes(b'known-good-snapshot')

    def interrupted_write(self, path, index=False):
        Path(path).write_bytes(b'incomplete')
        raise OSError('disk full')

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', interrupted_write)
    with pytest.raises(OSError, match='disk full'):
        atomic_parquet_write(pd.DataFrame({'OrderID': [1]}), target)
    assert target.read_bytes() == b'known-good-snapshot'
    assert list(tmp_path.iterdir()) == [target]
