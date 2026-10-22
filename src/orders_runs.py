"""Local SQLite run registry for the snapshot Orders pipeline.

Single-machine prototype only; database writes and dataset publication are NOT
one atomic transaction. An interrupted stage may be safely re-run.
"""
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = Path("data/operational/orders_runs.sqlite")
STAGES = ("extract", "bronze", "silver")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def connect(db=DEFAULT_DB):
    db = Path(db)
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""CREATE TABLE IF NOT EXISTS orders_runs (
        run_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, error TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS orders_stages (
        run_id TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL,
        input_path TEXT, output_path TEXT, row_count INTEGER,
        updated_at TEXT NOT NULL, error TEXT,
        PRIMARY KEY (run_id, stage),
        FOREIGN KEY (run_id) REFERENCES orders_runs(run_id))""")
    conn.commit()
    return conn


def create_run(conn, run_id):
    now = utc_now()
    conn.execute("INSERT INTO orders_runs (run_id, status, created_at, updated_at) VALUES (?,?,?,?)",
                 (run_id, "RUNNING", now, now))
    conn.commit()


def stage(conn, run_id, name):
    return conn.execute("SELECT status, input_path, output_path, row_count FROM orders_stages WHERE run_id=? AND stage=?",
                        (run_id, name)).fetchone()


def record_stage(conn, run_id, name, status, input_path=None, output_path=None, row_count=None, error=None):
    now = utc_now()
    conn.execute("""INSERT INTO orders_stages
        (run_id,stage,status,input_path,output_path,row_count,updated_at,error)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id,stage) DO UPDATE SET
        status=excluded.status, input_path=excluded.input_path,
        output_path=excluded.output_path, row_count=excluded.row_count,
        updated_at=excluded.updated_at, error=excluded.error""",
        (run_id, name, status, str(input_path) if input_path else None,
         str(output_path) if output_path else None, row_count, now, error))
    conn.commit()


def set_run_status(conn, run_id, status, error=None):
    conn.execute("UPDATE orders_runs SET status=?, updated_at=?, error=? WHERE run_id=?",
                 (status, utc_now(), error, run_id))
    conn.commit()
