"""Local SQLite run registry for the snapshot Orders pipeline.

Single-machine prototype only; database writes and dataset publication are NOT
one atomic transaction. An interrupted stage may be safely re-run.
"""

# OPERATIONAL STATE: The stage table records the output artifact checksum.
# A prior SUCCEEDED status is insufficient unless the file still matches.
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = Path("data/operational/orders_runs.sqlite")
STAGES = ("extract", "bronze", "silver")


def utc_now():
    """Return an unambiguous UTC timestamp for operational records."""
    return datetime.now(timezone.utc).isoformat()


def connect(db=DEFAULT_DB):
    """Open the SQLite registry and create or migrate its control tables so existing Stage 3B databases remain readable."""
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
        updated_at TEXT NOT NULL, error TEXT, output_sha256 TEXT,
        PRIMARY KEY (run_id, stage),
        FOREIGN KEY (run_id) REFERENCES orders_runs(run_id))""")
    # Backward-compatible upgrade for the Stage 3B SQLite file.
    existing = {row[1] for row in conn.execute("PRAGMA table_info(orders_stages)")}
    if "output_sha256" not in existing:
        conn.execute("ALTER TABLE orders_stages ADD COLUMN output_sha256 TEXT")
    conn.commit()
    return conn


def create_run(conn, run_id):
    """Register a new pipeline run before any extract or stage is executed."""
    now = utc_now()
    conn.execute("INSERT INTO orders_runs (run_id, status, created_at, updated_at) VALUES (?,?,?,?)",
                 (run_id, "RUNNING", now, now))
    conn.commit()


def stage(conn, run_id, name):
    """Read the persisted status, file paths, row count and checksum for one stage of a run."""
    return conn.execute("SELECT status, input_path, output_path, row_count, output_sha256 FROM orders_stages WHERE run_id=? AND stage=?",
                        (run_id, name)).fetchone()


def file_sha256(path):
    """Hash the complete output file in bounded chunks so resumed runs can detect an overwritten or modified artifact."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def record_stage(conn, run_id, name, status, input_path=None, output_path=None, row_count=None, error=None, output_sha256=None):
    """Upsert a stage result in SQLite. This database transaction is separate from the output-file write."""
    now = utc_now()
    conn.execute("""INSERT INTO orders_stages
        (run_id,stage,status,input_path,output_path,row_count,updated_at,error,output_sha256)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id,stage) DO UPDATE SET
        status=excluded.status, input_path=excluded.input_path,
        output_path=excluded.output_path, row_count=excluded.row_count,
        updated_at=excluded.updated_at, error=excluded.error,
        output_sha256=excluded.output_sha256""",
        (run_id, name, status, str(input_path) if input_path else None,
         str(output_path) if output_path else None, row_count, now, error, output_sha256))
    conn.commit()


def set_run_status(conn, run_id, status, error=None):
    """Persist the final or current overall status of a pipeline run."""
    conn.execute("UPDATE orders_runs SET status=?, updated_at=?, error=? WHERE run_id=?",
                 (status, utc_now(), error, run_id))
    conn.commit()
