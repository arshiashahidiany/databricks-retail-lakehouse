"""Run/restart the complete local Orders snapshot pipeline.

Usage: python src/run_orders_pipeline.py
       python src/run_orders_pipeline.py --run-id <printed-run-id>

Run from the repository root. Avoid running two copies concurrently.
"""
import argparse
import uuid
from pathlib import Path

import pandas as pd
from bronze_orders import process_to_bronze
from orders_runs import connect, create_run, stage, record_stage, set_run_status
from silver_orders import build_silver_orders


def run_pipeline(run_id=None, db=None, extract_fn=None,
                 bronze_fn=process_to_bronze, silver_fn=build_silver_orders):
    if extract_fn is None:
        from extract_orders import extract_orders
        extract_fn = extract_orders
    run_id = run_id or uuid.uuid4().hex
    conn = connect(db) if db else connect()
    try:
        existing = conn.execute("SELECT status FROM orders_runs WHERE run_id=?", (run_id,)).fetchone()
        if not existing:
            create_run(conn, run_id)
        elif existing[0] == "SUCCEEDED":
            print(f"Run {run_id} already succeeded. No reprocessing.")
            return run_id
        set_run_status(conn, run_id, "RUNNING")
        print(f"Run ID: {run_id}")

        previous_output = None
        for name, task in (("extract", lambda _: extract_fn()),
                           ("bronze", bronze_fn), ("silver", silver_fn)):
            old = stage(conn, run_id, name)
            if old and old[0] == "SUCCEEDED" and old[2] and Path(old[2]).exists():
                # The stage has a durable published output. Reuse it for this run.
                previous_output = Path(old[2])
                print(f"{name}: already complete ({previous_output})")
                continue
            if name != "extract" and (previous_output is None or not previous_output.exists()):
                raise FileNotFoundError(f"Missing prior stage output for {name}: {previous_output}")
            record_stage(conn, run_id, name, "RUNNING", input_path=previous_output)
            try:
                output = Path(task(previous_output))
                if not output.is_file():
                    raise FileNotFoundError(f"{name} did not publish an output file: {output}")
                count = len(pd.read_parquet(output))
                record_stage(conn, run_id, name, "SUCCEEDED", input_path=previous_output,
                             output_path=output, row_count=count)
                previous_output = output
            except Exception as exc:
                record_stage(conn, run_id, name, "FAILED", input_path=previous_output,
                             error=f"{type(exc).__name__}: {exc}")
                raise
        set_run_status(conn, run_id, "SUCCEEDED")
        print(f"Run {run_id}: SUCCEEDED")
        return run_id
    except Exception as exc:
        set_run_status(conn, run_id, "FAILED", f"{type(exc).__name__}: {exc}")
        print(f"Run {run_id}: FAILED — {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="Resume an existing failed run")
    args = parser.parse_args()
    run_pipeline(args.run_id)
