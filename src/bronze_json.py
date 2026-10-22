"""Convert a landed JSON customer extract to the Bronze Parquet format. Bronze should retain source attributes and the metadata needed to trace a record back to its incoming file. This prototype selects one landing file, not a stream of incremental events."""

# CURRENT LIMITATION: This file-level Bronze flow does not independently
# guarantee exactly-once processing when sources resend a JSON extract.

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


LANDING_DIR = Path("data/landing/json")
BRONZE_DIR = Path("data/bronze/customer")


def get_latest_landing_file():
    """Find the newest landed file matching the expected source pattern. The current policy assumes landing filenames and arrival order are trustworthy."""
    files = list(LANDING_DIR.glob("customer_updates_*.json"))

    if not files:
        raise FileNotFoundError(
            "No customer JSON files found in landing."
        )

    return max(files, key=lambda file: file.stat().st_mtime)


def process_to_bronze():

    """Read the chosen landing extract, add Bronze ingestion context where implemented, and publish a Parquet snapshot."""
    landing_file = get_latest_landing_file()

    with open(landing_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    ingestion_timestamp = datetime.now(timezone.utc)

    df["source_file"] = landing_file.name
    df["ingestion_timestamp"] = ingestion_timestamp

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    output_file = (
        BRONZE_DIR
        / f"customers_{ingestion_timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print("Bronze ingestion completed")
    print(f"Input : {landing_file}")
    print(f"Output: {output_file}")
    print(f"Rows  : {len(df):,}")


if __name__ == "__main__":
    process_to_bronze()