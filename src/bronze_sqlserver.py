from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


LANDING_DIR = Path(
    "data/landing/sqlserver/customers"
)

BRONZE_DIR = Path(
    "data/bronze/sqlserver/customers"
)


def get_latest_landing_file():

    files = list(
        LANDING_DIR.glob("customers_*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "No SQL Server customer files found in landing."
        )

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


def process_to_bronze():

    landing_file = get_latest_landing_file()

    df = pd.read_parquet(
        landing_file
    )

    ingestion_timestamp = datetime.now(
        timezone.utc
    )

    df["source_system"] = "SQL_SERVER"
    df["source_table"] = "Customers"
    df["source_file"] = landing_file.name
    df["ingestion_timestamp"] = ingestion_timestamp

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        BRONZE_DIR
        / f"customers_{ingestion_timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print("SQL Server Bronze ingestion completed")
    print(f"Input : {landing_file}")
    print(f"Output: {output_file}")
    print(f"Rows  : {len(df):,}")


if __name__ == "__main__":
    process_to_bronze()