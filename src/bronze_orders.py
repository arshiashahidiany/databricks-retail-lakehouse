from pathlib import Path

import pandas as pd
from orders_io import latest_snapshot, atomic_parquet_write


LANDING_DIR = Path(
    "data/landing/sqlserver/orders"
)

BRONZE_DIR = Path(
    "data/bronze/sqlserver/orders"
)


def latest_landing_file():
    return latest_snapshot(LANDING_DIR, "orders")


def process_to_bronze():

    landing_file = latest_landing_file()

    df = pd.read_parquet(
        landing_file
    )

    df["source_system"] = "SQL_SERVER"
    df["source_table"] = "Orders"
    df["source_file"] = landing_file.name
    df["ingestion_timestamp"] = (
        pd.Timestamp.now("UTC")
    )

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        BRONZE_DIR
        / landing_file.name
    )

    atomic_parquet_write(df, output_file)

    print("Orders Bronze transformation completed")
    print(f"Input : {landing_file}")
    print(f"Output: {output_file}")
    print(f"Rows  : {len(df):,}")


if __name__ == "__main__":
    process_to_bronze()