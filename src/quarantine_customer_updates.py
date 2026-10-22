"""Send customer updates that cannot be matched to quarantine for review."""

# Keep unmatched updates traceable.
# Dropping them hides a possible data quality problem.

from pathlib import Path

import pandas as pd


JSON_BRONZE_DIR = Path("data/bronze/customer")
IDENTITY_FILE = Path(
    "data/silver/customer/customer_identity_map.parquet"
)
OUTPUT_DIR = Path("data/quarantine/customer")


def latest_json_file():
    """Find the most recent landed JSON extract to inspect for unresolved customers."""
    files = list(
        JSON_BRONZE_DIR.glob("customers_*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "No JSON Bronze customer files found."
        )

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


def quarantine_unmatched():

    """Write unresolvable customer updates to quarantine rather than silently joining them to an incorrect customer."""
    json_file = latest_json_file()

    json_df = pd.read_parquet(json_file)

    identity_df = pd.read_parquet(
        IDENTITY_FILE
    )

    unmatched_ids = identity_df.loc[
        identity_df["match_method"] == "UNMATCHED",
        "json_customer_id"
    ]

    quarantine_df = json_df[
        json_df["customer_id"].isin(unmatched_ids)
    ].copy()

    quarantine_df["quarantine_reason"] = (
        "CUSTOMER_IDENTITY_UNMATCHED"
    )

    quarantine_df["quarantine_timestamp"] = (
        pd.Timestamp.now("UTC")
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR
        / "unmatched_customer_updates.parquet"
    )

    quarantine_df.to_parquet(
        output_file,
        index=False
    )

    print("Customer quarantine completed")
    print(f"Input updates : {len(json_df):,}")
    print(
        f"Unmatched     : {len(quarantine_df):,}"
    )
    print(f"Output        : {output_file}")


if __name__ == "__main__":
    quarantine_unmatched()