"""Validate a Bronze Orders snapshot and publish the current Silver dataset. Invalid rows are flagged and duplicate OrderIDs are rejected."""

# A full snapshot should have one row per OrderID.
# A CDC feed can have multiple rows per OrderID, so it must be reduced before this check.

from pathlib import Path

import pandas as pd
from orders_io import latest_snapshot, atomic_parquet_write


BRONZE_DIR = Path(
    "data/bronze/sqlserver/orders"
)

CUSTOMER_SILVER_FILE = Path(
    "data/silver/customer/customers.parquet"
)

OUTPUT_DIR = Path(
    "data/silver/orders"
)


VALID_STATUSES = {
    "Delivered",
    "Cancelled",
    "Returned",
    "Pending",
    "Processing",
    "Shipped",
}


def latest_bronze_file():
    """Resolve the latest Bronze Orders snapshot for standalone execution; orchestration should pass the exact pinned file instead."""
    return latest_snapshot(BRONZE_DIR, "orders")


def build_silver_orders(orders_file=None):

    """Validate the selected Bronze Orders snapshot, enrich it with customer integrity checks, and publish the resulting Silver dataset."""
    orders_file = Path(orders_file) if orders_file is not None else latest_bronze_file()

    orders = pd.read_parquet(
        orders_file
    )
    if orders["OrderID"].isna().any() or orders["OrderID"].duplicated().any():
        raise ValueError("Orders snapshot has null or duplicate OrderID; refusing to publish Silver")

    customers = pd.read_parquet(
        CUSTOMER_SILVER_FILE,
        columns=["customer_id"]
    )

    # Standardize field types

    orders["OrderID"] = pd.to_numeric(
        orders["OrderID"],
        errors="coerce"
    ).astype("Int64")

    orders["CustomerID"] = pd.to_numeric(
        orders["CustomerID"],
        errors="coerce"
    ).astype("Int64")

    orders["OrderDate"] = pd.to_datetime(
        orders["OrderDate"],
        errors="coerce"
    )

    orders["TotalAmount"] = pd.to_numeric(
        orders["TotalAmount"],
        errors="coerce"
    )

    orders["OrderStatus"] = (
        orders["OrderStatus"]
        .astype("string")
        .str.strip()
        .str.title()
    )

    # Data quality flags

    orders["order_id_valid"] = (
        orders["OrderID"].notna()
    )

    orders["customer_id_valid"] = (
        orders["CustomerID"].notna()
    )

    orders["order_date_valid"] = (
        orders["OrderDate"].notna()
    )

    orders["amount_valid"] = (
        orders["TotalAmount"].notna()
        & (orders["TotalAmount"] >= 0)
    )

    orders["status_valid"] = (
        orders["OrderStatus"].isin(
            VALID_STATUSES
        )
    )

    # Check customer references against the Silver customer data

    valid_customer_ids = set(
        customers["customer_id"]
        .dropna()
        .astype(int)
    )

    orders["customer_exists"] = (
        orders["CustomerID"]
        .isin(valid_customer_ids)
    )

    # Overall record quality

    orders["record_valid"] = (
        orders["order_id_valid"]
        & orders["customer_id_valid"]
        & orders["order_date_valid"]
        & orders["amount_valid"]
        & orders["status_valid"]
        & orders["customer_exists"]
    )

    # Useful derived fields

    orders["order_year"] = (
        orders["OrderDate"].dt.year
    )

    orders["order_month"] = (
        orders["OrderDate"].dt.month
    )

    orders["order_date_only"] = (
        orders["OrderDate"].dt.date
    )

    orders["silver_processed_at"] = (
        pd.Timestamp.now("UTC")
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR
        / "orders.parquet"
    )

    atomic_parquet_write(orders, output_file)

    print("Silver Orders transformation completed")
    print(f"Input rows       : {len(orders):,}")
    print(
        f"Valid records    : "
        f"{orders['record_valid'].sum():,}"
    )
    print(
        f"Invalid records  : "
        f"{(~orders['record_valid']).sum():,}"
    )
    print(
        f"Unknown customers: "
        f"{(~orders['customer_exists']).sum():,}"
    )
    print(
        f"Invalid amounts  : "
        f"{(~orders['amount_valid']).sum():,}"
    )
    print(
        f"Invalid statuses : "
        f"{(~orders['status_valid']).sum():,}"
    )
    print(f"Output           : {output_file}")
    return output_file


if __name__ == "__main__":
    build_silver_orders()