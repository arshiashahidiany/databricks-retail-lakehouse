from pathlib import Path

import pandas as pd


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

    files = list(
        BRONZE_DIR.glob("orders_*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "No Orders Bronze files found."
        )

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


def build_silver_orders():

    orders_file = latest_bronze_file()

    orders = pd.read_parquet(
        orders_file
    )

    customers = pd.read_parquet(
        CUSTOMER_SILVER_FILE,
        columns=["customer_id"]
    )

    # Standardize types

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

    # Referential integrity against Silver Customer

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

    orders.to_parquet(
        output_file,
        index=False
    )

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


if __name__ == "__main__":
    build_silver_orders()