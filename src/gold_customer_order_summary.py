from pathlib import Path

import pandas as pd


CUSTOMER_FILE = Path(
    "data/silver/customer/customers.parquet"
)

ORDERS_FILE = Path(
    "data/silver/orders/orders.parquet"
)

OUTPUT_DIR = Path(
    "data/gold/customer"
)


def build_customer_order_summary():

    customers = pd.read_parquet(
        CUSTOMER_FILE,
        columns=[
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "province",
        ],
    )

    orders = pd.read_parquet(
        ORDERS_FILE,
        columns=[
            "OrderID",
            "CustomerID",
            "OrderDate",
            "OrderStatus",
            "TotalAmount",
        ],
    )

    # Aggregate order metrics by customer

    order_summary = (
        orders
        .groupby("CustomerID")
        .agg(
            order_count=(
                "OrderID",
                "count",
            ),
            total_spend=(
                "TotalAmount",
                "sum",
            ),
            average_order_value=(
                "TotalAmount",
                "mean",
            ),
            first_order_date=(
                "OrderDate",
                "min",
            ),
            last_order_date=(
                "OrderDate",
                "max",
            ),
        )
        .reset_index()
    )

    # Status-specific counts

    status_counts = (
        pd.crosstab(
            orders["CustomerID"],
            orders["OrderStatus"],
        )
        .reset_index()
    )

    status_counts = status_counts.rename(
        columns={
            "CustomerID": "customer_id",
            "Delivered": "delivered_order_count",
            "Cancelled": "cancelled_order_count",
            "Returned": "returned_order_count",
            "Pending": "pending_order_count",
            "Processing": "processing_order_count",
            "Shipped": "shipped_order_count",
        }
    )

    # Delivered spend

    delivered_spend = (
        orders[
            orders["OrderStatus"] == "Delivered"
        ]
        .groupby("CustomerID")[
            "TotalAmount"
        ]
        .sum()
        .reset_index(
            name="delivered_spend"
        )
        .rename(
            columns={
                "CustomerID": "customer_id"
            }
        )
    )

    # Join everything back to Customer

    result = customers.merge(
        order_summary,
        left_on="customer_id",
        right_on="CustomerID",
        how="left",
    )

    result = result.drop(
        columns=["CustomerID"]
    )

    result = result.merge(
        status_counts,
        on="customer_id",
        how="left",
    )

    result = result.merge(
        delivered_spend,
        on="customer_id",
        how="left",
    )

    # Customers with no orders

    count_columns = [
        "order_count",
        "delivered_order_count",
        "cancelled_order_count",
        "returned_order_count",
        "pending_order_count",
        "processing_order_count",
        "shipped_order_count",
    ]

    for column in count_columns:

        result[column] = (
            result[column]
            .fillna(0)
            .astype("int64")
        )

    result["total_spend"] = (
        result["total_spend"]
        .fillna(0)
    )

    result["delivered_spend"] = (
        result["delivered_spend"]
        .fillna(0)
    )

    result["average_order_value"] = (
        result["average_order_value"]
        .fillna(0)
    )

    # Useful customer-level metrics

    result["customer_lifetime_days"] = (
        result["last_order_date"]
        - result["first_order_date"]
    ).dt.days

    result["customer_lifetime_days"] = (
        result["customer_lifetime_days"]
        .fillna(0)
        .astype("int64")
    )

    result["gold_processed_at"] = (
        pd.Timestamp.now("UTC")
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR
        / "customer_order_summary.parquet"
    )

    result.to_parquet(
        output_file,
        index=False
    )

    print(
        "Gold customer order summary completed"
    )

    print(
        f"Customers : {len(result):,}"
    )

    print(
        f"Customers with orders : "
        f"{(result['order_count'] > 0).sum():,}"
    )

    print(
        f"Customers without orders : "
        f"{(result['order_count'] == 0).sum():,}"
    )

    print(
        f"Total orders : "
        f"{result['order_count'].sum():,}"
    )

    print(
        f"Total spend : "
        f"{result['total_spend'].sum():,.2f}"
    )

    print(
        f"Output : {output_file}"
    )


if __name__ == "__main__":
    build_customer_order_summary()