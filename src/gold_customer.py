from pathlib import Path

import pandas as pd


SILVER_FILE = Path(
    "data/silver/customer/customers.parquet"
)

GOLD_DIR = Path(
    "data/gold/customer"
)


def build_customer_360():

    df = pd.read_parquet(SILVER_FILE)

    gold_columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "province",
        "email_valid",
        "updated_at",
    ]

    gold_df = df[gold_columns].copy()

    gold_df = gold_df.sort_values(
        "customer_id"
    )

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        GOLD_DIR / "customer_360.parquet"
    )

    gold_df.to_parquet(
        output_file,
        index=False
    )

    print("Gold customer 360 created")
    print(f"Output: {output_file}")
    print(f"Rows: {len(gold_df):,}")
    print(f"Columns: {len(gold_df.columns)}")


if __name__ == "__main__":
    build_customer_360()