import pyodbc
import pandas as pd
from datetime import datetime
from pathlib import Path


SERVER = "DESKTOP-H3LQUJ9"
DATABASE = "RetailCo"

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

LANDING_DIR = Path(
    "data/landing/sqlserver/orders"
)


def extract_orders():

    query = """
        SELECT
            OrderID,
            CustomerID,
            OrderDate,
            OrderStatus,
            TotalAmount
        FROM Orders;
    """

    with pyodbc.connect(
        CONNECTION_STRING
    ) as connection:

        df = pd.read_sql(
            query,
            connection
        )

    LANDING_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_file = (
        LANDING_DIR
        / f"orders_{timestamp}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print("SQL Server Orders extraction completed")
    print(f"Source : {SERVER}/{DATABASE}")
    print("Table  : Orders")
    print(f"Output : {output_file}")
    print(f"Rows   : {len(df):,}")


if __name__ == "__main__":
    extract_orders()