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

LANDING_DIR = Path("data/landing/sqlserver/customers")


def extract_customers():

    query = """
        SELECT
            CustomerID,
            FirstName,
            LastName,
            Email,
            Phone,
            DateOfBirth,
            City,
            Province,
            CreatedAt,
            UpdatedAt
        FROM Customers;
    """

    with pyodbc.connect(CONNECTION_STRING) as connection:

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
        / f"customers_{timestamp}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print("SQL Server extraction completed")
    print(f"Source : {SERVER}/{DATABASE}")
    print("Table  : Customers")
    print(f"Output : {output_file}")
    print(f"Rows   : {len(df):,}")


if __name__ == "__main__":
    extract_customers()