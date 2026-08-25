from pathlib import Path

import pandas as pd


SQL_BRONZE_DIR = Path(
    "data/bronze/sqlserver/customers"
)

JSON_BRONZE_DIR = Path(
    "data/bronze/customer"
)

IDENTITY_FILE = Path(
    "data/silver/customer/customer_identity_map.parquet"
)

OUTPUT_DIR = Path(
    "data/silver/customer"
)


def latest_file(directory, pattern):

    files = list(directory.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No files found in {directory}"
        )

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


def normalize_email(series):

    return (
        series
        .astype("string")
        .str.strip()
        .str.lower()
    )


def normalize_phone(series):

    return (
        series
        .astype("string")
        .str.strip()
    )


def normalize_city(series):

    return (
        series
        .astype("string")
        .str.strip()
        .str.title()
    )


def standardize_province(value):

    if pd.isna(value):
        return value

    value = str(value).strip()

    province_map = {
        "BC": "British Columbia",
        "AB": "Alberta",
        "SK": "Saskatchewan",
        "MB": "Manitoba",
        "ON": "Ontario",
        "QC": "Quebec",
        "NB": "New Brunswick",
        "NS": "Nova Scotia",
        "PE": "Prince Edward Island",
        "NL": "Newfoundland and Labrador",
        "YT": "Yukon",
        "NT": "Northwest Territories",
        "NU": "Nunavut",
    }

    return province_map.get(
        value.upper(),
        value
    )


def build_silver():

    sql_file = latest_file(
        SQL_BRONZE_DIR,
        "customers_*.parquet"
    )

    json_file = latest_file(
        JSON_BRONZE_DIR,
        "customers_*.parquet"
    )

    sql_df = pd.read_parquet(
        sql_file
    )

    json_df = pd.read_parquet(
        json_file
    )

    identity_df = pd.read_parquet(
        IDENTITY_FILE
    )

    # Keep only successfully resolved JSON updates

    identity_df = identity_df[
        identity_df["canonical_customer_id"]
        .notna()
    ].copy()

    resolved_ids = identity_df[
        [
            "json_customer_id",
            "canonical_customer_id",
            "match_method",
            "match_confidence",
        ]
    ]

    json_df = json_df.merge(
        resolved_ids,
        left_on="customer_id",
        right_on="json_customer_id",
        how="inner",
    )

    # SQL Server becomes the base dataset

    sql_df = sql_df.rename(
        columns={
            "CustomerID": "customer_id",
            "FirstName": "first_name",
            "LastName": "last_name",
            "Email": "email",
            "Phone": "phone",
            "DateOfBirth": "date_of_birth",
            "City": "city",
            "Province": "province",
            "CreatedAt": "created_at",
            "UpdatedAt": "updated_at",
        }
    )

    sql_df = sql_df.set_index(
        "customer_id"
    )

    # Apply resolved JSON updates

    for row in json_df.itertuples():

        customer_id = int(
            row.canonical_customer_id
        )

        if customer_id not in sql_df.index:
            continue

        if pd.notna(row.first_name):
            sql_df.at[
                customer_id,
                "first_name"
            ] = row.first_name

        if pd.notna(row.last_name):
            sql_df.at[
                customer_id,
                "last_name"
            ] = row.last_name

        if pd.notna(row.email):
            sql_df.at[
                customer_id,
                "email"
            ] = row.email

        if pd.notna(row.phone):
            sql_df.at[
                customer_id,
                "phone"
            ] = row.phone

        if pd.notna(row.city):
            sql_df.at[
                customer_id,
                "city"
            ] = row.city

        if pd.notna(row.province):
            sql_df.at[
                customer_id,
                "province"
            ] = row.province

        if pd.notna(row.updated_at):

            current_updated_at = sql_df.at[
                customer_id,
                "updated_at"
            ]

            if pd.isna(current_updated_at):

                sql_df.at[
                    customer_id,
                    "updated_at"
                ] = row.updated_at

            elif pd.Timestamp(row.updated_at) > pd.Timestamp(
                current_updated_at
            ):

                sql_df.at[
                    customer_id,
                    "updated_at"
                ] = row.updated_at

    # Data standardization

    sql_df["email"] = normalize_email(
        sql_df["email"]
    )

    sql_df["phone"] = normalize_phone(
        sql_df["phone"]
    )

    sql_df["city"] = normalize_city(
        sql_df["city"]
    )

    sql_df["province"] = sql_df[
        "province"
    ].apply(
        standardize_province
    )

    # Data-quality flags

    sql_df["email_valid"] = (
        sql_df["email"]
        .str.match(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            na=False
        )
    )

    sql_df["phone_present"] = (
        sql_df["phone"].notna()
    )

    sql_df["customer_name_valid"] = (
        sql_df["first_name"].notna()
        & sql_df["last_name"].notna()
    )

    # Lineage

    sql_df["record_source"] = "SQL_SERVER"

    resolved_customer_ids = set(
    json_df["canonical_customer_id"]
    .dropna()
    .astype(int)
    .tolist()
)


    sql_df.loc[
    sql_df.index.isin(resolved_customer_ids),
    "record_source"
] = "SQL_SERVER+JSON"
    
    sql_df["silver_processed_at"] = (
        pd.Timestamp.now("UTC")
    )

    result = sql_df.reset_index()

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR
        / "customers.parquet"
    )

    result.to_parquet(
        output_file,
        index=False
    )

    print(
        "Silver customer transformation completed"
    )

    print(
        f"SQL Server customers : {len(sql_df):,}"
    )

    print(
        f"Resolved JSON updates: {len(json_df):,}"
    )

    print(
        f"Silver rows          : {len(result):,}"
    )

    print(
        f"Invalid emails       : "
        f"{(~result['email_valid']).sum():,}"
    )

    print(
        f"Output               : {output_file}"
    )


if __name__ == "__main__":
    build_silver()