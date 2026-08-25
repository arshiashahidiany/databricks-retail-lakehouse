from pathlib import Path

import pandas as pd


SQL_BRONZE_DIR = Path(
    "data/bronze/sqlserver/customers"
)

JSON_BRONZE_DIR = Path(
    "data/bronze/customer"
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
        .str.replace(
            r"\D",
            "",
            regex=True
        )
        .replace(
            "",
            pd.NA
        )
    )


def normalize_name(series):

    return (
        series
        .astype("string")
        .str.strip()
        .str.lower()
    )


def resolve_identity():

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

    # Normalize identifiers

    sql_df["match_email"] = normalize_email(
        sql_df["Email"]
    )

    json_df["match_email"] = normalize_email(
        json_df["email"]
    )

    sql_df["match_phone"] = normalize_phone(
        sql_df["Phone"]
    )

    json_df["match_phone"] = normalize_phone(
        json_df["phone"]
    )

    sql_df["match_first_name"] = normalize_name(
        sql_df["FirstName"]
    )

    sql_df["match_last_name"] = normalize_name(
        sql_df["LastName"]
    )

    json_df["match_first_name"] = normalize_name(
        json_df["first_name"]
    )

    json_df["match_last_name"] = normalize_name(
        json_df["last_name"]
    )

    sql_df["match_dob"] = pd.to_datetime(
        sql_df["DateOfBirth"],
        errors="coerce"
    )

    # Email lookup

    email_lookup = (
        sql_df[
            ["CustomerID", "match_email"]
        ]
        .drop_duplicates(
            subset=["match_email"]
        )
        .set_index("match_email")[
            "CustomerID"
        ]
        .to_dict()
    )

    # Phone lookup

    phone_lookup = (
        sql_df[
            ["CustomerID", "match_phone"]
        ]
        .dropna(
            subset=["match_phone"]
        )
        .drop_duplicates(
            subset=["match_phone"]
        )
        .set_index("match_phone")[
            "CustomerID"
        ]
        .to_dict()
    )

    # Name + DOB lookup

    name_dob_lookup = (
        sql_df[
            [
                "CustomerID",
                "match_first_name",
                "match_last_name",
                "match_dob",
            ]
        ]
        .dropna(
            subset=[
                "match_first_name",
                "match_last_name",
                "match_dob",
            ]
        )
        .drop_duplicates(
            subset=[
                "match_first_name",
                "match_last_name",
                "match_dob",
            ]
        )
    )

    name_dob_lookup = {
        (
            row.match_first_name,
            row.match_last_name,
            row.match_dob,
        ): row.CustomerID
        for row in name_dob_lookup.itertuples()
    }

    results = []

    for row in json_df.itertuples():

        customer_id = None
        match_method = "UNMATCHED"
        confidence = 0.0

        # 1. Email

        if row.match_email in email_lookup:

            customer_id = email_lookup[
                row.match_email
            ]

            match_method = "EMAIL"
            confidence = 1.0

        # 2. Phone

        elif (
            pd.notna(row.match_phone)
            and row.match_phone in phone_lookup
        ):

            customer_id = phone_lookup[
                row.match_phone
            ]

            match_method = "PHONE"
            confidence = 0.95

        # 3. Name + DOB

        elif hasattr(row, "match_dob"):

            key = (
                row.match_first_name,
                row.match_last_name,
                row.match_dob,
            )

            if key in name_dob_lookup:

                customer_id = name_dob_lookup[
                    key
                ]

                match_method = "NAME_DOB"
                confidence = 0.90

        results.append(
            {
                "json_customer_id": row.customer_id,
                "canonical_customer_id": customer_id,
                "match_method": match_method,
                "match_confidence": confidence,
            }
        )

    result_df = pd.DataFrame(
        results
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR
        / "customer_identity_map.parquet"
    )

    result_df.to_parquet(
        output_file,
        index=False
    )

    print("Customer identity resolution completed")
    print(
        f"JSON records: {len(result_df):,}"
    )

    print("\nMatch methods:")

    print(
        result_df[
            "match_method"
        ].value_counts()
    )

    print(
        "\nUnmatched:",
        (
            result_df[
                "match_method"
            ] == "UNMATCHED"
        ).sum()
    )

    print(
        f"\nOutput: {output_file}"
    )


if __name__ == "__main__":
    resolve_identity()