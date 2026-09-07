from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

def extract_metadata(schema):
    primary_keys = []
    hash_key_keys = []
    pii_columns = []
    customer_identifier_columns = []

    for field in schema.fields:
        md = field.metadata
        if md.get("primary_key", False):
            primary_keys.append(field.name)
        if md.get("in_hash_key", False):
            hash_key_keys.append(field.name)
        if md.get("is_pii", False):
            pii_columns.append(field.name)
        if md.get("customer_identifier", False):
            customer_identifier_columns.append(field.name)

    return primary_keys, hash_key_keys, pii_columns, customer_identifier_columns

def build_data_vault(df, schema):
    primary_keys, hash_key_keys, _, customer_identifier_cols = extract_metadata(schema)

    for c in ["HubKey", "DIKey", "sha_key"]:
        if c in df.columns:
            df = df.drop(c)

    business_key_cols = primary_keys.copy()
    if "CustomerId" not in business_key_cols:
        business_key_cols.append("CustomerId")

    satellite_columns = [
        c for c in df.columns
        if c not in primary_keys
        and c != "CustomerId"
        and c != "InstanceId"
    ]

    hash_columns = hash_key_keys.copy() or satellite_columns.copy()

    df = df.withColumn(
        "HubKey",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit(""))
                    for c in business_key_cols
                ]
            ),
            256
        )
    )

    di_hash_cols = [
        "CustomerId",
        "InstanceId",
        *customer_identifier_cols
    ]

    df = df.withColumn(
        "DIKey",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit(""))
                    for c in di_hash_cols
                ]
            ),
            256
        )
    )

    di_df = (
        df.select(
            "DIKey",
            "HubKey",
            "CustomerId",
            "InstanceId",
            *customer_identifier_cols
        )
        .dropDuplicates()
        .withColumn("LoadDate", current_timestamp())
    )

    MASK_SALT = "DATALAKE_V1"

    for c in customer_identifier_cols:
        df = df.withColumn(
            c,
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(MASK_SALT),
                    F.coalesce(F.col(c).cast("string"), F.lit(""))
                ),
                256
            )
        )

    df = df.withColumn(
        "sha_key",
        F.sha2(
            F.concat_ws(
                "||",
                *(
                    [
                        F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in hash_columns
                    ]
                    + [F.col("DIKey")]
                )
            ),
            256
        )
    )

    hub_df = (
        df.select("HubKey", *business_key_cols)
        .dropDuplicates()
        .withColumn("LoadDate", current_timestamp())
    )

    satellite_df = (
        df.select(
            "HubKey",
            "DIKey",
            *satellite_columns,
            "sha_key"
        )
        .withColumn("LoadDate", current_timestamp())
    )

    return hub_df, satellite_df, di_df
