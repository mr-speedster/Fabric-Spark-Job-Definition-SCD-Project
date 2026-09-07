from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, broadcast

def merge_delta_hub(spark, table_name, source_df):
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(
            source_df.alias("source"),
            "target.HubKey = source.HubKey"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def merge_delta_di(spark, table_name, source_df):
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(
            source_df.alias("source"),
            "target.DIKey = source.DIKey"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def merge_delta_scd2(
    spark,
    sat_table,
    source_df,
    affected_customers,
    hub_table
):
    delta_table = DeltaTable.forName(spark, sat_table)
    target_df = delta_table.toDF()

    target_active = (
        target_df
        .filter("SCD_Current = true")
        .select("HubKey", "sha_key")
    ).cache()

    source_with_flag = (
        source_df.withColumn("src_present", lit(1))
    ).cache()

    join_condition = [
        source_with_flag["HubKey"] == target_active["HubKey"],
        source_with_flag["sha_key"] == target_active["sha_key"]
    ]

    changed_df = (
        source_with_flag.alias("src")
        .join(
            target_active.alias("tgt"),
            on=join_condition,
            how="left_anti"
        )
    ).cache()

    hub_df = (
        DeltaTable.forName(spark, hub_table)
        .toDF()
        .select("CustomerId", "HubKey")
    )

    affected_hub_keys_df = (
        hub_df
        .join(
            broadcast(affected_customers),
            on="CustomerId",
            how="inner"
        )
        .select("HubKey")
        .distinct()
    )

    source_cols = source_with_flag.columns

    insert_map = {
        **{
            c: f"source.{c}"
            for c in source_cols
            if c != "src_present"
        },
        "SCD_Start_Date": "to_date(source.CreatedOn)",
        "SCD_End_Date": "cast(null as timestamp)",
        "SCD_Current": "true"
    }

    staged_updates = changed_df.selectExpr("sha_key as mergeKey", "*")
    staged_inserts = changed_df.selectExpr("NULL as mergeKey", "*")
    staged_passthrough = source_with_flag.selectExpr("sha_key as mergeKey", "*")

    staged_df = (
        staged_updates
        .unionByName(staged_inserts)
        .unionByName(staged_passthrough)
    ).cache()

    merge_condition = '''
        target.HubKey = source.HubKey
        AND target.sha_key = source.mergeKey
        AND target.SCD_Current = true
    '''

    (
        delta_table.alias("target")
        .merge(staged_df.alias("source"), merge_condition)
        .whenMatchedUpdate(
            condition='''
                source.mergeKey IS NOT NULL
                AND target.sha_key <> source.sha_key
            ''',
            set={
                "SCD_End_Date": "to_date(source.CreatedOn)",
                "SCD_Current": "false"
            }
        )
        .whenNotMatchedInsert(
            condition="source.mergeKey IS NULL",
            values=insert_map
        )
        .execute()
    )

    if affected_hub_keys_df.isEmpty():
        scoped_active = target_active
    else:
        scoped_active = (
            target_active
            .join(
                broadcast(affected_hub_keys_df),
                on="HubKey",
                how="inner"
            )
        )

    to_expire = (
        scoped_active
        .join(
            staged_df
            .select(
                "HubKey",
                F.col("mergeKey").alias("sha_key")
            )
            .distinct(),
            on=["HubKey", "sha_key"],
            how="left_anti"
        )
    )

    (
        delta_table.alias("target")
        .merge(
            to_expire.alias("source"),
            '''
                target.HubKey = source.HubKey
                AND target.sha_key = source.sha_key
                AND target.SCD_Current = true
            '''
        )
        .whenMatchedUpdate(
            set={
                "SCD_End_Date": "to_date(target.CreatedOn)",
                "SCD_Current": "false"
            }
        )
        .execute()
    )

    changed_df.unpersist()
    source_with_flag.unpersist()
    target_active.unpersist()
    staged_df.unpersist()
