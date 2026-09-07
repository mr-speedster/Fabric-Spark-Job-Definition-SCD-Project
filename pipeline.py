from pyspark.sql.functions import col, lit, to_date
from bronze import (
    create_bronze_table,
    append_to_bronze,
    get_current_delta_version,
    get_incremental_bronze_data
)
from watermark import (
    create_watermark_table,
    get_last_processed_version,
    update_watermark
)
from data_vault import build_data_vault
from merges import (
    merge_delta_hub,
    merge_delta_di,
    merge_delta_scd2
)

def silver_tables_exist(spark, hub_table, di_table, sat_table):
    return (
        spark.catalog.tableExists(hub_table)
        and spark.catalog.tableExists(di_table)
        and spark.catalog.tableExists(sat_table)
    )

def initial_silver_load(
    source_df,
    schema,
    hub_table,
    hub_table_path,
    di_table,
    di_table_path,
    sat_table,
    sat_table_path
):
    hub_df, sat_df, di_df = build_data_vault(source_df, schema)

    (
        hub_df.write.format("delta").mode("overwrite")
        .option("path", hub_table_path)
        .saveAsTable(hub_table)
    )
    (
        di_df.write.format("delta").mode("overwrite")
        .option("path", di_table_path)
        .saveAsTable(di_table)
    )

    sat_initial_df = (
        sat_df
        .withColumn("SCD_Start_Date", to_date(col("CreatedOn")))
        .withColumn("SCD_End_Date", lit(None).cast("timestamp"))
        .withColumn("SCD_Current", lit(True))
    )

    (
        sat_initial_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("path", sat_table_path)
        .saveAsTable(sat_table)
    )

def incremental_silver_load(
    spark,
    source_df,
    schema,
    hub_table,
    di_table,
    sat_table
):
    affected_customers = source_df.select("CustomerId").distinct()

    hub_df, sat_df, di_df = build_data_vault(source_df, schema)

    merge_delta_hub(spark, hub_table, hub_df)
    merge_delta_di(spark, di_table, di_df)

    merge_delta_scd2(
        spark,
        sat_table,
        sat_df,
        affected_customers,
        hub_table
    )

def run_pipeline(
    spark,
    source_df,
    batch_id,
    schema,
    bronze_table,
    bronze_table_path,
    watermark_table,
    watermark_table_path,
    pipeline_name,
    hub_table,
    hub_table_path,
    di_table,
    di_table_path,
    sat_table,
    sat_table_path
):
    create_bronze_table(spark, bronze_table, bronze_table_path)
    create_watermark_table(spark, watermark_table, watermark_table_path)

    # Check initial/incremental state BEFORE processing.
    initial_run = not silver_tables_exist(
        spark, hub_table, di_table, sat_table
    )

    last_processed_version = get_last_processed_version(
        spark,
        watermark_table,
        pipeline_name
    )

    append_to_bronze(source_df, bronze_table, batch_id)

    current_bronze_version = get_current_delta_version(
        spark,
        bronze_table
    )

    if initial_run:
        initial_silver_load(
            source_df,
            schema,
            hub_table,
            hub_table_path,
            di_table,
            di_table_path,
            sat_table,
            sat_table_path
        )
    else:
        starting_version = (
            0
            if last_processed_version is None
            else last_processed_version + 1
        )

        incremental_df = get_incremental_bronze_data(
            spark,
            bronze_table,
            starting_version,
            current_bronze_version
        )

        if not incremental_df.isEmpty():
            incremental_silver_load(
                spark,
                incremental_df,
                schema,
                hub_table,
                di_table,
                sat_table
            )

    # Update only after successful Silver processing.
    update_watermark(
        spark,
        watermark_table,
        pipeline_name,
        current_bronze_version
    )

    return current_bronze_version
