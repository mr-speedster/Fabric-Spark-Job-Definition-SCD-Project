# =========================================================
# Spark Job Definition Entry Point
# =========================================================

from pyspark.sql import SparkSession

from config import (
    BRONZE_TABLE,
    HUB_TABLE,
    DI_TABLE,
    SAT_TABLE,
    WATERMARK_TABLE,
    PIPELINE_NAME,
    load_runtime_config
)

from schema import ORDER_SCHEMA
from sample_data import (
    batch_001,
    batch_002,
    save_sample_json
)
from bronze import read_json_batch
from pipeline import run_pipeline


def main():

    # -----------------------------------------------------
    # Spark session
    # -----------------------------------------------------

    spark = SparkSession.builder.getOrCreate()

    spark.conf.set(
        "spark.databricks.delta.optimizeWrite.enabled",
        "false"
    )

    spark.conf.set(
        "spark.databricks.delta.autoCompact.enabled",
        "false"
    )

    # -----------------------------------------------------
    # Runtime configuration.
    # Loaded from the JSON config file passed as the first
    # command-line argument (workspaceId, sourceLakehouseId,
    # targetLakehouseId), and used to build the ABFSS paths
    # for every Files/Tables location used below.
    # -----------------------------------------------------

    runtime_config = load_runtime_config(spark)

    sample_data_path = runtime_config["SAMPLE_DATA_PATH"]
    bronze_table_path = runtime_config["BRONZE_TABLE_PATH"]
    hub_table_path = runtime_config["HUB_TABLE_PATH"]
    di_table_path = runtime_config["DI_TABLE_PATH"]
    sat_table_path = runtime_config["SAT_TABLE_PATH"]
    watermark_table_path = runtime_config["WATERMARK_TABLE_PATH"]

    # -----------------------------------------------------
    # Learning-only: create sample JSON files.
    # Remove this section in production.
    # -----------------------------------------------------

    batch_001_path = f"{sample_data_path}/batch_001"
    batch_002_path = f"{sample_data_path}/batch_002"

    save_sample_json(
        spark,
        batch_001,
        batch_001_path
    )

    save_sample_json(
        spark,
        batch_002,
        batch_002_path
    )

    # -----------------------------------------------------
    # Batch 1
    # -----------------------------------------------------

    batch1_df = read_json_batch(
        spark,
        batch_001_path,
        ORDER_SCHEMA
    )

    version_1 = run_pipeline(
        spark=spark,
        source_df=batch1_df,
        batch_id="batch_001",
        schema=ORDER_SCHEMA,
        bronze_table=BRONZE_TABLE,
        bronze_table_path=bronze_table_path,
        watermark_table=WATERMARK_TABLE,
        watermark_table_path=watermark_table_path,
        pipeline_name=PIPELINE_NAME,
        hub_table=HUB_TABLE,
        hub_table_path=hub_table_path,
        di_table=DI_TABLE,
        di_table_path=di_table_path,
        sat_table=SAT_TABLE,
        sat_table_path=sat_table_path
    )

    print(f"Batch 001 completed at Bronze version: {version_1}")

    # -----------------------------------------------------
    # Batch 2
    # -----------------------------------------------------

    batch2_df = read_json_batch(
        spark,
        batch_002_path,
        ORDER_SCHEMA
    )

    version_2 = run_pipeline(
        spark=spark,
        source_df=batch2_df,
        batch_id="batch_002",
        schema=ORDER_SCHEMA,
        bronze_table=BRONZE_TABLE,
        bronze_table_path=bronze_table_path,
        watermark_table=WATERMARK_TABLE,
        watermark_table_path=watermark_table_path,
        pipeline_name=PIPELINE_NAME,
        hub_table=HUB_TABLE,
        hub_table_path=hub_table_path,
        di_table=DI_TABLE,
        di_table_path=di_table_path,
        sat_table=SAT_TABLE,
        sat_table_path=sat_table_path
    )

    print(f"Batch 002 completed at Bronze version: {version_2}")


if __name__ == "__main__":
    main()
