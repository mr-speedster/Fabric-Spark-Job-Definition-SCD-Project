from delta.tables import DeltaTable
from pyspark.sql.functions import col

def create_watermark_table(spark, table_name, table_path):
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            PipelineName STRING,
            LastProcessedBronzeVersion BIGINT,
            LastUpdatedTimestamp TIMESTAMP
        )
        USING DELTA
        LOCATION '{table_path}'
    ''')

def get_last_processed_version(spark, table_name, pipeline_name):
    rows = (
        spark.table(table_name)
        .filter(col("PipelineName") == pipeline_name)
        .select("LastProcessedBronzeVersion")
        .limit(1)
        .collect()
    )
    return None if not rows else rows[0][0]

def update_watermark(spark, table_name, pipeline_name, version):
    source_df = spark.createDataFrame(
        [(pipeline_name, int(version))],
        ["PipelineName", "LastProcessedBronzeVersion"]
    )

    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(
            source_df.alias("source"),
            "target.PipelineName = source.PipelineName"
        )
        .whenMatchedUpdate(set={
            "LastProcessedBronzeVersion": "source.LastProcessedBronzeVersion",
            "LastUpdatedTimestamp": "current_timestamp()"
        })
        .whenNotMatchedInsert(values={
            "PipelineName": "source.PipelineName",
            "LastProcessedBronzeVersion": "source.LastProcessedBronzeVersion",
            "LastUpdatedTimestamp": "current_timestamp()"
        })
        .execute()
    )
