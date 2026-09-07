from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col

def create_bronze_table(spark, table_name, table_path):
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            CustomerId STRING,
            InstanceId STRING,
            OrderId STRING,
            PaymentId STRING,
            OrderName STRING,
            PaymentName STRING,
            OrderCount INT,
            CreatedOn TIMESTAMP,
            IngestionTimestamp TIMESTAMP,
            BatchId STRING
        )
        USING DELTA
        LOCATION '{table_path}'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    ''')

def append_to_bronze(source_df, table_name, batch_id):
    (
        source_df
        .withColumn("IngestionTimestamp", current_timestamp())
        .withColumn("BatchId", lit(batch_id))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

def get_current_delta_version(spark, table_name):
    return (
        DeltaTable.forName(spark, table_name)
        .history(1)
        .select("version")
        .first()[0]
    )

def read_json_batch(spark, path, schema):
    return spark.read.schema(schema).json(path)

def read_bronze_cdf(spark, table_name, starting_version, ending_version):
    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .option("endingVersion", ending_version)
        .table(table_name)
    )

def get_incremental_bronze_data(spark, table_name, starting_version, ending_version):
    return (
        read_bronze_cdf(
            spark, table_name, starting_version, ending_version
        )
        .filter(col("_change_type") == "insert")
        .drop(
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
            "IngestionTimestamp",
            "BatchId"
        )
    )
