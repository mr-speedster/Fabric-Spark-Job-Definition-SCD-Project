from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

ORDER_SCHEMA = StructType([
    StructField("CustomerId", StringType(), False,
                {"primary_key": True, "in_hash_key": True}),
    StructField("InstanceId", StringType(), False,
                {"primary_key": False, "in_hash_key": False}),
    StructField("OrderId", StringType(), False,
                {"primary_key": True, "in_hash_key": True}),
    StructField("PaymentId", StringType(), False,
                {"primary_key": True, "in_hash_key": True}),
    StructField("OrderName", StringType(), True,
                {"customer_identifier": True, "is_pii": True}),
    StructField("PaymentName", StringType(), True,
                {"in_hash_key": True}),
    StructField("OrderCount", IntegerType(), True,
                {"in_hash_key": True}),
    StructField("CreatedOn", TimestampType(), False)
])
