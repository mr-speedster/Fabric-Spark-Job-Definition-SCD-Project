batch_001 = [
    {
        "CustomerId": "I1", "InstanceId": "A1",
        "OrderId": "ORD11", "PaymentId": "PAY11",
        "OrderName": "Order11", "PaymentName": "Standard",
        "OrderCount": 1, "CreatedOn": "2026-08-01T10:00:00"
    },
    {
        "CustomerId": "I1", "InstanceId": "A1",
        "OrderId": "ORD12", "PaymentId": "PAY12",
        "OrderName": "Order12", "PaymentName": "Premium",
        "OrderCount": 1, "CreatedOn": "2026-08-01T10:01:00"
    },
    {
        "CustomerId": "I2", "InstanceId": "A2",
        "OrderId": "ORD21", "PaymentId": "PAY21",
        "OrderName": "Order21", "PaymentName": "Standard",
        "OrderCount": 1, "CreatedOn": "2026-08-01T10:03:00"
    }
]

batch_002 = [
    {
        "CustomerId": "I1", "InstanceId": "A1",
        "OrderId": "ORD11", "PaymentId": "PAY11",
        "OrderName": "Order11-Updated",
        "PaymentName": "Standard-Updated",
        "OrderCount": 5, "CreatedOn": "2026-08-02T10:01:00"
    },
    {
        "CustomerId": "I1", "InstanceId": "A1",
        "OrderId": "ORD13", "PaymentId": "PAY13",
        "OrderName": "Order13", "PaymentName": "Premium",
        "OrderCount": 2, "CreatedOn": "2026-08-02T10:02:00"
    },
    {
        "CustomerId": "I3", "InstanceId": "A3",
        "OrderId": "ORD31", "PaymentId": "PAY31",
        "OrderName": "Order31", "PaymentName": "Premium",
        "OrderCount": 3, "CreatedOn": "2026-08-02T10:03:00"
    },
    {
        "CustomerId": "I4", "InstanceId": "A4",
        "OrderId": "ORD41", "PaymentId": "PAY41",
        "OrderName": "Order41", "PaymentName": "Premium",
        "OrderCount": 3, "CreatedOn": "2026-08-02T10:03:00"
    }
]

def save_sample_json(spark, data, output_path):
    (
        spark.createDataFrame(data)
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(output_path)
    )
