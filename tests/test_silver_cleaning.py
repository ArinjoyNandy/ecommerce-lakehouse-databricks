import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
from src.silver.cleaning import clean_orders_data, clean_order_items_data

def test_clean_orders_filters_null_and_parses_dates(spark):
    # Sample bronze input schema
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
        StructField("_ingested_at", TimestampType(), True)
    ])

    test_data = [
        # Valid order
        ("ord_001", "cust_001", " DELIVERED ", "2023-01-01 10:00:00", None, None, "2023-01-05 12:00:00", "2023-01-10 00:00:00", datetime.now()),
        # Corrupt order (Null order_id)
        (None, "cust_002", "shipped", "2023-01-02 11:00:00", None, None, None, None, datetime.now())
    ]

    raw_df = spark.createDataFrame(test_data, schema=schema)
    cleaned_df = clean_orders_data(raw_df)
    results = cleaned_df.collect()

    # Assertions
    assert len(results) == 1, "Rows with NULL order_id must be filtered out"
    assert results[0]["order_id"] == "ord_001"
    assert results[0]["order_status"] == "delivered", "Order status should be trimmed and lowercased"
    assert isinstance(results[0]["order_purchase_timestamp"], datetime), "Timestamp must be parsed into datetime"

def test_clean_order_items_handles_null_prices(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", StringType(), True),
        StructField("price", StringType(), True),
        StructField("freight_value", StringType(), True),
        StructField("_ingested_at", TimestampType(), True)
    ])

    test_data = [
        ("ord_001", "1", "prod_A", "seller_X", "2023-01-05 00:00:00", "49.90", "12.50", datetime.now()),
        ("ord_001", "2", "prod_B", "seller_Y", "2023-01-05 00:00:00", None, None, datetime.now())
    ]

    raw_df = spark.createDataFrame(test_data, schema=schema)
    cleaned_df = clean_order_items_data(raw_df)
    results = cleaned_df.orderBy("order_item_id").collect()

    assert len(results) == 2
    assert results[0]["price"] == 49.90
    assert results[1]["price"] == 0.0, "Null price values must default to 0.0"
    assert results[1]["freight_value"] == 0.0, "Null freight values must default to 0.0"
