from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

def clean_orders_data(bronze_orders_df: DataFrame) -> DataFrame:
    """
    Cleans Bronze raw orders:
    - Casts timestamps to TimestampType
    - Normalizes status text
    - Deduplicates by order_id picking the most recent ingested record
    """

    window_spec = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

    cleaned_df = (
        bronze_orders_df
        .select(
            F.col("order_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.to_timestamp(F.col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
            F.to_timestamp(F.col("order_approved_at")).alias("order_approved_at"),
            F.to_timestamp(F.col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
            F.to_timestamp(F.col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
            F.to_timestamp(F.col("order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
            F.col("_ingested_at")
        )
        .filter(F.col("order_id").isNotNull())
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return cleaned_df

def clean_order_items_data(bronze_order_item_df: DataFrame) -> DataFrame:
    """
    Cleans Bronze raw order items:
    - Casts numeric price and freight values
    - Handles null price values defensively
    """

    cleaned_df = (
        bronze_order_item_df.
        select(
            F.col("order_id").cast("string"),
            F.col("order_item_id").cast(IntegerType()),
            F.col("product_id").cast("string"),
            F.col("seller_id").cast("string"),
            F.to_timestamp(F.col("shipping_limit_date")).alias("shipping_limit_date"),
            F.coalesce(F.col("price").cast(DoubleType())).alias("price"),
            F.coalesce(F.col("freight_value").cast(DoubleType())).alias("freight_value"),
            F.col("_ingested_at")
        )
        .filter(F.col("order_id").isNotNull() & F.col("order_item_id").isNotNull())
    )

    return cleaned_df

def clean_customers_data(bronze_customers_df: DataFrame) -> DataFrame:
    """
    Cleans Bronze raw customers:
    - Normalizes state codes and city names
    """

    cleaned_df = (
        bronze_customers_df.
        select(
            F.col("customer_id").cast("string"),
            F.col("customer_unique_id").cast("string"),
            F.col("customer_zip_code_prefix").cast("string"),
            F.initcap(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            F.col("_ingested_at")
        )
        .filter(F.col("customer_id").isNotNull())
    )

    return cleaned_df