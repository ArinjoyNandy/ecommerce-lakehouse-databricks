from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

def upsert_orders_silver(
    spark: SparkSession,
    silver_table_name: str,
    incoming_orders_df: DataFrame
) -> None:
    """
    Performs an idempotent MERGE INTO upsert on the Silver orders Delta table.
    Ensures updates are only applied if the incoming record is newer or equal in timestamp.
    """

    # Create target table if it does not exist yet
    if not spark.catalog.tableExists(silver_table_name):
        (
            incoming_orders_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table_name)
        )
        return

    silver_table = DeltaTable.forName(spark, silver_table_name)
    (
        silver_table.alias("target")
        .merge(
            incoming_orders_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdate(
            condition="source.order_purchase_timestamp >= target.order_purchase_timestamp",
            set={
                "customer_id": "source.customer_id",
                "order_status": "source.order_status",
                "order_delivered_customer_date": "source.order_delivered_customer_date",
                "_ingested_at":"source._ingested_at"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def overwrite_silver_table(
    target_table_name: str,
    cleaned_df: DataFrame
) -> None:
    """
    Standard overwrite for immutable/dimension datasets like customers and order items.
    """

    (
        cleaned_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table_name)
    )