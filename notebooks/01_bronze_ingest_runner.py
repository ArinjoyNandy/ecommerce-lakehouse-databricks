# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Setup namespace parameters
catalog_name = "ecommerce_dev"
schema_name = "bronze"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")  

# COMMAND ----------

# DBTITLE 1,Configure repository path and import ingestion module
import sys
import os
import importlib

repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

if "src.bronze.ingestion" in sys.modules:
    importlib.reload(sys.modules["src.bronze.ingestion"])

from src.bronze.ingestion import ingest_raw_csv_to_bronze

# COMMAND ----------

# DBTITLE 1,Define datasets to ingest
landing_base = f"/Volumes/ecommerce_dev/bronze_stage/raw_landing"

datasets = [
    {
        "source":f"{landing_base}/olist_orders_dataset.csv",
        "target":f"{catalog_name}.{schema_name}.raw_orders" 
    },
    {
        "source":f"{landing_base}/olist_order_items_dataset.csv",
        "target":f"{catalog_name}.{schema_name}.raw_order_items" 
    },
    {
        "source":f"{landing_base}/olist_customers_dataset.csv",
        "target":f"{catalog_name}.{schema_name}.raw_customers" 
    }
]

# COMMAND ----------

# DBTITLE 1,Execute batch ingestion
for item in datasets:
    print(f"Ingesting {item['source']} -> {item['target']} ...")
    rows_loaded = ingest_raw_csv_to_bronze(
        spark = spark,
        source_csv_path = item["source"],
        target_table_name = item["target"]
    )
    print(f"Loaded {rows_loaded} records into {item['target']}.")

# COMMAND ----------

# DBTITLE 1,Exit notebook with success status for workflows
dbutils.notebook.exit("SUCCESS: Bronze tables ingested successfully.")