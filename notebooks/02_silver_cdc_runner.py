# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
catalog_name = "ecommerce_dev"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")

# COMMAND ----------

import sys
import os
import importlib

repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

if "src.silver.cleaning" in sys.modules:
    importlib.reload(sys.modules["src.silver.cleaning"])

if "src.silver.cdc_engine" in sys.modules:
    importlib.reload(sys.modules["src.silver.cdc_engine"])

from src.silver.cleaning import clean_orders_data, clean_order_items_data, clean_customers_data
from src.silver.cdc_engine import upsert_orders_silver, overwrite_silver_table

# COMMAND ----------

# 1. Read Bronze tables
bronze_orders = spark.read.table(f"{catalog_name}.bronze.raw_orders")
bronze_items = spark.read.table(f"{catalog_name}.bronze.raw_order_items")
bronze_custs = spark.table(f"{catalog_name}.bronze.raw_customers")

# 2. Clean data
silver_orders_df = clean_orders_data(bronze_orders)
silver_items_df = clean_order_items_data(bronze_items)
silver_custs_df = clean_customers_data(bronze_custs)

# COMMAND ----------

# 3. Apply CDC Merge to Orders
orders_target = f"{catalog_name}.silver.orders"
print("upserting orders into {orders_target}...")
upsert_orders_silver(spark, orders_target, silver_orders_df)

# 4. Write Dimension and Line-Item Tables
items_target = f"{catalog_name}.silver.order_items"
custs_target = f"{catalog_name}.silver.customers"
print(f"Writing {items_target} and {custs_target}...")
overwrite_silver_table(items_target, silver_items_df)
overwrite_silver_table(custs_target, silver_custs_df)

# COMMAND ----------

# 5. Delta Validation
display(spark.sql(f"DESCRIBE HISTORY {orders_target}"))

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: Silver CDC and Cleansing complete.")