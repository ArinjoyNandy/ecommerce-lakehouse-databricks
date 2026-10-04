from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def ingest_raw_csv_to_bronze(
    spark: SparkSession,
    source_csv_path: str,
    target_table_name: str,
    write_mode: str = "append"
) -> int:
    """
    Reads a raw CSV from volume storage, attaches operational audit metadata,
    and writes to a Bronze Delta table.
    """

    # 1. Read raw CSV with header preservation
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_csv_path)
    )

    # 2. Append operational metadata (lineage and audit trail)
    df_bronze = (
        df_raw
        .withColumn("_ingeated_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    ) 

    # 3. Write out as Delta table
    (
        df_bronze.write
        .format("delta")
        .mode(write_mode)
        .saveAsTable(target_table_name)
    )

    return df_bronze.count()