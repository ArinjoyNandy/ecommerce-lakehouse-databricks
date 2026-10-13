import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Builds a local SparkSession for isolated unit testing."""
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()