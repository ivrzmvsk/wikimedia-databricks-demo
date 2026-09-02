import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .appName("wikimedia-stream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    
    if "SPARK_REMOTE" not in os.environ and "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        builder = builder.master("local[2]")
        
    return builder.getOrCreate()