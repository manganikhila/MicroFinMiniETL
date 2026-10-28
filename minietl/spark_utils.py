import os
import sys

from pyspark.sql import SparkSession


def create_spark(app_name, spark_properties):
    """Start Spark. Mirrors SparkUtils.createSparkForStandalone."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    for key, value in spark_properties.items():
        builder = builder.config(key.strip('"'), str(value))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark