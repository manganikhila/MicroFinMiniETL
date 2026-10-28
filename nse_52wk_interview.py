import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("nse_52wk_high_low")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 1. READ: first line is the header; '-' means "no value"
raw = (
    spark.read
    .option("header", True)
    .option("nullValue", "-")
    .csv("input/nse/CM_52_wk_High_low_31072026.csv")
)
print("Raw rows:", raw.count())
raw.printSchema()

# 2. CLEAN: rename, cast numbers, parse dates, drop rows with no data
clean = (
    raw.select(
        F.trim("SYMBOL").alias("symbol"),
        F.trim("SERIES").alias("series"),
        F.col("Adjusted_52_Week_High").cast("double").alias("high_52w"),
        F.to_date("52_Week_High_Date", "dd-MMM-yy").alias("high_52w_date"),
        F.col("Adjusted_52_Week_Low").cast("double").alias("low_52w"),
        F.to_date("52_Week_Low_DT", "dd-MMM-yy").alias("low_52w_date"),
    )
    .dropna(subset=["high_52w", "low_52w"])
    .filter(F.col("low_52w") > 0)
)
print("Clean rows:", clean.count())

# 3. AS-OF DATE: the latest date that appears in the file
as_of = clean.select(F.max(F.greatest("high_52w_date", "low_52w_date"))).first()[0]
print("As-of date:", as_of)

# 4. DERIVE: % range, days since the high/low, trend
metrics = (
    clean
    .withColumn("range_pct", F.round((F.col("high_52w") - F.col("low_52w")) / F.col("low_52w") * 100, 2))
    .withColumn("days_since_high", F.datediff(F.lit(as_of), "high_52w_date"))
    .withColumn("days_since_low", F.datediff(F.lit(as_of), "low_52w_date"))
    .withColumn(
        "trend",
        F.when(F.col("high_52w_date") > F.col("low_52w_date"), "Recovering (high after low)")
        .otherwise("Falling (low after high)"),
        )
)

# 5. RANK: top 10 EQ stocks by 52-week range
window = Window.orderBy(F.col("range_pct").desc())
top10 = (
    metrics.filter(F.col("series") == "EQ")
    .withColumn("range_rank", F.dense_rank().over(window))
    .filter(F.col("range_rank") <= 10)
    .select("range_rank", "symbol", "low_52w", "high_52w", "range_pct", "trend")
)
print("Top 10 EQ stocks by 52-week range:")
top10.show(truncate=False)

# 6. AGGREGATE: summary per series
summary = (
    metrics.groupBy("series")
    .agg(
        F.count("*").alias("stocks"),
        F.sum(F.when(F.col("days_since_high") <= 7, 1).otherwise(0)).alias("new_highs_last_7d"),
        F.sum(F.when(F.col("days_since_low") <= 7, 1).otherwise(0)).alias("new_lows_last_7d"),
        F.round(F.avg("range_pct"), 2).alias("avg_range_pct"),
    )
    .orderBy(F.desc("stocks"))
)
print("Summary by series:")
summary.show(truncate=False)

spark.stop()