import argparse
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyodbc
from pyhocon import ConfigFactory
from pyspark.sql import SparkSession


def create_spark(app_name, spark_properties):
    """Start Spark. Mirrors SparkUtils.createSparkForStandalone."""
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


def read_fbank(query):
    """Read rows from FBANK using the Windows login (no password)."""
    odbc_drivers = sorted(d for d in pyodbc.drivers() if d.startswith("ODBC Driver"))
    driver = odbc_drivers[-1] if odbc_drivers else "SQL Server"
    print("Using driver:", driver)
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};"
        "SERVER=localhost;"
        "DATABASE=FBANK;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    rows = [tuple(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def load_source(spark):
    """Register the FBANK loan table (plays the role of ued_dbrm_dwh)."""
    rows = read_fbank("""
        SELECT UNIT,
               CAST(YCODE AS INT),
               CAST(XCODE AS INT),
               PCODE,
               CONVERT(varchar(10), OPENDATE, 23),
               CONVERT(varchar(10), CLOSEDATE, 23),
               CAST(LAMOUNT AS FLOAT),
               CAST(POS AS FLOAT),
               CAST(PDUE AS FLOAT),
               CAST(IDUE AS FLOAT),
               CAST(CLOSED AS INT)
        FROM HM_MEMBERLOAN
    """)
    schema = (
        "unit string, loan_id int, member_id int, product string, "
        "open_date string, close_date string, loan_amount double, "
        "outstanding double, principal_due double, interest_due double, closed int"
    )
    spark.createDataFrame(rows, schema).createOrReplaceTempView("hm_memberloan")
    print("Rows read from SQL Server:", len(rows))


def run_step(spark, step_name, step_conf):
    """Run every sub-step of one step, in 'order'. Mirrors SqlLoader."""
    sub_steps = sorted(step_conf["inputs"].items(), key=lambda item: item[1]["order"])
    for sub_name, sub_conf in sub_steps:
        table = sub_conf["sql"]["table"]
        sql = sub_conf["sql"]["sql"]
        spark.sql(sql).createOrReplaceTempView(table)
        print(f"Step done: {step_name} -> {table}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--conf", required=True, help="path to the pipeline conf file")
    args = parser.parse_args()

    # 1. Read the conf (mirrors SparkPipeline.configurePipeline)
    conf = ConfigFactory.parse_file(args.conf)
    pipeline_name = conf["pipeline"]["name"]
    step_names = conf["pipeline"]["steps"]
    print(f"Running {pipeline_name} with steps: {step_names}")

    # 2. Start Spark (mirrors SparkUtils.createSparkForStandalone)
    spark = create_spark(pipeline_name, conf.get("spark-properties", {}))

    # 3. Register the source table
    load_source(spark)

    # 4. Run the steps in the order listed (mirrors SparkPipeline.runPipeline)
    for step_name in step_names:
        run_step(spark, step_name, conf["steps"][step_name])

    # 5. Temporary check of the result
    spark.sql("""
        select ar_flag, count(*) as loans, round(sum(loan_amount), 2) as total_lent
        from final_loan_ar
        group by ar_flag
        order by ar_flag
    """).show()

    spark.stop()


if __name__ == "__main__":
    main()