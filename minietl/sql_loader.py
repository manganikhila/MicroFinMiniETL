def load_sql(spark_env, table, sql):
    """Run one SQL statement and register the result under a table name.
    Mirrors SqlLoader.loadSql: spark.sql(...) then createOrReplaceTempView(...)."""
    df = spark_env.spark.sql(sql)
    df.createOrReplaceTempView(table)
    return df