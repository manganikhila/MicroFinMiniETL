import datetime
import decimal

import pyodbc
from pyspark.sql import types as T


def connect(server, database):
    """Open a connection with the Windows login (no password)."""
    odbc_drivers = sorted(d for d in pyodbc.drivers() if d.startswith("ODBC Driver"))
    driver = odbc_drivers[-1] if odbc_drivers else "SQL Server"
    return pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )


def _sql_type(spark_type):
    """Spark column type -> SQL Server column type."""
    if isinstance(spark_type, T.BooleanType):
        return "BIT"
    if isinstance(spark_type, T.IntegerType):
        return "INT"
    if isinstance(spark_type, T.LongType):
        return "BIGINT"
    if isinstance(spark_type, T.DoubleType):
        return "FLOAT"
    if isinstance(spark_type, T.DecimalType):
        return f"DECIMAL({spark_type.precision},{spark_type.scale})"
    if isinstance(spark_type, T.DateType):
        return "DATE"
    if isinstance(spark_type, T.TimestampType):
        return "DATETIME2"
    return "NVARCHAR(400)"


def _spark_type(py_type, precision, scale):
    """SQL Server column (as reported by pyodbc) -> Spark column type."""
    if py_type is bool:
        return T.BooleanType()
    if py_type is int:
        return T.LongType()
    if py_type is float:
        return T.DoubleType()
    if py_type is decimal.Decimal:
        return T.DecimalType(precision, scale)
    if py_type is datetime.datetime:
        return T.TimestampType()
    if py_type is datetime.date:
        return T.DateType()
    return T.StringType()


def save_table(df, server, database, table):
    """Overwrite a SQL Server table with the DataFrame's rows.
    On a cluster this would be df.write.jdbc(...) or df.write.parquet(s3_path)."""
    fields = df.schema.fields
    col_defs = ", ".join(f"[{f.name}] {_sql_type(f.dataType)}" for f in fields)
    col_names = ", ".join(f"[{f.name}]" for f in fields)
    placeholders = ", ".join("?" for _ in fields)
    rows = [tuple(r) for r in df.collect()]

    conn = connect(server, database)
    cursor = conn.cursor()
    cursor.execute(f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.{table}")
    cursor.execute(f"CREATE TABLE dbo.{table} ({col_defs})")
    if rows:
        cursor.executemany(f"INSERT INTO dbo.{table} ({col_names}) VALUES ({placeholders})", rows)
    conn.commit()
    conn.close()
    return len(rows)


def load_table(spark, server, database, table):
    """Read a whole SQL Server table into a Spark DataFrame."""
    conn = connect(server, database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM dbo.{table}")
    schema = T.StructType([
        T.StructField(d[0], _spark_type(d[1], d[4], d[5]), True) for d in cursor.description
    ])
    rows = [tuple(r) for r in cursor.fetchall()]
    conn.close()
    return spark.createDataFrame(rows, schema)


def max_value(server, database, table, column):
    """Highest value of a column, or 0 if the table doesn't exist yet."""
    conn = connect(server, database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT OBJECT_ID('dbo.{table}', 'U')")
    if cursor.fetchone()[0] is None:
        conn.close()
        return 0
    cursor.execute(f"SELECT ISNULL(MAX([{column}]), 0) FROM dbo.{table}")
    value = cursor.fetchone()[0]
    conn.close()
    return int(value)