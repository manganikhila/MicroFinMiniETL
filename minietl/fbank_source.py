import pyodbc


def read_fbank(server, database, query):
    """Read rows from SQL Server using the Windows login (no password)."""
    odbc_drivers = sorted(d for d in pyodbc.drivers() if d.startswith("ODBC Driver"))
    driver = odbc_drivers[-1] if odbc_drivers else "SQL Server"
    print("Using driver:", driver)
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    rows = [tuple(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def load_source(spark, variables):
    """Register the FBANK loan table (plays the role of ued_dbrm_dwh)."""
    rows = read_fbank(variables["sourceServer"], variables["sourceDatabase"], """
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