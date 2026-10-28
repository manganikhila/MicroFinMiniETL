from minietl.sql_loader import load_sql
from minietl.sqlserver_storage import load_table, save_table


class SparkPipelineStep:
    """Base class for every step. Mirrors SparkPipelineStep.scala.
    Every step goes through: load -> validate_inputs -> process -> save -> validate_outputs."""

    def __init__(self, step_name, spark_env):
        self.step_name = step_name
        self.spark_env = spark_env
        self.metadata = spark_env.step_metadata

    def load(self):
        self.load_data_source()

    def validate_inputs(self):
        pass  # QuickETL runs input validations here

    def process(self):
        pass

    def save(self):
        self.save_outputs()

    def validate_outputs(self):
        pass  # QuickETL runs output validations here

    def load_data_source(self):
        """Run this step's inputs in 'order'. Mirrors SparkPipelineStep.loadDataSource + SqlLoader:
        if in-table is given, load it as the table first; if sql is given, run it and register the result."""
        variables = self.spark_env.variables
        inputs = self.metadata.get("inputs", {})
        for input_name, input_conf in sorted(inputs.items(), key=lambda item: item[1]["order"]):
            sql_conf = input_conf["sql"]
            table = sql_conf["table"]
            in_table = sql_conf.get("in-table", None)
            sql = sql_conf.get("sql", None)
            if in_table:
                df = load_table(self.spark_env.spark, variables["targetServer"], variables["targetDatabase"], in_table)
                df.createOrReplaceTempView(table)
                print(f"    {self.step_name}: loaded SQL Server table {in_table} as {table}")
            if sql:
                load_sql(self.spark_env, table, sql)
                print(f"    {self.step_name}: registered table {table}")

    def save_outputs(self):
        """Write every input marked is-save = true to its out-table.
        Mirrors SparkPipelineStep.saveParquet (which writes to out-path on S3)."""
        variables = self.spark_env.variables
        inputs = self.metadata.get("inputs", {})
        for input_name, input_conf in inputs.items():
            sql_conf = input_conf["sql"]
            is_save = input_conf.get("metadata", {}).get("is-save", False)
            out_table = sql_conf.get("out-table", None)
            if is_save and out_table:
                df = self.spark_env.spark.table(sql_conf["table"])
                count = save_table(df, variables["targetServer"], variables["targetDatabase"], out_table)
                print(f"    {self.step_name}: saved {count} rows to SQL Server table {out_table}")