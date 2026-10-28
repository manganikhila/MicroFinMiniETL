from pyspark.sql import functions as F
from pyspark.sql.window import Window

from minietl.spark_pipeline_step import SparkPipelineStep
from minietl.sqlserver_storage import max_value


class SurrogateKeyGenerator(SparkPipelineStep):
    """Adds a running surrogate key that continues from the target table's highest key.
    Mirrors customsteps/SurrogateKeyGenerator.scala."""

    def process(self):
        spark = self.spark_env.spark
        variables = self.spark_env.variables
        props = self.metadata.get("properties", {})

        key_column = self._required(props, "surrogateKeyColumnName").strip().lower()
        delta_table = self._required(props, "deltaTableName")
        table_name = self._required(props, "tableName")
        target_table = props.get("targetTableName", "")
        nk_constraints = props.get("nkConstraints", "")

        delta_df = spark.table(delta_table)

        # 1. Data-quality check: the natural key must be unique (QuickETL exits the job if not)
        if nk_constraints:
            duplicates = spark.sql(
                f"select {nk_constraints}, count(1) as cnt from {delta_table} "
                f"group by {nk_constraints} having count(1) > 1"
            ).count()
            if duplicates > 0:
                raise ValueError(f"{self.step_name}: {duplicates} duplicate keys ({nk_constraints}) in {delta_table}")
            print(f"    {self.step_name}: no duplicates on ({nk_constraints})")

        # 2. Offset = highest key already in the target table + 1 (getOffsetValue)
        offset = 1
        if target_table:
            offset = max_value(variables["targetServer"], variables["targetDatabase"], target_table, key_column) + 1
        print(f"    {self.step_name}: new keys start at {offset}")

        # 3. Keep keys that already exist; assign new ones only where missing
        if key_column in [c.lower() for c in delta_df.columns]:
            keep = delta_df.filter(F.col(key_column).isNotNull())
            missing = delta_df.filter(F.col(key_column).isNull()).drop(key_column)
            result = keep.unionByName(self._add_surrogate_key(missing, offset, key_column))
        else:
            result = self._add_surrogate_key(delta_df, offset, key_column)

        result.createOrReplaceTempView(table_name)
        print(f"    {self.step_name}: registered table {table_name}")

    @staticmethod
    def _add_surrogate_key(df, offset, key_column):
        """Number the rows offset, offset+1, ... using Spark's built-in row_number()."""
        window = Window.orderBy(F.monotonically_increasing_id())
        return df.withColumn(key_column, (F.row_number().over(window) + F.lit(offset - 1)).cast("bigint"))

    def _required(self, props, name):
        value = props.get(name, None)
        if not value:
            raise ValueError(f"For SurrogateKeyGenerator step {self.step_name}, {name} is mandatory")
        return value