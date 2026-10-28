from minietl.sql_loader import load_sql


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
        pass  # Step 7: write results to files when is-save = true

    def validate_outputs(self):
        pass  # QuickETL runs output validations here

    def load_data_source(self):
        """Run this step's inputs in 'order'. Mirrors SparkPipelineStep.loadDataSource."""
        inputs = self.metadata.get("inputs", {})
        for input_name, input_conf in sorted(inputs.items(), key=lambda item: item[1]["order"]):
            sql_conf = input_conf["sql"]
            table = sql_conf["table"]
            load_sql(self.spark_env, table, sql_conf["sql"])
            print(f"    {self.step_name}: registered table {table}")