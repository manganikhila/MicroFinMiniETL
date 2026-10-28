from minietl.spark_pipeline_step import SparkPipelineStep


class SparkLoaderStep(SparkPipelineStep):
    """Runs the SQL sub-steps defined in the conf. Mirrors SparkLoaderStep.scala.

    All the real work is inherited from SparkPipelineStep:
    load -> load_data_source -> load_sql (spark.sql + createOrReplaceTempView).
    Like the Scala version, process() itself does nothing."""

    def process(self):
        pass