import argparse

from minietl.config_parser import read_conf
from minietl.fbank_source import load_source
from minietl.pipeline_builder import build_pipeline
from minietl.pipeline_commons import SparkPipelineEnv
from minietl.spark_utils import create_spark


def main():
    """Mirrors PipelineRunner.run: read conf -> start Spark -> build pipeline -> run it."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--conf", required=True, help="path to the pipeline conf file")
    parser.add_argument("env", help="environment: dev, e2e or prd")
    args = parser.parse_args()

    # 1. Read the conf and its includes (SparkPipeline.configurePipeline)
    conf = read_conf(args.conf, args.env)
    pipeline_name = conf["pipeline"]["name"]
    print(f"Pipeline {pipeline_name}, env={args.env}")

    # 2. Start Spark (SparkUtils.createSparkForStandalone)
    spark = create_spark(pipeline_name, conf.get("spark-properties", {}))

    # 3. Register the source table from SQL Server
    load_source(spark, conf["variables"])

    # 4. Build the step objects from class-name (PipelineBuilder.buildPipeline)
    pipeline = build_pipeline(SparkPipelineEnv(spark), conf)

    # 5. Run them (SparkPipeline.runPipeline)
    pipeline.run_pipeline()

    # 6. Temporary check of the result
    spark.sql("""
        select ar_flag, count(*) as loans, round(sum(loan_amount), 2) as total_lent
        from final_loan_ar
        group by ar_flag
        order by ar_flag
    """).show()

    spark.stop()


if __name__ == "__main__":
    main()