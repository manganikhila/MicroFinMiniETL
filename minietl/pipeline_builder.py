import dataclasses
import importlib

from minietl.spark_pipeline import SparkPipeline


def instantiate(class_name, step_name, spark_env):
    """Create a step object from its class-name text. Mirrors ReflectUtils.instantiate.
    'minietl.spark_loader_step.SparkLoaderStep' -> module 'minietl.spark_loader_step', class 'SparkLoaderStep'."""
    module_name, cls_name = class_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name)
    return cls(step_name, spark_env)


def build_pipeline(spark_env, conf):
    """Build one step object per entry in pipeline.steps. Mirrors PipelineBuilder.buildPipeline."""
    steps = []
    for step_name in conf["pipeline"]["steps"]:
        step_conf = conf["steps"][step_name]
        step_env = dataclasses.replace(spark_env, step_metadata=step_conf)
        step = instantiate(step_conf["class-name"], step_name, step_env)
        print(f"Built step {step_name} as {type(step).__name__}")
        steps.append(step)
    return SparkPipeline(conf["pipeline"]["name"], steps)