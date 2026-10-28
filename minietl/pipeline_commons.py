from dataclasses import dataclass
from typing import Any


@dataclass
class SparkPipelineEnv:
    """The Spark session plus one step's settings from the conf.
    Mirrors PipelineCommons.SparkPipelineEnv."""
    spark: Any
    step_metadata: Any = None