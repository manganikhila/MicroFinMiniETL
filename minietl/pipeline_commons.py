from dataclasses import dataclass
from typing import Any


@dataclass
class SparkPipelineEnv:
    """The Spark session, one step's settings, and the pipeline variables.
    Mirrors PipelineCommons.SparkPipelineEnv (QuickETL keeps variables in StepMetadata.pipelineProperties)."""
    spark: Any
    step_metadata: Any = None
    variables: Any = None