import time

LIFECYCLE = ["load", "validate_inputs", "process", "save", "validate_outputs"]


class SparkPipeline:
    """Runs the steps in the order listed in the conf. Mirrors SparkPipeline.scala."""

    def __init__(self, name, steps):
        self.name = name
        self.steps = steps

    def run_pipeline(self):
        print(f"Running pipeline {self.name}")
        for step in self.steps:
            self.run_step(step)

    def run_step(self, step):
        """Call each lifecycle method in turn. Mirrors StepRunner.runStep.
        If a step fails, the error stops the whole pipeline (QuickETL's canContinue = false)."""
        start = time.time()
        print(f"#StepInfo: stage=started; step={step.step_name}; class={type(step).__name__}")
        try:
            for operation in LIFECYCLE:
                getattr(step, operation)()
        except Exception as error:
            print(f"#StepInfo: stage=completed; step={step.step_name}; status=failed; error={error}")
            raise
        duration = time.time() - start
        print(f"#StepInfo: stage=completed; step={step.step_name}; status=success; duration={duration:.1f}s")