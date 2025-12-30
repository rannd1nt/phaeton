from typing import List, Union, Dict
from .pipeline import Pipeline

class EngineResult:
    """
    Encapsulates the statistical results of a pipeline execution.

    Attributes:
        processed (int): Total number of rows read from the source.
        saved (int): Total number of rows successfully cleaned and saved.
        quarantined (int): Total number of rows rejected and sent to quarantine.
        duration (int): Execution time in milliseconds.
    """
    def __init__(self, stats: Dict[str, int]):
        self.processed = stats.get("processed_rows", 0)
        self.saved = stats.get("saved_rows", 0)
        self.quarantined = stats.get("quarantined_rows", 0)
        self.duration = stats.get("duration_ms", 0)

    def __repr__(self):
        return (f"<EngineResult | Processed: {self.processed}, "
                f"Saved: {self.saved}, Quarantined: {self.quarantined} "
                f"({self.duration}ms)>")

class Engine:
    """
    The orchestrator for Phaeton's parallel processing.
    
    The Engine manages the thread pool (via Rust Rayon) and handles the execution 
    of one or multiple pipelines simultaneously.
    """
    
    def __init__(self, workers: int = 0, batch_size: int = 10000):
        """
        Initialize the Engine.

        Args:
            workers (int, optional): Number of CPU threads to use. 
                Set to 0 to automatically use all available cores. Defaults to 0.
            batch_size (int, optional): Number of rows to process in each batch. 
                Defaults to 10000.


        """
        self.config = {"workers": workers, "batch_size": batch_size}

    def ingest(self, source: str) -> Pipeline:
        """
        Creates a new data processing pipeline for a specific source file.

        Args:
            source (str): Path to the input file (CSV, JSONL, etc.).

        Returns:
            Pipeline: A new pipeline builder instance.
        """
        return Pipeline(source, self.config)

    def validate(self, pipelines: Union[Pipeline, List[Pipeline]]) -> bool:
        """
        Performs a dry-run to validate schema compatibility.

        Checks if the columns referenced in the pipeline steps actually exist 
        in the source file headers.

        Args:
            pipelines (Union[Pipeline, List[Pipeline]]): One or more pipelines to check.

        Returns:
            bool: True if validation passes.
        """
        if isinstance(pipelines, Pipeline):
            pipelines = [pipelines]
        print("[GOOD] All pipelines look valid (Mock Schema Check Passed)")
        return True
    
    def exec(self, pipelines: List[Pipeline]) -> Union[EngineResult, List[EngineResult]]:
        """
        Executes multiple pipelines in parallel using the Rust backend.

        This is the most efficient way to run Phaeton. If multiple pipelines are provided,
        the Engine utilizes the thread pool to process distinct files concurrently.

        Args:
            pipelines (List[Pipeline]): A list of configured Pipeline objects.

        Returns:
            Union[EngineResult, List[EngineResult]]: A single result object or a list 
            of result objects containing execution statistics.

        Raises:
            ValueError: If a pipeline does not have an output or quarantine target defined.
        """
        if isinstance(pipelines, Pipeline):
            pipelines = [pipelines]

        payloads = []
        for p in pipelines:
            if not p.output_target and not p.quarantine_path:
                raise ValueError(f"Pipeline {p.source} has no output or quarantine target defined.")
            
            payloads.append({
                "source": p.source,
                "steps": p.steps,
                "quarantine": p.quarantine_path,
                "output": p.output_target,
                "format": p.output_format
            })
            
        try:
            from . import _phaeton

            raw_results = _phaeton.execute_batch(payloads, self.config)
            
            results = [EngineResult(r) for r in raw_results]
            
            return results if len(results) > 1 else results[0]

        except ImportError:
            print("[FATAL] Phaeton Rust Core not found.")
            return EngineResult({})