import threading
from typing import List, Union, Dict
from ._internal import ACCESS_TOKEN
from .exceptions import (
    ConfigurationError,
    EngineError,
    SchemaError,
    ValueError as PhaetonValueError
)
from .pipeline import (
    Pipeline, 
    ScrubMode, 
    MatchMode, 
    FillMethod, 
    CastType
)

class EngineResult:
    """
    Encapsulates the statistical results of a pipeline execution.
    This class is immutable (Read-Only).

    Attributes:
        processed (int): Total number of rows read from the source.
        saved (int): Total number of rows successfully cleaned and saved.
        quarantined (int): Total number of rows rejected and sent to quarantine.
        duration (int): Execution time in milliseconds.
    """

    __slots__ = ('_processed', '_saved', '_quarantined', '_duration')

    def __init__(self, stats: Dict[str, int]):
        self._processed = stats.get("processed_rows", 0)
        self._saved = stats.get("saved_rows", 0)
        self._quarantined = stats.get("quarantined_rows", 0)
        self._duration = stats.get("duration_ms", 0)

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def saved(self) -> int:
        return self._saved

    @property
    def quarantined(self) -> int:
        return self._quarantined

    @property
    def duration(self) -> int:
        return self._duration
    
    def __repr__(self):
        return (f"<EngineResult | Processed: {self.processed}, "
                f"Saved: {self.saved}, Quarantined: {self.quarantined} "
                f"({self.duration}ms)>")

class Engine:
    """
    The orchestrator for Phaeton's parallel processing.
    
    The Engine manages the thread pool and handles the execution 
    of one or multiple pipelines simultaneously. It implements the Singleton pattern.
    """
    
    _instance = None
    _lock = threading.Lock()
    _initialized = False
    _ingest_counter = 0
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Engine, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, workers: int = 0, batch_size: int = 10000, strict: bool = False):
        """
        Initialize the Engine configuration:

        Args:
            workers (int, optional): Number of CPU threads to use. 
                Set to 0 to automatically use all available cores. Defaults to 0.
            batch_size (int, optional): Number of rows to process in each batch. 
                Defaults to 10000.
            strict (bool): If True, performs schema validation (checks column existence) 
                            before execution. Defaults to False for maximum performance.

        Raises:
            ConfigurationError: If workers is negative or batch_size is <= 0.
        """

        if self._initialized:
            return
        
        with self._lock:
            if not self._initialized:
                if workers < 0:
                    raise ConfigurationError("Number of workers cannot be negative.")
                if batch_size <= 0:
                    raise ConfigurationError("Batch size must be greater than 0.")
                self._config = {"workers": workers, "batch_size": batch_size, "strict": strict}
                self._strict = strict
                self._initialized = True

    def ingest(self, source: str) -> Pipeline:
        """
        Creates a new data processing pipeline for a specific source file.

        Args:
            source (str): Path to the input file (CSV, parquet, etc.).

        Returns:
            Pipeline: A new pipeline builder instance.
        """
        self._ingest_counter += 1
        base_alias = f"PIPE-{self._ingest_counter}"
        return Pipeline(source, self._config, alias=base_alias, token=ACCESS_TOKEN)

    def validate(self, pipelines: Union[Pipeline, List[Pipeline]]) -> bool:
        """
        Manually triggers validation checks on pipelines without executing them.
        
        Useful for debugging schemas before running a heavy job.
        Outputs validation errors to stdout.

        Args:
            pipelines: Single Pipeline or List of Pipelines.

        Returns:
            bool: True if ALL pipelines are valid, False if ANY fail.
        """
        if isinstance(pipelines, Pipeline):
            pipelines = [pipelines]

        all_passed = True
        
        for p in pipelines:
            try:
                p._validate()
            except (SchemaError, PhaetonValueError, EngineError) as e:
                print(e)
                all_passed = False
            except Exception as e:
                print(f"[{p._alias}] Unexpected Validation Error: {e}")
                all_passed = False
        
        if all_passed:
            print("Schema Validation Passed.")
        
        return all_passed

    def exec(self, pipelines: Union[Pipeline, List[Pipeline]]) -> Union[EngineResult, List[EngineResult]]:
        """
        Executes multiple pipelines in parallel using the Rust backend.

        Args:
            pipelines (List[Pipeline]): A list of configured Pipeline objects.

        Returns:
            Union[EngineResult, List[EngineResult]]: Result object(s) containing statistics.

        Raises:
            EngineError: If the Rust backend fails or is missing.
            ConfigurationError: If output targets are missing.
            SchemaError: (If strict=True) If column validation fails.
        """
        single_pipe = False
        if isinstance(pipelines, Pipeline):
            pipelines = [pipelines]
            single_pipe = True
        
        if self._strict:
            for p in pipelines:
                p._validate()

        payloads = []
        
        for p in pipelines:
            if not p._output_target and not p._quarantine_path and not p._has_peeked:
                raise ConfigurationError(
                    f"Pipeline for '{p._source}' (Alias: {p._alias}) has no output target. "
                    "You must call .dump(), .quarantine(), or .peek() before execution."
                )
            
            if not p._output_target and not p._quarantine_path and p._has_peeked:
                continue 

            payloads.append({
                "source": p._source,
                "steps": p._steps,
                "quarantine": p._quarantine_path,
                "output": p._output_target,
                "format": p._output_format,
                "config": self._config
            })
            
        if not payloads:
            return None if single_pipe else []

        try:
            from . import _phaeton
            
            raw_results = _phaeton.execute_batch(payloads, self._config)
            results = [EngineResult(r) for r in raw_results]
            
            if single_pipe:
                return results[0] if results else None
            
            return results

        except ImportError:
            raise EngineError("Phaeton Rust Core not found. Cannot execute pipeline.")