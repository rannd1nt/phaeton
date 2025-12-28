from .pipeline import Pipeline

class Engine:
    """
    Phaeton Engine.
    Manages resources and acts as the entry point for data ingestion.
    """
    def __init__(self, workers: int = 0, verbose: bool = False):
        """
        Initialize the engine.
        
        Args:
            workers (int): Number of threads. 0 = Auto-detect.
            verbose (bool): Enable detailed logging.
        """
        self.config = {
            "workers": workers,
            "verbose": verbose
        }

    def ingest(self, source: str) -> Pipeline:
        """
        Ingest data from a source file/path to start the pipeline.
        
        Args:
            source (str): Path to the file (e.g., "server.log", "data.csv")
            
        Returns:
            Pipeline: A new pipeline instance ready for chaining.
        """
        return Pipeline(source, self.config)

    def __repr__(self):
        return f"<Phaeton Engine | Workers: {self.config['workers']}>"