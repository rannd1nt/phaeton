
class Pipeline:
    """
    Data processing pipeline builder.
    """
    def __init__(self, source: str, config: dict):
        self.source = source
        self.config = config
        self.steps = [] # List of instructions (Receipt)

    def filter(self, pattern: str) -> "Pipeline":
        """
        Keep lines containing the pattern.
        """
        self.steps.append({
            "action": "filter",
            "pattern": pattern
        })
        return self

    def sanitize(self, mode: str = "default") -> "Pipeline":
        """
        Sanitize sensitive data based on mode.
        Modes: 'default', 'no-html', 'email-mask'.
        """
        self.steps.append({
            "action": "sanitize",
            "mode": mode
        })
        return self

    def save(self, path: str) -> "Pipeline":
        """Save results to file immediately during execution."""
        self.steps.append({
            "action": "save", 
            "path": path
        })
        return self
    
    def run(self):
        """
        Execute the pipeline.
        """
        try:
            from . import _phaeton
        except ImportError:
            raise ImportError("Phaeton core is not compiled.")

        # TODO: Kirim path, list of steps, dan config ke Rust
        return _phaeton.execute_pipeline(self.source, self.steps, self.config)

    def __repr__(self):
        return f"<Pipeline | Source: {self.source} | Steps: {len(self.steps)}>"