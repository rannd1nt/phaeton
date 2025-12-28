from .engine import Engine
from .pipeline import Pipeline

__all__ = ["Engine", "Pipeline"]
_DEFAULT_ENGINE = Engine(workers=0)

try:
    from ._phaeton import __version__ as _rust_version
except ImportError:
    _rust_version = "0.0.0-dev"

def version() -> str:
    """Returns the library version string."""
    return f"Phaeton v0.1.0 (Engine: Rust v{_rust_version})"

def filter(source: str, filter: str) -> str:
    """Shortcut function to filter data from source."""
    results = (
        _DEFAULT_ENGINE.ingest(source)
            .filter(filter)
            .run()
    )
    return results

def clean(source: str, operation: dict) -> str:
    """Shortcut function to clean data from source."""
    pipeline = _DEFAULT_ENGINE.ingest(source)
    for op, mode in operation.items():
        if op == "sanitize":
            pipeline = pipeline.sanitize(mode)
        elif op == "filter":
            pipeline = pipeline.filter(mode)
    results = pipeline.run()
    return results