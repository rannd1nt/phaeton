from .engine import Engine, EngineResult
from .pipeline import Pipeline

__all__ = ["Engine", "EngineResult", "Pipeline"]
_DEFAULT_ENGINE = Engine(workers=0)

try:
    from ._phaeton import __version__ as _rust_version
except ImportError:
    _rust_version = "0.2.0-alpha"

def version() -> str:
    """
    Returns the current version of the Phaeton library and the underlying Rust engine.

    Returns:
        str: Version string (e.g., "Phaeton v0.2.0 (Engine: Rust v0.1.1)").
    """
    return f"Phaeton v0.2.0 (Engine: Rust v{_rust_version})"

def probe(source: str) -> dict:
    """
    Analyzes the first few bytes of a file to automatically detect metadata.
    
    This function is useful for determining the correct encoding and delimiter 
    before building a pipeline, preventing 'mojibake' or parsing errors.

    Args:
        source (str): Path to the input file.

    Returns:
        dict: A dictionary containing:
            - 'encoding' (str): Detected encoding (e.g., 'windows-1252', 'utf-8').
            - 'delimiter' (str): Detected CSV delimiter (e.g., ',', ';', '\t').
            - 'headers' (List[str]): Inferred column headers.
            - 'confidence' (float): Confidence score of the detection.

    Example:
        >>> info = phaeton.probe("raw_data.csv")
        >>> print(info['encoding'])
        'windows-1252'
    """
    try:
        from . import _phaeton
        return _phaeton.probe_file_header(source)
    except ImportError:
        print("[FATAL] Phaeton Core not found.")
        return {}