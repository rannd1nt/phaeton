"""
Phaeton
=======

A specialized, Rust-powered preprocessing and ETL engine designed to sanitize 
raw data streams before they reach your analytical environment.

Phaeton acts as the **"Gatekeeper"** of your data pipeline. Phaeton employs a 
**zero-copy streaming architecture** to ensure maximum performance and low memory usage.

-------------------------------------------------------------------------------

**Project Status: Stable Beta (v0.3.0)**

* **Format Support:** Currently optimized for **CSV files**.
* **Core Features:**
* **Streaming Architecture:** Processes files chunk-by-chunk with constant memory.
    * **Row Filtering:** Powerful `keep`, `discard`, and `prune` logic (Regex support).
    * **Advanced Scrubbing:** Smart cleaning for Currency, HTML, and Numeric data.
    * **Privacy & Security:** Secure `hash` (SHA-256) and Email Masking.
    * **Deduplication:** High-performance sharded deduplication (Full Row or Composite Key).
    * **Data Imputation:** Static or Streaming Forward Fill (`ffill`).
    * **Developer Experience:** Interactive `peek()` table preview and Strict Schema Validation.

-------------------------------------------------------------------------------

:copyright: (c) 2026 by rannd1nt.
:license: MIT, see LICENSE for more details.
"""

from .engine import Engine, EngineResult
from .pipeline import Pipeline
from .exceptions import (
    Error, 
    ValueError, 
    SchemaError, 
    ConfigurationError, 
    StateError, 
    EngineError
)

__all__ = [
    "Engine", "EngineResult", "Pipeline",
    "Error", "ValueError", "SchemaError", "ConfigurationError", "StateError", "EngineError"
]

_HARDCODED_VERSION = "0.3.0"

try:
    from ._phaeton import __version__ as _rust_version
except ImportError:
    _rust_version = _HARDCODED_VERSION



def version() -> str:
    """
    Returns the current version of the Phaeton library and the underlying Rust engine.

    Returns:
        str: Version string (e.g., "Phaeton v1.0.0 (Phaeton Rust Core: v1.0.0)").
    """
    return f"Phaeton v{_HARDCODED_VERSION} (Phaeton Rust Core: v{_rust_version})"

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
        raise EngineError("Phaeton Rust Core not found. Cannot probe file.")