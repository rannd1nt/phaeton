# python/phaeton/__init__.py

# Coba import binary Rust (ini bakal gagal kalau belum dicompile, 
# jadi kita kasih try-except biar gak error pas lagi coding python doang)
try:
    from ._phaeton import __version__ as _rust_version
except ImportError:
    _rust_version = "0.0.0-dev"

# Import submodule biar user bisa akses phaeton.text, dll
from . import text, io, guard

# --- ROOT FUNCTIONS & CONFIG ---

_CONFIG = {
    "threads": 4,
    "strict": True
}

def configure(threads: int = 4, strict: bool = True):
    """Global configuration settings for Phaeton engine."""
    global _CONFIG
    _CONFIG["threads"] = threads
    _CONFIG["strict"] = strict
    # Nanti di sini kita pass config ke Rust
    print(f"DEBUG: Config updated -> {_CONFIG}")

def version() -> str:
    """Check library and engine version."""
    return f"Phaeton v0.1.0 (Engine: Rust v{_rust_version})"

def sanitize(text: str) -> str:
    """
    [DUMMY] Otomatis mendeteksi PII umum dan menggantinya dengan ***.
    """
    # Placeholder logic (Python side)
    if not text: return ""
    return text.replace("@", "***").replace("08", "**")