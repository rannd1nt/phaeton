class Error(Exception):
    """
    Base exception class for all Phaeton-related errors.
    Catch this to handle any error raised by the library.
    """
    pass

class ValueError(Error):
    """
    Raised when an operation receives an argument with an invalid type or value.
    This typically happens during pipeline construction (e.g., invalid scrub mode).
    """
    pass

class SchemaError(Error):
    """
    Raised when a requested column does not exist in the source dataset.
    This ensures pipeline integrity before execution begins.
    """
    pass

class ConfigurationError(Error):
    """
    Raised when the Engine or Pipeline is improperly configured.
    Examples include missing output targets or invalid worker counts.
    """
    pass

class StateError(Error, RuntimeError):
    """
    Raised when an operation is attempted on an object in an invalid state.
    (Reserved for future use regarding internal state transitions).
    """
    pass

class EngineError(Error):
    """
    Raised when the underlying Rust core engine encounters a critical failure.
    This includes I/O errors, memory allocation failures, or panic states.
    """
    pass

for cls in [Error, ValueError, SchemaError, ConfigurationError, StateError, EngineError]:
    cls.__module__ = "phaeton"