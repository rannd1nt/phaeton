# Changelog 0.3.0 - Phaeton Update

This release introduces comprehensive data transformation capabilities, enhanced pipeline observability, and strict schema validation.

## New Features

### Core Transformations
- **feat(pipeline):** Added `dedupe()` method.
    - Supports **Full Row** deduplication (default).
    - Supports **Single Column** deduplication.
    - Supports **Composite Key** deduplication (List of columns).
- **feat(pipeline):** Added `fill()` method for data imputation.
    - Supports `fixed` value replacement.
    - Supports `ffill` (Streaming Forward Fill) for time-series/sequential data.
- **feat(pipeline):** Added `hash()` method for PII anonymization.
    - Uses SHA-256 algorithm.
    - Supports optional `salt` for security against Rainbow Table attacks.
- **feat(pipeline):** Added `map()` for dictionary-based value mapping (VLOOKUP style).
- **feat(structure):** Added `rename()` for column remapping `{old: new}`.
- **feat(structure):** Added `headers()` for casing normalization.
    - Supported styles: `snake`, `camel`, `pascal`, `kebab`, `constant`.

### Filter & Logic Enhancements
- **feat(pipeline):** Enhanced `prune()` to support **List[str]**.
    - Applies `ANY` logic (drops row if *any* specified column is empty).
- **feat(pipeline):** Enhanced `keep()` and `discard()` to support **List[str]** inputs.
    - Allows filtering based on multiple exact matches (e.g., `match=["A", "B"]`).

### Developer Experience (DX)
- **feat(engine):** Implemented **Strict Schema Validation**.
    - `Engine(strict=True)` now pre-validates column existence and parameter types before Rust execution.
- **feat(pipeline):** Enhanced `.fork()` with `tag` parameter.
    - Enables human-readable lineage in logs (e.g., `PIPE-1 - Active Users`).
- **feat(exceptions):** Introduced granular Exception hierarchy.
    - Added `SchemaError`, `ConfigurationError`, `ValueError`, `StateError`, and `EngineError`.

## Bug Fixes & Refactoring

- **fix(pipeline):** Rewrote `.peek()` implementation.
    - Now correctly executes a dry-run stream.
    - Added `col` parameter to preview specific columns only.
    - Fixed headers transformation not reflecting in preview.
- **fix(engine):** Fixed validation logic to correctly handle Enum/Literal types using `typing.get_args`.
- **refactor(validation):** Moved validation logic to `Pipeline._validate()` for better encapsulation and support for manual triggering.

## Breaking Changes

- `Engine` constructor now accepts a `strict` boolean parameter.
- `peek()` output format is now strictly controlled by `tabulate` with `disable_numparse=True`.