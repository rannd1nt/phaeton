# Phaeton

[![PyPI version](https://badge.fury.io/py/phaeton.svg)](https://badge.fury.io/py/phaeton)
[![Python Versions](https://img.shields.io/pypi/pyversions/phaeton.svg)](https://pypi.org/project/phaeton/)
[![Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> ⚠️ **Project Status:** Phaeton is currently in **Stable Beta (v0.3.0)**.
> The core streaming engine is fully functional. However, please note that some auxiliary methods (marked in docs) are currently placeholders and will be implemented in future versions.


**Phaeton** is a specialized, Rust-powered preprocessing and ETL engine designed to sanitize raw data streams before they reach your analytical environment.

It acts as the strictly typed **"Gatekeeper"** of your data pipeline. Unlike traditional DataFrame libraries that attempt to load entire datasets into RAM, Phaeton employs a **zero-copy streaming architecture**. It processes data chunk-by-chunk filtering noise, fixing encodings, and standardizing formats ensuring **O(1) memory complexity** relative to file size.

This allows you to process massive datasets on standard hardware without memory spikes, delivering clean, high-quality data to downstream tools like Pandas, Polars, or ML models.

> **The Philosophy:** Don't waste memory loading garbage. Clean the stream first, then analyze the gold.

---

## Key Features

* **Streaming Architecture:** Processes files chunk-by-chunk. Memory usage remains stable regardless of whether the file is 100MB or 100GB.
* **Parallel Execution:** Utilizes all CPU cores via **Rust Rayon** to handle heavy lifting (Regex, Fuzzy Matching) without blocking Python.
* **Strict Quarantine:** Bad data isn't just dropped silently; it's quarantined into a separate file with a generated `_phaeton_reason` column for auditing.
* **Smart Casting:** Automatically handles messy formats (e.g., `"Rp 5.250.000,00"` → `5250000` int) without complex manual parsing.
* **Privacy & Security:** Built-in email masking and SHA-256 hashing for PII compliance.
* **Configurable Engine:** Full control over `batch_size` and worker threads to tune performance for low-memory devices or high-end servers.


---

##  Performance Benchmark

Phaeton is optimized for "Dirty Data" scenarios involving heavy string parsing, regex filtering, and fuzzy matching.


**Test Scenario:**
* **Dataset:** 1 Million Rows of generated mixed dirty data.
* **Operations:** Trim whitespace, Currency scrubbing (`$ 50.000,00` -> `50000`), Type casting, Fuzzy Alignment (Typo correction for City names), and Filtering.
* **Hardware:** Entry-level Laptop (Intel Core i3-1220P, 16GB RAM).

**Results:**

| OS Environment | Speed (Rows/sec) | Duration (1M Rows) | Throughput |
| :--- | :--- | :--- | :--- |
| **Windows 11** | **~820,000 rows/s** | **1.21s** | **~70 MB/s** |
| **Linux (Arch)** | ~575,000 rows/s | 1.73s | ~49 MB/s |

<br>

> ⚠️ Note on I/O Bottleneck: The performance difference above is due to hardware configuration during testing.
> * Windows: Ran on internal NVMe SSD (High I/O speed).
> * Linux: Ran on External SSD via USB 3.2 enclosure (I/O Bottleneck).

In an equal hardware environment, Phaeton performs identically on Linux and Windows. The engine is heavily I/O bound; faster disk = faster processing.

---
##  Usage Example
Based on the features available in the current version.

```python
import phaeton

# 1. Initialize Engine
# 'strict=True' enables schema validation before execution starts.
eng = phaeton.Engine(workers=0, batch_size=25_000, strict=True)

# 2. Define Base Pipeline (Shared Logic)
base = (
    eng.ingest("dirty_data.csv")
        # Critical Data Filter: Drop row if 'email' OR 'username' is missing
        .prune(['email', 'username'])
        
        # Deduplication: Ensure email uniqueness across the dataset
        .dedupe('email')
        
        # Cleaning & Normalization
        .scrub('username', mode='trim')             # Remove whitespace
        .scrub('salary', mode='currency')           # Normalize format ("$ 5,000" -> "5000")
        
        # Type Enforcement: Validate data is integer, strip noise if needed
        .cast('salary', dtype='int', clean=True)
        
        # Imputation: Fill missing status with a default value
        .fill('status', value='UNKNOWN')
        
        # Correction: Fix typos using Jaro-Winkler distance
        .fuzzyalign('city',
            ref=['Jakarta', 'Minnesota'],
            threshold=0.85 
        ) # e.g., "Jkarta" -> "Jakarta"
)

# 3. Pipeline Branching using .fork()

# Pipeline 1: Secure & Clean Active Users
p1 = (
    base.fork('Active Users')
        .keep('status', match='ACTIVE', mode='exact')
        .hash('email', salt='s3cret')               # Anonymize PII (SHA-256)
        .dump('clean_active.csv')
)

# Pipeline 2: Audit Banned Users
p2 = (
    base.fork('Banned Analysis')
        .keep('status', match='BANNED', mode='exact')
        .quarantine('quarantine_banned.csv')        # Isolate bad rows for review
        .dump('clean_banned.csv')
)

# 4. Execute Pipelines in Parallel
# Returns a list of result statistics
stats = eng.exec([p1, p2])

print(f"Pipeline 1 (Active) | Processed: {stats[0].processed}, Saved: {stats[0].saved}")
print(f"Pipeline 2 (Banned) | Processed: {stats[1].processed}, Saved: {stats[1].saved}")
```

---

## Installation

Phaeton provides **Universal Wheels (ABI3)**. No Rust compiler needed.
```bash
pip install phaeton
```
> **Supported:** Python 3.8+ on Windows, Linux, and macOS (Intel & Apple Silicon).

---

## API Reference

### 1. Engine & Diagnostics <br>
| Method | Description |
| :--- | :--- | 
| `phaeton.probe(path)` | Detects encoding and delimiter automatically. |
| `eng.ingest(source)` | Creates a new pipeline builder. |
| `eng.exec(pipelines)` | Executes pipelines in parallel threads. |
| `eng.validate(pipelines)` | Runs a schema dry-run check without executing data processing. | 


### 2. Pipeline: Cleaning & Transformation <br>
Methods to sanitize data content.

| Method | Description |
| :--- | :--- |
| `.decode(encoding)` | Fixes file encoding (e.g., `latin-1` or `cp1252`). **Mandatory** as the first step if encoding is broken. |
| `.scrub(col, mode)` | Basic string cleaning. <br> **Modes:** `'trim'`, `'lower'`, `'upper'`, `'currency'`, `'html'`, `numeric_only`, `email (masking)` . |
|`.fill(col, val, method)`|**Methods:** `fixed` (constant value) or `ffill` (forward fill).|
|`.dedupe(col)`|Removes duplicates. `col` can be `None` (full row), `str` (single col), or `list` (composite key).|
| `.fuzzyalign(col, ref, threshold)` | Fixes typos using Jaro-Winkler distance against a reference list. |
| `.cast(col, dtype, clean)` | **Smart Cast.** Converts types (`int`/`float`/`bool`). <br> Set `clean=True` to strip non-numeric chars before casting. |

### 3. Pipeline: Structure & Security
Methods to manage columns and privacy.

| Method | Description |
| :--- | :--- |
| `.headers(style)` | Standardizes header casing. <br> **Styles:** `'snake'`, `'camel'`, `'pascal'`, `'kebab', 'constant`. |
| `.rename(mapping)` | Renames specific columns using a dictionary mapping `({'old': 'new'})`. |
| `.hash(col, salt)` | Applies hashing (SHA-256) to specific columns for PII anonymization. |
|`.map(col, mapping)`| Maps values using a dictionary lookup (VLOOKUP style).|


### 4. Pipeline: Output & Flow

Methods to save the final results or handle rejected data.

| Method | Description |
| :--- | :--- |
| `.quarantine(path)` | Saves rejected rows (with reasons) to a separate CSV file. |
| `.dump(path, format)` | Saves clean data to `.csv`. |
|`.fork(tag)`|Creates a branch of the pipeline.|
|`.peek(n, col)`| Runs a dry-run preview. `n`: rows limit. `col`: specific column(s) to inspect (optional). |

<br>

> ⚠️ **Placeholder Methods (Coming Soon)**
>
> These methods are present in the API for compatibility but do not perform operations yet in v0.3.0.
> * `reformat(col, ...)`: Date parsing/reformatting.
> * `split(col, ...)`: Splitting columns.
> * `combine(cols, ...)`: Merging columns.
---

## Roadmap

Phaeton is currently in **Stable Beta (v0.3.0)**. Here is the status of our development:

| Feature | Status | Implementation Notes |
| :--- | :---: | :--- |
| **Parallel Streaming Engine** | ✅ Ready | Powered by Rust Rayon (Multi-core) |
| **Filter Logic & Regex** | ✅ Ready | `keep`, `discard`, `prune` implemented |
| **Text Scrubbing** | ✅ Ready | HTML, Currency, Email Masking, etc. |
| **Type Enforcement** | ✅ Ready | Validates data types & scrubs noise for clean CSV output |
| **Fuzzy Alignment** | ✅ Ready | Jaro-Winkler for typo correction |
| **Quarantine System** | ✅ Ready | Full audit trail for rejected rows |
| **Deduplication** | ✅ Ready | Row-level & Column-level dedupe |
| **Hashing & Anonymization** | ✅ Ready | SHA-256 for PII data |
| **Header Normalization** | ✅ Ready  | `snake_case`, `camelCase` conversions |
|**Strict Schema Validation**| ✅ Ready | `Engine(strict=True)`|
| **Inspector Engine** | 📝 Planned | Dedicated stream for data profiling (Read-Only) |
| **Date Normalization** | 📝 Planned | Auto-detect & reformat dates |
| **Parquet/Arrow Support** | 📝 Planned | Native output integration |

---

## Contributing

This project is built with **Maturin** (PyO3 + Rust). Interested in contributing?

1.  **Clone** this repository.
2.  Ensure **Rust & Cargo** are installed.
3.  Set up the environment and build:

```bash
# Setup virtual environment (optional)
python -m venv .venv
source .venv/bin/activate

# Build & Install package in development mode
maturin develop --release
```