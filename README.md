# Phaeton

[![PyPI version](https://badge.fury.io/py/phaeton.svg)](https://badge.fury.io/py/phaeton)
[![Python Versions](https://img.shields.io/pypi/pyversions/phaeton.svg)](https://pypi.org/project/phaeton/)
[![Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> ⚠️ **Project Status:** Phaeton is currently in **Experimental Beta (v0.2.3)**.
> The core streaming engine is functional, but the library is currently under limited maintenance due to the author's personal schedule. So, some methods are still not working or are only dummy or mockup methods.


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
* **Configurable Engine:** Full control over `batch_size` and worker threads to tune performance for low-memory devices or high-end servers.

---

##  Performance Benchmark

Phaeton is optimized for "Dirty Data" scenarios involving heavy string parsing, regex filtering, and fuzzy matching.


**Test Scenario:**
We generated a **Chaos Dataset** containing **1 Million Rows** of mixed dirty data:
* **Operations:** Trim whitespace, Currency scrubbing (`$ 50.000,00` -> `50000`), Type casting, Fuzzy Alignment (Typo correction for City names), and Regex Filtering.
* **Hardware:** Entry-level Laptop (Intel Core i3-1220P, 16GB RAM).

**Results:**

| OS Environment | Speed (Rows/sec) | Duration (1M Rows) | Throughput |
| :--- | :--- | :--- | :--- |
| **Windows 11** | **~820,000 rows/s** | **1.21s** | **~70 MB/s** |
| **Linux (Arch)** | ~575,000 rows/s | 1.73s | ~49 MB/s |

> *Note: Phaeton maintains a low and predictable memory footprint (~10-20MB overhead) regardless of the input file size due to its streaming nature.*

---
##  Usage Example
Based on the features available in the current version.

```python
import phaeton

# 1. Initialize Engine (Auto-detect cores)
eng = phaeton.Engine(workers=0, batch_size=25_000)

# 2. Define Pipeline

# Base Pipeline
base = (
    eng.ingest("dirty_data.csv")
        .prune(col="email")                         # Drop rows if email is empty
        .prune(col="salary")                        # Drop rows if salary is empty
        .scrub("username", "trim")                  # Clean whitespace
        .scrub("salary", "currency")                # Parse "Rp 5.000" to number
        .cast("salary", "int", clean=True)          # Safely cast to Integer
        .fuzzyalign("city",
            ref=["Jakarta", "Bandung"],
            threshold=0.85
        ) # Fix typos
)

# 3 Pipeline branching using .fork() (Optional)

# Pipeline 1: Keep all rows except status 'BANNED'
p1 = (
    base.fork()
        .discard("status", "BANNED", mode="exact")  # Filter specific values (BANNED)
        .quarantine("quarantine_1.csv")             # Save bad data here
        .dump("clean_data_1.csv")                   # Save good data here
)

# Pipeline 2: Only rows with 'ACTIVE' status keeped
p2 = (
    base.fork()
        .keep("status", "ACTIVE", mode="exact")     # Keep specific values (ACTIVE)
        .quarantine("quarantine_output_2.csv")      # Save bad data here
        .dump("cleaned_output_2.csv", format="csv") # Save good data here
)

# 4. Execute Two Pipeline in Parallel
stats = engine.exec([p1, p2])
print(f"Pipeline 1 = Processed: {stats[0].processed}, Saved: {stats[0].saved}")
print(f"Pipeline 2 = Processed: {stats[1].processed}, Saved: {stats[1].saved}")
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

### Root Module <br>
| Method | Description 
| :---: | :---: | 
| `phaeton.probe(path)` | Detects encoding (e.g., Windows-1252) and delimiter automatically. |

### Engine Methods <br>

Methods to save the final results or handle rejected data.

| Method | Description |
| :--- | :--- |
| `.ingest(source)` | Creates a new data processing pipeline for a specific source file. |
| `.exec(pipelines)` | Executes multiple pipelines in parallel. |
| `.validate(pipelines)` | Performs a dry-run to validate schema compatibility. | 

### Pipeline Methods <br>
These methods are chainable.

#### Transformation & Cleaning

Methods focused on data sanitization and ETL logic.

| Method | Description |
| :--- | :--- |
| `.decode(encoding)` | Fixes file encoding (e.g., `latin-1` or `cp1252`). **Mandatory** as the first step if encoding is broken. |
| `.scrub(col, mode)` | Basic string cleaning. <br> **Modes:** `'trim'`, `'lower'`, `'upper'`, `'currency'`, `'html'`. |
| `.fuzzyalign(col, ref, threshold)` | Fixes typos using *Levenshtein distance* against a reference list. |
| `.reformat(col, to, from)` | Standardizes date formats to ISO-8601 or custom formats. |
| `.cast(col, type, clean)` | **Smart Cast.** Converts types (`int`/`float`/`bool`). <br> Set `clean=True` to strip non-numeric chars before casting. |

#### Structure & Security

Methods for column management and data privacy.

| Method | Description |
| :--- | :--- |
| `.headers(style)` | Standardizes header casing. <br> **Styles:** `'snake'`, `'camel'`, `'pascal'`, `'kebab'`. |
| `.hash(col, salt)` | Applies hashing (SHA-256) to specific columns for PII anonymization. |
| `.rename(mapping)` | Renames specific columns using a dictionary mapping. |

#### Output

Methods to save the final results or handle rejected data.

| Method | Description |
| :--- | :--- |
| `.quarantine(path)` | Saves rejected rows (with reasons) to a separate CSV file. |
| `.dump(path, format)` | Saves clean data to `.csv`, `.parquet`, or `.json` formats. |

#### Utility & Workflow
Methods to save the final results or handle rejected data.

| Method | Description |
| :--- | :--- |
| `.fork()` | Creates a deep copy of the current pipeline branch. Useful for splitting logic (e.g., saving to multiple formats or creating different clean levels) without rewriting steps. |
| `.peek(n)` | Previews the first n rows. |

---

## Roadmap

Phaeton is currently in **Beta (v0.2.3)**. Here is the status of our development:

| Feature | Status | Implementation Notes |
| :--- | :---: | :--- |
| **Parallel Streaming Engine** | ✅ Ready | Powered by Rust Rayon (Multi-core) |
| **Regex & Filter Logic** | ✅ Ready | `keep`, `discard`, `prune` implemented |
| **Smart Type Casting** | ✅ Ready | Auto-clean numeric strings (`"Rp 5,000"` -> `5000`) |
| **Fuzzy Alignment** | ✅ Ready | Jaro-Winkler for typo correction |
| **Quarantine System** | ✅ Ready | Full audit trail for rejected rows |
| **Basic Text Scrubbing** | ✅ Ready | Trim, HTML strip, Case conversion |
| **Inspector Engine** | 📝 Planned | Dedicated stream for data profiling (Read-Only) |
| **Header Normalization** | 📝 Planned  | `snake_case`, `camelCase` conversions |
| **Date Normalization** | 📝 Planned | Auto-detect & reformat dates |
| **Deduplication** | 📝 Planned | Row-level & Column-level dedupe |
| **Hashing & Anonymization** | 📝 Planned | SHA-256 for PII data |
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