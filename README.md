# Phaeton

[![PyPI version](https://badge.fury.io/py/phaeton.svg)](https://badge.fury.io/py/phaeton)
[![Python Versions](https://img.shields.io/pypi/pyversions/phaeton.svg)](https://pypi.org/project/phaeton/)
[![Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> ⚠️ **Project Status:** Phaeton is currently in **Experimental Beta (v0.2.0)**.
> The core streaming engine is functional, but the library is currently under limited maintenance due to the author's personal schedule.


**Phaeton** is a specialized, Rust-powered preprocessing engine designed to sanitize raw data streams before they reach your analytical environment.

It acts as the strictly typed **"Gatekeeper"** of your data pipeline. Unlike traditional DataFrame libraries that load entire datasets into RAM, Phaeton employs a **zero-copy streaming architecture**. It processes data chunk-by-chunk—filtering noise, fixing encodings, and standardizing formats ensuring **O(1) memory complexity**.

This allows you to process massive datasets (GBs/TBs) on standard hardware without memory spikes, delivering clean, high-quality data to downstream tools like Pandas, Polars, or ML models.

> **The Philosophy:** Don't waste memory loading garbage. Clean the stream first, then analyze the gold.

---

## 🚀 Key Features

* **Streaming Architecture:** Processes files chunk-by-chunk. Memory usage remains flat and low regardless of file size.
* **Parallel Execution:** Utilizes all CPU cores via Rayon (Rust) for heavy lifting (Regex, Fuzzy Matching).
* **Strict Quarantine:** Bad data isn't just dropped; it's quarantined into a separate file with a generated `_phaeton_reason` column for auditing.
* **Smart Casting:** Automatically handles messy currency formats (e.g., `"$ 5.000,00"` → `5000.0` float) without manual string parsing.
* **Zero-Copy Logic:** Built on Rust's `Cow<str>` to minimize memory allocation during processing.

---

## 📦 Installation

```bash
pip install phaeton
```

## ⚡ Key Features

**1. The Scenario**

You have a dirty CSV `(raw_data.csv)` with mixed encodings, typos in city names, and messy currency strings. You want a clean Parquet file for Pandas.

**2. The Code**

```python
import phaeton

# 1. Probe the file (Auto-detect encoding, delimiter, headers, etc)
info = phaeton.probe("raw_data.csv")
print(f"Detected: {info['encoding']} with delimiter '{info['delimiter']}'")

# 2. Initialize Engine (0 = Use all CPU cores)
eng = phaeton.Engine(workers=0)

# 3. Build the Pipeline
pipeline = (
    eng.ingest("raw_data.csv")
       
       # GATEKEEPING: Fix encoding & standardize headers
       .decode(encoding=info['encoding'])
       .headers(style="snake")
       
       # ELIMINATION: Remove useless rows
       .prune(col="email") # Drop rows with empty email
       .discard(col="status", match="BANNED", mode="exact")
       
       # TRANSFORMATION: Smart Cleaning
       # "$ 30.000,00" -> 30000 (Integer)
       # If it fails (e.g., "Free"), send row to Quarantine
       .cast("salary", type="int", clean=True, on_error="quarantine")
       
       # FUZZY FIXING: Fix typos ("Cihcago" -> "Chicago")
       .fuzzyalign(
           col="city", 
           ref=["Chicago", "Jakarta", "Shanghai"], 
           threshold=0.85
        )
       
       # OUTPUT: Split into Clean Data & Audit Log
       .quarantine("bad_data_audit.csv")
       .dump("clean_data.parquet")
)

# 4. Execute (Rust takes over)
stats = eng.exec([pipeline])

print(f"Processed: {stats.processed} rows")
print(f"Saved: {stats.saved} | Quarantined: {stats.quarantined}")
```

<br>

## 📊 Performance Benchmark

Phaeton is optimized for "Dirty Data" scenarios (String parsing, Regex filtering, Fuzzy matching).

**Test Environment:**
- **Dataset:** 1 Million Rows (Mixed dirty data: Typos, Currency strings, Encoding issues).
- **Hardware:** Entry Level Laptop.

**Result:**
| Metric | Phaeton |
| :---: | :---: |
| Speed | ~575,000 rows/sec |
| Memory Usage | ~50MB (Constant) |
| Strategy | Parallel Streaming |

<br>

> Note: Phaeton maintains low memory footprint even when processing multi-gigabyte files due to its zero-copy streaming architecture.

<br>

## 📚 API Reference

### Root Module <br>
| Method | Description 
| :---: | :---: | 
| `phaeton.probe(path)` | Detects encoding (e.g., Windows-1252) and delimiter automatically. |

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

---

## 🗺️ Roadmap

Phaeton is currently in **Beta (v0.2.0)**. Here is the status of our development pipeline:

| Feature | Status | Notes |
| :--- | :---: | :--- |
| **Parallel Streaming Engine** | ✅ Ready | Powered by Rayon |
| **Smart Type Casting** | ✅ Ready | Auto-clean numeric strings |
| **Quarantine Logic** | ✅ Ready | Audit logs for bad data |
| **Fuzzy Alignment** | ✅ Ready | Jaro-Winkler / Levenshtein |
| **SHA-256 Hashing** | 📝 Planned | Security for PII data |
| **Column Splitting & Combining** | 📝 Planned | - |
| **Imputation (`.fill()`)** | 📝 Planned | Mean/Median/Mode fill |
| **Parquet/Arrow Integration** | 📝 Planned | Native output support |

---

## 🤝 Contributing

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