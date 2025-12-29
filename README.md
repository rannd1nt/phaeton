# Phaeton

[![PyPI version](https://badge.fury.io/py/phaeton.svg)](https://badge.fury.io/py/phaeton)
[![Python Versions](https://img.shields.io/pypi/pyversions/phaeton.svg)](https://pypi.org/project/phaeton/)
[![Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Phaeton** is a high-performance, memory-efficient data cleaning engine for Python, powered by **Rust**.

It is designed to be the **"Gatekeeper"** of your data pipeline. Phaeton sanitizes, validates, and standardizes massive datasets (GBs/TBs) using a streaming architecture before they enter your analysis tools (like Pandas, Polars, or ML models).

> **Why Phaeton?** Because cleaning 10GB of dirty CSVs shouldn't require 32GB of RAM.

---

## 🚀 Key Features

* **Streaming Architecture:** Processes files chunk-by-chunk. Memory usage remains flat and low regardless of file size.
* **Parallel Execution:** Utilizes all CPU cores via Rayon (Rust) for heavy lifting (Regex, Fuzzy Matching).
* **Strict Quarantine:** Bad data isn't just dropped; it's quarantined into a separate file with a generated `_phaeton_reason` column for auditing.
* **Smart Casting:** Automatically handles messy currency formats (e.g., `"Rp 5.000,00"` → `5000.0` float) without manual string parsing.
* **Zero-Copy Logic:** Built on Rust's `Cow<str>` to minimize memory allocation during processing.

---

## 📦 Installation

```bash
pip install phaeton
```

## ⚡ Key Features

1. The Scenario