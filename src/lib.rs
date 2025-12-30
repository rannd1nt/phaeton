use pyo3::prelude::*;
use std::collections::HashMap;
use pythonize::depythonize;
use serde_json::Value;

mod engine;
mod processors;
mod streaming;
mod error;

use engine::Engine;
use streaming::StreamProcessor;

/// Probe file to detect metadata: encoding, delimiter, headers, etc.
#[pyfunction]
fn probe_file_header(path: String) -> PyResult<HashMap<String, String>> {
    let result = processors::probe::detect_file_metadata(&path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
    
    Ok(result)
}

/// Preview n rows of the pipeline
#[pyfunction]
fn preview_pipeline(_py: Python, source: String, steps_py: PyObject, n: usize) -> PyResult<Vec<HashMap<String, String>>> {
    let steps: Vec<HashMap<String, Value>> = depythonize(steps_py.as_ref(_py))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid steps format: {}", e)))?;

    let processor = StreamProcessor::new(source, steps, n, 10000);
    let preview = processor.peek()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    
    Ok(preview)
}

/// Execute single pipeline (.run())
#[pyfunction]
fn execute_pipeline(_py: Python, payload_py: PyObject) -> PyResult<HashMap<String, u64>> {
    let payload: HashMap<String, Value> = depythonize(payload_py.as_ref(_py))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid payload format: {}", e)))?;

    let engine = Engine::new(0, 10000); 
    let stats = engine.execute_single(payload)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    
    Ok(stats)
}

/// Execute BATCH pipelines (Parallel)
#[pyfunction]
fn execute_batch(
    _py: Python,
    payloads_py: PyObject, 
    config_py: PyObject
) -> PyResult<Vec<HashMap<String, u64>>> {
    
    // Translating Payloads (List of Dicts)
    let payloads: Vec<HashMap<String, Value>> = depythonize(payloads_py.as_ref(_py))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid payloads: {}", e)))?;
    
    // Translating Config (Dict)
    let config: HashMap<String, Value> = depythonize(config_py.as_ref(_py))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid config: {}", e)))?;

    // Get number of workers
    let workers = config.get("workers")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    // Get batch size
    let batch_size = config.get("batch_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(10_000) as usize;
    
    let engine = Engine::new(workers, batch_size);
    let results = engine.execute_parallel(payloads)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    
    Ok(results)
}

#[pymodule]
fn _phaeton(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(probe_file_header, m)?)?;
    m.add_function(wrap_pyfunction!(preview_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(execute_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(execute_batch, m)?)?;
    Ok(())
}