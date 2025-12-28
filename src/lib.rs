use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

mod engine;
mod processors;

use processors::{io, text};

#[pyfunction]
fn execute_pipeline(_py: Python, source: String, steps: &PyList, config: &PyDict) -> PyResult<Vec<String>> {
    
    // 1. Engine setup
    let workers = match config.get_item("workers")? {
        Some(item) => item.extract::<usize>().unwrap_or(0),
        None => 0,
    };

    engine::setup(workers);

    // 2. Read File
    let mut data = io::read_file(&source).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read {}: {}", source, e))
    })?;

    let mut output_path: Option<String> = None;

    // 3. Loop through steps
    for step in steps.iter() {
        let step_dict = step.downcast::<PyDict>()?;
        
        let action_item = step_dict.get_item("action")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'action' key"))?;
        
        let action: String = action_item.extract()?;

        match action.as_str() {
            "filter" => {
                let pattern_item = step_dict.get_item("pattern")?
                    .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'pattern' for filter"))?;
                let pattern: String = pattern_item.extract()?;
                
                data = text::filter(data, &pattern);
            },
            "sanitize" => {
                let mode = match step_dict.get_item("mode")? {
                    Some(item) => item.extract::<String>().unwrap_or_else(|_| "default".to_string()),
                    None => "default".to_string(),
                };
                
                data = text::sanitize(data, &mode);
            },
            "save" => {
                 let path_item = step_dict.get_item("path")?
                    .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'path' for save"))?;
                 let path: String = path_item.extract()?;
                 
                 io::write_file(&path, &data).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write to {}: {}", path, e))
                 })?;
                 
                 output_path = Some(path);
            },
            _ => {
                println!("Warning: Unknown action '{}'", action);
            }
        }
    }

    if let Some(path) = output_path {
        println!("Success: Processed {} lines. Saved to: {}", data.len(), path);
    } else {
        // pass
    }

    // Vec<String>
    Ok(data) 
}

#[pymodule]
fn _phaeton(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute_pipeline, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}