use rayon::prelude::*;
use std::collections::HashMap;
use crate::streaming::StreamProcessor;
use crate::error::Result;
use std::time::Instant;

pub struct Engine {
    workers: usize,
    batch_size: usize,
}

impl Engine {
    pub fn new(workers: usize, batch_size: usize) -> Self {
        let actual_workers = if workers == 0 {
            // Auto-detect CPU cores
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4) // Fallback to 4 cores
        } else {
            workers
        };
        
        // Configure Rayon thread pool
        rayon::ThreadPoolBuilder::new()
            .num_threads(actual_workers)
            .build_global()
            .ok(); // Ignore if already built
        
        Self { 
            workers: actual_workers,
            batch_size: if batch_size == 0 { 10_000 } else { batch_size } 
        }
    }

    /// Execute single pipeline (non-parallel)
    pub fn execute_single(&self, payload: HashMap<String, serde_json::Value>) -> Result<HashMap<String, u64>> {
        let start = Instant::now();
        
        let source = payload.get("source")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::error::PhaetonError::InvalidStep("Missing 'source'".into()))?
            .to_string();
        
        let steps: Vec<HashMap<String, serde_json::Value>> = payload.get("steps")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        
        let output = payload.get("output")
            .and_then(|v| v.as_str())
            .unwrap_or("output.csv");
        
        let quarantine = payload.get("quarantine")
            .and_then(|v| v.as_str());
        
        let processor = StreamProcessor::new(source, steps, 0, self.batch_size);
        let stats = processor.execute(output, quarantine)?;
        
        let mut result = HashMap::new();
        result.insert("processed_rows".to_string(), stats.processed);
        result.insert("saved_rows".to_string(), stats.saved);
        result.insert("quarantined_rows".to_string(), stats.quarantined);
        result.insert("duration_ms".to_string(), start.elapsed().as_millis() as u64);
        
        Ok(result)
    }
    
    /// Execute BATCH pipelines in PARALLEL 
    pub fn execute_parallel(&self, payloads: Vec<HashMap<String, serde_json::Value>>) -> Result<Vec<HashMap<String, u64>>> {
        let results: Result<Vec<_>> = payloads
            .par_iter() // RAYON PARALLEL ITERATOR
            .map(|payload| self.execute_single(payload.clone()))
            .collect();
        
        results
    }
}