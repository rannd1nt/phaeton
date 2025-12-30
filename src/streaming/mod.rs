use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::collections::HashMap;
use csv::{ReaderBuilder, WriterBuilder, StringRecord};
use rayon::prelude::*;
use serde_json::Value;
use regex::Regex;

use crate::error::{PhaetonError, Result};
use crate::processors::{text, cast, filter}; 


enum RowResult {
    Keep(StringRecord),
    Discarded(StringRecord, String),
}

// save pre-compiled steps
enum PreparedStep {
    Scrub { col_idx: usize, mode: String },
    
    KeepRegex { col_idx: usize, re: Regex }, 
    KeepString { col_idx: usize, pattern: String, mode: String }, // Exact/Contains
    
    DiscardRegex { col_idx: usize, re: Regex },
    DiscardString { col_idx: usize, pattern: String, mode: String },
    
    Prune { col_idx: Option<usize> },
    Cast { col_idx: usize, dtype: String, clean: bool },
    
    Align { col_idx: usize, ref_list: Vec<String>, threshold: f64 },
}

pub struct StreamProcessor {
    source: String,
    steps: Vec<HashMap<String, Value>>,
    limit: Option<usize>,
    batch_size: usize,
}

pub struct ExecutionStats {
    pub processed: u64,
    pub saved: u64,
    pub quarantined: u64,
    pub duration_ms: u64,
}

impl StreamProcessor {
    pub fn new(source: String, steps: Vec<HashMap<String, Value>>, limit: usize, batch_size: usize) -> Self {
        let effective_batch = if batch_size == 0 { 10_000 } else { batch_size };
        
        Self { 
            source, 
            steps, 
            limit: if limit == 0 { None } else { Some(limit) },
            batch_size: effective_batch 
        }
    }

    pub fn peek(&self) -> Result<Vec<HashMap<String, String>>> {
         let file = File::open(&self.source).map_err(|_| PhaetonError::FileNotFound(self.source.clone()))?;
         let reader = BufReader::new(file);
         let mut csv_reader = ReaderBuilder::new().has_headers(true).flexible(true).from_reader(reader);
         let headers = csv_reader.headers()?.clone();
         let mut results = Vec::new();
         for (idx, result) in csv_reader.records().enumerate() {
             if let Some(limit) = self.limit { if idx >= limit { break; } }
             let record = result?;
             let mut row = HashMap::new();
             for (i, field) in record.iter().enumerate() {
                 if let Some(header) = headers.get(i) { row.insert(header.to_string(), field.to_string()); }
             }
             results.push(row);
         }
         Ok(results)
    }

    pub fn execute(&self, output_path: &str, quarantine_path: Option<&str>) -> Result<ExecutionStats> {
        let file = File::open(&self.source).map_err(|_| PhaetonError::FileNotFound(self.source.clone()))?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(BufReader::new(file));
        let headers = reader.headers()?.clone();

        let out_file = File::create(output_path)?;
        let mut clean_writer = WriterBuilder::new().from_writer(BufWriter::new(out_file));
        clean_writer.write_record(&headers)?;

        let mut quarantine_writer = if let Some(path) = quarantine_path {
            let q_file = File::create(path)?;
            let mut w = WriterBuilder::new().from_writer(BufWriter::new(q_file));
            let mut q_headers = headers.clone();
            q_headers.push_field("_phaeton_reason");
            w.write_record(&q_headers)?;
            Some(w)
        } else { None };

        // --- PREPARE STEPS (OPTIMIZED) ---
        let mut prepared_steps = Vec::new();
        
        let get_idx = |col_name: &str| -> Result<usize> {
            headers.iter().position(|h| h == col_name)
                .ok_or_else(|| PhaetonError::ColumnNotFound(col_name.to_string()))
        };

        for step in &self.steps {
            let action = step.get("action").and_then(|v| v.as_str()).unwrap_or("");
            
            let p_step = match action {
                "keep" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let match_val = step.get("match").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let mode = step.get("mode").and_then(|v| v.as_str()).unwrap_or("exact");
                    let idx = get_idx(col)?;
                    
                    if mode == "regex" {
                        // COMPILE REGEX HERE (ONCE!)
                        let re = Regex::new(&match_val).map_err(|_| PhaetonError::InvalidStep(format!("Invalid Regex: {}", match_val)))?;
                        PreparedStep::KeepRegex { col_idx: idx, re }
                    } else {
                        PreparedStep::KeepString { col_idx: idx, pattern: match_val, mode: mode.to_string() }
                    }
                },
                "discard" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let match_val = step.get("match").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let mode = step.get("mode").and_then(|v| v.as_str()).unwrap_or("exact");
                    let idx = get_idx(col)?;

                    if mode == "regex" {
                         let re = Regex::new(&match_val).map_err(|_| PhaetonError::InvalidStep(format!("Invalid Regex: {}", match_val)))?;
                         PreparedStep::DiscardRegex { col_idx: idx, re }
                    } else {
                        PreparedStep::DiscardString { col_idx: idx, pattern: match_val, mode: mode.to_string() }
                    }
                },
                "prune" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("*");
                    let col_idx = if col == "*" { None } else { Some(get_idx(col)?) };
                    PreparedStep::Prune { col_idx }
                },
                "scrub" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let mode = step.get("mode").and_then(|v| v.as_str()).unwrap_or("trim").to_string();
                    PreparedStep::Scrub { col_idx: get_idx(col)?, mode }
                },
                "cast" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let dtype = step.get("type").and_then(|v| v.as_str()).unwrap_or("str").to_string();
                    let clean = step.get("clean").and_then(|v| v.as_bool()).unwrap_or(false);
                    PreparedStep::Cast { col_idx: get_idx(col)?, dtype, clean }
                },
                "align" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let threshold = step.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.85);
                    let ref_list: Vec<String> = step.get("ref")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();
                    
                    PreparedStep::Align { col_idx: get_idx(col)?, ref_list, threshold }
                }
                _ => continue, 
            };
            prepared_steps.push(p_step);
        }

        // --- EXECUTION LOOP ---
        let mut total_processed = 0;
        let mut total_saved = 0;
        let mut total_quarantined = 0;
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut iter = reader.into_records();

        loop {
            batch.clear();
            for _ in 0..self.batch_size {
                match iter.next() {
                    Some(Ok(record)) => batch.push(record),
                    Some(Err(_)) => continue,
                    None => break,
                }
            }

            if batch.is_empty() { break; }
            total_processed += batch.len() as u64;

            let results: Vec<RowResult> = batch.par_iter()
                .map(|record| apply_pipeline(record, &prepared_steps))
                .collect();

            for res in results {
                match res {
                    RowResult::Keep(rec) => {
                        clean_writer.write_record(&rec)?;
                        total_saved += 1;
                    },
                    RowResult::Discarded(rec, reason) => {
                        if let Some(ref mut q_writer) = quarantine_writer {
                            let mut q_rec = rec.clone();
                            q_rec.push_field(&reason);
                            q_writer.write_record(&q_rec)?;
                        }
                        total_quarantined += 1;
                    }
                }
            }
        }

        clean_writer.flush()?;
        if let Some(mut qw) = quarantine_writer { qw.flush()?; }

        Ok(ExecutionStats {
            processed: total_processed,
            saved: total_saved,
            quarantined: total_quarantined,
            duration_ms: 0,
        })
    }
}

// --- CORE LOGIC ---
fn apply_pipeline(record: &StringRecord, steps: &[PreparedStep]) -> RowResult {
    let mut rec = record.clone();

    for step in steps {
        match step {
            // Prune
            PreparedStep::Prune { col_idx } => {
                let should_prune = match col_idx {
                    Some(idx) => rec.get(*idx).map(|val| filter::is_empty(val)).unwrap_or(false),
                    None => rec.iter().any(|field| filter::is_empty(field))
                };
                if should_prune { return RowResult::Discarded(rec, "Prune: Empty value".into()); }
            },
            
            // Regex
            PreparedStep::KeepRegex { col_idx, re } => {
                let violation = if let Some(val) = rec.get(*col_idx) {
                    if !re.is_match(val) { Some(format!("Keep: Regex mismatch")) } else { None }
                } else { None };
                if let Some(reason) = violation { return RowResult::Discarded(rec, reason); }
            },

            // Match
            PreparedStep::KeepString { col_idx, pattern, mode } => {
                let violation = if let Some(val) = rec.get(*col_idx) {
                    let matches = match mode.as_str() {
                        "exact" => filter::matches_exact(val, pattern),
                        "contains" => filter::contains_pattern(val, pattern),
                         _ => false
                    };
                    if !matches { Some(format!("Keep: Mismatch '{}'", pattern)) } else { None }
                } else { None };
                if let Some(reason) = violation { return RowResult::Discarded(rec, reason); }
            },
            
            // Discard
            PreparedStep::DiscardString { col_idx, pattern, mode } => {
                 let violation = if let Some(val) = rec.get(*col_idx) {
                    let matches = match mode.as_str() {
                        "exact" => filter::matches_exact(val, pattern),
                        "contains" => filter::contains_pattern(val, pattern),
                        _ => false
                    };
                    if matches { Some(format!("Discard: Matched forbidden '{}'", pattern)) } else { None }
                } else { None };
                if let Some(reason) = violation { return RowResult::Discarded(rec, reason); }
            },
            
            // Scrub
            PreparedStep::Scrub { col_idx, mode } => {
                if let Some(val) = rec.get(*col_idx) {
                    let new_val = match mode.as_str() {
                        "currency" => text::scrub_currency(val),
                        "numeric_only" => text::scrub_numeric_only(val),
                        "trim" => text::trim_whitespace(val),
                        "html" => text::remove_html_tags(val),
                        "lower" => text::to_lowercase(val),
                        "upper" => text::to_uppercase(val),
                        _ => Cow::Borrowed(val) 
                    };
                    if let Cow::Owned(v) = new_val {
                        let mut new_rec = StringRecord::new();
                        for (i, field) in rec.iter().enumerate() {
                            if i == *col_idx { new_rec.push_field(&v); } else { new_rec.push_field(field); }
                        }
                        rec = new_rec;
                    }
                }
            },
            
            // Align / fuzzyalign
            PreparedStep::Align { col_idx, ref_list, threshold } => {
                if let Some(val) = rec.get(*col_idx) {
                    // Only process non-empty values
                    if !filter::is_empty(val) {
                        // Search best match
                        let mut best_match: Option<&String> = None;
                        let mut best_score = 0.0;

                        for target in ref_list {
                            let score = strsim::jaro_winkler(val, target);
                            if score > best_score {
                                best_score = score;
                                best_match = Some(target);
                            }
                        }

                        // If best score >= threshold, replace
                        if best_score >= *threshold {
                            if let Some(target) = best_match {
                                if val != target { // Only update if different
                                    let mut new_rec = StringRecord::new();
                                    for (i, field) in rec.iter().enumerate() {
                                        if i == *col_idx { new_rec.push_field(target); } else { new_rec.push_field(field); }
                                    }
                                    rec = new_rec;
                                }
                            }
                        }
                    }
                }
            },

            // Cast
            PreparedStep::Cast { col_idx, dtype, clean } => {
                 let cast_error = if let Some(val) = rec.get(*col_idx) {
                     // Handle empty string before casting
                     if filter::is_empty(val) {
                         Some("Cannot convert empty string".to_string())
                     } else {
                         let result = match dtype.as_str() {
                             "float" => cast::to_float(val, "unknown", *clean).map(|_| ()),
                             "int" => cast::to_int(val, "unknown", *clean).map(|_| ()),
                             "bool" => cast::to_bool(val, "unknown").map(|_| ()),
                             _ => Ok(())
                         };
                         if let Err(e) = result { Some(e.to_string()) } else { None }
                     }
                 } else { None };

                 if let Some(reason) = cast_error { return RowResult::Discarded(rec, reason); }
            }

            _ => {}
        }
    }

    RowResult::Keep(rec)
}