use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::collections::HashMap;
use csv::{ReaderBuilder, WriterBuilder, StringRecord};
use rayon::prelude::*;
use serde_json::Value;
use regex::Regex;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use sha2::{Sha256, Digest};
use heck::*;

use crate::error::{PhaetonError, Result};
use crate::processors::{text, cast, filter}; 


enum RowResult {
    Keep(StringRecord),
    Discarded(StringRecord, String),
}

// save pre-compiled steps
enum PreparedStep {
    Prune { col_idx: Option<usize> },
    PruneSelected { col_idxs: Vec<usize> },
    
    KeepRegex { col_idx: usize, re: Regex }, 
    KeepString { col_idx: usize, pattern: String, mode: String },
    KeepMultiString { col_idx: usize, patterns: Vec<String>, mode: String },
    
    DiscardRegex { col_idx: usize, re: Regex },
    DiscardString { col_idx: usize, pattern: String, mode: String },
    DiscardMultiString { col_idx: usize, patterns: Vec<String>, mode: String },

    Scrub { col_idx: usize, mode: String },

    Cast { col_idx: usize, dtype: String, clean: bool },

    FillFixed { col_idx: usize, value: String },
    FillForward { col_idx: usize, last_valid: Arc<Mutex<String>> },

    Dedupe { col_idxs: Option<Vec<usize>>, state: Arc<DedupeState> },

    Align { col_idx: usize, ref_list: Vec<String>, threshold: f64 },

    Map { col_idx: usize, mapping: HashMap<String, String>, default: Option<String> },

    Hash { col_idxs: Vec<usize>, salt: String },
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

const NUM_SHARDS: usize = 256;

struct DedupeState {
    shards: Vec<Mutex<HashSet<u64>>>,
}

impl DedupeState {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(HashSet::new()));
        }
        Self { shards }
    }

    fn check_and_insert(&self, hash: u64) -> bool {
        let shard_idx = (hash as usize) % NUM_SHARDS;
        let mut shard = self.shards[shard_idx].lock().unwrap();
        if shard.contains(&hash) {
            true
        } else {
            shard.insert(hash);
            false
        }
    }
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

    fn transform_headers(&self, original_headers: &StringRecord) -> StringRecord {
        let mut new_headers = original_headers.clone();
        
        for step in &self.steps {
            let action = step.get("action").and_then(|v| v.as_str()).unwrap_or("");
            
            match action {
                "rename" => {
                    if let Some(mapping) = step.get("mapping").and_then(|v| v.as_object()) {
                        let mut temp_headers = StringRecord::new();
                        for h in new_headers.iter() {
                            if let Some(new_name) = mapping.get(h).and_then(|v| v.as_str()) {
                                temp_headers.push_field(new_name);
                            } else {
                                temp_headers.push_field(h);
                            }
                        }
                        new_headers = temp_headers;
                    }
                },
                "headers" => {
                    let style = step.get("style").and_then(|v| v.as_str()).unwrap_or("snake");
                    let mut temp_headers = StringRecord::new();
                    
                    for h in new_headers.iter() {
                        let converted = match style {
                            "snake" => h.to_snake_case(),
                            "kebab" => h.to_kebab_case(),
                            "camel" => h.to_lower_camel_case(),
                            "pascal" => h.to_upper_camel_case(),
                            "constant" => h.to_shouty_snake_case(),
                            _ => h.to_string(),
                        };
                        temp_headers.push_field(&converted);
                    }
                    new_headers = temp_headers;
                },
                _ => {}
            }
        }
        new_headers
    }

    fn compile_steps(&self, headers: &StringRecord) -> Result<Vec<PreparedStep>> {
        let mut prepared_steps = Vec::new();
        
        let get_idx = |col_name: &str| -> Result<usize> {
            headers.iter().position(|h| h == col_name)
                .ok_or_else(|| PhaetonError::ColumnNotFound(col_name.to_string()))
        };

        for step in &self.steps {
            let action = step.get("action").and_then(|v| v.as_str()).unwrap_or("");
            
            let extract_match_val = |key: &str| -> String {
                step.get(key).map(|v| match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => String::new(),
                }).unwrap_or_default()
            };

            let p_step = match action {
                "keep" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let mode = step.get("mode").and_then(|v| v.as_str()).unwrap_or("exact");
                    let idx = get_idx(col)?;
                    
                    if mode == "regex" {
                        let match_val = extract_match_val("match");
                        let re = Regex::new(&match_val).map_err(|_| PhaetonError::InvalidStep(format!("Invalid Regex: {}", match_val)))?;
                        PreparedStep::KeepRegex { col_idx: idx, re }
                    } else if let Some(Value::Array(arr)) = step.get("match") {
                        let patterns: Vec<String> = arr.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect();
                        PreparedStep::KeepMultiString { col_idx: idx, patterns, mode: mode.to_string() }
                    } else {
                        let match_val = extract_match_val("match");
                        PreparedStep::KeepString { col_idx: idx, pattern: match_val, mode: mode.to_string() }
                    }
                },
                "discard" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let mode = step.get("mode").and_then(|v| v.as_str()).unwrap_or("exact");
                    let idx = get_idx(col)?;

                    if mode == "regex" {
                        let match_val = extract_match_val("match");
                        let re = Regex::new(&match_val).map_err(|_| PhaetonError::InvalidStep(format!("Invalid Regex: {}", match_val)))?;
                        PreparedStep::DiscardRegex { col_idx: idx, re }
                    } else if let Some(Value::Array(arr)) = step.get("match") {
                        let patterns: Vec<String> = arr.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect();
                        PreparedStep::DiscardMultiString { col_idx: idx, patterns, mode: mode.to_string() }
                    } else {
                        let match_val = extract_match_val("match");
                        PreparedStep::DiscardString { col_idx: idx, pattern: match_val, mode: mode.to_string() }
                    }
                },
                "prune" => {
                    let col_val = step.get("col").unwrap_or(&Value::Null);
                    match col_val {
                        Value::Array(arr) => {
                            let mut indices = Vec::new();
                            for v in arr {
                                if let Some(col_name) = v.as_str() { indices.push(get_idx(col_name)?); }
                            }
                            PreparedStep::PruneSelected { col_idxs: indices }
                        },
                        Value::String(s) => {
                            if s == "*" { PreparedStep::Prune { col_idx: None } } 
                            else { PreparedStep::Prune { col_idx: Some(get_idx(s)?) } }
                        },
                        _ => PreparedStep::Prune { col_idx: None }
                    }
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
                "fill" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let idx = get_idx(col)?;
                    
                    let method = step.get("method").and_then(|v| v.as_str()).unwrap_or("fixed");
                    
                    if method == "ffill" {
                        let state = Arc::new(Mutex::new(String::new()));
                        PreparedStep::FillForward { col_idx: idx, last_valid: state }
                    } else {
                        let fill_val = extract_match_val("value"); 
                        PreparedStep::FillFixed { col_idx: idx, value: fill_val }
                    }
                },
                "dedupe" => {
                    let col_val = step.get("col").unwrap_or(&Value::Null);
                    
                    let col_idxs = match col_val {
                        Value::Array(arr) => {
                            let mut indices = Vec::new();
                            for v in arr {
                                if let Some(col_name) = v.as_str() {
                                    indices.push(get_idx(col_name)?);
                                }
                            }
                            Some(indices)
                        },
                        
                        Value::String(s) => {
                            if s == "*" {
                                None
                            } else {
                                Some(vec![get_idx(s)?])
                            }
                        },
                        
                        _ => None,
                    };
                    
                    let state = Arc::new(DedupeState::new());
                    
                    PreparedStep::Dedupe { col_idxs, state }
                },
                "align" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let threshold = step.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.85);
                    let ref_list: Vec<String> = step.get("ref").and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();
                    PreparedStep::Align { col_idx: get_idx(col)?, ref_list, threshold }
                },
                "map" => {
                    let col = step.get("col").and_then(|v| v.as_str()).unwrap_or("");
                    let default_val = step.get("default").and_then(|v| v.as_str()).map(|s| s.to_string());
                    
                    let mut map_lookup = HashMap::new();
                    if let Some(mapping) = step.get("mapping").and_then(|v| v.as_object()) {
                        for (k, v) in mapping {
                            if let Some(val_str) = v.as_str() {
                                map_lookup.insert(k.clone(), val_str.to_string());
                            }
                        }
                    }
                    
                    PreparedStep::Map { 
                        col_idx: get_idx(col)?, 
                        mapping: map_lookup, 
                        default: default_val 
                    }
                },
                "hash" => {
                    let col_val = step.get("col").unwrap_or(&Value::Null);
                    let salt = step.get("salt").and_then(|v| v.as_str()).unwrap_or("phaeton_salt").to_string();
                    
                    let col_idxs = match col_val {
                        Value::Array(arr) => {
                            let mut indices = Vec::new();
                            for v in arr {
                                if let Some(name) = v.as_str() { indices.push(get_idx(name)?); }
                            }
                            indices
                        },
                        Value::String(s) => vec![get_idx(s)?],
                        
                        _ => return Err(PhaetonError::InvalidStep("Hash step missing 'col' parameter".to_string())),
                    };
                    
                    PreparedStep::Hash { col_idxs, salt }
                },
                "rename" | "headers" => continue,
                _ => continue, 
            };
            prepared_steps.push(p_step);
        }
        Ok(prepared_steps)
    }

    pub fn peek(&self, n: usize, target_columns: Option<Vec<String>>) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let file = File::open(&self.source).map_err(|_| PhaetonError::FileNotFound(self.source.clone()))?;
        let reader = BufReader::new(file);
        let mut csv_reader = ReaderBuilder::new().has_headers(true).flexible(true).from_reader(reader);
        
        let file_headers = csv_reader.headers()?.clone();

        let display_headers = self.transform_headers(&file_headers);

        let prepared_steps = self.compile_steps(&file_headers)?;

        let (indices_to_show, final_output_headers): (Vec<usize>, Vec<String>) = match target_columns {
            Some(cols) => {
                let mut idxs = Vec::new();
                let mut headers_vec = Vec::new();
                
                for requested_col in cols {
                    if let Some(idx) = file_headers.iter().position(|h| h == requested_col) {
                        idxs.push(idx);
                        headers_vec.push(display_headers.get(idx).unwrap_or("").to_string());
                    }
                }
                (idxs, headers_vec)
            },
            None => {
                let len = file_headers.len();
                (
                    (0..len).collect(), 
                    display_headers.iter().map(|h| h.to_string()).collect()
                )
            }
        };

        if indices_to_show.is_empty() && !final_output_headers.is_empty() {
             return Ok((vec![], vec![]));
        }
        
        let mut rows = Vec::new();
        let mut collected_count = 0;

        for result in csv_reader.records() {
            let record = result?;
            
            let processed = apply_pipeline(&record, &prepared_steps);
            
            if let RowResult::Keep(final_rec) = processed {
                let mut row_values = Vec::new();
                for &i in &indices_to_show {
                    let val = final_rec.get(i).unwrap_or("").to_string();
                    row_values.push(val);
                }
                rows.push(row_values);
                collected_count += 1;
                
                if collected_count >= n {
                    break;
                }
            }
        }

        Ok((final_output_headers, rows))
    }

    pub fn execute(&self, output_path: &str, quarantine_path: Option<&str>) -> Result<ExecutionStats> {
        let file = File::open(&self.source).map_err(|_| PhaetonError::FileNotFound(self.source.clone()))?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(BufReader::new(file));
        let original_headers = reader.headers()?.clone();
        let final_headers = self.transform_headers(&original_headers);

        let out_file = File::create(output_path)?;
        let mut clean_writer = WriterBuilder::new().from_writer(BufWriter::new(out_file));
        clean_writer.write_record(&final_headers)?;

        let mut quarantine_writer = if let Some(path) = quarantine_path {
            let q_file = File::create(path)?;
            let mut w = WriterBuilder::new().from_writer(BufWriter::new(q_file));
            let mut q_headers = final_headers.clone();
            q_headers.push_field("_phaeton_reason");
            w.write_record(&q_headers)?;
            Some(w)
        } else { None };

        let prepared_steps = self.compile_steps(&original_headers)?;

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
                    Some(Err(e)) => eprintln!("CSV Parse Error: {:?}", e),
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
    let mut owned_rec: Option<StringRecord> = None;

    let get_val = |idx: usize, owned: &Option<StringRecord>, original: &StringRecord| -> Option<String> {
        match owned {
            Some(r) => r.get(idx).map(|s| s.to_string()),
            None => original.get(idx).map(|s| s.to_string()),
        }
    };

    let finalize_rec = |owned: Option<StringRecord>, original: &StringRecord| -> StringRecord {
        owned.unwrap_or_else(|| original.clone())
    };

    for step in steps {
        match step {
            // Prune
            PreparedStep::Prune { col_idx } => {
                let current = owned_rec.as_ref().unwrap_or(record);
                let should_prune = match col_idx {
                    Some(idx) => current.get(*idx)
                        .map(|val| filter::is_empty(val))
                        .unwrap_or(true),
                    
                    None => current.iter().any(|field| filter::is_empty(field))
                };
                
                if should_prune { 
                    return RowResult::Discarded(finalize_rec(owned_rec, record), "Prune: Empty value".into()); 
                }
            },

            PreparedStep::PruneSelected { col_idxs } => {
                let current = owned_rec.as_ref().unwrap_or(record);
                
                let should_prune = col_idxs.iter().any(|&idx| {
                    current.get(idx)
                        .map(|val| filter::is_empty(val))
                        .unwrap_or(true)
                });

                if should_prune {
                    return RowResult::Discarded(finalize_rec(owned_rec, record), "Prune: Selected column empty".into());
                }
            },
            
            // Keep Regex
            PreparedStep::KeepRegex { col_idx, re } => {
                let val_opt = get_val(*col_idx, &owned_rec, record);
                let violation = if let Some(raw_val) = val_opt {
                    if raw_val.trim().is_empty() {
                        Some("Keep: Column missing/null".to_string())
                    } else if !re.is_match(&raw_val) { 
                        Some(format!("Keep: Regex mismatch")) 
                    } else { None }
                } else {
                    Some("Keep: Column missing/null".to_string()) 
                };
                if let Some(reason) = violation { return RowResult::Discarded(finalize_rec(owned_rec, record), reason); }
            },

            // Keep Single String
            PreparedStep::KeepString { col_idx, pattern, mode } => {
                let val_opt = get_val(*col_idx, &owned_rec, record);
                let violation = if let Some(raw_val) = val_opt {
                    let val = raw_val.trim();
                    let pat = pattern.trim();
                    let matches = match mode.as_str() {
                        "exact" => filter::matches_exact(val, pat),
                        "contains" => filter::contains_pattern(val, pat),
                        "startswith" => filter::starts_with_pattern(val, pat),
                        "endswith" => filter::ends_with_pattern(val, pat),
                        unknown => return RowResult::Discarded(finalize_rec(owned_rec, record), format!("Error: Unknown match mode '{}'", unknown)),
                    };
                    if !matches { Some(format!("Keep: Mismatch '{}'", pattern)) } else { None }
                } else {
                    Some("Keep: Column missing/null".to_string())
                };
                if let Some(reason) = violation { return RowResult::Discarded(finalize_rec(owned_rec, record), reason); }
            },

            // Keep Multi String
            PreparedStep::KeepMultiString { col_idx, patterns, mode } => {
                let violation = if let Some(raw_val) = get_val(*col_idx, &owned_rec, record) {
                    let val = raw_val.trim();
                    let matched = patterns.iter().any(|pat| {
                        match mode.as_str() {
                            "exact" => val == pat,
                            "contains" => val.contains(pat),
                            "startswith" => val.starts_with(pat),
                            "endswith" => val.ends_with(pat),
                            _ => false
                        }
                    });
                    if !matched { Some("Keep: No match found".to_string()) } else { None }
                } else {
                    Some("Keep: Column missing".to_string())
                };

                if let Some(reason) = violation { 
                    return RowResult::Discarded(finalize_rec(owned_rec, record), reason); 
                }
            },
            
            // Discard Regex
            PreparedStep::DiscardRegex { col_idx, re } => {
                let val_opt = get_val(*col_idx, &owned_rec, record);
                let violation = if let Some(raw_val) = val_opt {
                    if raw_val.trim().is_empty() {
                         Some("Discard: Column missing/null".to_string())
                    
                    } else if re.is_match(&raw_val) { 
                        Some(format!("Discard: Regex matched pattern")) 
                    
                    } else { 
                        None
                    }
                } else {
                    Some("Discard: Column missing/null".to_string())
                };
                if let Some(reason) = violation { return RowResult::Discarded(finalize_rec(owned_rec, record), reason); }
            },

            // Discard String
            PreparedStep::DiscardString { col_idx, pattern, mode } => {
                 let val_opt = get_val(*col_idx, &owned_rec, record);
                 let violation = if let Some(raw_val) = val_opt {
                    let val = raw_val.trim();
                    let pat = pattern.trim();
                    let matches = match mode.as_str() {
                        "exact" => filter::matches_exact(val, pat),
                        "contains" => filter::contains_pattern(val, pat),
                        "startswith" => filter::starts_with_pattern(val, pat),
                        "endswith" => filter::ends_with_pattern(val, pat),
                         unknown => return RowResult::Discarded(finalize_rec(owned_rec, record), format!("Error: Unknown match mode '{}'", unknown)),
                    };
                    if matches { Some(format!("Discard: Matched forbidden '{}'", pattern)) } else { None }
                } else { 
                    Some("Discard: Column missing/null".to_string())
                };
                if let Some(reason) = violation { return RowResult::Discarded(finalize_rec(owned_rec, record), reason); }
            },
            
            // Discard Multi String
            PreparedStep::DiscardMultiString { col_idx, patterns, mode } => {
                let violation = if let Some(raw_val) = get_val(*col_idx, &owned_rec, record) {
                    let val = raw_val.trim();
                    let matched = patterns.iter().any(|pat| {
                        match mode.as_str() {
                            "exact" => val == pat,
                            "contains" => val.contains(pat),
                            "startswith" => val.starts_with(pat),
                            "endswith" => val.ends_with(pat),
                            _ => false
                        }
                    });
                    if matched { Some(format!("Discard: Matched forbidden value")) } else { None }
                } else {
                    Some("Discard: Column missing".to_string())
                };

                if let Some(reason) = violation { 
                    return RowResult::Discarded(finalize_rec(owned_rec, record), reason); 
                }
            },
            
            // Scrub
            PreparedStep::Scrub { col_idx, mode } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                
                if let Some(val) = current_ref.get(*col_idx) {
                    let new_val = match mode.as_str() {
                        "email" => text::mask_email(val),
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
                        for (i, field) in current_ref.iter().enumerate() {
                            if i == *col_idx { new_rec.push_field(&v); } else { new_rec.push_field(field); }
                        }
                        owned_rec = Some(new_rec);
                    }
                }
            },
            
            // Fill
            PreparedStep::FillFixed { col_idx, value } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                if let Some(val) = current_ref.get(*col_idx) {
                    if filter::is_empty(val) {
                        let mut new_rec = StringRecord::new();
                        for (i, field) in current_ref.iter().enumerate() {
                            if i == *col_idx { new_rec.push_field(value); } 
                            else { new_rec.push_field(field); }
                        }
                        owned_rec = Some(new_rec);
                    }
                }
            },

            PreparedStep::FillForward { col_idx, last_valid } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                
                if let Some(val) = current_ref.get(*col_idx) {
                    let mut last = last_valid.lock().unwrap();

                    if filter::is_empty(val) {
                        if !last.is_empty() {
                            let mut new_rec = StringRecord::new();
                            for (i, field) in current_ref.iter().enumerate() {
                                if i == *col_idx { new_rec.push_field(&last); } 
                                else { new_rec.push_field(field); }
                            }
                            owned_rec = Some(new_rec);
                        }
                    } else {
                        *last = val.to_string();
                    }
                }
            },
            
            // Dedupe
            PreparedStep::Dedupe { col_idxs, state } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                let mut hasher = DefaultHasher::new();
                
                match col_idxs {
                    Some(indices) => {
                        for idx in indices {
                            if let Some(val) = current_ref.get(*idx) {
                                val.trim().hash(&mut hasher);
                            }
                            hasher.write_u8(0xFF); 
                        }
                    },
                    
                    None => {
                        for field in current_ref.iter() {
                            field.trim().hash(&mut hasher);
                            hasher.write_u8(0xFF); 
                        }
                    }
                };
                
                if state.check_and_insert(hasher.finish()) {
                    return RowResult::Discarded(
                        finalize_rec(owned_rec, record), 
                        "Dedupe: Duplicate found".to_string()
                    );
                }
            },

            // Align / fuzzyalign
            PreparedStep::Align { col_idx, ref_list, threshold } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                if let Some(val) = current_ref.get(*col_idx) {
                    if !filter::is_empty(val) {
                        let mut best_match: Option<&String> = None;
                        let mut best_score = 0.0;
                        for target in ref_list {
                            let score = strsim::jaro_winkler(val, target);
                            if score > best_score {
                                best_score = score;
                                best_match = Some(target);
                            }
                        }
                        if best_score >= *threshold {
                            if let Some(target) = best_match {
                                if val != target { 
                                    let mut new_rec = StringRecord::new();
                                    for (i, field) in current_ref.iter().enumerate() {
                                        if i == *col_idx { new_rec.push_field(target); } else { new_rec.push_field(field); }
                                    }
                                    owned_rec = Some(new_rec);
                                }
                            }
                        }
                    }
                }
            },

            // Cast
            PreparedStep::Cast { col_idx, dtype, clean } => {
                 let val_opt = get_val(*col_idx, &owned_rec, record);
                 let cast_error = if let Some(val) = val_opt {
                     if filter::is_empty(&val) {
                         Some("Cannot convert empty string".to_string())
                     } else {
                         let result = match dtype.as_str() {
                             "float" => cast::to_float(&val, "unknown", *clean).map(|_| ()),
                             "int" => cast::to_int(&val, "unknown", *clean).map(|_| ()),
                             "bool" => cast::to_bool(&val, "unknown").map(|_| ()),
                             _ => Ok(())
                         };
                         if let Err(e) = result { Some(e.to_string()) } else { None }
                     }
                 } else { None };

                 if let Some(reason) = cast_error { 
                     return RowResult::Discarded(finalize_rec(owned_rec, record), reason); 
                 }
            },
            PreparedStep::Map { col_idx, mapping, default } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                if let Some(val) = current_ref.get(*col_idx) {
                    // Cari di dictionary
                    let new_val_opt = mapping.get(val).cloned();
                    
                    // Logic replacement
                    let replacement = match new_val_opt {
                        Some(v) => Some(v), // Ketemu
                        None => default.clone(), // Gak ketemu, pake default (kalo ada)
                    };

                    if let Some(res) = replacement {
                        // Kalau ada replacement, update record
                         let mut new_rec = StringRecord::new();
                         for (i, field) in current_ref.iter().enumerate() {
                             if i == *col_idx { new_rec.push_field(&res); } 
                             else { new_rec.push_field(field); }
                         }
                         owned_rec = Some(new_rec);
                    }
                    // Kalau gak ketemu & gak ada default, biarkan nilai asli
                }
            },

            // IMPLEMENTASI HASH
            PreparedStep::Hash { col_idxs, salt } => {
                let current_ref = owned_rec.as_ref().unwrap_or(record);
                
                let needs_update = col_idxs.iter().any(|idx| {
                     current_ref.get(*idx).map(|v| !filter::is_empty(v)).unwrap_or(false)
                });

                if needs_update {
                    let mut new_rec = StringRecord::new();

                    for (i, field) in current_ref.iter().enumerate() {
                        if col_idxs.contains(&i) {
                             let mut hasher = Sha256::new();
                             hasher.update(field.as_bytes());
                             hasher.update(salt.as_bytes());
                             let res = hex::encode(hasher.finalize());
                             new_rec.push_field(&res);
                        } else {
                             new_rec.push_field(field);
                        }
                    }
                    owned_rec = Some(new_rec);
                }
            },
        }
    }

    RowResult::Keep(finalize_rec(owned_rec, record))
}