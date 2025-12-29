use std::fs::File;
use std::io::{BufReader, Read};
use std::collections::HashMap;
use encoding_rs::Encoding;
use crate::error::Result;

const PROBE_SIZE: usize = 8192; // Read first 8KB

pub fn detect_file_metadata(path: &str) -> Result<HashMap<String, String>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0u8; PROBE_SIZE];
    
    let bytes_read = reader.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    
    // Detect encoding
    let (encoding, confidence) = detect_encoding(&buffer);
    
    // Decode to UTF-8
    let (decoded, _) = encoding.decode_without_bom_handling(&buffer);
    
    // Detect delimiter
    let delimiter = detect_delimiter(&decoded);
    
    // Extract headers
    let headers = extract_headers(&decoded, &delimiter);
    
    let mut result = HashMap::new();
    result.insert("encoding".to_string(), encoding.name().to_string());
    result.insert("delimiter".to_string(), delimiter);
    result.insert("confidence".to_string(), format!("{:.2}", confidence));
    result.insert("headers".to_string(), headers.join(","));
    
    Ok(result)
}

fn detect_encoding(bytes: &[u8]) -> (&'static Encoding, f32) {
    // BOM detection (highest priority)
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return (encoding_rs::UTF_8, 1.0);
    }
    
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return (encoding_rs::UTF_16LE, 1.0);
    }
    
    // Statistical detection
    let mut utf8_score = 0.0;
    let mut latin1_score = 0.0;
    let mut windows1252_score = 0.0;
    
    // Count valid UTF-8 sequences
    if std::str::from_utf8(bytes).is_ok() {
        utf8_score = 0.9;
    }
    
    // Check for Windows-1252 specific bytes (0x80-0x9F range)
    let special_chars = bytes.iter()
        .filter(|&&b| (0x80..=0x9F).contains(&b))
        .count();
    
    if special_chars > 0 {
        windows1252_score = 0.7;
    } else if bytes.iter().any(|&b| b > 127) {
        latin1_score = 0.6;
    }
    
    // Return highest score
    let scores = [
        (encoding_rs::UTF_8, utf8_score),
        (encoding_rs::WINDOWS_1252, windows1252_score),
        (encoding_rs::WINDOWS_1252, latin1_score),
    ];
    
    scores.iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(enc, score)| (*enc, *score))
        .unwrap_or((encoding_rs::UTF_8, 0.5))
}

fn detect_delimiter(text: &str) -> String {
    let first_line = text.lines().next().unwrap_or("");
    
    let candidates = [',', ';', '\t', '|', ':'];
    let mut scores: HashMap<char, usize> = HashMap::new();
    
    for &delimiter in &candidates {
        scores.insert(delimiter, first_line.matches(delimiter).count());
    }
    
    scores.iter()
        .max_by_key(|(_, &count)| count)
        .map(|(&delim, _)| delim.to_string())
        .unwrap_or_else(|| ",".to_string())
}

fn extract_headers(text: &str, delimiter: &str) -> Vec<String> {
    text.lines()
        .next()
        .unwrap_or("")
        .split(delimiter)
        .map(|s| s.trim().to_string())
        .collect()
}