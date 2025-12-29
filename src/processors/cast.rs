use crate::error::{PhaetonError, Result};
use crate::processors::text;
use std::borrow::Cow;

pub fn to_float(input: &str, col_name: &str, clean_first: bool) -> Result<f64> {
    let cleaned = if clean_first {
        text::scrub_currency(input) // Returns Cow
    } else {
        Cow::Borrowed(input)
    };
    
    // Parse directly
    cleaned.trim().parse::<f64>()
        .map_err(|_| PhaetonError::CastError {
            col: col_name.to_string(),
            reason: format!("Cannot convert '{}' to float", input)
        })
}

pub fn to_int(input: &str, col_name: &str, clean_first: bool) -> Result<i64> {
    let cleaned = if clean_first {
        text::scrub_numeric_only(input)
    } else {
        Cow::Borrowed(input)
    };
    
    // Parse directly
    cleaned.trim().parse::<i64>()
        .map_err(|_| PhaetonError::CastError {
            col: col_name.to_string(),
            reason: format!("Cannot convert '{}' to int", input)
        })
}

pub fn to_bool(input: &str, col_name: &str) -> Result<bool> {
    let normalized = input.trim().to_lowercase();
    match normalized.as_str() {
        "true" | "1" | "yes" | "y" | "t" => Ok(true),
        "false" | "0" | "no" | "n" | "f" => Ok(false),
        _ => Err(PhaetonError::CastError {
            col: col_name.to_string(),
            reason: format!("Cannot convert '{}' to bool", input)
        })
    }
}