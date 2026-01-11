// FILTERING MODE (DISCARD/KEEP)

/// Check if string is empty or whitespace-only
pub fn is_empty(input: &str) -> bool {
    input.trim().is_empty()
}

/// Exact match
pub fn matches_exact(input: &str, pattern: &str) -> bool {
    input == pattern
}

/// Contains substring
pub fn contains_pattern(input: &str, pattern: &str) -> bool {
    input.contains(pattern)
}

/// Starts with pattern
pub fn starts_with_pattern(input: &str, pattern: &str) -> bool {
    input.starts_with(pattern)
}

/// Ends with pattern
pub fn ends_with_pattern(input: &str, pattern: &str) -> bool {
    input.ends_with(pattern)
}