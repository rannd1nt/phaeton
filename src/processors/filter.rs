use regex::Regex;

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

/// Regex match (cached for performance)
pub fn matches_regex(input: &str, pattern: &str) -> bool {
    match Regex::new(pattern) {
        Ok(re) => re.is_match(input),
        Err(_) => false, // Invalid regex = no match
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_is_empty() {
        assert!(is_empty(""));
        assert!(is_empty("   "));
        assert!(!is_empty("data"));
    }
    
    #[test]
    fn test_matches() {
        assert!(matches_exact("OK", "OK"));
        assert!(contains_pattern("Hello World", "World"));
        assert!(starts_with_pattern("prefix_data", "prefix"));
        assert!(ends_with_pattern("data_suffix", "suffix"));
    }
    
    #[test]
    fn test_regex() {
        assert!(matches_regex("test123", r"\d+"));
        assert!(!matches_regex("test", r"\d+"));
    }
}