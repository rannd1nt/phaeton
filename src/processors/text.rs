use std::borrow::Cow;

/// Remove currency symbols and thousand separators
pub fn scrub_currency(input: &str) -> Cow<'_, str> {
    let needs_scrub = input.chars().any(|c| !c.is_ascii_digit() && c != '.' && c != ',');
    if !needs_scrub {
        return Cow::Borrowed(input);
    }

    let mut result = String::with_capacity(input.len());
    let comma_pos = input.rfind(',');
    let dot_pos = input.rfind('.');

    // Format Detection:
    // ID (10.000,00) -> comma index > dot index
    // US (10,000.00) -> dot index > comma index
    let is_indo = match (comma_pos, dot_pos) {
        (Some(c), Some(d)) => c > d,
        (Some(_), None) => true,  // "10,00" -> ID
        (None, Some(_)) => false, // "10.00" -> US
        (None, None) => false,
    };

    for c in input.chars() {
        if c.is_ascii_digit() {
            result.push(c);
        } else if c == ',' {
            if is_indo { result.push('.'); } // ID: replace comma with dot
        } else if c == '.' {
            if !is_indo { result.push('.'); } // US: keep dot
        }
        // Another characters are ignored
    }
    
    Cow::Owned(result)
}

pub fn scrub_numeric_only(input: &str) -> Cow<'_, str> {
    if input.chars().all(|c| c.is_ascii_digit()) {
        Cow::Borrowed(input)
    } else {
        let cleaned: String = input.chars().filter(|c| c.is_ascii_digit()).collect();
        Cow::Owned(cleaned)
    }
}

pub fn trim_whitespace(input: &str) -> Cow<'_, str> {
    // trim() returns &str, so it's always zero-copy
    Cow::Borrowed(input.trim())
}

pub fn remove_html_tags(input: &str) -> Cow<'_, str> {
    if !input.contains('<') {
        return Cow::Borrowed(input);
    }
    
    let mut result = String::with_capacity(input.len());
    let mut inside_tag = false;
    
    for ch in input.chars() {
        match ch {
            '<' => inside_tag = true,
            '>' => inside_tag = false,
            _ if !inside_tag => result.push(ch),
            _ => {}
        }
    }
    Cow::Owned(result)
}

pub fn to_lowercase(input: &str) -> Cow<'_, str> {
    if input.chars().all(|c| !c.is_uppercase()) {
        Cow::Borrowed(input)
    } else {
        Cow::Owned(input.to_lowercase())
    }
}

pub fn to_uppercase(input: &str) -> Cow<'_, str> {
    if input.chars().all(|c| !c.is_lowercase()) {
        Cow::Borrowed(input)
    } else {
        Cow::Owned(input.to_uppercase())
    }
}