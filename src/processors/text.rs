use std::borrow::Cow;

pub fn mask_email(input: &str) -> Cow<'_, str> {
    let parts: Vec<&str> = input.split('@').collect();
    if parts.len() != 2 {
        return Cow::Borrowed(input);
    }

    let username = parts[0];
    let domain = parts[1];

    if username.len() <= 2 {
        return Cow::Owned(format!("{}*@{}", &username[0..1], domain));
    }

    let first_char = &username[0..1];
    let last_char = &username[username.len()-1..];
    
    Cow::Owned(format!("{}****{}@{}", first_char, last_char, domain))
}

pub fn scrub_currency(input: &str) -> Cow<'_, str> {
    let skeleton: String = input.chars()
        .filter(|c| c.is_ascii_digit() || ".,-()".contains(*c))
        .collect();

    if skeleton.is_empty() {
        return Cow::Owned("0".to_string());
    }

    let is_negative_paren = skeleton.starts_with('(') && skeleton.ends_with(')');
    let is_negative_sign = skeleton.starts_with('-');

    let mut cleaned: String = skeleton.chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == ',')
        .collect();

    if cleaned.is_empty() {
        return Cow::Owned("0".to_string());
    }

    let last_comma = cleaned.rfind(',');
    let last_dot = cleaned.rfind('.');
    let dot_count = cleaned.chars().filter(|c| *c == '.').count();
    let comma_count = cleaned.chars().filter(|c| *c == ',').count();

    if let (Some(d), Some(c)) = (last_dot, last_comma) {
        if c > d {
            cleaned = cleaned.replace('.', "").replace(',', ".");
        } else {
            cleaned = cleaned.replace(',', "");
        }
    } 

    else if let Some(d_idx) = last_dot {
        if dot_count > 1 {
            cleaned = cleaned.replace('.', "");
        } else {
            let digits_after = cleaned.len() - 1 - d_idx;
            if digits_after == 3 { cleaned = cleaned.replace('.', ""); }
        }
    }

    else if let Some(c_idx) = last_comma {
        if comma_count > 1 {
            cleaned = cleaned.replace(',', "");
        } else {
             let digits_after = cleaned.len() - 1 - c_idx;
             if digits_after == 3 { cleaned = cleaned.replace(',', ""); }
             else { cleaned = cleaned.replace(',', "."); }
        }
    }

    if is_negative_paren || is_negative_sign {
        cleaned.insert(0, '-');
    }

    Cow::Owned(cleaned)
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