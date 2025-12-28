use rayon::prelude::*;

pub fn filter(data: Vec<String>, pattern: &str) -> Vec<String> {
    data.into_par_iter()
        .filter(|line: &String| line.contains(pattern))
        .collect()
}

pub fn sanitize(data: Vec<String>, mode: &str) -> Vec<String> {
    data.into_par_iter()
        .map(|line: String| {
            match mode {
                "no-html" => line.replace("<", "").replace(">", ""),
                "email-mask" => line.replace("@", "***@"),
                _ => line // Return as is
            }
        })
        .collect()
}