use std::fs;
use std::io::Error;
use std::fs::{File};
use std::io::{BufWriter, Write};

pub fn read_file(path: &str) -> Result<Vec<String>, Error> {
    let content = fs::read_to_string(path)?;
    Ok(content.lines().map(|s| s.to_string()).collect())
}

pub fn write_file(path: &str, data: &[String]) -> Result<(), Error> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    for line in data {
        writeln!(writer, "{}", line)?;
    }
    writer.flush()?;
    Ok(())
}