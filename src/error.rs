use thiserror::Error;

#[derive(Error, Debug)]
pub enum PhaetonError {
    #[error("File not found: {0}")]
    FileNotFound(String),
    
    #[error("Encoding detection failed: {0}")]
    EncodingError(String),
    
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Invalid pipeline step: {0}")]
    InvalidStep(String),
    
    #[error("Type cast failed for column '{col}': {reason}")]
    CastError { col: String, reason: String },
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, PhaetonError>;