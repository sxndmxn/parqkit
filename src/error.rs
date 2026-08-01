//! Custom error types with user-friendly messages

use arrow::error::ArrowError;
use std::io;
use std::path::Path;
use thiserror::Error;

/// User-facing error with context
#[derive(Debug, Error)]
pub enum ParqkitError {
    #[error("No input files specified")]
    NoInputFiles,

    #[error("Expected exactly one input file, got {count}")]
    TooManyInputFiles { count: usize },

    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("Not a valid Parquet file: {path}\n  {details}")]
    InvalidParquet { path: String, details: String },

    #[error("File appears corrupted: {path}\n  {details}")]
    CorruptedFile { path: String, details: String },

    #[error("Cannot read file: {path}\n  {details}")]
    ReadError { path: String, details: String },

    #[error("Cannot write file: {path}\n  {details}")]
    WriteError { path: String, details: String },

    #[error("Cannot write output\n  {details}")]
    OutputError { details: String },

    #[error("Invalid glob pattern: {pattern}\n  {details}")]
    InvalidGlobPattern { pattern: String, details: String },

    #[error("No files matched pattern: {pattern}")]
    NoFilesMatched { pattern: String },

    #[error(
        "Pattern '{pattern}' matched more than {max_matches} files. Use a more specific pattern."
    )]
    TooManyFilesMatched { pattern: String, max_matches: usize },

    #[error("Schema mismatch between files:\n  {file1}\n  {file2}\n  {details}")]
    SchemaMismatch {
        file1: String,
        file2: String,
        details: String,
    },

    #[error("Unsupported format: {format}\n  Supported formats: {supported}")]
    UnsupportedFormat { format: String, supported: String },

    #[error("Path is a directory, not a file: {path}")]
    IsDirectory { path: String },

    #[error("Column not found in {path}: {column}")]
    ColumnNotFound { path: String, column: String },

    #[error("Invalid Parquet metadata in {path}\n  {details}")]
    InvalidMetadata { path: String, details: String },

    #[error("Invalid query\n  {details}")]
    InvalidQuery { details: String },
}

impl ParqkitError {
    /// Create a file-not-found error with path context
    pub fn file_not_found(path: &Path) -> Self {
        Self::FileNotFound {
            path: path.display().to_string(),
        }
    }

    /// Create an invalid parquet error from a library error
    pub fn invalid_parquet(path: &Path, err: impl std::fmt::Display) -> Self {
        let details = err.to_string();
        let details = simplify_parquet_error(&details);
        Self::InvalidParquet {
            path: path.display().to_string(),
            details,
        }
    }

    /// Create a corrupted file error
    pub fn corrupted(path: &Path, err: impl std::fmt::Display) -> Self {
        let details = err.to_string();
        let details = simplify_parquet_error(&details);
        Self::CorruptedFile {
            path: path.display().to_string(),
            details,
        }
    }

    /// Create a read error with path context
    pub fn read_error(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::ReadError {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    /// Classify a read error into a user-facing error with path context
    pub fn from_read(path: &Path, err: impl std::fmt::Display) -> Self {
        let message = err.to_string();
        let normalized = message.to_lowercase();

        if normalized.contains("no such file")
            || normalized.contains("not found")
            || normalized.contains("does not exist")
        {
            Self::file_not_found(path)
        } else if normalized.contains("is a directory") {
            Self::is_directory(path)
        } else if normalized.contains("permission denied") {
            Self::read_error(path, "Permission denied")
        } else if normalized.contains("eof")
            || normalized.contains("truncat")
            || normalized.contains("corrupt")
        {
            Self::corrupted(path, message)
        } else if normalized.contains("parquet")
            || normalized.contains("magic")
            || normalized.contains("thrift")
        {
            Self::invalid_parquet(path, message)
        } else {
            Self::read_error(path, message)
        }
    }

    /// Create a write error with path context
    pub fn write_error(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::WriteError {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    /// Create an output error without file path context
    pub fn output_error(err: impl std::fmt::Display) -> Self {
        Self::OutputError {
            details: err.to_string(),
        }
    }

    pub fn invalid_glob_pattern(pattern: &str, err: impl std::fmt::Display) -> Self {
        Self::InvalidGlobPattern {
            pattern: pattern.to_string(),
            details: err.to_string(),
        }
    }

    pub fn column_not_found(path: &Path, column: &str) -> Self {
        Self::ColumnNotFound {
            path: path.display().to_string(),
            column: column.to_string(),
        }
    }

    pub fn invalid_metadata(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::InvalidMetadata {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    pub fn invalid_query(err: impl std::fmt::Display) -> Self {
        Self::InvalidQuery {
            details: err.to_string(),
        }
    }

    /// Create an "is directory" error
    pub fn is_directory(path: &Path) -> Self {
        Self::IsDirectory {
            path: path.display().to_string(),
        }
    }
}

impl From<io::Error> for ParqkitError {
    fn from(error: io::Error) -> Self {
        Self::output_error(error)
    }
}

impl From<serde_json::Error> for ParqkitError {
    fn from(error: serde_json::Error) -> Self {
        Self::output_error(error)
    }
}

impl From<ArrowError> for ParqkitError {
    fn from(error: ArrowError) -> Self {
        Self::output_error(error)
    }
}

/// Simplify parquet library error messages to be more user-friendly
fn simplify_parquet_error(msg: &str) -> String {
    if msg.contains("not a valid Parquet file") || msg.contains("Invalid Parquet file") {
        return "File does not have valid Parquet magic bytes".to_string();
    }

    if msg.contains("eof") || msg.contains("EOF") || msg.contains("unexpected end") {
        return "File is truncated or incomplete".to_string();
    }

    if msg.contains("Invalid thrift") || msg.contains("thrift") {
        return "File metadata is corrupted".to_string();
    }

    if msg.contains("out of spec") || msg.contains("out-of-spec") {
        return "File contains invalid or out-of-spec data".to_string();
    }

    msg.to_string()
}

/// Extension trait for adding path context to Results
pub trait ResultExt<T> {
    /// Add path context to an error, converting it to a user-friendly message
    fn with_path_context(self, path: &Path) -> Result<T, ParqkitError>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn with_path_context(self, path: &Path) -> Result<T, ParqkitError> {
        self.map_err(|error| ParqkitError::from_read(path, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ParqkitError::file_not_found(Path::new("/tmp/missing.parquet"));
        assert!(err.to_string().contains("missing.parquet"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_simplify_parquet_error() {
        assert_eq!(
            simplify_parquet_error("not a valid Parquet file: missing magic"),
            "File does not have valid Parquet magic bytes"
        );
        assert_eq!(
            simplify_parquet_error("unexpected eof while reading"),
            "File is truncated or incomplete"
        );
    }

    #[test]
    fn test_from_read_classifies_invalid_parquet() {
        let err =
            ParqkitError::from_read(Path::new("/tmp/invalid.parquet"), "Invalid thrift footer");
        assert!(matches!(err, ParqkitError::InvalidParquet { .. }));
    }

    #[test]
    fn test_from_read_classifies_corruption() {
        let err = ParqkitError::from_read(Path::new("/tmp/truncated.parquet"), "unexpected EOF");
        assert!(matches!(err, ParqkitError::CorruptedFile { .. }));
    }
}
