//! Custom error types with user-friendly messages

use arrow::error::ArrowError;
use std::io;
use std::path::Path;
use thiserror::Error;

/// User-facing error with context
#[derive(Debug, Error)]
pub enum ParqkitError {
    /// Standard output was closed by a downstream pipeline consumer.
    #[error("Broken pipe")]
    BrokenPipe,

    /// No input paths were provided.
    #[error("No input files specified")]
    NoInputFiles,

    /// A single-source operation resolved more than one input file.
    #[error("Expected exactly one input file, got {count}")]
    TooManyInputFiles {
        /// Number of resolved input files.
        count: usize,
    },

    /// An input path does not exist.
    #[error("File not found: {path}")]
    FileNotFound {
        /// User-provided or resolved input path.
        path: String,
    },

    /// A file does not contain a valid Parquet structure.
    #[error("Not a valid Parquet file: {path}\n  {details}")]
    InvalidParquet {
        /// Path to the invalid file.
        path: String,
        /// Simplified validation details.
        details: String,
    },

    /// Parquet data is truncated or corrupted.
    #[error("File appears corrupted: {path}\n  {details}")]
    CorruptedFile {
        /// Path to the corrupted file.
        path: String,
        /// Simplified corruption details.
        details: String,
    },

    /// A file could not be read.
    #[error("Cannot read file: {path}\n  {details}")]
    ReadError {
        /// Path that could not be read.
        path: String,
        /// Underlying read failure details.
        details: String,
    },

    /// A requested output file could not be written.
    #[error("Cannot write file: {path}\n  {details}")]
    WriteError {
        /// User-requested output path.
        path: String,
        /// Underlying write failure details.
        details: String,
    },

    /// Output rendering failed without an associated file path.
    #[error("Cannot write output\n  {details}")]
    OutputError {
        /// Underlying rendering or standard-output failure details.
        details: String,
    },

    /// A glob expression could not be parsed.
    #[error("Invalid glob pattern: {pattern}\n  {details}")]
    InvalidGlobPattern {
        /// Invalid pattern text.
        pattern: String,
        /// Glob parser failure details.
        details: String,
    },

    /// A valid glob expression matched no files.
    #[error("No files matched pattern: {pattern}")]
    NoFilesMatched {
        /// Pattern that produced no matches.
        pattern: String,
    },

    /// A glob expression exceeded Parqkit's bounded match count.
    #[error(
        "Pattern '{pattern}' matched more than {max_matches} files. Use a more specific pattern."
    )]
    TooManyFilesMatched {
        /// Pattern that produced too many matches.
        pattern: String,
        /// Maximum accepted match count.
        max_matches: usize,
    },

    /// Multiple inputs have incompatible Arrow schemas.
    #[error("Schema mismatch between files:\n  {file1}\n  {file2}\n  {details}")]
    SchemaMismatch {
        /// First compared source path.
        file1: String,
        /// Incompatible source path.
        file2: String,
        /// Description of the incompatibility.
        details: String,
    },

    /// An output format or extension is unsupported.
    #[error("Unsupported format: {format}\n  Supported formats: {supported}")]
    UnsupportedFormat {
        /// Unsupported requested format.
        format: String,
        /// Human-readable supported-format list.
        supported: String,
    },

    /// An input path points to a directory instead of a file.
    #[error("Path is a directory, not a file: {path}")]
    IsDirectory {
        /// Directory path supplied as an input file.
        path: String,
    },

    /// A requested column is absent from a source schema.
    #[error("Column not found in {path}: {column}")]
    ColumnNotFound {
        /// Source path whose schema was checked.
        path: String,
        /// Missing column name or full leaf path.
        column: String,
    },

    /// File metadata is internally invalid or cannot be represented safely.
    #[error("Invalid Parquet metadata in {path}\n  {details}")]
    InvalidMetadata {
        /// Source path containing invalid metadata.
        path: String,
        /// Metadata validation details.
        details: String,
    },

    /// Streaming query options are invalid.
    #[error("Invalid query\n  {details}")]
    InvalidQuery {
        /// Query validation details.
        details: String,
    },
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

    /// Create an invalid-glob error with parser context.
    pub fn invalid_glob_pattern(pattern: &str, err: impl std::fmt::Display) -> Self {
        Self::InvalidGlobPattern {
            pattern: pattern.to_string(),
            details: err.to_string(),
        }
    }

    /// Create a missing-column error with source-path context.
    pub fn column_not_found(path: &Path, column: &str) -> Self {
        Self::ColumnNotFound {
            path: path.display().to_string(),
            column: column.to_string(),
        }
    }

    /// Create an invalid-metadata error with source-path context.
    pub fn invalid_metadata(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::InvalidMetadata {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    /// Create an invalid-query error.
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
        if error.kind() == io::ErrorKind::BrokenPipe {
            Self::BrokenPipe
        } else {
            Self::output_error(error)
        }
    }
}

impl From<serde_json::Error> for ParqkitError {
    fn from(error: serde_json::Error) -> Self {
        if error.io_error_kind() == Some(io::ErrorKind::BrokenPipe) {
            Self::BrokenPipe
        } else {
            Self::output_error(error)
        }
    }
}

impl From<ArrowError> for ParqkitError {
    fn from(error: ArrowError) -> Self {
        match error {
            ArrowError::IoError(_, ref source) if source.kind() == io::ErrorKind::BrokenPipe => {
                Self::BrokenPipe
            }
            // arrow-csv currently erases the io::Error kind while adapting
            // csv writer failures, leaving the platform message as the only
            // available signal.
            ArrowError::CsvError(ref message) | ArrowError::JsonError(ref message)
                if message.to_ascii_lowercase().contains("broken pipe") =>
            {
                Self::BrokenPipe
            }
            _ => Self::output_error(error),
        }
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

    #[test]
    fn output_conversions_preserve_broken_pipe() {
        let io_error = io::Error::new(io::ErrorKind::BrokenPipe, "closed");
        assert!(matches!(
            ParqkitError::from(io_error),
            ParqkitError::BrokenPipe
        ));

        let arrow_io_error = ArrowError::from(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
        assert!(matches!(
            ParqkitError::from(arrow_io_error),
            ParqkitError::BrokenPipe
        ));

        let arrow_csv_error = ArrowError::CsvError("Broken pipe (os error 32)".to_string());
        assert!(matches!(
            ParqkitError::from(arrow_csv_error),
            ParqkitError::BrokenPipe
        ));
    }
}
