use crate::error::ParqkitError;
use crate::model::{
    ColumnInfo, ColumnStats, CountResult, FileInfo, LogicalTypeKind, SchemaResult, StatValue,
    StatsResult,
};
use crate::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::Serialize;
use serde_json::Value;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod csv;
mod csv_support;
mod info;
mod json;
mod schema;
mod stats;
mod table;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum OutputFormat {
    #[default]
    Table,
    Json,
    Jsonl,
    Csv,
}

impl OutputFormat {
    pub fn structured(self) -> Option<StructuredOutputFormat> {
        match self {
            Self::Table => None,
            Self::Json => Some(StructuredOutputFormat::Json),
            Self::Jsonl => Some(StructuredOutputFormat::Jsonl),
            Self::Csv => Some(StructuredOutputFormat::Csv),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StructuredOutputFormat {
    Json,
    Jsonl,
    Csv,
}

#[derive(Debug)]
enum FileOutputFormat {
    Csv,
    Json,
    Jsonl,
}

#[derive(Serialize)]
struct SchemaJsonRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    name: String,
    #[serde(rename = "type")]
    display_type: String,
    nullable: bool,
    physical_type: String,
    logical_type: Option<String>,
}

#[derive(Serialize)]
struct StatsJsonRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    column: String,
    #[serde(rename = "type")]
    display_type: String,
    null_count: Option<u64>,
    min: Option<Value>,
    max: Option<Value>,
    statistics_complete: bool,
    physical_type: String,
    logical_type: Option<String>,
}

#[derive(Serialize)]
struct FileInfoJsonRow {
    file: String,
    file_size_bytes: u64,
    num_rows: i64,
    num_columns: usize,
    num_row_groups: usize,
    compression: String,
    created_by: Option<String>,
    version: i32,
}

pub fn write_table_batches(quiet: bool, batches: &[RecordBatch]) -> Result<()> {
    table::write_batches(io::stdout().lock(), batches, quiet)?;
    Ok(())
}

pub fn write_structured_batches(
    output: StructuredOutputFormat,
    quiet: bool,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    match output {
        StructuredOutputFormat::Json => json::write_json(io::stdout().lock(), batches)?,
        StructuredOutputFormat::Jsonl => json::write_jsonl(io::stdout().lock(), batches)?,
        StructuredOutputFormat::Csv => {
            if batches.is_empty() {
                let empty_batch = RecordBatch::new_empty(Arc::clone(schema));
                csv::write_batches(
                    io::stdout().lock(),
                    std::slice::from_ref(&empty_batch),
                    !quiet,
                )?;
            } else {
                csv::write_batches(io::stdout().lock(), batches, !quiet)?;
            }
        }
    }
    Ok(())
}

pub fn write_schema_table(quiet: bool, columns: &[ColumnInfo]) -> Result<()> {
    table::write_schema_table(io::stdout().lock(), columns, quiet)?;
    Ok(())
}

pub fn write_schema_results(
    output: StructuredOutputFormat,
    quiet: bool,
    results: &[SchemaResult],
) -> Result<()> {
    if let [result] = results {
        return write_schema_result(output, quiet, result);
    }

    match output {
        StructuredOutputFormat::Json => {
            json::write_value(io::stdout().lock(), &schema_result_rows(results))?;
        }
        StructuredOutputFormat::Jsonl => {
            json::write_json_lines(io::stdout().lock(), &schema_result_rows(results))?;
        }
        StructuredOutputFormat::Csv => {
            schema::write_csv_results(io::stdout().lock(), results, !quiet)?
        }
    }
    Ok(())
}

fn write_schema_result(
    output: StructuredOutputFormat,
    quiet: bool,
    result: &SchemaResult,
) -> Result<()> {
    match output {
        StructuredOutputFormat::Json => {
            json::write_value(io::stdout().lock(), &schema_rows(&result.columns))?;
        }
        StructuredOutputFormat::Jsonl => {
            json::write_json_lines(io::stdout().lock(), &schema_rows(&result.columns))?;
        }
        StructuredOutputFormat::Csv => {
            schema::write_csv(io::stdout().lock(), &result.columns, !quiet)?
        }
    }
    Ok(())
}

pub fn write_stats_table(quiet: bool, rows: &[ColumnStats]) -> Result<()> {
    stats::write_table(io::stdout().lock(), rows, quiet)?;
    Ok(())
}

pub fn write_stats_results(
    output: StructuredOutputFormat,
    quiet: bool,
    results: &[StatsResult],
) -> Result<()> {
    if let [result] = results {
        return write_stats_result(output, quiet, result);
    }

    match output {
        StructuredOutputFormat::Json => {
            json::write_value(io::stdout().lock(), &stats_result_rows(results))?
        }
        StructuredOutputFormat::Jsonl => {
            json::write_json_lines(io::stdout().lock(), &stats_result_rows(results))?;
        }
        StructuredOutputFormat::Csv => {
            stats::write_csv_results(io::stdout().lock(), results, !quiet)?
        }
    }
    Ok(())
}

fn write_stats_result(
    output: StructuredOutputFormat,
    quiet: bool,
    result: &StatsResult,
) -> Result<()> {
    match output {
        StructuredOutputFormat::Json => {
            json::write_value(io::stdout().lock(), &stats_rows(&result.rows))?
        }
        StructuredOutputFormat::Jsonl => {
            json::write_json_lines(io::stdout().lock(), &stats_rows(&result.rows))?;
        }
        StructuredOutputFormat::Csv => stats::write_csv(io::stdout().lock(), &result.rows, !quiet)?,
    }
    Ok(())
}

pub fn write_file_info(quiet: bool, row: &FileInfo) -> Result<()> {
    info::write_table(io::stdout().lock(), std::slice::from_ref(row), quiet)
}

pub fn write_file_infos(
    output: StructuredOutputFormat,
    quiet: bool,
    rows: &[FileInfo],
) -> Result<()> {
    match output {
        StructuredOutputFormat::Json => {
            json::write_value(io::stdout().lock(), &file_info_rows(rows))?
        }
        StructuredOutputFormat::Jsonl => {
            json::write_json_lines(io::stdout().lock(), &file_info_rows(rows))?;
        }
        StructuredOutputFormat::Csv => info::write_csv(io::stdout().lock(), rows, !quiet)?,
    }
    Ok(())
}

pub fn write_counts(quiet: bool, is_multi_source: bool, counts: &CountResult) -> Result<()> {
    let mut writer = io::stdout().lock();

    for entry in &counts.entries {
        if quiet || !is_multi_source {
            writeln!(writer, "{}", entry.rows)?;
        } else {
            writeln!(writer, "{}: {}", entry.path.display(), entry.rows)?;
        }
    }

    if is_multi_source && !quiet {
        writeln!(writer, "Total: {}", counts.total_rows)?;
    }

    Ok(())
}

pub(crate) struct BatchFileWriter {
    path: PathBuf,
    inner: BatchFileWriterKind,
}

enum BatchFileWriterKind {
    Csv(Box<csv::BatchFileWriter>),
    Json(json::JsonBatchFileWriter),
    Jsonl(json::JsonlBatchFileWriter),
}

impl BatchFileWriter {
    pub fn create(file: File, error_path: &Path, schema: &SchemaRef) -> Result<Self> {
        let inner = match file_output_format(error_path)? {
            FileOutputFormat::Csv => BatchFileWriterKind::Csv(Box::new(
                csv::BatchFileWriter::create(file, Arc::clone(schema)),
            )),
            FileOutputFormat::Json => {
                BatchFileWriterKind::Json(json::JsonBatchFileWriter::create(file))
            }
            FileOutputFormat::Jsonl => {
                BatchFileWriterKind::Jsonl(json::JsonlBatchFileWriter::create(file))
            }
        };
        Ok(Self {
            path: error_path.to_path_buf(),
            inner,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match &mut self.inner {
            BatchFileWriterKind::Csv(writer) => writer.write(batch),
            BatchFileWriterKind::Json(writer) => writer.write(batch),
            BatchFileWriterKind::Jsonl(writer) => writer.write(batch),
        }
        .map_err(|error| ParqkitError::write_error(&self.path, error))
    }

    pub fn finish(self) -> Result<()> {
        match self.inner {
            BatchFileWriterKind::Csv(mut writer) => writer.finish(),
            BatchFileWriterKind::Json(writer) => writer.finish(),
            BatchFileWriterKind::Jsonl(writer) => writer.finish(),
        }
        .map_err(|error| ParqkitError::write_error(&self.path, error))
    }
}

fn file_output_format(path: &Path) -> Result<FileOutputFormat> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_lowercase)
        .as_deref()
    {
        Some("csv") => Ok(FileOutputFormat::Csv),
        Some("json") => Ok(FileOutputFormat::Json),
        Some("jsonl") => Ok(FileOutputFormat::Jsonl),
        Some(format) => Err(ParqkitError::UnsupportedFormat {
            format: format.to_string(),
            supported: "csv, json, jsonl".to_string(),
        }),
        None => Err(ParqkitError::UnsupportedFormat {
            format: "(no extension)".to_string(),
            supported: "csv, json, jsonl".to_string(),
        }),
    }
}

fn schema_rows(columns: &[ColumnInfo]) -> Vec<SchemaJsonRow> {
    columns
        .iter()
        .map(|column| schema_row(None, column))
        .collect()
}

fn schema_result_rows(results: &[SchemaResult]) -> Vec<SchemaJsonRow> {
    results
        .iter()
        .flat_map(|result| {
            result
                .columns
                .iter()
                .map(|column| schema_row(Some(result.path.as_path()), column))
        })
        .collect()
}

fn schema_row(file: Option<&Path>, column: &ColumnInfo) -> SchemaJsonRow {
    SchemaJsonRow {
        file: file.map(|path| path.display().to_string()),
        name: column.name.clone(),
        display_type: column.display_type(),
        nullable: column.nullable,
        physical_type: column.column_type.physical.to_string(),
        logical_type: column
            .column_type
            .logical
            .as_ref()
            .map(|logical| logical.display_name()),
    }
}

fn stats_rows(rows: &[ColumnStats]) -> Vec<StatsJsonRow> {
    rows.iter().map(|row| stats_row(None, row)).collect()
}

fn stats_result_rows(results: &[StatsResult]) -> Vec<StatsJsonRow> {
    results
        .iter()
        .flat_map(|result| {
            result
                .rows
                .iter()
                .map(|row| stats_row(Some(result.path.as_path()), row))
        })
        .collect()
}

fn stats_row(file: Option<&Path>, row: &ColumnStats) -> StatsJsonRow {
    StatsJsonRow {
        file: file.map(|path| path.display().to_string()),
        column: row.column.clone(),
        display_type: row.display_type(),
        null_count: row.null_count,
        min: row.min.as_ref().map(|value| stat_value_json(row, value)),
        max: row.max.as_ref().map(|value| stat_value_json(row, value)),
        statistics_complete: row.statistics_complete,
        physical_type: row.column_type.physical.to_string(),
        logical_type: row
            .column_type
            .logical
            .as_ref()
            .map(|logical| logical.display_name()),
    }
}

fn file_info_rows(rows: &[FileInfo]) -> Vec<FileInfoJsonRow> {
    rows.iter()
        .map(|row| FileInfoJsonRow {
            file: row.path().display().to_string(),
            file_size_bytes: row.file_size_bytes,
            num_rows: row.num_rows,
            num_columns: row.num_columns,
            num_row_groups: row.num_row_groups,
            compression: row.compression.to_string(),
            created_by: row.created_by.clone(),
            version: row.version,
        })
        .collect()
}

fn stat_value_json(row: &ColumnStats, value: &StatValue) -> Value {
    let logical_type = row.column_type.logical.as_ref();
    if matches!(logical_type, Some(LogicalTypeKind::Decimal { .. })) {
        return Value::from(row.display_stat_value(value));
    }

    match value {
        StatValue::Int32(inner) => Value::from(*inner),
        StatValue::UInt32(inner) => Value::from(*inner),
        StatValue::Int64(inner) => Value::from(*inner),
        StatValue::UInt64(inner) => Value::from(*inner),
        StatValue::Float(inner) => json_float(f64::from(*inner)),
        StatValue::Float16(inner) => json_float(f64::from(*inner)),
        StatValue::Double(inner) => json_float(*inner),
        StatValue::Binary(inner) | StatValue::FixedLenBinary(inner) => {
            if logical_type == Some(&LogicalTypeKind::String) {
                match std::str::from_utf8(inner) {
                    Ok(value) => Value::from(value),
                    Err(_) => Value::from(hex_string(inner)),
                }
            } else {
                Value::from(hex_string(inner))
            }
        }
        StatValue::DecimalBytes(inner) => Value::from(hex_string(inner)),
        StatValue::Boolean(inner) => Value::from(*inner),
        StatValue::Int96(inner) => Value::from(inner.as_str()),
    }
}

fn json_float(value: f64) -> Value {
    if value.is_nan() {
        Value::from("NaN")
    } else if value == f64::INFINITY {
        Value::from("Infinity")
    } else if value == f64::NEG_INFINITY {
        Value::from("-Infinity")
    } else {
        Value::from(value)
    }
}

fn hex_string(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(extension: &str) -> Result<std::path::PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ParqkitError::output_error)?
            .as_nanos();
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(std::env::temp_dir().join(format!("parqkit_output_{unique}_{counter}.{extension}")))
    }

    fn sample_batch() -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .map_err(Into::into)
    }

    #[test]
    fn infers_jsonl_output_from_path() -> Result<()> {
        let path = temp_path("jsonl")?;
        let batch = sample_batch()?;
        let file = File::create(&path)?;

        let mut writer = BatchFileWriter::create(file, &path, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;

        let contents = fs::read_to_string(&path)?;
        assert!(contents.lines().all(|line| line.starts_with('{')));
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn infers_file_format_from_error_path_when_writing_elsewhere() -> Result<()> {
        let write_path = temp_path("tmp")?;
        let error_path = temp_path("jsonl")?;
        let batch = sample_batch()?;
        let file = File::create(&write_path)?;

        let mut writer = BatchFileWriter::create(file, &error_path, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;

        let contents = fs::read_to_string(&write_path)?;
        assert!(contents.lines().all(|line| line.starts_with('{')));
        fs::remove_file(write_path)?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_output_extension() {
        let path_buf = std::env::temp_dir().join("output.unknown");
        let path = path_buf.as_path();
        assert!(matches!(
            file_output_format(path),
            Err(ref err) if err.to_string().contains("Unsupported format")
        ));
    }

    #[test]
    fn renders_non_finite_json_numbers_explicitly() {
        assert_eq!(json_float(f64::NAN), Value::from("NaN"));
        assert_eq!(json_float(f64::INFINITY), Value::from("Infinity"));
        assert_eq!(json_float(f64::NEG_INFINITY), Value::from("-Infinity"));
    }
}
