use crate::error::{ParqkitError, ResultExt};
use crate::model::{ColumnInfo, ColumnType, CompressionCodec, CompressionSummary, FileInfo};
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

pub fn read_head(path: &Path, rows: usize) -> Result<Vec<RecordBatch>> {
    if rows == 0 {
        return Ok(Vec::new());
    }

    let builder = reader_builder(path)?;
    let reader = builder
        .with_batch_size(rows.min(1024))
        .build()
        .map_err(|error| ParqkitError::from_read(path, error))?;

    let mut batches = Vec::new();
    let mut total_rows = 0usize;

    for batch_result in reader {
        let batch = batch_result.map_err(|error| ParqkitError::corrupted(path, &error))?;
        let rows_needed = rows.saturating_sub(total_rows);
        if rows_needed == 0 {
            break;
        }

        let batch = if batch.num_rows() > rows_needed {
            batch.slice(0, rows_needed)
        } else {
            batch
        };

        total_rows += batch.num_rows();
        batches.push(batch);
    }

    Ok(batches)
}

pub fn read_tail(path: &Path, rows: usize) -> Result<Vec<RecordBatch>> {
    if rows == 0 {
        return Ok(Vec::new());
    }

    let builder = reader_builder(path)?;
    let metadata = Arc::clone(builder.metadata());
    if metadata.num_row_groups() == 0 {
        return Ok(Vec::new());
    }

    let (row_groups, rows_to_skip) = tail_row_groups(path, &metadata, rows)?;
    let reader = builder
        .with_row_groups(row_groups)
        .build()
        .map_err(|error| ParqkitError::from_read(path, error))?;

    let mut result_batches = Vec::new();
    let mut skipped = 0usize;

    for batch_result in reader {
        let batch = batch_result.map_err(|error| ParqkitError::corrupted(path, &error))?;

        if skipped + batch.num_rows() <= rows_to_skip {
            skipped += batch.num_rows();
            continue;
        }

        let offset = rows_to_skip.saturating_sub(skipped);
        let sliced = batch.slice(offset, batch.num_rows() - offset);
        result_batches.push(sliced);
        skipped = rows_to_skip;
    }

    Ok(result_batches)
}

pub fn row_count(path: &Path) -> Result<i64> {
    let reader = serialized_reader(path)?;
    let rows = reader.metadata().file_metadata().num_rows();
    if rows < 0 {
        return Err(ParqkitError::invalid_metadata(path, "negative row count"));
    }
    Ok(rows)
}

pub fn schema_columns(path: &Path) -> Result<Vec<ColumnInfo>> {
    let reader = serialized_reader(path)?;
    let schema = reader.metadata().file_metadata().schema_descr();

    Ok(schema
        .columns()
        .iter()
        .map(|column| ColumnInfo {
            name: column.name().to_string(),
            column_type: ColumnType::from_parquet(column),
            nullable: column.self_type().is_optional(),
        })
        .collect())
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).with_path_context(path)?.len())
}

pub fn file_info(path: &Path) -> Result<FileInfo> {
    let reader = serialized_reader(path)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows();
    if num_rows < 0 {
        return Err(ParqkitError::invalid_metadata(path, "negative row count"));
    }
    let num_row_groups = metadata.num_row_groups();

    let compression = compression_summary(metadata);

    Ok(FileInfo {
        path: path.to_path_buf(),
        file_size_bytes: file_size(path)?,
        num_rows,
        num_columns: file_metadata.schema_descr().num_columns(),
        num_row_groups,
        compression,
        created_by: file_metadata.created_by().map(ToOwned::to_owned),
        version: file_metadata.version(),
    })
}

pub fn reader_builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(path).with_path_context(path)?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ParqkitError::from_read(path, error))
}

pub fn serialized_reader(path: &Path) -> Result<SerializedFileReader<File>> {
    let file = File::open(path).with_path_context(path)?;
    SerializedFileReader::new(file).map_err(|error| ParqkitError::from_read(path, error))
}

pub fn merge_files(paths: &[&Path], output: &Path) -> Result<()> {
    if paths.is_empty() {
        return Err(ParqkitError::NoInputFiles);
    }

    let first_builder = reader_builder(paths[0])?;
    let schema = Arc::clone(first_builder.schema());

    for path in paths.iter().skip(1) {
        let builder = reader_builder(path)?;
        if builder.schema().as_ref() != schema.as_ref() {
            return Err(ParqkitError::SchemaMismatch {
                file1: paths[0].display().to_string(),
                file2: path.display().to_string(),
                details: "Column names or types differ".to_string(),
            });
        }
    }

    let pending_output = crate::atomic_output::PendingOutput::new(output)?;
    let output_file = File::create(pending_output.path())
        .map_err(|error| ParqkitError::write_error(output, error))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, Arc::clone(&schema), Some(props))
        .map_err(|error| ParqkitError::write_error(output, error))?;

    for path in paths {
        let builder = reader_builder(path)?;
        let reader = builder
            .build()
            .map_err(|error| ParqkitError::from_read(path, error))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|error| ParqkitError::corrupted(path, error))?;
            writer
                .write(&batch)
                .map_err(|error| ParqkitError::write_error(output, error))?;
        }
    }

    writer
        .close()
        .map_err(|error| ParqkitError::write_error(output, error))?;
    pending_output.commit()
}

fn tail_row_groups(
    path: &Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
    rows: usize,
) -> Result<(Vec<usize>, usize)> {
    let mut selected_groups = Vec::new();
    let mut selected_rows = 0usize;

    for row_group_index in (0..metadata.num_row_groups()).rev() {
        let row_group = metadata.row_group(row_group_index);
        let row_group_rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            ParqkitError::invalid_metadata(
                path,
                format!(
                    "row group {} in {} cannot be represented on this platform",
                    row_group_index,
                    metadata.file_metadata().schema_descr().root_schema().name()
                ),
            )
        })?;

        selected_groups.push(row_group_index);
        selected_rows = selected_rows.saturating_add(row_group_rows);
        if selected_rows >= rows {
            break;
        }
    }

    selected_groups.reverse();
    Ok((selected_groups, selected_rows.saturating_sub(rows)))
}

fn compression_summary(metadata: &parquet::file::metadata::ParquetMetaData) -> CompressionSummary {
    let mut compression = None;

    for row_group_index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(row_group_index);
        for column_index in 0..row_group.num_columns() {
            let codec = CompressionCodec::from(row_group.column(column_index).compression());
            match compression {
                None => compression = Some(codec),
                Some(existing) if existing == codec => {}
                Some(_) => return CompressionSummary::Mixed,
            }
        }
    }

    compression.map_or(CompressionSummary::Unknown, CompressionSummary::Single)
}
