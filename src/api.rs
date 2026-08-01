use crate::dataset::Dataset;
use crate::engine;
use crate::model::{
    CountEntry, CountResult, FileInfo, QueryOptions, ScanKind, ScanOptions, ScanResult,
    SchemaResult, StatsResult,
};
use crate::{QueryStream, Result};
use std::path::{Path, PathBuf};

/// Build a validated dataset from explicit paths and glob patterns.
///
/// Explicit repeated paths are preserved. Glob matches are sorted and
/// deduplicated against other glob matches and overlapping explicit inputs.
pub fn dataset_from_inputs(inputs: Vec<PathBuf>) -> Result<Dataset> {
    Dataset::from_inputs(inputs)
}

/// Plan a streaming query over every source in `dataset`.
///
/// Planning validates the query and reads each source's Parquet metadata before
/// returning. Record batches are decoded lazily as the returned stream is
/// iterated. See [`QueryStream::sources`] for the projected schema of every
/// source, including sources with no rows. Producer metadata is normalized,
/// while Arrow extension-type metadata remains schema-significant.
pub fn execute_query(dataset: &Dataset, options: QueryOptions) -> Result<QueryStream> {
    QueryStream::try_new(dataset, options)
}

/// Read the Parquet leaf-column schema for every dataset source.
pub fn schema(dataset: &Dataset) -> Result<Vec<SchemaResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let columns = engine::parquet::schema_columns(&path)?;
            Ok(SchemaResult { path, columns })
        })
        .collect()
}

/// Eagerly read the first or last rows from every dataset source.
///
/// Each result retains its Arrow schema even when `options.rows` is zero or
/// the source contains no rows, and every returned batch uses that schema.
/// Producer metadata is normalized while Arrow extension types are preserved.
/// Use [`execute_query`] for lazy, projected scans.
pub fn scan(dataset: &Dataset, kind: ScanKind, options: ScanOptions) -> Result<Vec<ScanResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let (schema, batches) = match kind {
                ScanKind::Head => engine::parquet::read_head(&path, options.rows)?,
                ScanKind::Tail => engine::parquet::read_tail(&path, options.rows)?,
            };
            Ok(ScanResult {
                path,
                schema,
                batches,
            })
        })
        .collect()
}

/// Read row counts from Parquet metadata for every dataset source.
///
/// The total includes every explicit repeated source in dataset order.
pub fn count(dataset: &Dataset) -> Result<CountResult> {
    let mut entries = Vec::new();
    let mut total_rows = 0i64;

    for path in dataset.paths() {
        let rows = engine::parquet::row_count(path)?;
        total_rows = total_rows.checked_add(rows).ok_or_else(|| {
            crate::ParqkitError::invalid_metadata(path, "row count total overflow")
        })?;
        entries.push(CountEntry {
            path: path.to_path_buf(),
            rows,
        });
    }

    Ok(CountResult {
        entries,
        total_rows,
    })
}

/// Read and aggregate column statistics from Parquet row-group metadata.
///
/// When `column_name` is `Some`, only that full Parquet leaf path is returned.
/// Missing or partial metadata is represented explicitly in [`crate::ColumnStats`].
pub fn stats(dataset: &Dataset, column_name: Option<&str>) -> Result<Vec<StatsResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let rows = engine::stats::column_stats(&path, column_name)?;
            Ok(StatsResult { path, rows })
        })
        .collect()
}

/// Read file-level Parquet metadata for every dataset source.
pub fn info(dataset: &Dataset) -> Result<Vec<FileInfo>> {
    dataset.paths().map(engine::parquet::file_info).collect()
}

pub(crate) fn convert(input: &Path, output: &Path) -> Result<()> {
    let builder = engine::parquet::reader_builder(input)?;
    let schema = std::sync::Arc::clone(builder.schema());
    let reader = builder
        .build()
        .map_err(|error| crate::ParqkitError::from_read(input, error))?;
    let mut pending_output = crate::atomic_output::PendingOutput::new(output)?;
    let output_file = pending_output.take_file()?;
    let mut writer = crate::output::BatchFileWriter::create(output_file, output, &schema)?;

    for batch_result in reader {
        let batch = batch_result.map_err(|error| crate::ParqkitError::corrupted(input, &error))?;
        writer.write(&batch)?;
    }

    writer.finish()?;
    pending_output.commit()
}

/// Merge all dataset sources into one Snappy-compressed Parquet file.
///
/// All Arrow schemas are validated before the output is created. The output
/// is replaced atomically only after every source has been read and the writer
/// has closed successfully. Producer metadata differences are ignored, but
/// different Arrow extension types remain incompatible.
pub fn merge(dataset: &Dataset, output: &Path) -> Result<()> {
    let paths: Vec<_> = dataset.paths().collect();
    engine::parquet::merge_files(&paths, output)
}
