use crate::dataset::Dataset;
use crate::engine;
use crate::model::{
    CountEntry, CountResult, FileInfo, ScanKind, ScanOptions, ScanResult, SchemaResult, StatsResult,
};
use crate::Result;
use std::path::{Path, PathBuf};

pub fn dataset_from_inputs(inputs: Vec<PathBuf>) -> Result<Dataset> {
    Dataset::from_inputs(inputs)
}

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

pub fn info(dataset: &Dataset) -> Result<Vec<FileInfo>> {
    dataset.paths().map(engine::parquet::file_info).collect()
}

pub(crate) fn convert(input: &Path, output: &Path) -> Result<()> {
    let builder = engine::parquet::reader_builder(input)?;
    let schema = std::sync::Arc::clone(builder.schema());
    let reader = builder
        .build()
        .map_err(|error| crate::ParqkitError::from_read(input, error))?;
    let pending_output = crate::atomic_output::PendingOutput::new(output)?;
    let mut writer =
        crate::output::BatchFileWriter::create_at(pending_output.path(), output, &schema)?;

    for batch_result in reader {
        let batch = batch_result.map_err(|error| crate::ParqkitError::corrupted(input, &error))?;
        writer.write(&batch)?;
    }

    writer.finish()?;
    pending_output.commit()
}

pub fn merge(dataset: &Dataset, output: &Path) -> Result<()> {
    let paths: Vec<_> = dataset.paths().collect();
    engine::parquet::merge_files(&paths, output)
}
