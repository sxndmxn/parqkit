use crate::dataset::Dataset;
use crate::engine;
use crate::model::{Projection, QueryBatch, QueryOptions, QuerySource};
use crate::{ParqkitError, Result};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use std::collections::BTreeSet;
use std::fmt;
use std::fs::File;
use std::iter::FusedIterator;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A pull-based stream of projected Arrow record batches.
///
/// Sources are processed in [`Dataset`] order and batches remain grouped by
/// source. Query planning is eager, but row decoding begins only when the
/// iterator is advanced. After the first decoding error, the stream is fused.
pub struct QueryStream {
    sources: Vec<QuerySource>,
    plans: Vec<SourcePlan>,
    options: QueryOptions,
    next_source: usize,
    active: Option<ActiveSource>,
    finished: bool,
}

impl QueryStream {
    pub(crate) fn try_new(dataset: &Dataset, options: QueryOptions) -> Result<Self> {
        validate_options(&options)?;

        let plans = dataset
            .paths()
            .map(|path| SourcePlan::new(path, &options.projection))
            .collect::<Result<Vec<_>>>()?;
        let sources = plans
            .iter()
            .map(|plan| QuerySource {
                path: plan.path.clone(),
                schema: Arc::clone(&plan.schema),
            })
            .collect();

        Ok(Self {
            sources,
            plans,
            options,
            next_source: 0,
            active: None,
            finished: false,
        })
    }

    /// Planned source paths and projected schemas in dataset order.
    ///
    /// This includes empty sources and is available before any rows are read,
    /// allowing consumers to validate multi-source schema compatibility before
    /// writing output.
    pub fn sources(&self) -> &[QuerySource] {
        &self.sources
    }

    /// Return the common projected schema for all sources.
    ///
    /// Structured multi-source consumers should call this before iterating so
    /// an incompatible schema is reported before any output is written.
    pub fn compatible_schema(&self) -> Result<SchemaRef> {
        let first = self.sources.first().ok_or(ParqkitError::NoInputFiles)?;
        for source in self.sources.iter().skip(1) {
            if source.schema.as_ref() != first.schema.as_ref() {
                return Err(ParqkitError::SchemaMismatch {
                    file1: first.path.display().to_string(),
                    file2: source.path.display().to_string(),
                    details: "Projected query schemas differ".to_string(),
                });
            }
        }
        Ok(Arc::clone(&first.schema))
    }

    fn open_next_source(&mut self) -> Result<bool> {
        let Some(plan) = self.plans.get(self.next_source) else {
            return Ok(false);
        };
        let source_index = self.next_source;
        self.next_source += 1;
        self.active = Some(ActiveSource::open(source_index, plan, &self.options)?);
        Ok(true)
    }
}

impl Iterator for QueryStream {
    type Item = Result<QueryBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(active) = self.active.as_mut() {
                match active.reader.next() {
                    Some(Ok(batch)) => {
                        let batch = match active.project_batch(batch) {
                            Ok(batch) => batch,
                            Err(error) => {
                                self.active = None;
                                self.finished = true;
                                return Some(Err(error));
                            }
                        };
                        return Some(Ok(QueryBatch {
                            source_index: active.source_index,
                            path: active.path.clone(),
                            batch,
                        }));
                    }
                    Some(Err(error)) => {
                        let error = ParqkitError::corrupted(&active.path, error);
                        self.active = None;
                        self.finished = true;
                        return Some(Err(error));
                    }
                    None => self.active = None,
                }
            }

            match self.open_next_source() {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl FusedIterator for QueryStream {}

impl fmt::Debug for QueryStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryStream")
            .field("sources", &self.sources)
            .field("options", &self.options)
            .field("next_source", &self.next_source)
            .field("has_active_source", &self.active.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}

struct SourcePlan {
    path: PathBuf,
    schema: SchemaRef,
}

impl SourcePlan {
    fn new(path: &Path, projection: &Projection) -> Result<Self> {
        let builder = engine::parquet::reader_builder(path)?;
        let projection = ProjectionPlan::new(path, &builder, projection)?;
        Ok(Self {
            path: path.to_path_buf(),
            schema: projection.schema,
        })
    }
}

struct ProjectionPlan {
    schema: SchemaRef,
    projection_mask: ProjectionMask,
    output_order: Option<Vec<usize>>,
}

impl ProjectionPlan {
    fn new(
        path: &Path,
        builder: &ParquetRecordBatchReaderBuilder<File>,
        projection: &Projection,
    ) -> Result<Self> {
        match projection {
            Projection::All => Ok(Self {
                schema: schema_without_metadata(builder.schema()),
                projection_mask: ProjectionMask::all(),
                output_order: None,
            }),
            Projection::Columns(columns) => {
                let root_indices = resolve_columns(path, builder.schema(), columns)?;
                let mut file_order = root_indices.clone();
                file_order.sort_unstable();

                let output_order = root_indices
                    .iter()
                    .map(|root_index| {
                        file_order.binary_search(root_index).map_err(|_| {
                            ParqkitError::invalid_query(
                                "projected column could not be resolved in reader schema",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let output_order = (output_order != (0..output_order.len()).collect::<Vec<_>>())
                    .then_some(output_order);
                let fields = root_indices
                    .iter()
                    .map(|index| Arc::clone(&builder.schema().fields()[*index]))
                    .collect::<Vec<_>>();

                Ok(Self {
                    schema: Arc::new(Schema::new(fields)),
                    projection_mask: ProjectionMask::roots(builder.parquet_schema(), root_indices),
                    output_order,
                })
            }
        }
    }
}

struct ActiveSource {
    source_index: usize,
    path: PathBuf,
    schema: SchemaRef,
    output_order: Option<Vec<usize>>,
    reader: ParquetRecordBatchReader,
}

impl ActiveSource {
    fn open(source_index: usize, planned: &SourcePlan, options: &QueryOptions) -> Result<Self> {
        let builder = engine::parquet::reader_builder(&planned.path)?;
        let current = ProjectionPlan::new(&planned.path, &builder, &options.projection)?;
        if current.schema.as_ref() != planned.schema.as_ref() {
            return Err(ParqkitError::invalid_metadata(
                &planned.path,
                "projected schema changed after query planning",
            ));
        }

        let mut builder = builder
            .with_projection(current.projection_mask)
            .with_batch_size(options.batch_size);
        if let Some(limit) = options.limit {
            builder = builder.with_limit(limit);
        }
        let reader = builder
            .build()
            .map_err(|error| ParqkitError::from_read(&planned.path, error))?;

        Ok(Self {
            source_index,
            path: planned.path.clone(),
            schema: Arc::clone(&planned.schema),
            output_order: current.output_order,
            reader,
        })
    }

    fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch = if let Some(output_order) = self.output_order.as_ref() {
            batch.project(output_order).map_err(|error| {
                ParqkitError::invalid_metadata(
                    &self.path,
                    format!("could not order projected columns: {error}"),
                )
            })?
        } else {
            batch
        };
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ParqkitError::invalid_metadata(
                &self.path,
                "decoded batch schema does not match planned projection",
            ));
        }
        Ok(batch)
    }
}

fn validate_options(options: &QueryOptions) -> Result<()> {
    if options.batch_size == 0 {
        return Err(ParqkitError::invalid_query(
            "query batch size must be greater than zero",
        ));
    }
    if matches!(&options.projection, Projection::Columns(columns) if columns.is_empty()) {
        return Err(ParqkitError::invalid_query(
            "query projection must contain at least one column",
        ));
    }
    Ok(())
}

fn resolve_columns(path: &Path, schema: &SchemaRef, columns: &[String]) -> Result<Vec<usize>> {
    let mut seen = BTreeSet::new();
    let mut indices = Vec::with_capacity(columns.len());

    for column in columns {
        if !seen.insert(column) {
            return Err(ParqkitError::invalid_query(format!(
                "duplicate projected column: {column}"
            )));
        }
        let index = schema
            .index_of(column)
            .map_err(|_| ParqkitError::column_not_found(path, column))?;
        indices.push(index);
    }

    Ok(indices)
}

fn schema_without_metadata(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(schema.fields().clone()))
}
