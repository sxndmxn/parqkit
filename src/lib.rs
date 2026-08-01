//! Typed Parquet inspection, streaming, conversion, and merge operations.
//!
//! The library keeps input expansion and validation in [`Dataset`], returns
//! typed metadata and Arrow results, and leaves presentation to callers. Use
//! [`execute_query`] when rows should be decoded lazily in bounded batches.
//!
//! # Example
//!
//! ```no_run
//! use parqkit::{dataset_from_inputs, execute_query, Projection, QueryOptions, Result};
//! use std::path::PathBuf;
//!
//! fn main() -> Result<()> {
//!     let dataset = dataset_from_inputs(vec![PathBuf::from("data.parquet")])?;
//!     let options = QueryOptions {
//!         projection: Projection::Columns(vec!["name".into(), "id".into()]),
//!         limit: Some(1_000),
//!         batch_size: 256,
//!     };
//!
//!     let stream = execute_query(&dataset, options)?;
//!     let _schema = stream.compatible_schema()?;
//!     for result in stream {
//!         let batch = result?.batch;
//!         // Consume this bounded Arrow record batch.
//!         let _row_count = batch.num_rows();
//!     }
//!     Ok(())
//! }
//! ```

mod api;
mod atomic_output;
mod cli;
mod commands;
mod dataset;
mod engine;
mod error;
mod model;
mod output;
mod query;

pub use api::{count, dataset_from_inputs, execute_query, info, merge, scan, schema, stats};
use clap::Parser;
pub use dataset::Dataset;
pub use error::ParqkitError;
pub use model::{
    ColumnInfo, ColumnStats, ColumnType, CompressionCodec, CompressionSummary, CountEntry,
    CountResult, FileInfo, LogicalTypeKind, PhysicalType, Projection, QueryBatch, QueryOptions,
    QuerySource, ScanKind, ScanOptions, ScanResult, SchemaResult, StatValue, StatsResult, TimeUnit,
    DEFAULT_QUERY_BATCH_SIZE,
};
pub use query::QueryStream;

/// Result type returned by Parqkit library operations.
pub type Result<T> = std::result::Result<T, ParqkitError>;

#[doc(hidden)]
pub fn run_cli() -> Result<()> {
    let cli = cli::args::Cli::parse();
    run(cli.command)
}

fn run(command: cli::args::Command) -> Result<()> {
    commands::run(command)
}
