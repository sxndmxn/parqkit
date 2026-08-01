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

pub type Result<T> = std::result::Result<T, ParqkitError>;

#[doc(hidden)]
pub fn run_cli() -> Result<()> {
    let cli = cli::args::Cli::parse();
    run(cli.command)
}

fn run(command: cli::args::Command) -> Result<()> {
    commands::run(command)
}
