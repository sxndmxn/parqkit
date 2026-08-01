mod api;
mod atomic_output;
mod cli;
mod commands;
mod dataset;
mod engine;
mod error;
mod model;
mod output;

pub use api::{count, dataset_from_inputs, info, merge, scan, schema, stats};
use clap::Parser;
pub use dataset::Dataset;
pub use error::ParqkitError;
pub use model::{
    ColumnInfo, ColumnStats, ColumnType, CompressionCodec, CompressionSummary, CountEntry,
    CountResult, FileInfo, LogicalTypeKind, PhysicalType, ScanKind, ScanOptions, ScanResult,
    SchemaResult, StatValue, StatsResult, TimeUnit,
};

pub type Result<T> = std::result::Result<T, ParqkitError>;

#[doc(hidden)]
pub fn run_cli() -> Result<()> {
    let cli = cli::args::Cli::parse();
    run(cli.command)
}

fn run(command: cli::args::Command) -> Result<()> {
    commands::run(command)
}
