use crate::output::OutputFormat;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "parqkit")]
#[command(
    about = "A jq-like CLI for Parquet files. Fast startup, pretty output, sensible defaults."
)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Show schema (column names, types, nullability)
    Schema(SchemaArgs),
    /// Show first N rows
    Head(HeadArgs),
    /// Show last N rows
    Tail(TailArgs),
    /// Count total rows
    Count(CountArgs),
    /// Column statistics (min, max, nulls)
    Stats(StatsArgs),
    /// Convert to CSV, JSON, or JSONL
    Convert(ConvertArgs),
    /// Merge multiple parquet files
    Merge(MergeArgs),
    /// File metadata (row groups, compression, size)
    Info(InfoArgs),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, clap::ValueEnum)]
pub enum OutputFormatArg {
    #[default]
    Table,
    Json,
    Jsonl,
    Csv,
}

impl From<OutputFormatArg> for OutputFormat {
    fn from(value: OutputFormatArg) -> Self {
        match value {
            OutputFormatArg::Table => Self::Table,
            OutputFormatArg::Json => Self::Json,
            OutputFormatArg::Jsonl => Self::Jsonl,
            OutputFormatArg::Csv => Self::Csv,
        }
    }
}

#[derive(Debug, Args)]
pub struct SchemaArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Output format
    #[arg(short, long, default_value = "table")]
    pub output: OutputFormatArg,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Debug, Args)]
pub struct HeadArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Number of rows to show
    #[arg(short = 'n', long = "rows", default_value = "10")]
    pub rows: usize,
    /// Output format
    #[arg(short, long, default_value = "table")]
    pub output: OutputFormatArg,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Debug, Args)]
pub struct TailArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Number of rows to show
    #[arg(short = 'n', long = "rows", default_value = "10")]
    pub rows: usize,
    /// Output format
    #[arg(short, long, default_value = "table")]
    pub output: OutputFormatArg,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Debug, Args)]
pub struct CountArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Debug, Args)]
pub struct StatsArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Specific column to show stats for
    #[arg(short, long)]
    pub column: Option<String>,
    /// Output format
    #[arg(short, long, default_value = "table")]
    pub output: OutputFormatArg,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}

#[derive(Debug, Args)]
pub struct ConvertArgs {
    /// Input parquet file
    #[arg(required = true)]
    pub input: PathBuf,
    /// Output file path
    #[arg(required = true)]
    pub output_path: PathBuf,
}

#[derive(Debug, Args)]
pub struct MergeArgs {
    /// Input parquet files
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Output file path
    #[arg(short, long, required = true)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct InfoArgs {
    /// Parquet file(s) to read
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Output format
    #[arg(short, long, default_value = "table")]
    pub output: OutputFormatArg,
    /// Suppress headers and formatting
    #[arg(short, long)]
    pub quiet: bool,
}
