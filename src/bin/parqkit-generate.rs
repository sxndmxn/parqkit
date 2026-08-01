//! Parquet test file generator for stress testing Parqkit
//!
//! Generates Parquet files with configurable size, schema, and data characteristics.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "parqkit-generate")]
#[command(about = "Generate Parquet files for stress testing")]
struct Cli {
    /// Number of rows to generate
    #[arg(short, long, default_value = "100000")]
    rows: usize,

    /// Number of columns to generate
    #[arg(short, long, default_value = "10")]
    cols: usize,

    /// Output file path
    #[arg(short, long)]
    output: PathBuf,

    /// Batch size for writing (rows per batch)
    #[arg(short, long, default_value = "65536")]
    batch_size: usize,

    /// Random seed for reproducibility
    #[arg(short, long, default_value = "42")]
    seed: u64,

    /// Null ratio (0.0 to 1.0)
    #[arg(long, default_value = "0.05")]
    null_ratio: f64,

    /// Average string length for string columns
    #[arg(long, default_value = "32")]
    string_len: usize,

    /// Data profile to generate
    #[arg(long, default_value = "mixed")]
    profile: DataProfile,

    /// Compression codec
    #[arg(long, default_value = "snappy")]
    compression: CompressionCodec,
}

#[derive(Clone, Copy, ValueEnum)]
enum DataProfile {
    /// Mixed data types: int, float, string, bool
    Mixed,
    /// All integer columns
    Integers,
    /// All string columns
    Strings,
    /// All columns with high null ratio (90%)
    Sparse,
    /// String columns with very long values (1KB+)
    LongStrings,
    /// Unicode stress test: emoji, RTL, special chars
    Unicode,
    /// Edge cases: min/max values, special floats
    EdgeCases,
    /// All null values
    AllNulls,
    /// Empty file (0 rows, just schema)
    Empty,
}

#[derive(Clone, Copy, ValueEnum)]
enum CompressionCodec {
    None,
    Snappy,
    Gzip,
    Zstd,
}

impl From<CompressionCodec> for Compression {
    fn from(codec: CompressionCodec) -> Self {
        use parquet::basic::{GzipLevel, ZstdLevel};
        match codec {
            CompressionCodec::None => Compression::UNCOMPRESSED,
            CompressionCodec::Snappy => Compression::SNAPPY,
            CompressionCodec::Gzip => Compression::GZIP(GzipLevel::default()),
            CompressionCodec::Zstd => Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_cli(&cli)?;

    eprintln!(
        "Generating {} rows x {} cols -> {}",
        cli.rows,
        cli.cols,
        cli.output.display()
    );

    let schema = build_schema(&cli);
    let file = File::create(&cli.output)
        .with_context(|| format!("Failed to create output file: {}", cli.output.display()))?;

    let props = WriterProperties::builder()
        .set_compression(cli.compression.into())
        .build();

    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
        .context("Failed to create Arrow writer")?;

    let mut rng = StdRng::seed_from_u64(cli.seed);
    let mut rows_written = 0;

    // Handle empty profile specially
    if matches!(cli.profile, DataProfile::Empty) {
        writer.close().context("Failed to close writer")?;
        eprintln!("Created empty Parquet file with schema only");
        return Ok(());
    }

    while rows_written < cli.rows {
        let batch_rows = std::cmp::min(cli.batch_size, cli.rows - rows_written);
        let batch = generate_batch(&cli, &schema, batch_rows, &mut rng)?;
        writer
            .write(&batch)
            .context("Failed to write record batch")?;
        rows_written += batch_rows;

        if rows_written % 1_000_000 == 0 {
            eprintln!("  {rows_written} rows written...");
        }
    }

    writer.close().context("Failed to close writer")?;

    let file_size = std::fs::metadata(&cli.output)?.len();
    #[allow(clippy::cast_precision_loss)]
    let file_size_mb = file_size as f64 / 1_048_576.0;
    eprintln!("Done! {rows_written} rows, {file_size} bytes ({file_size_mb:.2} MB)");

    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<()> {
    anyhow::ensure!(cli.cols > 0, "--cols must be greater than zero");
    anyhow::ensure!(cli.batch_size > 0, "--batch-size must be greater than zero");
    anyhow::ensure!(
        cli.null_ratio.is_finite() && (0.0..=1.0).contains(&cli.null_ratio),
        "--null-ratio must be between 0 and 1"
    );
    anyhow::ensure!(cli.string_len > 0, "--string-len must be greater than zero");
    Ok(())
}

fn build_schema(cli: &Cli) -> Arc<Schema> {
    let fields: Vec<Field> = (0..cli.cols)
        .map(|i| {
            let (name, dtype) = match cli.profile {
                DataProfile::Integers => (format!("int_{i}"), DataType::Int64),
                DataProfile::Strings | DataProfile::LongStrings | DataProfile::Unicode => {
                    (format!("str_{i}"), DataType::Utf8)
                }
                DataProfile::Sparse => (format!("sparse_{i}"), DataType::Int64),
                DataProfile::AllNulls => (format!("null_{i}"), DataType::Null),
                DataProfile::Empty => (format!("col_{i}"), DataType::Int64),
                DataProfile::Mixed | DataProfile::EdgeCases => match i % 4 {
                    0 => (format!("int_{i}"), DataType::Int64),
                    1 => (format!("float_{i}"), DataType::Float64),
                    2 => (format!("str_{i}"), DataType::Utf8),
                    _ => (format!("bool_{i}"), DataType::Boolean),
                },
            };
            Field::new(name, dtype, true)
        })
        .collect();

    Arc::new(Schema::new(fields))
}

fn generate_batch(
    cli: &Cli,
    schema: &Arc<Schema>,
    num_rows: usize,
    rng: &mut StdRng,
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| generate_column(cli, field, num_rows, rng))
        .collect();

    RecordBatch::try_new(Arc::clone(schema), columns).context("Failed to create record batch")
}

fn generate_column(cli: &Cli, field: &Field, num_rows: usize, rng: &mut StdRng) -> ArrayRef {
    let null_ratio = match cli.profile {
        DataProfile::Sparse => 0.9,
        DataProfile::AllNulls => 1.0,
        _ => cli.null_ratio,
    };

    match field.data_type() {
        DataType::Int64 => generate_int64(cli, num_rows, null_ratio, rng),
        DataType::Float64 => generate_float64(cli, num_rows, null_ratio, rng),
        DataType::Utf8 => generate_string(cli, num_rows, null_ratio, rng),
        DataType::Boolean => generate_boolean(num_rows, null_ratio, rng),
        _ => Arc::new(NullArray::new(num_rows)),
    }
}

fn generate_int64(cli: &Cli, num_rows: usize, null_ratio: f64, rng: &mut StdRng) -> ArrayRef {
    let values: Vec<Option<i64>> = (0..num_rows)
        .map(|i| {
            if rng.gen::<f64>() < null_ratio {
                None
            } else if matches!(cli.profile, DataProfile::EdgeCases) {
                // Cycle through edge cases
                Some(match i % 5 {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    2 => 0,
                    3 => -1,
                    _ => rng.gen(),
                })
            } else {
                Some(rng.gen_range(-1_000_000..1_000_000))
            }
        })
        .collect();
    Arc::new(Int64Array::from(values))
}

fn generate_float64(cli: &Cli, num_rows: usize, null_ratio: f64, rng: &mut StdRng) -> ArrayRef {
    let values: Vec<Option<f64>> = (0..num_rows)
        .map(|i| {
            if rng.gen::<f64>() < null_ratio {
                None
            } else if matches!(cli.profile, DataProfile::EdgeCases) {
                Some(match i % 6 {
                    0 => f64::MIN,
                    1 => f64::MAX,
                    2 => 0.0,
                    3 => f64::NEG_INFINITY,
                    4 => f64::INFINITY,
                    _ => f64::NAN,
                })
            } else {
                Some(rng.gen_range(-1_000_000.0..1_000_000.0))
            }
        })
        .collect();
    Arc::new(Float64Array::from(values))
}

fn generate_string(cli: &Cli, num_rows: usize, null_ratio: f64, rng: &mut StdRng) -> ArrayRef {
    let mut builder = StringBuilder::new();

    for _ in 0..num_rows {
        if rng.gen::<f64>() < null_ratio {
            builder.append_null();
        } else {
            let s = match cli.profile {
                DataProfile::LongStrings => generate_long_string(rng, 1024),
                DataProfile::Unicode => generate_unicode_string(rng),
                _ => generate_random_string(rng, cli.string_len),
            };
            builder.append_value(&s);
        }
    }

    Arc::new(builder.finish())
}

fn generate_boolean(num_rows: usize, null_ratio: f64, rng: &mut StdRng) -> ArrayRef {
    let values: Vec<Option<bool>> = (0..num_rows)
        .map(|_| {
            if rng.gen::<f64>() < null_ratio {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect();
    Arc::new(BooleanArray::from(values))
}

fn generate_random_string(rng: &mut StdRng, avg_len: usize) -> String {
    let len = rng.gen_range(1..=avg_len * 2);
    (0..len)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect()
}

fn generate_long_string(rng: &mut StdRng, min_len: usize) -> String {
    let len = rng.gen_range(min_len..min_len * 2);
    (0..len)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect()
}

fn generate_unicode_string(rng: &mut StdRng) -> String {
    let templates = [
        "Hello 🌍🌎🌏 World",
        "مرحبا بالعالم", // Arabic (RTL)
        "שלום עולם",     // Hebrew (RTL)
        "你好世界",      // Chinese
        "こんにちは",    // Japanese
        "🎉🎊🎁🎈🎂",    // Emoji
        "a]0;titleb",    // Control chars
        "line1\nline2\ttab",
        "quote\"here",
        "comma,here",
        "Ψ ≠ ∞ × ÷ √",       // Math symbols
        "café résumé naïve", // Accented
        "🧑‍🤝‍🧑👨‍👩‍👧‍👦",              // ZWJ sequences
    ];
    templates[rng.gen_range(0..templates.len())].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_cli() -> Cli {
        Cli {
            rows: 10,
            cols: 2,
            output: PathBuf::from("fixture.parquet"),
            batch_size: 10,
            seed: 42,
            null_ratio: 0.1,
            string_len: 8,
            profile: DataProfile::Mixed,
            compression: CompressionCodec::Snappy,
        }
    }

    #[test]
    fn rejects_invalid_generation_bounds() {
        let mut cli = valid_cli();
        cli.batch_size = 0;
        assert!(validate_cli(&cli).is_err());

        cli = valid_cli();
        cli.cols = 0;
        assert!(validate_cli(&cli).is_err());

        cli = valid_cli();
        cli.null_ratio = f64::NAN;
        assert!(validate_cli(&cli).is_err());

        cli = valid_cli();
        cli.string_len = 0;
        assert!(validate_cli(&cli).is_err());
    }
}
