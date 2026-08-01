use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::basic::{
    Compression as ParquetCompression, ConvertedType as ParquetConvertedType,
    LogicalType as ParquetLogicalType, TimeUnit as ParquetTimeUnit, Type as ParquetPhysicalType,
};
use parquet::schema::types::ColumnDescriptor;
use std::fmt;
use std::path::{Path, PathBuf};

/// Default maximum rows yielded in one streaming query batch.
pub const DEFAULT_QUERY_BATCH_SIZE: usize = 1024;

/// Columns decoded by a streaming query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Projection {
    /// Decode every top-level Arrow column.
    #[default]
    All,
    /// Decode the named top-level Arrow columns in the requested order.
    Columns(Vec<String>),
}

/// Controls streaming query execution for each source in a dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOptions {
    /// Top-level columns to decode.
    pub projection: Projection,
    /// Maximum rows decoded from each source. `None` reads every row.
    pub limit: Option<usize>,
    /// Maximum rows in each yielded record batch. Must be greater than zero.
    pub batch_size: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            projection: Projection::All,
            limit: None,
            batch_size: DEFAULT_QUERY_BATCH_SIZE,
        }
    }
}

/// Metadata produced while planning one source in a streaming query.
#[derive(Clone, Debug)]
pub struct QuerySource {
    /// Source path in dataset order.
    pub path: PathBuf,
    /// Projected Arrow schema, retained even when the source has no rows.
    pub schema: SchemaRef,
}

/// One decoded batch from a streaming query.
#[derive(Clone, Debug)]
pub struct QueryBatch {
    /// Position of this batch's source in [`crate::QueryStream::sources`].
    pub source_index: usize,
    /// Path of the source that produced this batch.
    pub path: PathBuf,
    /// Decoded, projected Arrow record batch.
    pub batch: RecordBatch,
}

/// Direction used by an eager row scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanKind {
    /// Read rows from the beginning of each source.
    Head,
    /// Read rows from the end of each source.
    Tail,
}

/// Options for an eager row scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanOptions {
    /// Maximum rows to read from each source.
    pub rows: usize,
}

/// Leaf-column schema information for one source.
#[derive(Clone, Debug)]
pub struct SchemaResult {
    /// Source path.
    pub path: PathBuf,
    /// Parquet leaf columns in schema order.
    pub columns: Vec<ColumnInfo>,
}

/// Eager scan output for one source.
#[derive(Clone, Debug)]
pub struct ScanResult {
    /// Source path.
    pub path: PathBuf,
    /// Arrow schema for the source, retained even when `batches` is empty.
    pub schema: SchemaRef,
    /// Decoded Arrow batches in row order.
    pub batches: Vec<RecordBatch>,
}

/// Metadata row count for one source occurrence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountEntry {
    /// Source path.
    pub path: PathBuf,
    /// Non-negative row count read from file metadata.
    pub rows: i64,
}

/// Per-source and aggregate row counts for a dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountResult {
    /// Counts in dataset order, including explicit repeated inputs.
    pub entries: Vec<CountEntry>,
    /// Checked sum of all entry row counts.
    pub total_rows: i64,
}

/// Column-statistics rows for one source.
#[derive(Clone, Debug)]
pub struct StatsResult {
    /// Source path.
    pub path: PathBuf,
    /// Requested column statistics in Parquet leaf order.
    pub rows: Vec<ColumnStats>,
}

/// Typed schema metadata for one Parquet leaf column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    /// Full dotted leaf path.
    pub name: String,
    /// Physical and optional logical Parquet type.
    pub column_type: ColumnType,
    /// Whether any definition level in the leaf path is optional.
    pub nullable: bool,
}

impl ColumnInfo {
    /// Return the preferred logical type name, or the physical type name.
    pub fn display_type(&self) -> String {
        self.column_type.display_name()
    }
}

/// File-level Parquet metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    /// Source path.
    pub path: PathBuf,
    /// File size from filesystem metadata.
    pub file_size_bytes: u64,
    /// Non-negative row count from Parquet metadata.
    pub num_rows: i64,
    /// Number of Parquet leaf columns.
    pub num_columns: usize,
    /// Number of row groups.
    pub num_row_groups: usize,
    /// Compression codec summary across every column chunk.
    pub compression: CompressionSummary,
    /// Optional writer identification from Parquet metadata.
    pub created_by: Option<String>,
    /// Parquet file metadata version.
    pub version: i32,
}

impl FileInfo {
    /// Return the source path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Aggregated Parquet metadata statistics for one leaf column.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    /// Full dotted leaf path.
    pub column: String,
    /// Physical and optional logical Parquet type.
    pub column_type: ColumnType,
    /// Aggregated null count, or `None` when any row group lacks statistics.
    pub null_count: Option<u64>,
    /// Minimum value, or `None` when bounds are absent or incomplete.
    pub min: Option<StatValue>,
    /// Maximum value, or `None` when bounds are absent or incomplete.
    pub max: Option<StatValue>,
    /// Whether every row group supplied enough metadata for the reported values.
    pub statistics_complete: bool,
}

impl ColumnStats {
    /// Return the preferred logical type name, or the physical type name.
    pub fn display_type(&self) -> String {
        self.column_type.display_name()
    }

    /// Render a typed statistic consistently for human-readable output.
    pub fn display_stat_value(&self, value: &StatValue) -> String {
        match value {
            StatValue::Int32(value)
                if matches!(
                    self.column_type.logical,
                    Some(LogicalTypeKind::Decimal { .. })
                ) =>
            {
                format_decimal(&value.to_string(), decimal_scale(&self.column_type.logical))
            }
            StatValue::Int64(value)
                if matches!(
                    self.column_type.logical,
                    Some(LogicalTypeKind::Decimal { .. })
                ) =>
            {
                format_decimal(&value.to_string(), decimal_scale(&self.column_type.logical))
            }
            StatValue::DecimalBytes(bytes) => format_decimal(
                &signed_bytes_to_decimal(bytes),
                decimal_scale(&self.column_type.logical),
            ),
            StatValue::Binary(bytes) | StatValue::FixedLenBinary(bytes)
                if self.column_type.logical == Some(LogicalTypeKind::String) =>
            {
                display_utf8_or_hex(bytes)
            }
            StatValue::Binary(bytes) | StatValue::FixedLenBinary(bytes) => display_hex(bytes),
            _ => value.to_string(),
        }
    }
}

/// Physical and optional logical type metadata for a Parquet leaf column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnType {
    /// Required Parquet physical storage type.
    pub physical: PhysicalType,
    /// Optional Parquet logical or legacy converted type.
    pub logical: Option<LogicalTypeKind>,
}

impl ColumnType {
    pub(crate) fn from_parquet(column: &ColumnDescriptor) -> Self {
        let logical = column
            .logical_type_ref()
            .map(LogicalTypeKind::from_parquet)
            .or_else(|| {
                LogicalTypeKind::from_converted(
                    column.converted_type(),
                    column.type_precision(),
                    column.type_scale(),
                )
            });

        Self {
            physical: column.physical_type().into(),
            logical,
        }
    }

    /// Return the preferred logical type name, or the physical type name.
    pub fn display_name(&self) -> String {
        self.logical
            .as_ref()
            .map_or_else(|| self.physical.to_string(), LogicalTypeKind::display_name)
    }
}

/// Parquet physical storage type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalType {
    /// Boolean bit-packed values.
    Boolean,
    /// 32-bit signed integer storage.
    Int32,
    /// 64-bit signed integer storage.
    Int64,
    /// Deprecated 96-bit integer storage, commonly used for timestamps.
    Int96,
    /// 32-bit IEEE 754 floating-point storage.
    Float,
    /// 64-bit IEEE 754 floating-point storage.
    Double,
    /// Variable-length byte-array storage.
    ByteArray,
    /// Fixed-length byte-array storage.
    FixedLenByteArray,
}

impl From<ParquetPhysicalType> for PhysicalType {
    fn from(value: ParquetPhysicalType) -> Self {
        match value {
            ParquetPhysicalType::BOOLEAN => Self::Boolean,
            ParquetPhysicalType::INT32 => Self::Int32,
            ParquetPhysicalType::INT64 => Self::Int64,
            ParquetPhysicalType::INT96 => Self::Int96,
            ParquetPhysicalType::FLOAT => Self::Float,
            ParquetPhysicalType::DOUBLE => Self::Double,
            ParquetPhysicalType::BYTE_ARRAY => Self::ByteArray,
            ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => Self::FixedLenByteArray,
        }
    }
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Boolean => "BOOLEAN",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Int96 => "INT96",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::ByteArray => "BYTE_ARRAY",
            Self::FixedLenByteArray => "FIXED_LEN_BYTE_ARRAY",
        };
        f.write_str(name)
    }
}

/// Supported Parquet logical type annotation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalTypeKind {
    /// UTF-8 string annotation.
    String,
    /// Key-value map annotation.
    Map,
    /// List annotation.
    List,
    /// Enumerated string annotation.
    Enum,
    /// Fixed-precision decimal annotation.
    Decimal {
        /// Number of digits to the right of the decimal point.
        scale: i32,
        /// Total number of significant digits.
        precision: i32,
    },
    /// Calendar date annotation.
    Date,
    /// Time-of-day annotation.
    Time {
        /// Whether the value is normalized to UTC.
        is_adjusted_to_utc: bool,
        /// Stored time resolution.
        unit: TimeUnit,
    },
    /// Date-and-time annotation.
    Timestamp {
        /// Whether the value is normalized to UTC.
        is_adjusted_to_utc: bool,
        /// Stored timestamp resolution.
        unit: TimeUnit,
    },
    /// Signed or unsigned integer-width annotation.
    Integer {
        /// Logical integer width in bits.
        bit_width: i8,
        /// Whether values are signed.
        is_signed: bool,
    },
    /// Unknown logical value annotation.
    Unknown,
    /// JSON document annotation.
    Json,
    /// BSON document annotation.
    Bson,
    /// Legacy 12-byte interval annotation.
    Interval,
    /// UUID annotation.
    Uuid,
    /// IEEE 754 binary16 annotation.
    Float16,
    /// Variant document annotation and optional specification version.
    Variant {
        /// Variant specification version recorded in the file.
        specification_version: Option<i8>,
    },
    /// Planar geometry annotation and optional coordinate reference system.
    Geometry {
        /// Coordinate reference system recorded in the file.
        crs: Option<String>,
    },
    /// Geography annotation with optional coordinate and edge metadata.
    Geography {
        /// Coordinate reference system recorded in the file.
        crs: Option<String>,
        /// Edge-interpolation algorithm recorded in the file.
        algorithm: Option<String>,
    },
}

impl LogicalTypeKind {
    fn from_parquet(value: &ParquetLogicalType) -> Self {
        match value {
            ParquetLogicalType::String => Self::String,
            ParquetLogicalType::Map => Self::Map,
            ParquetLogicalType::List => Self::List,
            ParquetLogicalType::Enum => Self::Enum,
            ParquetLogicalType::Decimal(decimal) => Self::Decimal {
                scale: decimal.scale,
                precision: decimal.precision,
            },
            ParquetLogicalType::Date => Self::Date,
            ParquetLogicalType::Time(time) => Self::Time {
                is_adjusted_to_utc: time.is_adjusted_to_u_t_c,
                unit: time.unit.into(),
            },
            ParquetLogicalType::Timestamp(timestamp) => Self::Timestamp {
                is_adjusted_to_utc: timestamp.is_adjusted_to_u_t_c,
                unit: timestamp.unit.into(),
            },
            ParquetLogicalType::Integer(integer) => Self::Integer {
                bit_width: integer.bit_width,
                is_signed: integer.is_signed,
            },
            ParquetLogicalType::Unknown => Self::Unknown,
            ParquetLogicalType::Json => Self::Json,
            ParquetLogicalType::Bson => Self::Bson,
            ParquetLogicalType::Uuid => Self::Uuid,
            ParquetLogicalType::Float16 => Self::Float16,
            ParquetLogicalType::Variant(variant) => Self::Variant {
                specification_version: variant.specification_version,
            },
            ParquetLogicalType::Geometry(geometry) => Self::Geometry {
                crs: geometry.crs.clone(),
            },
            ParquetLogicalType::Geography(geography) => Self::Geography {
                crs: geography.crs.clone(),
                algorithm: geography.algorithm.map(|algorithm| algorithm.to_string()),
            },
            ParquetLogicalType::_Unknown { .. } => Self::Unknown,
        }
    }

    fn from_converted(value: ParquetConvertedType, precision: i32, scale: i32) -> Option<Self> {
        match value {
            ParquetConvertedType::NONE => None,
            ParquetConvertedType::UTF8 => Some(Self::String),
            ParquetConvertedType::MAP | ParquetConvertedType::MAP_KEY_VALUE => Some(Self::Map),
            ParquetConvertedType::LIST => Some(Self::List),
            ParquetConvertedType::ENUM => Some(Self::Enum),
            ParquetConvertedType::DECIMAL => Some(Self::Decimal { scale, precision }),
            ParquetConvertedType::DATE => Some(Self::Date),
            ParquetConvertedType::TIME_MILLIS => Some(Self::Time {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Millis,
            }),
            ParquetConvertedType::TIME_MICROS => Some(Self::Time {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Micros,
            }),
            ParquetConvertedType::TIMESTAMP_MILLIS => Some(Self::Timestamp {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Millis,
            }),
            ParquetConvertedType::TIMESTAMP_MICROS => Some(Self::Timestamp {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Micros,
            }),
            ParquetConvertedType::UINT_8 => Some(Self::Integer {
                bit_width: 8,
                is_signed: false,
            }),
            ParquetConvertedType::UINT_16 => Some(Self::Integer {
                bit_width: 16,
                is_signed: false,
            }),
            ParquetConvertedType::UINT_32 => Some(Self::Integer {
                bit_width: 32,
                is_signed: false,
            }),
            ParquetConvertedType::UINT_64 => Some(Self::Integer {
                bit_width: 64,
                is_signed: false,
            }),
            ParquetConvertedType::INT_8 => Some(Self::Integer {
                bit_width: 8,
                is_signed: true,
            }),
            ParquetConvertedType::INT_16 => Some(Self::Integer {
                bit_width: 16,
                is_signed: true,
            }),
            ParquetConvertedType::INT_32 => Some(Self::Integer {
                bit_width: 32,
                is_signed: true,
            }),
            ParquetConvertedType::INT_64 => Some(Self::Integer {
                bit_width: 64,
                is_signed: true,
            }),
            ParquetConvertedType::JSON => Some(Self::Json),
            ParquetConvertedType::BSON => Some(Self::Bson),
            ParquetConvertedType::INTERVAL => Some(Self::Interval),
        }
    }

    /// Return a compact, stable display name for the logical type.
    pub fn display_name(&self) -> String {
        match self {
            Self::String => "STRING".to_string(),
            Self::Map => "MAP".to_string(),
            Self::List => "LIST".to_string(),
            Self::Enum => "ENUM".to_string(),
            Self::Decimal { scale, precision } => format!("DECIMAL({precision},{scale})"),
            Self::Date => "DATE".to_string(),
            Self::Time { unit, .. } => format!("TIME({unit})"),
            Self::Timestamp { unit, .. } => format!("TIMESTAMP({unit})"),
            Self::Integer {
                bit_width,
                is_signed,
            } => {
                if *is_signed {
                    format!("INT{bit_width}")
                } else {
                    format!("UINT{bit_width}")
                }
            }
            Self::Unknown => "UNKNOWN".to_string(),
            Self::Json => "JSON".to_string(),
            Self::Bson => "BSON".to_string(),
            Self::Interval => "INTERVAL".to_string(),
            Self::Uuid => "UUID".to_string(),
            Self::Float16 => "FLOAT16".to_string(),
            Self::Variant {
                specification_version,
            } => specification_version.map_or_else(
                || "VARIANT".to_string(),
                |version| format!("VARIANT({version})"),
            ),
            Self::Geometry { crs } => crs
                .as_ref()
                .map_or_else(|| "GEOMETRY".to_string(), |crs| format!("GEOMETRY({crs})")),
            Self::Geography { crs, algorithm } => match (crs, algorithm) {
                (None, None) => "GEOGRAPHY".to_string(),
                (Some(crs), None) => format!("GEOGRAPHY({crs})"),
                (None, Some(algorithm)) => format!("GEOGRAPHY(edges={algorithm})"),
                (Some(crs), Some(algorithm)) => {
                    format!("GEOGRAPHY(crs={crs},edges={algorithm})")
                }
            },
        }
    }
}

/// Resolution for Parquet time and timestamp logical types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeUnit {
    /// Millisecond resolution.
    Millis,
    /// Microsecond resolution.
    Micros,
    /// Nanosecond resolution.
    Nanos,
}

impl From<ParquetTimeUnit> for TimeUnit {
    fn from(value: ParquetTimeUnit) -> Self {
        match value {
            ParquetTimeUnit::MILLIS => Self::Millis,
            ParquetTimeUnit::MICROS => Self::Micros,
            ParquetTimeUnit::NANOS => Self::Nanos,
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Millis => "MILLIS",
            Self::Micros => "MICROS",
            Self::Nanos => "NANOS",
        };
        f.write_str(name)
    }
}

/// Parquet column-chunk compression codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionCodec {
    /// No compression.
    Uncompressed,
    /// Snappy compression.
    Snappy,
    /// Gzip compression.
    Gzip,
    /// LZO compression.
    Lzo,
    /// Brotli compression.
    Brotli,
    /// Deprecated Hadoop-compatible LZ4 compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
    /// Raw LZ4 block compression.
    Lz4Raw,
}

/// File-level summary of column-chunk compression codecs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionSummary {
    /// No column chunks are available to identify a codec.
    Unknown,
    /// Every column chunk uses one codec.
    Single(CompressionCodec),
    /// Column chunks use more than one codec.
    Mixed,
}

impl fmt::Display for CompressionSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("UNKNOWN"),
            Self::Single(codec) => write!(f, "{codec}"),
            Self::Mixed => f.write_str("MIXED"),
        }
    }
}

impl From<ParquetCompression> for CompressionCodec {
    fn from(value: ParquetCompression) -> Self {
        match value {
            ParquetCompression::UNCOMPRESSED => Self::Uncompressed,
            ParquetCompression::SNAPPY => Self::Snappy,
            ParquetCompression::GZIP(_) => Self::Gzip,
            ParquetCompression::LZO => Self::Lzo,
            ParquetCompression::BROTLI(_) => Self::Brotli,
            ParquetCompression::LZ4 => Self::Lz4,
            ParquetCompression::ZSTD(_) => Self::Zstd,
            ParquetCompression::LZ4_RAW => Self::Lz4Raw,
        }
    }
}

impl fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Uncompressed => "UNCOMPRESSED",
            Self::Snappy => "SNAPPY",
            Self::Gzip => "GZIP",
            Self::Lzo => "LZO",
            Self::Brotli => "BROTLI",
            Self::Lz4 => "LZ4",
            Self::Zstd => "ZSTD",
            Self::Lz4Raw => "LZ4_RAW",
        };
        f.write_str(name)
    }
}

/// Typed value read from Parquet statistics metadata.
#[derive(Clone, Debug, PartialEq)]
pub enum StatValue {
    /// A physical `INT32` value.
    Int32(i32),
    /// An unsigned logical integer stored as physical `INT32`.
    UInt32(u32),
    /// A physical `INT64` value.
    Int64(i64),
    /// An unsigned logical integer stored as physical `INT64`.
    UInt64(u64),
    /// A physical `FLOAT` value.
    Float(f32),
    /// A logical `FLOAT16` value decoded to `f32` without losing precision.
    Float16(f32),
    /// A physical `DOUBLE` value.
    Double(f64),
    /// A physical `BYTE_ARRAY` value.
    Binary(Vec<u8>),
    /// A physical `BOOLEAN` value.
    Boolean(bool),
    /// A physical `FIXED_LEN_BYTE_ARRAY` value.
    FixedLenBinary(Vec<u8>),
    /// A byte-backed decimal's signed big-endian unscaled value.
    DecimalBytes(Vec<u8>),
    /// A deterministic representation of a physical `INT96` value.
    Int96(String),
}

impl fmt::Display for StatValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(value) => write!(f, "{value}"),
            Self::UInt32(value) => write!(f, "{value}"),
            Self::Int64(value) => write!(f, "{value}"),
            Self::UInt64(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
            Self::Float16(value) => write!(f, "{value}"),
            Self::Double(value) => write!(f, "{value}"),
            Self::Binary(value) | Self::FixedLenBinary(value) => f.write_str(&display_hex(value)),
            Self::DecimalBytes(value) => f.write_str(&signed_bytes_to_decimal(value)),
            Self::Boolean(value) => write!(f, "{value}"),
            Self::Int96(value) => f.write_str(value),
        }
    }
}

fn display_utf8_or_hex(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(text) => text.to_string(),
        Err(_) => display_hex(value),
    }
}

fn display_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decimal_scale(logical_type: &Option<LogicalTypeKind>) -> i32 {
    match logical_type {
        Some(LogicalTypeKind::Decimal { scale, .. }) => *scale,
        _ => 0,
    }
}

fn format_decimal(unscaled: &str, scale: i32) -> String {
    let Ok(scale) = usize::try_from(scale) else {
        return unscaled.to_string();
    };
    if scale == 0 {
        return unscaled.to_string();
    }

    let (sign, digits) = unscaled
        .strip_prefix('-')
        .map_or(("", unscaled), |digits| ("-", digits));
    if digits.len() > scale {
        let split = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    } else {
        let zero_count = scale - digits.len();
        format!("{sign}0.{}{digits}", "0".repeat(zero_count))
    }
}

fn signed_bytes_to_decimal(value: &[u8]) -> String {
    if value.is_empty() {
        return "0".to_string();
    }

    let negative = value[0] & 0x80 != 0;
    let mut magnitude = value.to_vec();
    if negative {
        for byte in &mut magnitude {
            *byte = !*byte;
        }
        for byte in magnitude.iter_mut().rev() {
            let (incremented, overflowed) = byte.overflowing_add(1);
            *byte = incremented;
            if !overflowed {
                break;
            }
        }
    }

    let mut decimal_digits = vec![0u8];
    for byte in magnitude {
        let mut carry = u16::from(byte);
        for digit in &mut decimal_digits {
            let value = u16::from(*digit) * 256 + carry;
            *digit = (value % 10).to_le_bytes()[0];
            carry = value / 10;
        }
        while carry > 0 {
            decimal_digits.push((carry % 10).to_le_bytes()[0]);
            carry /= 10;
        }
    }
    while decimal_digits.len() > 1 && decimal_digits.last() == Some(&0) {
        decimal_digits.pop();
    }

    let digits = decimal_digits
        .iter()
        .rev()
        .map(|digit| char::from(b'0' + *digit))
        .collect::<String>();
    if negative && digits != "0" {
        format!("-{digits}")
    } else {
        digits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_time_annotations_are_utc_adjusted() {
        assert_eq!(
            LogicalTypeKind::from_converted(ParquetConvertedType::TIME_MILLIS, 0, 0),
            Some(LogicalTypeKind::Time {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Millis,
            })
        );
        assert_eq!(
            LogicalTypeKind::from_converted(ParquetConvertedType::TIME_MICROS, 0, 0),
            Some(LogicalTypeKind::Time {
                is_adjusted_to_utc: true,
                unit: TimeUnit::Micros,
            })
        );
    }

    #[test]
    fn legacy_interval_annotation_is_preserved() {
        assert_eq!(
            LogicalTypeKind::from_converted(ParquetConvertedType::INTERVAL, 0, 0),
            Some(LogicalTypeKind::Interval)
        );
        assert_eq!(LogicalTypeKind::Interval.display_name(), "INTERVAL");
    }

    #[test]
    fn current_logical_types_have_stable_display_names() {
        let variant = LogicalTypeKind::from_parquet(&ParquetLogicalType::variant(Some(1)));
        assert_eq!(variant.display_name(), "VARIANT(1)");

        let geometry = LogicalTypeKind::from_parquet(&ParquetLogicalType::geometry(Some(
            "EPSG:4326".to_string(),
        )));
        assert_eq!(geometry.display_name(), "GEOMETRY(EPSG:4326)");

        let geography = LogicalTypeKind::from_parquet(&ParquetLogicalType::geography(
            Some("EPSG:4326".to_string()),
            Some(parquet::basic::EdgeInterpolationAlgorithm::VINCENTY),
        ));
        assert_eq!(
            geography.display_name(),
            "GEOGRAPHY(crs=EPSG:4326,edges=VINCENTY)"
        );
    }

    #[test]
    fn byte_backed_decimals_render_exactly() {
        let stats = ColumnStats {
            column: "amount".to_string(),
            column_type: ColumnType {
                physical: PhysicalType::FixedLenByteArray,
                logical: Some(LogicalTypeKind::Decimal {
                    scale: 2,
                    precision: 30,
                }),
            },
            null_count: Some(0),
            min: None,
            max: None,
            statistics_complete: true,
        };

        assert_eq!(
            stats.display_stat_value(&StatValue::DecimalBytes(vec![0x00, 0x64])),
            "1.00"
        );
        assert_eq!(
            stats.display_stat_value(&StatValue::DecimalBytes(vec![0xff, 0x38])),
            "-2.00"
        );

        for value in [
            i128::MIN,
            -1_000_000,
            -129,
            -1,
            0,
            1,
            128,
            1_000_000,
            i128::MAX,
        ] {
            assert_eq!(
                signed_bytes_to_decimal(&value.to_be_bytes()),
                value.to_string()
            );
        }
    }
}
