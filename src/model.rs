use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::basic::{
    Compression as ParquetCompression, ConvertedType as ParquetConvertedType,
    LogicalType as ParquetLogicalType, TimeUnit as ParquetTimeUnit, Type as ParquetPhysicalType,
};
use parquet::schema::types::ColumnDescriptor;
use std::fmt;
use std::path::{Path, PathBuf};

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
    pub path: PathBuf,
    /// Projected Arrow schema, retained even when the source has no rows.
    pub schema: SchemaRef,
}

/// One decoded batch from a streaming query.
#[derive(Clone, Debug)]
pub struct QueryBatch {
    /// Position of this batch's source in [`crate::QueryStream::sources`].
    pub source_index: usize,
    pub path: PathBuf,
    pub batch: RecordBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanKind {
    Head,
    Tail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanOptions {
    pub rows: usize,
}

#[derive(Clone, Debug)]
pub struct SchemaResult {
    pub path: PathBuf,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub path: PathBuf,
    /// Arrow schema for the source, retained even when `batches` is empty.
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountEntry {
    pub path: PathBuf,
    pub rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountResult {
    pub entries: Vec<CountEntry>,
    pub total_rows: i64,
}

#[derive(Clone, Debug)]
pub struct StatsResult {
    pub path: PathBuf,
    pub rows: Vec<ColumnStats>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

impl ColumnInfo {
    pub fn display_type(&self) -> String {
        self.column_type.display_name()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: PathBuf,
    pub file_size_bytes: u64,
    pub num_rows: i64,
    pub num_columns: usize,
    pub num_row_groups: usize,
    pub compression: CompressionSummary,
    pub created_by: Option<String>,
    pub version: i32,
}

impl FileInfo {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    pub column: String,
    pub column_type: ColumnType,
    /// Aggregated null count, or `None` when any row group lacks statistics.
    pub null_count: Option<u64>,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
    /// Whether every row group supplied enough metadata for the reported values.
    pub statistics_complete: bool,
}

impl ColumnStats {
    pub fn display_type(&self) -> String {
        self.column_type.display_name()
    }

    pub fn display_stat_value(&self, value: &StatValue) -> String {
        match value {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnType {
    pub physical: PhysicalType,
    pub logical: Option<LogicalTypeKind>,
}

impl ColumnType {
    pub(crate) fn from_parquet(column: &ColumnDescriptor) -> Self {
        let logical = column
            .logical_type()
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

    pub fn display_name(&self) -> String {
        self.logical
            .as_ref()
            .map_or_else(|| self.physical.to_string(), LogicalTypeKind::display_name)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalTypeKind {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        is_adjusted_to_utc: bool,
        unit: TimeUnit,
    },
    Timestamp {
        is_adjusted_to_utc: bool,
        unit: TimeUnit,
    },
    Integer {
        bit_width: i8,
        is_signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
    Float16,
}

impl LogicalTypeKind {
    fn from_parquet(value: ParquetLogicalType) -> Self {
        match value {
            ParquetLogicalType::String => Self::String,
            ParquetLogicalType::Map => Self::Map,
            ParquetLogicalType::List => Self::List,
            ParquetLogicalType::Enum => Self::Enum,
            ParquetLogicalType::Decimal { scale, precision } => Self::Decimal { scale, precision },
            ParquetLogicalType::Date => Self::Date,
            ParquetLogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => Self::Time {
                is_adjusted_to_utc: is_adjusted_to_u_t_c,
                unit: unit.into(),
            },
            ParquetLogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => Self::Timestamp {
                is_adjusted_to_utc: is_adjusted_to_u_t_c,
                unit: unit.into(),
            },
            ParquetLogicalType::Integer {
                bit_width,
                is_signed,
            } => Self::Integer {
                bit_width,
                is_signed,
            },
            ParquetLogicalType::Unknown => Self::Unknown,
            ParquetLogicalType::Json => Self::Json,
            ParquetLogicalType::Bson => Self::Bson,
            ParquetLogicalType::Uuid => Self::Uuid,
            ParquetLogicalType::Float16 => Self::Float16,
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
                is_adjusted_to_utc: false,
                unit: TimeUnit::Millis,
            }),
            ParquetConvertedType::TIME_MICROS => Some(Self::Time {
                is_adjusted_to_utc: false,
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
            ParquetConvertedType::INTERVAL => None,
        }
    }

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
            Self::Uuid => "UUID".to_string(),
            Self::Float16 => "FLOAT16".to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl From<ParquetTimeUnit> for TimeUnit {
    fn from(value: ParquetTimeUnit) -> Self {
        match value {
            ParquetTimeUnit::MILLIS(_) => Self::Millis,
            ParquetTimeUnit::MICROS(_) => Self::Micros,
            ParquetTimeUnit::NANOS(_) => Self::Nanos,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionCodec {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionSummary {
    Unknown,
    Single(CompressionCodec),
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

#[derive(Clone, Debug, PartialEq)]
pub enum StatValue {
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Binary(Vec<u8>),
    Boolean(bool),
    FixedLenBinary(Vec<u8>),
    Int96(String),
}

impl fmt::Display for StatValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(value) => write!(f, "{value}"),
            Self::Int64(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
            Self::Double(value) => write!(f, "{value}"),
            Self::Binary(value) | Self::FixedLenBinary(value) => f.write_str(&display_hex(value)),
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
