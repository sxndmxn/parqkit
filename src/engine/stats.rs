use crate::model::{ColumnStats, ColumnType, StatValue};
use crate::ParqkitError;
use crate::Result;
use parquet::basic::{ColumnOrder, SortOrder};
use parquet::data_type::Int96;
use parquet::file::reader::FileReader;
use parquet::file::statistics::Statistics;
use std::path::Path;

pub fn column_stats(path: &Path, column_name: Option<&str>) -> Result<Vec<ColumnStats>> {
    let reader = super::parquet::serialized_reader(path)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let mut column_stats: Vec<AccumulatedColumnStats> = (0..schema.num_columns())
        .map(|index| {
            let column = schema.column(index);
            AccumulatedColumnStats {
                column: column.path().string(),
                column_type: ColumnType::from_parquet(&column),
                null_count: Some(0),
                min: None,
                max: None,
                bounds_complete: true,
                bounds_ordered: has_ordered_bounds(metadata.file_metadata().column_order(index)),
            }
        })
        .collect();

    for row_group_index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(row_group_index);
        if row_group.num_columns() != column_stats.len() {
            return Err(ParqkitError::invalid_metadata(
                path,
                format!(
                    "row group {row_group_index} has {} columns but the file schema has {}",
                    row_group.num_columns(),
                    column_stats.len()
                ),
            ));
        }
        let row_group_rows = u64::try_from(row_group.num_rows()).map_err(|_| {
            ParqkitError::invalid_metadata(
                path,
                format!("row group {row_group_index} has a negative row count"),
            )
        })?;
        for (column_index, stats) in column_stats.iter_mut().enumerate() {
            if let Some(column_statistics) = row_group.column(column_index).statistics() {
                stats.add_null_count(path, column_statistics.null_count_opt())?;

                if stats.bounds_ordered
                    && column_statistics.min_bytes_opt().is_some()
                    && column_statistics.max_bytes_opt().is_some()
                    && column_statistics.min_is_exact()
                    && column_statistics.max_is_exact()
                {
                    update_min_max(stats, column_statistics);
                } else if column_statistics.null_count_opt() != Some(row_group_rows) {
                    stats.bounds_complete = false;
                }
            } else {
                stats.null_count = None;
                stats.bounds_complete = false;
            }
        }
    }

    if let Some(name) = column_name {
        if !column_stats.iter().any(|stats| stats.column == name) {
            return Err(ParqkitError::column_not_found(path, name));
        }
    }

    Ok(column_stats
        .into_iter()
        .filter(|stats| column_name.is_none_or(|name| stats.column == name))
        .map(AccumulatedColumnStats::into_row)
        .collect())
}

fn has_ordered_bounds(column_order: ColumnOrder) -> bool {
    matches!(
        column_order,
        ColumnOrder::TYPE_DEFINED_ORDER(order) if order != SortOrder::UNDEFINED
    )
}

struct AccumulatedColumnStats {
    column: String,
    column_type: ColumnType,
    null_count: Option<u64>,
    min: Option<StatValue>,
    max: Option<StatValue>,
    bounds_complete: bool,
    bounds_ordered: bool,
}

impl AccumulatedColumnStats {
    fn add_null_count(&mut self, path: &Path, candidate: Option<u64>) -> Result<()> {
        self.null_count = match (self.null_count, candidate) {
            (Some(current), Some(candidate)) => {
                Some(current.checked_add(candidate).ok_or_else(|| {
                    ParqkitError::invalid_metadata(
                        path,
                        format!("null count overflow for column {}", self.column),
                    )
                })?)
            }
            _ => None,
        };
        Ok(())
    }

    fn into_row(self) -> ColumnStats {
        let statistics_complete = self.null_count.is_some() && self.bounds_complete;
        let min = if self.bounds_complete { self.min } else { None };
        let max = if self.bounds_complete { self.max } else { None };
        ColumnStats {
            column: self.column,
            column_type: self.column_type,
            null_count: self.null_count,
            min,
            max,
            statistics_complete,
        }
    }
}

fn update_min_max(stats: &mut AccumulatedColumnStats, parquet_stats: &Statistics) {
    match parquet_stats {
        Statistics::Int32(source) => {
            let min = source
                .min_opt()
                .copied()
                .map(|value| int32_stat_value(&stats.column_type, value));
            let max = source
                .max_opt()
                .copied()
                .map(|value| int32_stat_value(&stats.column_type, value));
            merge_min(&mut stats.min, min);
            merge_max(&mut stats.max, max);
        }
        Statistics::Int64(source) => {
            let min = source
                .min_opt()
                .copied()
                .map(|value| int64_stat_value(&stats.column_type, value));
            let max = source
                .max_opt()
                .copied()
                .map(|value| int64_stat_value(&stats.column_type, value));
            merge_min(&mut stats.min, min);
            merge_max(&mut stats.max, max);
        }
        Statistics::Float(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Float),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Float),
            );
        }
        Statistics::Double(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Double),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Double),
            );
        }
        Statistics::ByteArray(source) => {
            let is_decimal = matches!(
                stats.column_type.logical,
                Some(crate::model::LogicalTypeKind::Decimal { .. })
            );
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .map(|value| byte_array_stat_value(value.data().to_vec(), is_decimal)),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .map(|value| byte_array_stat_value(value.data().to_vec(), is_decimal)),
            );
        }
        Statistics::Boolean(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Boolean),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Boolean),
            );
        }
        Statistics::FixedLenByteArray(source) => {
            let logical_type = stats.column_type.logical.as_ref();
            merge_min(
                &mut stats.min,
                source.min_opt().map(|value| {
                    fixed_len_byte_array_stat_value(value.data().to_vec(), logical_type)
                }),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().map(|value| {
                    fixed_len_byte_array_stat_value(value.data().to_vec(), logical_type)
                }),
            );
        }
        Statistics::Int96(source) => {
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .copied()
                    .map(display_int96)
                    .map(StatValue::Int96),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .copied()
                    .map(display_int96)
                    .map(StatValue::Int96),
            );
        }
    }
}

fn int32_stat_value(column_type: &ColumnType, value: i32) -> StatValue {
    if matches!(
        column_type.logical,
        Some(crate::model::LogicalTypeKind::Integer {
            is_signed: false,
            ..
        })
    ) {
        StatValue::UInt32(u32::from_ne_bytes(value.to_ne_bytes()))
    } else {
        StatValue::Int32(value)
    }
}

fn int64_stat_value(column_type: &ColumnType, value: i64) -> StatValue {
    if matches!(
        column_type.logical,
        Some(crate::model::LogicalTypeKind::Integer {
            is_signed: false,
            ..
        })
    ) {
        StatValue::UInt64(u64::from_ne_bytes(value.to_ne_bytes()))
    } else {
        StatValue::Int64(value)
    }
}

fn byte_array_stat_value(value: Vec<u8>, is_decimal: bool) -> StatValue {
    if is_decimal {
        StatValue::DecimalBytes(value)
    } else {
        StatValue::Binary(value)
    }
}

fn fixed_len_byte_array_stat_value(
    value: Vec<u8>,
    logical_type: Option<&crate::model::LogicalTypeKind>,
) -> StatValue {
    match logical_type {
        Some(crate::model::LogicalTypeKind::Decimal { .. }) => StatValue::DecimalBytes(value),
        Some(crate::model::LogicalTypeKind::Float16) => match value.as_slice() {
            [low, high] => {
                let bits = u16::from_le_bytes([*low, *high]);
                StatValue::Float16(half::f16::from_bits(bits).to_f32())
            }
            _ => StatValue::FixedLenBinary(value),
        },
        _ => StatValue::FixedLenBinary(value),
    }
}

fn merge_min(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    merge_bound(current, candidate, |ordering| ordering.is_lt());
}

fn merge_max(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    merge_bound(current, candidate, |ordering| ordering.is_gt());
}

fn merge_bound(
    current: &mut Option<StatValue>,
    candidate: Option<StatValue>,
    should_replace: impl Fn(std::cmp::Ordering) -> bool,
) {
    let Some(candidate) = candidate else {
        return;
    };

    let replace = match current.as_ref() {
        None => true,
        Some(existing) => partial_cmp_value(&candidate, existing).is_some_and(should_replace),
    };

    if replace {
        *current = Some(candidate);
    }
}

fn partial_cmp_value(left: &StatValue, right: &StatValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (StatValue::Int32(lhs), StatValue::Int32(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::UInt32(lhs), StatValue::UInt32(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Int64(lhs), StatValue::Int64(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::UInt64(lhs), StatValue::UInt64(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Float(lhs), StatValue::Float(rhs)) => Some(lhs.total_cmp(rhs)),
        (StatValue::Float16(lhs), StatValue::Float16(rhs)) => Some(lhs.total_cmp(rhs)),
        (StatValue::Double(lhs), StatValue::Double(rhs)) => Some(lhs.total_cmp(rhs)),
        (StatValue::Binary(lhs), StatValue::Binary(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Boolean(lhs), StatValue::Boolean(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::FixedLenBinary(lhs), StatValue::FixedLenBinary(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::DecimalBytes(lhs), StatValue::DecimalBytes(rhs)) => {
            Some(compare_signed_big_endian(lhs, rhs))
        }
        (StatValue::Int96(lhs), StatValue::Int96(rhs)) => lhs.partial_cmp(rhs),
        _ => None,
    }
}

fn compare_signed_big_endian(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let left = trim_sign_extension(left);
    let right = trim_sign_extension(right);
    let left_negative = left.first().is_some_and(|byte| byte & 0x80 != 0);
    let right_negative = right.first().is_some_and(|byte| byte & 0x80 != 0);

    match (left_negative, right_negative) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => left.len().cmp(&right.len()).then_with(|| left.cmp(right)),
        (true, true) => right.len().cmp(&left.len()).then_with(|| left.cmp(right)),
    }
}

fn trim_sign_extension(mut value: &[u8]) -> &[u8] {
    while let [first, second, ..] = value {
        let redundant_positive = *first == 0 && second & 0x80 == 0;
        let redundant_negative = *first == 0xff && second & 0x80 != 0;
        if !redundant_positive && !redundant_negative {
            break;
        }
        value = &value[1..];
    }
    value
}

fn display_int96(value: Int96) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_big_endian_comparison_handles_sign_and_extension() {
        assert!(compare_signed_big_endian(&[0xff, 0x38], &[0x00, 0x64]).is_lt());
        assert!(compare_signed_big_endian(&[0xff, 0x38], &[0xff, 0x9c]).is_lt());
        assert_eq!(
            compare_signed_big_endian(&[0xff, 0xff], &[0xff]),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_signed_big_endian(&[0x00, 0x01], &[0x01]),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn floating_bounds_use_ieee_total_order() {
        assert!(
            partial_cmp_value(&StatValue::Float(-0.0), &StatValue::Float(0.0))
                .is_some_and(std::cmp::Ordering::is_lt)
        );
        assert!(partial_cmp_value(
            &StatValue::Double(f64::INFINITY),
            &StatValue::Double(f64::NAN)
        )
        .is_some_and(std::cmp::Ordering::is_lt));
    }

    #[test]
    fn only_type_defined_column_orders_are_authoritative() {
        assert!(has_ordered_bounds(ColumnOrder::TYPE_DEFINED_ORDER(
            SortOrder::SIGNED
        )));
        assert!(has_ordered_bounds(ColumnOrder::TYPE_DEFINED_ORDER(
            SortOrder::UNSIGNED
        )));
        assert!(!has_ordered_bounds(ColumnOrder::TYPE_DEFINED_ORDER(
            SortOrder::UNDEFINED
        )));
        assert!(!has_ordered_bounds(ColumnOrder::UNDEFINED));
        assert!(!has_ordered_bounds(ColumnOrder::UNKNOWN));
    }
}
