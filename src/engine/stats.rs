use crate::model::{ColumnStats, ColumnType, StatValue};
use crate::ParqkitError;
use crate::Result;
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
            }
        })
        .collect();

    for row_group_index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(row_group_index);
        let row_group_rows = u64::try_from(row_group.num_rows()).map_err(|_| {
            ParqkitError::invalid_metadata(
                path,
                format!("row group {row_group_index} has a negative row count"),
            )
        })?;
        for (column_index, stats) in column_stats
            .iter_mut()
            .enumerate()
            .take(row_group.num_columns())
        {
            if let Some(column_statistics) = row_group.column(column_index).statistics() {
                stats.add_null_count(path, column_statistics.null_count_opt())?;

                if column_statistics.min_bytes_opt().is_some()
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

struct AccumulatedColumnStats {
    column: String,
    column_type: ColumnType,
    null_count: Option<u64>,
    min: Option<StatValue>,
    max: Option<StatValue>,
    bounds_complete: bool,
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
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Int32),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Int32),
            );
        }
        Statistics::Int64(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Int64),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Int64),
            );
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
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .map(|value| StatValue::Binary(value.data().to_vec())),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .map(|value| StatValue::Binary(value.data().to_vec())),
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
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .map(|value| StatValue::FixedLenBinary(value.data().to_vec())),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .map(|value| StatValue::FixedLenBinary(value.data().to_vec())),
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
        (StatValue::Int64(lhs), StatValue::Int64(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Float(lhs), StatValue::Float(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Double(lhs), StatValue::Double(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Binary(lhs), StatValue::Binary(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Boolean(lhs), StatValue::Boolean(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::FixedLenBinary(lhs), StatValue::FixedLenBinary(rhs)) => lhs.partial_cmp(rhs),
        (StatValue::Int96(lhs), StatValue::Int96(rhs)) => lhs.partial_cmp(rhs),
        _ => None,
    }
}

fn display_int96(value: Int96) -> String {
    format!("{value:?}")
}
