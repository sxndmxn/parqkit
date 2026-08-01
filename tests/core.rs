//! Public library and foundation contract tests.

use anyhow::Result;
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("test.parquet")
}

fn temp_path(name: &str, extension: &str) -> Result<std::path::PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(std::env::temp_dir().join(format!("parqkit_{name}_{unique}_{counter}.{extension}")))
}

fn temp_dir(name: &str) -> Result<std::path::PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("parqkit_{name}_{unique}_{counter}"));
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn write_parquet(
    path: &std::path::Path,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[test]
fn empty_dataset_input_is_typed_error() -> Result<()> {
    let Err(error) = parqkit::dataset_from_inputs(Vec::new()) else {
        return Err(anyhow::anyhow!("empty dataset input should fail"));
    };

    assert!(matches!(error, parqkit::ParqkitError::NoInputFiles));
    Ok(())
}

#[test]
fn dataset_glob_expansion_is_sorted() -> Result<()> {
    let dir = temp_dir("dataset_glob_order")?;
    let first = dir.join("a.parquet");
    let second = dir.join("b.parquet");
    fs::write(&second, b"PAR1")?;
    fs::write(&first, b"PAR1")?;

    let dataset = parqkit::dataset_from_inputs(vec![dir.join("*.parquet")])?;
    let paths = dataset.paths().collect::<Vec<_>>();

    assert_eq!(paths, vec![first.as_path(), second.as_path()]);

    fs::remove_file(first)?;
    fs::remove_file(second)?;
    fs::remove_dir(dir)?;
    Ok(())
}

#[test]
fn repeated_explicit_dataset_inputs_are_preserved() -> Result<()> {
    let dir = temp_dir("dataset_explicit_repeats")?;
    let file = dir.join("sample.parquet");
    fs::write(&file, b"PAR1")?;

    let dataset = parqkit::dataset_from_inputs(vec![file.clone(), file.clone()])?;
    let paths = dataset.paths().collect::<Vec<_>>();

    assert_eq!(paths, vec![file.as_path(), file.as_path()]);

    fs::remove_file(file)?;
    fs::remove_dir(dir)?;
    Ok(())
}

#[test]
fn existing_paths_with_glob_metacharacters_are_literal() -> Result<()> {
    let dir = temp_dir("dataset_literal_metacharacters")?;
    let file = dir.join("sample[1].parquet");
    fs::write(&file, b"PAR1")?;

    let dataset = parqkit::dataset_from_inputs(vec![file.clone()])?;
    assert_eq!(dataset.paths().collect::<Vec<_>>(), vec![file.as_path()]);

    fs::remove_file(file)?;
    fs::remove_dir(dir)?;
    Ok(())
}

#[test]
fn dataset_glob_matches_are_deduplicated_against_explicit_inputs() -> Result<()> {
    let dir = temp_dir("dataset_glob_dedup")?;
    let file = dir.join("sample.parquet");
    fs::write(&file, b"PAR1")?;
    let glob = dir.join("*.parquet");

    let dataset = parqkit::dataset_from_inputs(vec![file.clone(), glob])?;
    let paths = dataset.paths().collect::<Vec<_>>();

    assert_eq!(paths, vec![file.as_path()]);

    fs::remove_file(file)?;
    fs::remove_dir(dir)?;
    Ok(())
}

#[test]
fn dataset_glob_without_matches_is_typed_error() -> Result<()> {
    let dir = temp_dir("dataset_empty_glob")?;
    let glob = dir.join("*.parquet");

    let Err(error) = parqkit::dataset_from_inputs(vec![glob]) else {
        fs::remove_dir(dir)?;
        return Err(anyhow::anyhow!("empty glob should fail"));
    };

    assert!(matches!(
        error,
        parqkit::ParqkitError::NoFilesMatched { .. }
    ));

    fs::remove_dir(dir)?;
    Ok(())
}

#[test]
fn file_info_comes_from_public_api() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let infos = parqkit::info(&dataset)?;
    let info = &infos[0];

    assert_eq!(info.num_rows, 5);
    assert_eq!(info.num_columns, 4);
    assert_eq!(info.num_row_groups, 1);
    assert_eq!(
        info.compression,
        parqkit::CompressionSummary::Single(parqkit::CompressionCodec::Snappy)
    );
    assert!(info
        .path
        .display()
        .to_string()
        .ends_with("tests/fixtures/test.parquet"));

    Ok(())
}

#[test]
fn legacy_compression_fixtures_remain_readable() -> Result<()> {
    let cases = [
        (
            "edge-none.parquet",
            40,
            8,
            parqkit::CompressionCodec::Uncompressed,
        ),
        (
            "mixed-snappy.parquet",
            64,
            8,
            parqkit::CompressionCodec::Snappy,
        ),
        (
            "sparse-gzip.parquet",
            96,
            6,
            parqkit::CompressionCodec::Gzip,
        ),
        (
            "unicode-zstd.parquet",
            48,
            4,
            parqkit::CompressionCodec::Zstd,
        ),
    ];

    for (name, rows, columns, compression) in cases {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/volatile")
            .join(name);
        let dataset = parqkit::dataset_from_inputs(vec![path])?;
        let info = &parqkit::info(&dataset)?[0];
        assert_eq!(info.num_rows, rows);
        assert_eq!(info.num_columns, columns);
        assert_eq!(
            info.compression,
            parqkit::CompressionSummary::Single(compression)
        );
        assert_eq!(parqkit::count(&dataset)?.total_rows, rows);
        assert_eq!(parqkit::schema(&dataset)?[0].columns.len(), columns);
        assert_eq!(parqkit::stats(&dataset, None)?[0].rows.len(), columns);

        let scan = parqkit::scan(
            &dataset,
            parqkit::ScanKind::Head,
            parqkit::ScanOptions { rows: 1 },
        )?;
        assert_eq!(scan[0].batches[0].num_rows(), 1);
    }

    Ok(())
}

#[test]
fn column_stats_come_from_public_api() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let results = parqkit::stats(&dataset, Some("id"))?;
    let rows = &results[0].rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column, "id");
    assert_eq!(
        rows[0].min.as_ref().map(ToString::to_string).as_deref(),
        Some("1")
    );
    assert_eq!(
        rows[0].max.as_ref().map(ToString::to_string).as_deref(),
        Some("5")
    );
    assert_eq!(rows[0].null_count, Some(0));
    assert!(rows[0].statistics_complete);

    Ok(())
}

#[test]
fn missing_stats_column_is_typed_error() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let Err(error) = parqkit::stats(&dataset, Some("missing_column")) else {
        return Err(anyhow::anyhow!("missing stats column should fail"));
    };

    assert!(matches!(
        error,
        parqkit::ParqkitError::ColumnNotFound { .. }
    ));

    Ok(())
}

#[test]
fn binary_stats_display_is_deterministic() {
    let binary_stats = parqkit::ColumnStats {
        column: "payload".to_string(),
        column_type: parqkit::ColumnType {
            physical: parqkit::PhysicalType::ByteArray,
            logical: None,
        },
        null_count: Some(0),
        min: None,
        max: None,
        statistics_complete: true,
    };
    let string_stats = parqkit::ColumnStats {
        column: "name".to_string(),
        column_type: parqkit::ColumnType {
            physical: parqkit::PhysicalType::ByteArray,
            logical: Some(parqkit::LogicalTypeKind::String),
        },
        null_count: Some(0),
        min: None,
        max: None,
        statistics_complete: true,
    };

    assert_eq!(
        binary_stats.display_stat_value(&parqkit::StatValue::Binary(vec![0xff, b'a'])),
        "ff61"
    );
    assert_eq!(
        string_stats.display_stat_value(&parqkit::StatValue::Binary(b"Alice".to_vec())),
        "Alice"
    );
}

#[test]
fn library_api_exposes_typed_schema_results() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let schema = parqkit::schema(&dataset)?;

    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].columns[0].name, "id");
    assert_eq!(
        schema[0].columns[0].column_type.physical,
        parqkit::PhysicalType::Int64
    );

    Ok(())
}

#[test]
fn streaming_query_pushes_down_projection_limit_and_batch_size() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let options = parqkit::QueryOptions {
        projection: parqkit::Projection::Columns(vec!["name".to_string(), "id".to_string()]),
        limit: Some(3),
        batch_size: 2,
    };
    let mut stream = parqkit::execute_query(&dataset, options)?;

    assert_eq!(stream.sources().len(), 1);
    assert_eq!(
        stream.sources()[0]
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["name", "id"]
    );
    assert_eq!(stream.compatible_schema()?.fields().len(), 2);

    let batches = stream.by_ref().collect::<parqkit::Result<Vec<_>>>()?;
    assert_eq!(
        batches
            .iter()
            .map(|result| result.batch.num_rows())
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(batches.iter().all(|result| result.source_index == 0));
    let names = batches[0].batch.column(0);
    let names = names
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("name projection should be a string array"))?;
    let ids = batches[0].batch.column(1);
    let ids = ids
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("id projection should be an int64 array"))?;
    assert_eq!(names.value(0), "Alice");
    assert_eq!(ids.value(0), 1);
    assert!(stream.next().is_none());

    Ok(())
}

#[test]
fn streaming_query_normalizes_file_schema_metadata() -> Result<()> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("field_owner".to_string(), "parqkit".to_string())]),
        )],
        std::collections::HashMap::from([("owner".to_string(), "parqkit".to_string())]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )?;
    let input = temp_path("query_schema_metadata", "parquet")?;
    write_parquet(&input, schema, &[batch])?;
    let dataset = parqkit::dataset_from_inputs(vec![input.clone()])?;

    for kind in [parqkit::ScanKind::Head, parqkit::ScanKind::Tail] {
        let results = parqkit::scan(&dataset, kind, parqkit::ScanOptions { rows: 1 })?;
        assert!(results[0].schema.metadata().is_empty());
        assert!(results[0].schema.field(0).metadata().is_empty());
        assert_eq!(results[0].batches[0].schema(), results[0].schema);
    }

    for projection in [
        parqkit::Projection::All,
        parqkit::Projection::Columns(vec!["value".to_string()]),
    ] {
        let options = parqkit::QueryOptions {
            projection,
            ..parqkit::QueryOptions::default()
        };
        let mut stream = parqkit::execute_query(&dataset, options)?;

        assert!(stream.sources()[0].schema.metadata().is_empty());
        assert!(stream.sources()[0].schema.field(0).metadata().is_empty());
        let result = stream
            .next()
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("query should produce one batch"))?;
        assert_eq!(result.batch.num_rows(), 2);
        assert!(result.batch.schema().metadata().is_empty());
        assert!(result.batch.schema().field(0).metadata().is_empty());
    }

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn streaming_query_preserves_repeated_sources_and_per_source_limits() -> Result<()> {
    let fixture = fixture_path();
    let dataset = parqkit::dataset_from_inputs(vec![fixture.clone(), fixture])?;
    let options = parqkit::QueryOptions {
        limit: Some(1),
        ..parqkit::QueryOptions::default()
    };
    let stream = parqkit::execute_query(&dataset, options)?;

    assert_eq!(stream.sources().len(), 2);
    let batches = stream.collect::<parqkit::Result<Vec<_>>>()?;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].source_index, 0);
    assert_eq!(batches[1].source_index, 1);
    assert_eq!(batches[0].batch.num_rows(), 1);
    assert_eq!(batches[1].batch.num_rows(), 1);

    Ok(())
}

#[test]
fn streaming_query_retains_projected_schema_for_empty_sources() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("first", DataType::Int64, false),
        Field::new("second", DataType::Int64, true),
    ]));
    let input = temp_path("empty_query", "parquet")?;
    write_parquet(&input, schema, &[])?;
    let dataset = parqkit::dataset_from_inputs(vec![input.clone()])?;
    let options = parqkit::QueryOptions {
        projection: parqkit::Projection::Columns(vec!["second".to_string()]),
        ..parqkit::QueryOptions::default()
    };
    let mut stream = parqkit::execute_query(&dataset, options)?;

    assert_eq!(stream.sources()[0].schema.fields().len(), 1);
    assert_eq!(stream.sources()[0].schema.field(0).name(), "second");
    assert!(stream.next().is_none());

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn streaming_query_validates_options_and_columns_during_planning() -> Result<()> {
    let dataset = parqkit::dataset_from_inputs(vec![fixture_path()])?;
    let invalid_batch_size = parqkit::QueryOptions {
        batch_size: 0,
        ..parqkit::QueryOptions::default()
    };
    let Err(error) = parqkit::execute_query(&dataset, invalid_batch_size) else {
        return Err(anyhow::anyhow!("zero query batch size should fail"));
    };
    assert!(matches!(error, parqkit::ParqkitError::InvalidQuery { .. }));

    let missing_column = parqkit::QueryOptions {
        projection: parqkit::Projection::Columns(vec!["missing".to_string()]),
        ..parqkit::QueryOptions::default()
    };
    let Err(error) = parqkit::execute_query(&dataset, missing_column) else {
        return Err(anyhow::anyhow!("missing projected column should fail"));
    };
    assert!(matches!(
        error,
        parqkit::ParqkitError::ColumnNotFound { .. }
    ));

    let duplicate_column = parqkit::QueryOptions {
        projection: parqkit::Projection::Columns(vec!["id".to_string(), "id".to_string()]),
        ..parqkit::QueryOptions::default()
    };
    let Err(error) = parqkit::execute_query(&dataset, duplicate_column) else {
        return Err(anyhow::anyhow!("duplicate projected column should fail"));
    };
    assert!(matches!(error, parqkit::ParqkitError::InvalidQuery { .. }));

    Ok(())
}

#[test]
fn streaming_query_reports_incompatible_projected_schemas_before_reading() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let right_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(StringArray::from(vec!["one"])) as ArrayRef],
    )?;
    let left = temp_path("query_schema_left", "parquet")?;
    let right = temp_path("query_schema_right", "parquet")?;
    write_parquet(&left, left_schema, &[left_batch])?;
    write_parquet(&right, right_schema, &[right_batch])?;
    let dataset = parqkit::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    let mut stream = parqkit::execute_query(&dataset, parqkit::QueryOptions::default())?;

    assert!(matches!(
        stream.compatible_schema(),
        Err(parqkit::ParqkitError::SchemaMismatch { .. })
    ));
    assert_eq!(
        stream.next().transpose()?.map(|batch| batch.source_index),
        Some(0)
    );

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    Ok(())
}

#[test]
fn streaming_query_defers_data_access_and_fuses_after_an_error() -> Result<()> {
    let input = temp_path("lazy_query", "parquet")?;
    fs::copy(fixture_path(), &input)?;
    let dataset = parqkit::dataset_from_inputs(vec![input.clone()])?;
    let mut stream = parqkit::execute_query(&dataset, parqkit::QueryOptions::default())?;
    assert_eq!(stream.sources().len(), 1);

    fs::remove_file(input)?;
    assert!(matches!(
        stream.next(),
        Some(Err(parqkit::ParqkitError::FileNotFound { .. }))
    ));
    assert!(stream.next().is_none());

    Ok(())
}

#[test]
fn nested_columns_use_full_paths_and_effective_nullability() -> Result<()> {
    let child_fields = Fields::from(vec![Arc::new(Field::new("child", DataType::Int64, false))]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "parent",
        DataType::Struct(child_fields.clone()),
        true,
    )]));
    let values = StructArray::new(
        child_fields,
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        None,
    );
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])?;
    let input = temp_path("nested_columns", "parquet")?;
    write_parquet(&input, schema, &[batch])?;

    let dataset = parqkit::dataset_from_inputs(vec![input.clone()])?;
    let schema_results = parqkit::schema(&dataset)?;
    assert_eq!(schema_results[0].columns[0].name, "parent.child");
    assert!(schema_results[0].columns[0].nullable);

    let stats_results = parqkit::stats(&dataset, Some("parent.child"))?;
    assert_eq!(stats_results[0].rows[0].column, "parent.child");

    let query_options = parqkit::QueryOptions {
        projection: parqkit::Projection::Columns(vec!["parent".to_string()]),
        ..parqkit::QueryOptions::default()
    };
    let query = parqkit::execute_query(&dataset, query_options)?;
    assert_eq!(query.sources()[0].schema.field(0).name(), "parent");
    assert_eq!(
        query
            .collect::<parqkit::Result<Vec<_>>>()?
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn merge_comes_from_public_api() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )?;
    let left = temp_path("merge_left", "parquet")?;
    let right = temp_path("merge_right", "parquet")?;
    let output = temp_path("merge_output", "parquet")?;

    write_parquet(&left, Arc::clone(&schema), std::slice::from_ref(&batch))?;
    write_parquet(&right, schema, &[batch])?;

    let merge_dataset = parqkit::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    parqkit::merge(&merge_dataset, &output)?;

    let output_dataset = parqkit::dataset_from_inputs(vec![output.clone()])?;
    let count = parqkit::count(&output_dataset)?;
    assert_eq!(count.total_rows, 4);

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}

#[test]
fn merge_ignores_schema_metadata_differences() -> Result<()> {
    let left_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("field_owner".to_string(), "left".to_string())]),
        )],
        std::collections::HashMap::from([("producer".to_string(), "left".to_string())]),
    ));
    let right_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("field_owner".to_string(), "right".to_string())]),
        )],
        std::collections::HashMap::from([("producer".to_string(), "right".to_string())]),
    ));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
    )?;
    let left = temp_path("merge_metadata_left", "parquet")?;
    let right = temp_path("merge_metadata_right", "parquet")?;
    let output = temp_path("merge_metadata_output", "parquet")?;
    write_parquet(&left, left_schema, &[left_batch])?;
    write_parquet(&right, right_schema, &[right_batch])?;

    let dataset = parqkit::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    parqkit::merge(&dataset, &output)?;
    let merged = parqkit::dataset_from_inputs(vec![output.clone()])?;
    assert_eq!(parqkit::count(&merged)?.total_rows, 2);

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}

#[test]
fn merge_rejects_different_arrow_extension_types() -> Result<()> {
    let extension_field = |extension_name: &str| {
        Field::new("value", DataType::Int64, false).with_metadata(std::collections::HashMap::from(
            [(
                "ARROW:extension:name".to_string(),
                extension_name.to_string(),
            )],
        ))
    };
    let left_schema = Arc::new(Schema::new(vec![extension_field("parqkit.left")]));
    let right_schema = Arc::new(Schema::new(vec![extension_field("parqkit.right")]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
    )?;
    let left = temp_path("merge_extension_left", "parquet")?;
    let right = temp_path("merge_extension_right", "parquet")?;
    let output = temp_path("merge_extension_output", "parquet")?;
    write_parquet(&left, left_schema, &[left_batch])?;
    write_parquet(&right, right_schema, &[right_batch])?;
    fs::write(&output, b"sentinel")?;

    let dataset = parqkit::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    assert!(matches!(
        parqkit::merge(&dataset, &output),
        Err(parqkit::ParqkitError::SchemaMismatch { .. })
    ));
    assert_eq!(fs::read(&output)?, b"sentinel");

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}

#[test]
fn merge_schema_mismatch_does_not_truncate_existing_output() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let right_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        false,
    )]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
    )?;
    let left = temp_path("mismatch_left", "parquet")?;
    let right = temp_path("mismatch_right", "parquet")?;
    let output = temp_path("mismatch_output", "parquet")?;

    write_parquet(&left, left_schema, &[left_batch])?;
    write_parquet(&right, right_schema, &[right_batch])?;
    fs::write(&output, b"sentinel")?;

    let dataset = parqkit::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    let Err(error) = parqkit::merge(&dataset, &output) else {
        fs::remove_file(left)?;
        fs::remove_file(right)?;
        fs::remove_file(output)?;
        return Err(anyhow::anyhow!("schema mismatch should fail"));
    };

    assert!(matches!(
        error,
        parqkit::ParqkitError::SchemaMismatch { .. }
    ));
    assert_eq!(fs::read(&output)?, b"sentinel");

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}
