//! CLI integration tests for Parqkit

use anyhow::Result;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn parqkit() -> Command {
    Command::new(env!("CARGO_BIN_EXE_parqkit"))
}

fn fixture_path() -> String {
    format!("{}/tests/fixtures/test.parquet", env!("CARGO_MANIFEST_DIR"))
}

fn temp_path(name: &str, extension: &str) -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(std::env::temp_dir().join(format!("parqkit_{name}_{unique}_{counter}.{extension}")))
}

fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    max_row_group_size: Option<usize>,
) -> Result<()> {
    let file = fs::File::create(path)?;
    let props = max_row_group_size.map(|size| {
        WriterProperties::builder()
            .set_max_row_group_size(size)
            .build()
    });
    let mut writer = ArrowWriter::try_new(file, schema, props)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

fn assert_no_source_headers(output: &[u8]) {
    let stdout = String::from_utf8_lossy(output);
    assert!(!stdout.contains("==>"));
}

#[test]
fn test_help() -> Result<()> {
    let output = parqkit().arg("--help").output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("parqkit"));
    assert!(stdout.contains("schema"));
    assert!(stdout.contains("head"));
    assert!(stdout.contains("stats"));
    Ok(())
}

#[test]
fn test_version() -> Result<()> {
    let output = parqkit().arg("--version").output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("parqkit"));
    Ok(())
}

#[test]
fn test_schema() -> Result<()> {
    let output = parqkit().args(["schema", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Type"));
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
    Ok(())
}

#[test]
fn test_schema_json() -> Result<()> {
    let output = parqkit()
        .args(["schema", &fixture_path(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["name"], serde_json::json!("id"));
    assert_eq!(rows[0]["type"], serde_json::json!("INT64"));
    assert_eq!(rows[0]["physical_type"], serde_json::json!("INT64"));
    assert!(rows[0].get("file").is_none());
    Ok(())
}

#[test]
fn test_schema_multi_file_json_is_parseable() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["schema", &file, &file, "-o", "json"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("schema json output should be an array"))?;
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[0]["file"], serde_json::json!(file));
    assert_eq!(rows[4]["file"], serde_json::json!(file));
    Ok(())
}

#[test]
fn test_schema_multi_file_csv_includes_source_file() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["schema", &file, &file, "-o", "csv"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    assert_eq!(lines.next(), Some("file,column,type,nullable"));
    let first_row = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("schema csv should contain rows"))?;
    assert!(first_row.starts_with(&format!("{file},id,")));
    Ok(())
}

#[test]
fn test_head() -> Result<()> {
    let output = parqkit().args(["head", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
    Ok(())
}

#[test]
fn test_head_with_limit() -> Result<()> {
    let output = parqkit()
        .args(["head", &fixture_path(), "-n", "2"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
    Ok(())
}

#[test]
fn test_head_json() -> Result<()> {
    let output = parqkit()
        .args(["head", &fixture_path(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with('['));
    assert!(stdout.contains("\"name\""));
    Ok(())
}

#[test]
fn test_head_multi_file_json_is_parseable() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["head", &file, &file, "-o", "json"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("head json output should be an array"))?;
    assert_eq!(rows.len(), 10);
    Ok(())
}

#[test]
fn test_tail() -> Result<()> {
    let output = parqkit()
        .args(["tail", &fixture_path(), "-n", "2"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Diana") || stdout.contains("Eve"));
    Ok(())
}

#[test]
fn test_count() -> Result<()> {
    let output = parqkit().args(["count", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "5");
    Ok(())
}

#[test]
fn test_stats() -> Result<()> {
    let output = parqkit().args(["stats", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Min"));
    assert!(stdout.contains("Max"));
    assert!(stdout.contains("id"));
    Ok(())
}

#[test]
fn test_stats_multi_file_json_is_parseable() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["stats", &file, &file, "-o", "json"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("stats json output should be an array"))?;
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[0]["file"], serde_json::json!(file));
    assert_eq!(rows[4]["file"], serde_json::json!(file));
    Ok(())
}

#[test]
fn test_stats_multi_file_csv_includes_source_file() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["stats", &file, &file, "-o", "csv"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    assert_eq!(lines.next(), Some("file,column,type,null_count,min,max"));
    let first_row = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("stats csv should contain rows"))?;
    assert!(first_row.starts_with(&format!("{file},id,")));
    Ok(())
}

#[test]
fn test_multi_file_jsonl_outputs_only_json_lines() -> Result<()> {
    let file = fixture_path();

    for command in ["schema", "stats", "head"] {
        let output = parqkit()
            .args([command, &file, &file, "-o", "jsonl"])
            .output()?;
        assert!(output.status.success());
        assert_no_source_headers(&output.stdout);

        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines = stdout.lines().collect::<Vec<_>>();
        assert!(!lines.is_empty());
        for line in lines {
            let value: serde_json::Value = serde_json::from_str(line)?;
            assert!(value.is_object());
            if command != "head" {
                assert_eq!(value["file"], serde_json::json!(file));
            }
        }
    }

    Ok(())
}

#[test]
fn test_multi_file_table_output_keeps_source_headers() -> Result<()> {
    let file = fixture_path();
    let output = parqkit().args(["schema", &file, &file]).output()?;
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.matches("==>").count(), 2);
    Ok(())
}

#[test]
fn test_info() -> Result<()> {
    let output = parqkit().args(["info", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Rows"));
    assert!(stdout.contains("Columns"));
    assert!(stdout.contains("Compression"));
    Ok(())
}

#[test]
fn test_info_multi_file_json_is_parseable() -> Result<()> {
    let file = fixture_path();
    let output = parqkit()
        .args(["info", &file, &file, "-o", "json"])
        .output()?;
    assert!(output.status.success());
    assert_no_source_headers(&output.stdout);

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("info json output should be an array"))?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["file"], serde_json::json!(file));
    assert_eq!(rows[1]["file"], serde_json::json!(file));
    Ok(())
}

#[test]
fn test_convert_csv() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("parqkit_test_output.csv");

    let output = parqkit()
        .args([
            "convert",
            &fixture_path(),
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = fs::read_to_string(&output_path)?;
    assert!(contents.contains("id,name,amount,active"));
    assert!(contents.contains("Alice"));

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_convert_json() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("parqkit_test_output.json");

    let output = parqkit()
        .args([
            "convert",
            &fixture_path(),
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = fs::read_to_string(&output_path)?;
    assert!(contents.starts_with('['));
    assert!(contents.contains("\"name\""));

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_convert_invalid_input_preserves_existing_output() -> Result<()> {
    let input_path = temp_path("invalid_convert_input", "parquet")?;
    let output_path = temp_path("invalid_convert_output", "csv")?;
    fs::write(&input_path, b"not parquet")?;
    fs::write(&output_path, b"sentinel")?;

    let output = parqkit()
        .args([
            "convert",
            &input_path.display().to_string(),
            &output_path.display().to_string(),
        ])
        .output()?;

    assert!(!output.status.success());
    assert_eq!(fs::read(&output_path)?, b"sentinel");

    fs::remove_file(input_path)?;
    fs::remove_file(output_path)?;
    Ok(())
}

#[test]
fn test_convert_unsupported_format_preserves_existing_output() -> Result<()> {
    let output_path = temp_path("unsupported_convert_output", "unsupported")?;
    fs::write(&output_path, b"sentinel")?;

    let output = parqkit()
        .args([
            "convert",
            &fixture_path(),
            &output_path.display().to_string(),
        ])
        .output()?;

    assert!(!output.status.success());
    assert_eq!(fs::read(&output_path)?, b"sentinel");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Unsupported format"));

    fs::remove_file(output_path)?;
    Ok(())
}

#[test]
fn test_merge() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("parqkit_test_merged.parquet");

    let output = parqkit()
        .args([
            "merge",
            &fixture_path(),
            &fixture_path(),
            "-o",
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let count_output = parqkit()
        .args(["count", &output_path.display().to_string()])
        .output()?;
    let stdout = String::from_utf8_lossy(&count_output.stdout);
    assert_eq!(stdout.trim(), "10");

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_convert_json_preserves_types() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("0012"),
                Some(""),
                Some("true"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(12), None, Some(-7), Some(0)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])) as ArrayRef,
        ],
    )?;

    let input_path = temp_path("typed_input", "parquet")?;
    let json_path = temp_path("typed_output", "json")?;
    let jsonl_path = temp_path("typed_output", "jsonl")?;
    write_parquet(&input_path, schema, &[batch], None)?;

    let json_output = parqkit()
        .args([
            "convert",
            &input_path.display().to_string(),
            &json_path.display().to_string(),
        ])
        .output()?;
    assert!(json_output.status.success());

    let rows: serde_json::Value = serde_json::from_str(&fs::read_to_string(&json_path)?)?;
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("json output should be an array"))?;
    assert_eq!(
        rows[0]["code"],
        serde_json::Value::String("0012".to_string())
    );
    assert_eq!(rows[1]["code"], serde_json::Value::String(String::new()));
    assert_eq!(
        rows[2]["code"],
        serde_json::Value::String("true".to_string())
    );
    assert_eq!(rows[0]["qty"], serde_json::json!(12));
    assert_eq!(rows[1]["qty"], serde_json::Value::Null);
    assert_eq!(rows[0]["flag"], serde_json::json!(true));
    assert_eq!(rows[2]["flag"], serde_json::Value::Null);

    let jsonl_output = parqkit()
        .args([
            "convert",
            &input_path.display().to_string(),
            &jsonl_path.display().to_string(),
        ])
        .output()?;
    assert!(jsonl_output.status.success());

    let lines: Vec<serde_json::Value> = fs::read_to_string(&jsonl_path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(lines[1]["code"], serde_json::Value::String(String::new()));
    assert_eq!(lines[1]["qty"], serde_json::Value::Null);
    assert_eq!(
        lines[2]["code"],
        serde_json::Value::String("true".to_string())
    );
    assert_eq!(lines[2]["flag"], serde_json::Value::Null);

    let _ignored = fs::remove_file(&input_path);
    let _ignored = fs::remove_file(&json_path);
    let _ignored = fs::remove_file(&jsonl_path);
    Ok(())
}

#[test]
fn test_head_multi_file_json_rejects_incompatible_schemas() -> Result<()> {
    assert_scan_multi_file_json_rejects_incompatible_schemas("head")
}

#[test]
fn test_tail_multi_file_json_rejects_incompatible_schemas() -> Result<()> {
    assert_scan_multi_file_json_rejects_incompatible_schemas("tail")
}

fn assert_scan_multi_file_json_rejects_incompatible_schemas(command: &str) -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let right_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Utf8,
        false,
    )]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
    )?;
    let left = temp_path("head_schema_left", "parquet")?;
    let right = temp_path("head_schema_right", "parquet")?;
    write_parquet(&left, left_schema, &[left_batch], None)?;
    write_parquet(&right, right_schema, &[right_batch], None)?;

    let output = parqkit()
        .args([
            command,
            &left.display().to_string(),
            &right.display().to_string(),
            "-o",
            "json",
        ])
        .output()?;

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Schema mismatch"));

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    Ok(())
}

#[test]
fn test_convert_rejects_multi_match_glob() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let first = temp_path("convert_glob_pair", "parquet")?;
    let stem = first
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("temp path should have utf-8 stem"))?;
    let second = first.with_file_name(format!("{stem}_second.parquet"));
    let output_path = temp_path("convert_glob_output", "csv")?;
    write_parquet(
        &first,
        Arc::clone(&schema),
        std::slice::from_ref(&batch),
        None,
    )?;
    write_parquet(&second, schema, &[batch], None)?;
    let glob = first.with_file_name(format!("{stem}*.parquet"));

    let output = parqkit()
        .args([
            "convert",
            &glob.display().to_string(),
            &output_path.display().to_string(),
        ])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Expected exactly one input file"));
    assert!(!output_path.exists());

    fs::remove_file(first)?;
    fs::remove_file(second)?;
    Ok(())
}

#[test]
fn test_stats_aggregates_across_row_groups() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![5, 10, 1, 3])) as ArrayRef],
    )?;
    let input_path = temp_path("stats_groups", "parquet")?;
    write_parquet(&input_path, schema, &[batch], Some(2))?;

    let output = parqkit()
        .args(["stats", &input_path.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        rows[0]["column"],
        serde_json::Value::String("value".to_string())
    );
    assert_eq!(rows[0]["min"], serde_json::json!(1));
    assert_eq!(rows[0]["max"], serde_json::json!(10));

    let _ignored = fs::remove_file(&input_path);
    Ok(())
}

#[test]
fn test_schema_jsonl_outputs_one_object_per_line() -> Result<()> {
    let output = parqkit()
        .args(["schema", &fixture_path(), "-o", "jsonl"])
        .output()?;
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<_> = stdout.lines().collect();
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|line| line.starts_with('{')));
    assert!(lines.iter().all(|line| !line.starts_with('[')));

    Ok(())
}
