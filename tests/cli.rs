//! CLI integration tests for Parqkit

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float16Array, Float64Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(unix)]
use std::process::Stdio;
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
            .set_max_row_group_row_count(Some(size))
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

#[cfg(unix)]
#[test]
fn test_closed_stdout_pipe_is_successful() -> Result<()> {
    let input_path = temp_path("broken_pipe", "parquet")?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let value = "x".repeat(256);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(
            (0..10_000).map(|_| value.as_str()),
        )) as ArrayRef],
    )?;
    write_parquet(&input_path, schema, &[batch], None)?;

    for output_format in ["table", "json", "jsonl", "csv"] {
        let mut child = parqkit()
            .args([
                "head",
                input_path.to_string_lossy().as_ref(),
                "--rows",
                "10000",
                "--output",
                output_format,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        drop(child.stdout.take());
        let output = child.wait_with_output()?;

        assert!(
            output.status.success(),
            "{output_format} pipeline failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
    }

    let input = input_path.to_string_lossy();
    let mut child = parqkit()
        .args([
            "head",
            input.as_ref(),
            input.as_ref(),
            "--rows",
            "10000",
            "--output",
            "table",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    drop(child.stdout.take());
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "multi-source table pipeline failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());

    fs::remove_file(input_path)?;
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
fn test_head_multi_file_json_ignores_schema_metadata_differences() -> Result<()> {
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
    let left = temp_path("scan_metadata_left", "parquet")?;
    let right = temp_path("scan_metadata_right", "parquet")?;
    write_parquet(&left, left_schema, &[left_batch], None)?;
    write_parquet(&right, right_schema, &[right_batch], None)?;

    let output = parqkit()
        .args([
            "head",
            &left.display().to_string(),
            &right.display().to_string(),
            "-o",
            "json",
        ])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows, serde_json::json!([{"value": 1}, {"value": 2}]));

    fs::remove_file(left)?;
    fs::remove_file(right)?;
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
    assert_eq!(
        lines.next(),
        Some("file,column,type,null_count,min,max,statistics_complete")
    );
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
fn test_convert_write_error_reports_requested_path() -> Result<()> {
    let missing_parent = temp_path("missing_output_parent", "dir")?;
    let output_path = missing_parent.join("requested.json");
    let input_path = fixture_path();
    let output_path_text = output_path.display().to_string();

    let output = parqkit()
        .args(["convert", input_path.as_str(), output_path_text.as_str()])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(&output_path_text));
    assert!(!stderr.contains(".requested.json.tmp."));
    assert!(!output_path.exists());
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
fn test_empty_scan_rejects_incompatible_schemas() -> Result<()> {
    let empty_schema = Arc::new(Schema::new(vec![Field::new(
        "empty_value",
        DataType::Int64,
        false,
    )]));
    let populated_schema = Arc::new(Schema::new(vec![Field::new(
        "populated_value",
        DataType::Utf8,
        false,
    )]));
    let populated_batch = RecordBatch::try_new(
        Arc::clone(&populated_schema),
        vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
    )?;
    let empty = temp_path("empty_schema_left", "parquet")?;
    let populated = temp_path("empty_schema_right", "parquet")?;
    write_parquet(&empty, empty_schema, &[], None)?;
    write_parquet(&populated, populated_schema, &[populated_batch], None)?;

    for command in ["head", "tail"] {
        let output = parqkit()
            .args([
                command,
                &empty.display().to_string(),
                &populated.display().to_string(),
                "-o",
                "json",
            ])
            .output()?;

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("Schema mismatch"));
    }

    fs::remove_file(empty)?;
    fs::remove_file(populated)?;
    Ok(())
}

#[test]
fn test_zero_row_scan_still_validates_parquet() -> Result<()> {
    let input = temp_path("zero_row_invalid", "parquet")?;
    fs::write(&input, b"not parquet")?;

    let output = parqkit()
        .args([
            "head",
            &input.display().to_string(),
            "-n",
            "0",
            "-o",
            "json",
        ])
        .output()?;

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn test_empty_csv_outputs_preserve_schema_headers() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let input = temp_path("empty_csv_input", "parquet")?;
    let converted = temp_path("empty_csv_output", "csv")?;
    write_parquet(&input, schema, &[], None)?;

    let head_output = parqkit()
        .args(["head", &input.display().to_string(), "-o", "csv"])
        .output()?;
    assert!(head_output.status.success());
    assert_eq!(String::from_utf8(head_output.stdout)?, "value,label\n");

    let convert_output = parqkit()
        .args([
            "convert",
            &input.display().to_string(),
            &converted.display().to_string(),
        ])
        .output()?;
    assert!(convert_output.status.success());
    assert_eq!(fs::read_to_string(&converted)?, "value,label\n");

    fs::remove_file(input)?;
    fs::remove_file(converted)?;
    Ok(())
}

#[test]
fn test_empty_json_outputs_are_valid() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let input = temp_path("empty_json_input", "parquet")?;
    write_parquet(&input, schema, &[], None)?;

    let head = parqkit()
        .args(["head", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(head.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&head.stdout)?,
        serde_json::json!([])
    );

    for (extension, expected) in [("json", "[]"), ("jsonl", "")] {
        let converted = temp_path("empty_json_output", extension)?;
        let output = parqkit()
            .args([
                "convert",
                &input.display().to_string(),
                &converted.display().to_string(),
            ])
            .output()?;
        assert!(output.status.success());
        assert_eq!(fs::read_to_string(&converted)?, expected);
        fs::remove_file(converted)?;
    }

    fs::remove_file(input)?;
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
    assert_eq!(rows[0]["statistics_complete"], serde_json::json!(true));

    let _ignored = fs::remove_file(&input_path);
    Ok(())
}

#[test]
fn test_stats_preserves_unsigned_bounds_across_row_groups() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("u32", DataType::UInt32, false),
        Field::new("u64", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(vec![u32::MAX, 0])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![u64::MAX, 0])) as ArrayRef,
        ],
    )?;
    let input = temp_path("stats_unsigned", "parquet")?;
    write_parquet(&input, schema, &[batch], Some(1))?;

    let output = parqkit()
        .args(["stats", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["min"], serde_json::json!(0));
    assert_eq!(rows[0]["max"], serde_json::json!(u32::MAX));
    assert_eq!(rows[1]["min"], serde_json::json!(0));
    assert_eq!(rows[1]["max"], serde_json::json!(u64::MAX));
    assert_eq!(rows[0]["logical_type"], serde_json::json!("UINT32"));
    assert_eq!(rows[1]["logical_type"], serde_json::json!("UINT64"));

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn test_stats_aggregates_and_renders_byte_backed_decimals() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(30, 2),
        false,
    )]));
    let values = Decimal128Array::from(vec![100, -200]).with_precision_and_scale(30, 2)?;
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])?;
    let input = temp_path("stats_decimal", "parquet")?;
    write_parquet(&input, schema, &[batch], Some(1))?;

    let output = parqkit()
        .args(["stats", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["min"], serde_json::json!("-2.00"));
    assert_eq!(rows[0]["max"], serde_json::json!("1.00"));
    assert_eq!(rows[0]["logical_type"], serde_json::json!("DECIMAL(30,2)"));

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn test_stats_aggregates_float16_by_numeric_order() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float16,
        false,
    )]));
    let values = Float16Array::from(vec![half::f16::from_f32(1.0), half::f16::from_f32(-1.0)]);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])?;
    let input = temp_path("stats_float16", "parquet")?;
    write_parquet(&input, schema, &[batch], Some(1))?;

    let output = parqkit()
        .args(["stats", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["min"], serde_json::json!(-1.0));
    assert_eq!(rows[0]["max"], serde_json::json!(1.0));
    assert_eq!(rows[0]["logical_type"], serde_json::json!("FLOAT16"));

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn test_stats_reports_missing_metadata_as_incomplete() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef],
    )?;
    let input = temp_path("stats_disabled", "parquet")?;
    let file = fs::File::create(&input)?;
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let output = parqkit()
        .args(["stats", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["null_count"], serde_json::Value::Null);
    assert_eq!(rows[0]["min"], serde_json::Value::Null);
    assert_eq!(rows[0]["max"], serde_json::Value::Null);
    assert_eq!(rows[0]["statistics_complete"], serde_json::json!(false));

    fs::remove_file(input)?;
    Ok(())
}

#[test]
fn test_stats_json_preserves_non_finite_bounds() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Float64Array::from(vec![
            f64::NEG_INFINITY,
            0.0,
            f64::INFINITY,
        ])) as ArrayRef],
    )?;
    let input = temp_path("stats_non_finite", "parquet")?;
    write_parquet(&input, schema, &[batch], None)?;

    let output = parqkit()
        .args(["stats", &input.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(rows[0]["min"], serde_json::json!("-Infinity"));
    assert_eq!(rows[0]["max"], serde_json::json!("Infinity"));
    assert_eq!(rows[0]["statistics_complete"], serde_json::json!(true));

    fs::remove_file(input)?;
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
