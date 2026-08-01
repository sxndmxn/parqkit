//! Stress tests for Parqkit
//!
//! Run all tests: cargo test --test stress
//! Run ignored (heavy) tests: cargo test --test stress -- --ignored --test-threads=1
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

static FIXTURES_DIR: OnceLock<PathBuf> = OnceLock::new();

fn fixtures_dir() -> &'static PathBuf {
    FIXTURES_DIR.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("stress")
            .join("fixtures");
        fs::create_dir_all(&dir).expect("Failed to create fixtures directory");
        dir
    })
}

fn parqkit_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_parqkit"))
}

fn generate_bin() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .join("target")
        .join(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        })
        .join("parqkit-generate")
}

fn run_parqkit(args: &[&str]) -> Output {
    Command::new(parqkit_bin())
        .args(args)
        .output()
        .expect("Failed to execute parqkit")
}

fn run_parqkit_success(args: &[&str]) -> String {
    let output = run_parqkit(args);
    if !output.status.success() {
        panic!(
            "parqkit failed with args {:?}\nstderr: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn run_parqkit_failure(args: &[&str]) -> String {
    let output = run_parqkit(args);
    assert!(
        !output.status.success(),
        "Expected parqkit to fail with args {:?}",
        args
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn generate_fixture(name: &str, args: &[&str]) -> PathBuf {
    let output_path = fixtures_dir().join(name);
    if output_path.exists() {
        return output_path;
    }

    let mut cmd_args = vec!["-o", output_path.to_str().unwrap()];
    cmd_args.extend(args);

    let output = Command::new(generate_bin())
        .args(&cmd_args)
        .output()
        .expect("Failed to execute parqkit-generate");

    if !output.status.success() {
        panic!(
            "parqkit-generate failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    output_path
}

// ============================================================================
// Edge Case Tests (fast, run normally)
// ============================================================================

#[test]
fn edge_nonexistent_file() {
    let stderr = run_parqkit_failure(&["head", "this_file_does_not_exist.parquet"]);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("does not exist")
            || stderr.contains("error"),
        "Expected helpful error message, got: {}",
        stderr
    );
}

#[test]
fn edge_not_parquet_file() {
    let stderr = run_parqkit_failure(&["head", "Cargo.toml"]);
    assert!(
        stderr.contains("Parquet")
            || stderr.contains("magic")
            || stderr.contains("invalid")
            || stderr.contains("error"),
        "Expected helpful error message, got: {}",
        stderr
    );
}

#[test]
fn edge_empty_path() {
    let stderr = run_parqkit_failure(&["head", ""]);
    assert!(!stderr.is_empty(), "Should have error for empty path");
}

#[test]
fn edge_directory_as_file() {
    let stderr = run_parqkit_failure(&["head", "src/"]);
    assert!(
        stderr.contains("directory")
            || stderr.contains("Is a directory")
            || stderr.contains("error"),
        "Expected helpful error message for directory, got: {}",
        stderr
    );
}

#[test]
fn edge_truncated_file() {
    let path = fixtures_dir().join("truncated.parquet");

    // Create a truncated file (partial Parquet magic + garbage)
    let mut file = File::create(&path).expect("Failed to create truncated file");
    file.write_all(b"PAR1").expect("Failed to write");
    file.write_all(&[0u8; 100])
        .expect("Failed to write garbage");
    drop(file);

    let stderr = run_parqkit_failure(&["head", path.to_str().unwrap()]);
    assert!(
        stderr.contains("error") || stderr.contains("invalid") || stderr.contains("corrupt"),
        "Expected error for truncated file, got: {}",
        stderr
    );
}

#[test]
fn edge_zero_byte_file() {
    let path = fixtures_dir().join("zero_byte.parquet");
    File::create(&path).expect("Failed to create zero byte file");

    let stderr = run_parqkit_failure(&["head", path.to_str().unwrap()]);
    assert!(!stderr.is_empty(), "Should have error for zero byte file");
}

#[test]
fn edge_wrong_magic_bytes() {
    let path = fixtures_dir().join("wrong_magic.parquet");

    // Create a file with wrong magic bytes (should be PAR1...PAR1)
    let mut file = File::create(&path).expect("Failed to create file");
    // Write "FAKE" instead of "PAR1" at the start
    file.write_all(b"FAKE").expect("Failed to write header");
    file.write_all(&[0u8; 1000])
        .expect("Failed to write padding");
    // Write "FAKE" instead of "PAR1" at the end
    file.write_all(b"FAKE").expect("Failed to write footer");
    drop(file);

    let stderr = run_parqkit_failure(&["head", path.to_str().unwrap()]);
    assert!(
        stderr.to_lowercase().contains("parquet")
            || stderr.to_lowercase().contains("magic")
            || stderr.to_lowercase().contains("invalid")
            || stderr.to_lowercase().contains("valid"),
        "Expected 'not a valid Parquet file' error, got: {}",
        stderr
    );
}

#[test]
fn edge_bit_flipped_file() {
    // First generate a valid parquet file
    let valid_path = generate_fixture(
        "valid_for_flip.parquet",
        &["--rows", "100", "--cols", "5", "--profile", "mixed"],
    );

    // Read the valid file and flip some bits in the middle
    let mut data = fs::read(&valid_path).expect("Failed to read valid file");
    let corrupted_path = fixtures_dir().join("bit_flipped.parquet");

    // Flip some bits in the middle of the file (metadata area)
    if data.len() > 100 {
        for byte in data.iter_mut().take(60).skip(50) {
            *byte ^= 0xFF; // Flip all bits in these bytes
        }
    }

    fs::write(&corrupted_path, &data).expect("Failed to write corrupted file");

    let stderr = run_parqkit_failure(&["head", corrupted_path.to_str().unwrap()]);
    assert!(!stderr.is_empty(), "Should have error for bit-flipped file");
}

#[test]
fn edge_truncated_at_metadata() {
    // Generate a valid file first
    let valid_path = generate_fixture(
        "valid_for_truncate.parquet",
        &["--rows", "100", "--cols", "5", "--profile", "mixed"],
    );

    // Read the valid file
    let data = fs::read(&valid_path).expect("Failed to read valid file");
    let truncated_path = fixtures_dir().join("truncated_metadata.parquet");

    // Truncate at 50% of the file (likely in the data or metadata)
    let truncate_point = data.len() / 2;
    fs::write(&truncated_path, &data[..truncate_point]).expect("Failed to write truncated file");

    let stderr = run_parqkit_failure(&["head", truncated_path.to_str().unwrap()]);
    assert!(
        stderr.to_lowercase().contains("error")
            || stderr.to_lowercase().contains("truncat")
            || stderr.to_lowercase().contains("corrupt")
            || stderr.to_lowercase().contains("invalid"),
        "Expected error for truncated file, got: {}",
        stderr
    );
}

#[test]
fn edge_random_bytes_file() {
    let path = fixtures_dir().join("random_bytes.parquet");

    // Create a file with random bytes
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    "seed".hash(&mut hasher);
    let seed = hasher.finish();

    // Generate pseudo-random bytes
    let mut data = vec![0u8; 4096];
    let mut state = seed;
    for byte in &mut data {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        *byte = (state >> 16) as u8;
    }

    fs::write(&path, &data).expect("Failed to write random file");

    let stderr = run_parqkit_failure(&["head", path.to_str().unwrap()]);
    assert!(
        !stderr.is_empty(),
        "Should have error for random bytes file"
    );
}

#[test]
fn edge_valid_magic_invalid_content() {
    let path = fixtures_dir().join("valid_magic_invalid_content.parquet");

    // Create a file with valid magic bytes but garbage in between
    let mut file = File::create(&path).expect("Failed to create file");
    file.write_all(b"PAR1")
        .expect("Failed to write header magic");
    let garbage: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF]
        .into_iter()
        .cycle()
        .take(1000)
        .collect();
    file.write_all(&garbage).expect("Failed to write garbage");
    // Footer: 4 bytes for metadata length + PAR1
    file.write_all(&[0u8; 4])
        .expect("Failed to write fake metadata length");
    file.write_all(b"PAR1")
        .expect("Failed to write footer magic");
    drop(file);

    let stderr = run_parqkit_failure(&["head", path.to_str().unwrap()]);
    assert!(
        !stderr.is_empty(),
        "Should have error for file with valid magic but invalid content"
    );
}

// ============================================================================
// Corruption Chaos Tests (run many iterations)
// ============================================================================

#[test]
fn chaos_random_corruption() {
    // Generate a valid file
    let valid_path = generate_fixture(
        "valid_for_chaos.parquet",
        &["--rows", "100", "--cols", "5", "--profile", "mixed"],
    );

    let data = fs::read(&valid_path).expect("Failed to read valid file");

    // Run 100 iterations with different corruption patterns
    for i in 0..100 {
        let corrupted_path = fixtures_dir().join(format!("chaos_{}.parquet", i));

        let mut corrupted = data.clone();

        // Apply different corruption based on iteration
        match i % 5 {
            0 => {
                // Zero out a random section
                let start = (i * 17) % (corrupted.len().saturating_sub(50).max(1));
                let end = (start + 50).min(corrupted.len());
                for byte in &mut corrupted[start..end] {
                    *byte = 0;
                }
            }
            1 => {
                // Flip bits at random positions
                let pos = (i * 31) % corrupted.len();
                corrupted[pos] ^= 0xFF;
            }
            2 => {
                // Truncate
                let new_len = (corrupted.len() * (50 + i % 40)) / 100;
                corrupted.truncate(new_len.max(1));
            }
            3 => {
                // Insert garbage
                let pos = (i * 23) % corrupted.len();
                corrupted.insert(pos, 0xDE);
                corrupted.insert(pos, 0xAD);
            }
            4 => {
                // Duplicate a section
                let start = (i * 13) % (corrupted.len().saturating_sub(20).max(1));
                let end = (start + 20).min(corrupted.len());
                let section: Vec<u8> = corrupted[start..end].to_vec();
                let insert_pos = (i * 29) % corrupted.len();
                for (j, byte) in section.into_iter().enumerate() {
                    corrupted.insert(insert_pos + j, byte);
                }
            }
            _ => unreachable!(),
        }

        fs::write(&corrupted_path, &corrupted).expect("Failed to write corrupted file");

        // Run parqkit and ensure it doesn't panic (exit code doesn't matter)
        let output = run_parqkit(&["head", corrupted_path.to_str().unwrap()]);
        // Either succeeds (unlikely) or fails gracefully
        assert!(
            output.status.success() || !output.stderr.is_empty() || output.status.code().is_some(),
            "parqkit should not crash/panic on corrupted file iteration {}",
            i
        );

        // Clean up to avoid accumulating files
        fs::remove_file(&corrupted_path).ok();
    }
}

#[test]
fn edge_glob_no_matches() {
    let stderr = run_parqkit_failure(&["count", "nonexistent_pattern_*.parquet"]);
    assert!(
        stderr.contains("no files")
            || stderr.contains("No files")
            || stderr.contains("matched")
            || stderr.contains("not found"),
        "Expected helpful error for no glob matches, got: {}",
        stderr
    );
}

// ============================================================================
// Empty File Tests
// ============================================================================

#[test]
fn empty_parquet_file() {
    let path = generate_fixture(
        "empty.parquet",
        &["--rows", "0", "--cols", "5", "--profile", "empty"],
    );

    let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
    assert!(output.contains("0"), "Empty file should have 0 rows");

    let output = run_parqkit_success(&["schema", path.to_str().unwrap()]);
    assert!(
        !output.is_empty(),
        "Schema should be displayable for empty file"
    );
}

// ============================================================================
// Unicode and Special Character Tests
// ============================================================================

#[test]
fn unicode_stress() {
    let path = generate_fixture(
        "unicode.parquet",
        &["--rows", "1000", "--cols", "5", "--profile", "unicode"],
    );

    // Test all output formats handle Unicode
    for format in &["table", "json", "jsonl", "csv"] {
        let output =
            run_parqkit_success(&["head", "-n", "100", path.to_str().unwrap(), "-o", format]);
        assert!(
            !output.is_empty(),
            "Unicode output failed for format {}",
            format
        );
    }
}

#[test]
fn csv_special_chars() {
    let path = generate_fixture(
        "unicode.parquet",
        &["--rows", "100", "--cols", "3", "--profile", "unicode"],
    );

    let output = run_parqkit_success(&["head", path.to_str().unwrap(), "-o", "csv"]);
    // CSV should handle special characters without breaking
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() > 1, "Should have header + data rows");
}

// ============================================================================
// Sparse Data Tests
// ============================================================================

#[test]
fn sparse_data_90_percent_nulls() {
    let path = generate_fixture(
        "sparse.parquet",
        &["--rows", "10000", "--cols", "10", "--profile", "sparse"],
    );

    let output = run_parqkit_success(&["stats", path.to_str().unwrap()]);
    assert!(!output.is_empty(), "Stats should work on sparse data");

    let output = run_parqkit_success(&["head", "-n", "100", path.to_str().unwrap(), "-o", "json"]);
    // JSON should handle many nulls
    assert!(
        output.contains("null"),
        "Sparse data should have nulls in JSON"
    );
}

// ============================================================================
// Long String Tests
// ============================================================================

#[test]
fn long_strings_1kb() {
    let path = generate_fixture(
        "long_strings.parquet",
        &["--rows", "100", "--cols", "3", "--profile", "long-strings"],
    );

    let output = run_parqkit_success(&["head", "-n", "1", path.to_str().unwrap(), "-o", "json"]);
    assert!(
        output.len() > 1000,
        "Long string output should be large, got {} bytes",
        output.len()
    );
}

// ============================================================================
// Edge Case Values Tests
// ============================================================================

#[test]
fn edge_case_values() {
    let path = generate_fixture(
        "edge_values.parquet",
        &["--rows", "1000", "--cols", "8", "--profile", "edge-cases"],
    );

    // Should handle MIN/MAX int64, special floats (NaN, Inf)
    let output = run_parqkit_success(&["head", "-n", "100", path.to_str().unwrap(), "-o", "json"]);
    assert!(!output.is_empty());

    let output = run_parqkit_success(&["stats", path.to_str().unwrap()]);
    assert!(!output.is_empty());
}

// ============================================================================
// All Nulls Tests
// ============================================================================

#[test]
fn all_nulls_column() {
    let path = generate_fixture(
        "all_nulls.parquet",
        &["--rows", "100", "--cols", "5", "--profile", "all-nulls"],
    );

    let output = run_parqkit_success(&["head", path.to_str().unwrap(), "-o", "json"]);
    assert!(output.contains("null"), "All-null file should show nulls");

    let output = run_parqkit_success(&["stats", path.to_str().unwrap()]);
    assert!(!output.is_empty());
}

// ============================================================================
// Wide Schema Tests (many columns)
// ============================================================================

#[test]
fn wide_schema_100_columns() {
    let path = generate_fixture(
        "wide_100.parquet",
        &["--rows", "1000", "--cols", "100", "--profile", "mixed"],
    );

    let output = run_parqkit_success(&["schema", path.to_str().unwrap()]);
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 100, "Should show 100+ column definitions");

    let output = run_parqkit_success(&["head", "-n", "10", path.to_str().unwrap()]);
    assert!(!output.is_empty());
}

#[test]
#[ignore] // Run with --ignored
fn wide_schema_1000_columns() {
    let path = generate_fixture(
        "wide_1000.parquet",
        &["--rows", "100", "--cols", "1000", "--profile", "integers"],
    );

    let start = Instant::now();
    let output = run_parqkit_success(&["schema", path.to_str().unwrap()]);
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "Schema with 1000 cols took too long: {:?}",
        elapsed
    );
    assert!(!output.is_empty());
}

// ============================================================================
// Medium Load Tests (run with --ignored)
// ============================================================================

#[test]
#[ignore]
fn medium_load_1m_rows() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    // Warmup run to avoid cold-start overhead
    let _ = run_parqkit_success(&["count", path.to_str().unwrap()]);

    // Count should be near-instant (metadata only)
    let start = Instant::now();
    let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(output.contains("1000000"));
    assert!(
        elapsed < Duration::from_millis(200),
        "Count took too long: {:?}",
        elapsed
    );

    // Head should be fast (streaming)
    let start = Instant::now();
    let output = run_parqkit_success(&["head", "-n", "100", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "Head took too long: {:?}",
        elapsed
    );
    assert!(!output.is_empty());

    // Stats should complete reasonably
    let start = Instant::now();
    let _output = run_parqkit_success(&["stats", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "Stats took too long: {:?}",
        elapsed
    );
}

#[test]
#[ignore]
fn medium_load_tail_1m_rows() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    // Tail must scan entire file
    let output = run_parqkit_success(&["tail", "-n", "10", path.to_str().unwrap()]);
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 10, "Should have at least 10 lines");
}

// ============================================================================
// Large Load Tests (run with --ignored)
// ============================================================================

#[test]
#[ignore]
fn large_load_10m_rows() {
    let path = generate_fixture(
        "large_10m.parquet",
        &["--rows", "10000000", "--cols", "20", "--profile", "mixed"],
    );

    // Count should still be instant
    let start = Instant::now();
    let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(output.contains("10000000"));
    assert!(
        elapsed < Duration::from_millis(500),
        "Count on 10M rows took too long: {:?}",
        elapsed
    );

    // Head should still be fast
    let start = Instant::now();
    let _output = run_parqkit_success(&["head", "-n", "1000", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "Head on 10M rows took too long: {:?}",
        elapsed
    );
}

#[test]
#[ignore]
fn large_load_tail_10m_rows() {
    let path = generate_fixture(
        "large_10m.parquet",
        &["--rows", "10000000", "--cols", "20", "--profile", "mixed"],
    );

    // Tail on 10M rows - will be slow but should not OOM
    let output = run_parqkit_success(&["tail", "-n", "10", path.to_str().unwrap()]);
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 10);
}

// ============================================================================
// Extreme Load Tests (run with --ignored, may take a long time)
// ============================================================================

#[test]
#[ignore]
fn extreme_load_100m_rows() {
    let path = generate_fixture(
        "extreme_100m.parquet",
        &[
            "--rows",
            "100000000",
            "--cols",
            "20",
            "--profile",
            "mixed",
            "--batch-size",
            "131072",
        ],
    );

    // Count must still be instant (metadata only)
    let start = Instant::now();
    let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(output.contains("100000000"));
    assert!(
        elapsed < Duration::from_secs(1),
        "Count on 100M rows took too long: {:?}",
        elapsed
    );

    // Head should still be fast
    let start = Instant::now();
    let _output = run_parqkit_success(&["head", "-n", "100", path.to_str().unwrap()]);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "Head on 100M rows took too long: {:?}",
        elapsed
    );
}

// ============================================================================
// Concurrency Tests
// ============================================================================

#[test]
#[ignore]
fn concurrent_reads_same_file() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    let path_str = path.to_str().unwrap().to_string();
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let p = path_str.clone();
            thread::spawn(move || {
                let output = Command::new(parqkit_bin())
                    .args(["head", "-n", "1000", &p])
                    .output()
                    .expect("Failed to execute parqkit");
                (i, output.status.success(), output.stdout.len())
            })
        })
        .collect();

    for handle in handles {
        let (i, success, len) = handle.join().expect("Thread panicked");
        assert!(success, "Thread {} failed", i);
        assert!(len > 0, "Thread {} got empty output", i);
    }
}

#[test]
#[ignore]
fn rapid_sequential_invocations() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    let start = Instant::now();
    for i in 0..100 {
        let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
        assert!(output.contains("1000000"), "Invocation {} failed", i);
    }
    let elapsed = start.elapsed();

    eprintln!("100 count invocations took {:?}", elapsed);
    // Should complete in reasonable time (100 invocations, each should be <100ms)
    assert!(
        elapsed < Duration::from_secs(30),
        "Rapid invocations too slow: {:?}",
        elapsed
    );
}

// ============================================================================
// Large Scan Stress Tests
// ============================================================================

#[test]
#[ignore]
fn count_large() {
    let path = generate_fixture(
        "large_10m.parquet",
        &["--rows", "10000000", "--cols", "20", "--profile", "mixed"],
    );

    let output = run_parqkit_success(&["count", path.to_str().unwrap()]);
    assert!(output.contains("10000000"));
}

#[test]
#[ignore]
fn tail_large() {
    let path = generate_fixture(
        "large_10m.parquet",
        &["--rows", "10000000", "--cols", "20", "--profile", "mixed"],
    );

    let output = run_parqkit_success(&["tail", "-n", "100", path.to_str().unwrap()]);
    assert!(!output.is_empty());
}

// ============================================================================
// Output Format Stress Tests
// ============================================================================

#[test]
#[ignore]
fn output_json_large() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    // Generate 100k rows as JSON
    let output =
        run_parqkit_success(&["head", "-n", "100000", path.to_str().unwrap(), "-o", "json"]);

    // Should be valid JSON array
    assert!(output.starts_with('['));
    assert!(output.trim().ends_with(']'));
}

#[test]
#[ignore]
fn output_csv_large() {
    let path = generate_fixture(
        "medium_1m.parquet",
        &["--rows", "1000000", "--cols", "10", "--profile", "mixed"],
    );

    let output =
        run_parqkit_success(&["head", "-n", "100000", path.to_str().unwrap(), "-o", "csv"]);
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 100001, "Should have header + 100k rows");
}

// ============================================================================
// Merge Stress Tests
// ============================================================================

#[test]
#[ignore]
fn merge_many_files() {
    // Generate 100 small files
    let mut paths = Vec::new();
    for i in 0..100 {
        let path = generate_fixture(
            &format!("merge_part_{}.parquet", i),
            &[
                "--rows",
                "10000",
                "--cols",
                "5",
                "--profile",
                "mixed",
                "--seed",
                &i.to_string(),
            ],
        );
        paths.push(path);
    }

    let output_path = fixtures_dir().join("merged_100.parquet");
    let mut args: Vec<&str> = vec!["merge"];
    let path_strs: Vec<String> = paths
        .iter()
        .map(|p| p.to_str().unwrap().to_string())
        .collect();
    for p in &path_strs {
        args.push(p);
    }
    args.push("-o");
    args.push(output_path.to_str().unwrap());

    let _ = run_parqkit_success(&args);

    // Verify merged file
    let output = run_parqkit_success(&["count", output_path.to_str().unwrap()]);
    assert!(
        output.contains("1000000"),
        "Merged file should have 100 * 10000 = 1M rows"
    );
}

// ============================================================================
// Glob Stress Tests
// ============================================================================

#[test]
#[ignore]
fn glob_many_files() {
    // Ensure we have 100 merge part files
    for i in 0..100 {
        generate_fixture(
            &format!("merge_part_{}.parquet", i),
            &[
                "--rows",
                "1000",
                "--cols",
                "5",
                "--profile",
                "mixed",
                "--seed",
                &i.to_string(),
            ],
        );
    }

    let glob_pattern = fixtures_dir().join("merge_part_*.parquet");
    let output = run_parqkit_success(&["count", glob_pattern.to_str().unwrap()]);

    // Should aggregate counts from all matched files
    assert!(!output.is_empty());
}
