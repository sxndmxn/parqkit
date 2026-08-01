# Parqkit — Fast Parquet CLI and Rust library

Inspect, preview, convert, and merge Parquet files with pretty interactive output and predictable formats for scripts. The typed, streaming library API is the foundation for future jq-like querying.

## Installation

Parqkit requires Rust 1.85 or newer.

```bash
cargo install parqkit
```

From a source checkout:

```bash
cargo install --path . --locked
```

## Usage

```
parqkit <COMMAND>

Commands:
  schema    Show schema (column names, types, nullability)
  head      Show first N rows (default 10)
  tail      Show last N rows (default 10)
  count     Count total rows
  stats     Column statistics (min, max, nulls)
  convert   Convert to CSV, JSON, or JSONL
  merge     Merge multiple parquet files
  info      File metadata (row groups, compression, size)
```

### Common command options

- `schema`, `head`, `tail`, `stats`, and `info` support `-o, --output <table|json|jsonl|csv>`
- `head` and `tail` support `-n, --rows <N>`
- `schema`, `head`, `tail`, `count`, `stats`, and `info` support `-q, --quiet`
- `convert` infers the output format from the destination file extension: `.csv`, `.json`, or `.jsonl`

## Examples

### View schema

```bash
$ parqkit schema data.parquet
+--------+------------+----------+
| Column | Type       | Nullable |
+================================+
| id     | INT64      | Yes      |
| name   | STRING     | Yes      |
| amount | DOUBLE     | Yes      |
+--------+------------+----------+
```

### Preview data

```bash
$ parqkit head data.parquet -n 5
+----+---------+--------+
| id | name    | amount |
+=========================+
| 1  | Alice   | 100.5  |
| 2  | Bob     | 200.75 |
| 3  | Charlie | 150.25 |
+----+---------+--------+

$ parqkit tail data.parquet -n 2
```

### Count rows

```bash
$ parqkit count data.parquet
1000000

$ parqkit count '*.parquet'
part1.parquet: 500000
part2.parquet: 500000
Total: 1000000
```

### Column statistics

```bash
$ parqkit stats data.parquet
+--------+--------+-------+-------+------+----------+
| Column | Type   | Nulls | Min   | Max  | Complete |
+=====================================================+
| id     | INT64  | 0     | 1     | 1000 | Yes      |
| name   | STRING | 5     | Alice | Zoe  | Yes      |
+--------+--------+-------+-------+------+----------+
```

### File info

```bash
$ parqkit info data.parquet
+-------------+----------------------------------+
| Key         | Value                            |
+================================================+
| File        | data.parquet                     |
| File Size   | 1.26 KB                          |
| Rows        | 1000                             |
| Columns     | 4                                |
| Row Groups  | 1                                |
| Compression | SNAPPY                           |
+-------------+----------------------------------+
```

### Convert formats

```bash
$ parqkit convert data.parquet output.csv
$ parqkit convert data.parquet output.json
$ parqkit convert data.parquet output.jsonl
```

### Merge files

```bash
$ parqkit merge part1.parquet part2.parquet -o combined.parquet
```

### Output formats

Read-oriented commands support multiple output formats:

```bash
$ parqkit head data.parquet --output table   # Pretty table (default)
$ parqkit head data.parquet --output json    # JSON array
$ parqkit head data.parquet --output jsonl   # JSON Lines
$ parqkit head data.parquet --output csv     # CSV
$ parqkit schema data.parquet --output jsonl # One JSON object per schema column
```

Schema and stats JSON output include display type plus explicit physical/logical type
metadata. Nested Parquet columns use their full dotted paths. Stats output includes a
`statistics_complete` field; unavailable null counts and bounds are emitted as `null`
instead of misleading zeroes or partial values. Signed and unsigned integers, finite
floats, and booleans use native JSON types. Decimals use exact scaled strings,
non-finite floats use `"NaN"`, `"Infinity"`, or `"-Infinity"`, and non-logical binary
values use deterministic hexadecimal strings. Parqkit ignores bounds whose column order
is legacy, unknown, or undefined rather than presenting them as authoritative.

Empty Parquet scans retain their schema, so CSV output still includes headers and
multi-file structured scans still validate schema compatibility.

`count` prints plain text counts, `convert` writes the format implied by the output file extension, and `merge` writes a Parquet file.

### Glob support

```bash
$ parqkit count 'data/*.parquet'
$ parqkit schema '*.parquet'
```

Quote patterns to let Parqkit expand, sort, and deduplicate glob matches itself. Explicitly repeated file arguments remain repeated.
An existing literal path takes precedence over pattern syntax, so names containing `*`, `?`, or `[` remain usable.

## Features

- Fast startup
- Batch-oriented reads and conversions
- Multiple output formats
- Glob pattern support
- Snappy compression for merge output

## Development

- [Core contracts](docs/core-contracts.md) captures the foundation invariants for input handling, output rendering, safe writes, and error behavior.
- Stress fixtures and the `parqkit-generate` helper are opt-in: `cargo test --all-features` or `cargo build --features stress-tools --bin parqkit-generate`.
- CI audits dependencies, builds, formats, lints, tests, documents, and packages the crate with the committed lockfile.

### Streaming library queries

The library API can project and stream Arrow batches without loading complete scan results:

```rust
let dataset = parqkit::dataset_from_inputs(vec!["data.parquet".into()])?;
let options = parqkit::QueryOptions {
    projection: parqkit::Projection::Columns(vec!["name".into(), "id".into()]),
    limit: Some(1_000),
    batch_size: 256,
};
let stream = parqkit::execute_query(&dataset, options)?;

let _output_schema = stream.compatible_schema()?;
for result in stream {
    let batch = result?.batch;
    // Transform or write this bounded Arrow batch.
}
```

Planning resolves every projected source schema before iteration. Projection, limits, and batching are pushed into the Parquet reader; consumers validate multi-source compatibility before output, and existing output code remains responsible for atomic file replacement.

## License

[MIT](LICENSE)
