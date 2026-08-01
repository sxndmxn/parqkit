# Parqkit — Fast Parquet CLI

A jq-like CLI for Parquet files. Fast startup, pretty output, sensible defaults.

## Installation

```bash
cargo install --path .
```

## Usage

```
parqkit <COMMAND> [OPTIONS] <FILE>

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

$ parqkit count *.parquet
part1.parquet: 500000
part2.parquet: 500000
Total: 1000000
```

### Column statistics

```bash
$ parqkit stats data.parquet
+--------+--------+-------+-------+------+
| Column | Type   | Nulls | Min   | Max  |
+==========================================+
| id     | INT64  | 0     | 1     | 1000 |
| name   | STRING | 5     | Alice | Zoe  |
+--------+--------+-------+-------+------+
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
metadata. Stats JSON preserves numeric and boolean min/max values as native JSON
types, and renders physical binary values as deterministic hexadecimal strings.

`count` prints plain text counts, `convert` writes the format implied by the output file extension, and `merge` writes a Parquet file.

### Glob support

```bash
$ parqkit count data/*.parquet
$ parqkit schema *.parquet
```

## Features

- Sub-100ms startup time
- Batch-oriented reads and conversions
- Multiple output formats
- Glob pattern support
- Snappy compression for merge output

## Development

- [Core contracts](docs/core-contracts.md) captures the foundation invariants for input handling, output rendering, safe writes, and error behavior.

## License

MIT
