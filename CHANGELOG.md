# Changelog

All notable changes to Parqkit are documented here.

## 0.1.0 - 2026-08-01

- Inspect Parquet schemas, row counts, row groups, compression, and column statistics.
- Preview the first or last rows as tables, JSON, JSON Lines, or CSV.
- Convert one Parquet source to CSV, JSON, or JSON Lines with atomic output replacement.
- Merge compatible Parquet sources into a Snappy-compressed Parquet file atomically.
- Expand bounded glob inputs while preserving explicit repeats and deterministic ordering.
- Treat existing paths containing glob metacharacters as literal file names.
- Stream projected Arrow record batches through the typed Rust library API.
- Normalize non-semantic producer metadata across eager scans, streaming batches, and merges while preserving Arrow extension-type identity.
- Preserve exact unsigned, decimal, and half-precision statistics across multiple row groups.
- Exit quietly and successfully when a downstream Unix pipeline closes standard output early.
- Use Arrow and Parquet 59, preserving missing null-count metadata and current Variant and geospatial logical types.
- Retain read compatibility coverage for Parquet 53 fixtures using Snappy, Gzip, Zstd, and uncompressed data.
- Declare and continuously check the Rust 1.85 minimum supported version.
- Make stress, chaos, benchmark, and memory-profile runners reproducible and safe to invoke from any working directory.
