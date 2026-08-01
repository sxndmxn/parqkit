# Core Contracts

This document captures the boring foundation decisions that should stay stable while Parqkit grows toward jq-like querying for Parquet.

## Goal

Parqkit should expose a small, predictable core for reading Parquet files, producing typed metadata/data results, and rendering those results without surprising callers or scripts.

Until the core is stable, changes should harden existing behavior instead of adding user-facing features.

## Public API Boundary

- `Dataset` is the public input collection type.
- Public library functions return typed results from `src/model.rs`; they should not print, parse CLI arguments, or depend on command modules.
- The hidden `run_cli` entry point exists only to wire the package binary to crate-private CLI modules; it is not part of the stable library core.
- CLI-only helpers stay crate-private. For example, single-input command plumbing belongs behind `InputFile`, not in the public library API.
- `convert` is currently CLI plumbing, not a public library API. Expose conversion publicly only after its typed API and output contract are deliberately designed.

## Dataset And Input Rules

- Empty input lists return `ParqkitError::NoInputFiles`.
- Glob inputs are expanded, sorted, validated, and bounded.
- Explicit repeated files are preserved because the user asked for them.
- Files matched by globs are deduplicated against other glob matches and against explicit repeats that overlap a glob.
- Commands that require exactly one input use the crate-private single-input path and reject multi-match globs.

## Output Contracts

- Human table output is rendered per source in the command layer because that is where source headers belong.
- Machine-readable output must not include human source headers.
- Structured aggregate output is limited to JSON, JSONL, and CSV.
- Single-file structured output keeps the historical single-file shape.
- Multi-file structured metadata output includes a `file` field.
- `head` and `tail` structured output may combine batches only when schemas are compatible.
- Scan results retain their Arrow schema even when they contain no record batches. Empty CSV scans therefore keep headers, and empty inputs participate in schema validation.
- Nested Parquet leaves use their full dotted column paths and effective nullability across parent groups.
- Stats results distinguish complete metadata from unavailable or partial metadata. Unknown counts and bounds are not rendered as authoritative zeroes.
- Non-finite stats bounds use explicit JSON strings because JSON has no native NaN or infinity values.
- `count` intentionally prints plain text counts instead of using the structured output system.

## Safe Write Contract

- Generated output is written to a same-directory temporary file and renamed into place only after the writer successfully finishes and buffered data is flushed.
- Temporary paths are internal implementation details and must not determine output format.
- User-requested output paths determine format inference and user-facing write errors.
- Failed reads, unsupported formats, schema mismatches, and writer failures must not truncate an existing output file.

## Error Contract

- Library errors use `ParqkitError`.
- File read errors should carry path context and be classified into typed variants where practical.
- File write errors should report the user-requested path, not an internal temporary path.
- Machine-readable stdout should stay empty when a command fails before producing a complete valid payload.

## Future Query Foundation

- Query/projection/filter features should build on typed library results and Arrow batches, not on rendered text.
- Output contracts should remain independent from query parsing.
- New query features should not weaken dataset validation, safe writes, or machine-readable output guarantees.

## Non-Goals For The Foundation Phase

- Do not add new user-facing query syntax yet.
- Do not expose public APIs just because command code needs a helper.
- Do not make output formatting responsible for dataset/source decisions.
- Do not infer behavior from temporary paths or rendered strings.
