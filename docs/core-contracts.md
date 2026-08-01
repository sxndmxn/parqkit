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
- Existing literal paths take precedence over glob syntax, so file names containing metacharacters remain addressable.
- Explicit repeated files are preserved because the user asked for them.
- Files matched by globs are deduplicated against other glob matches and against explicit repeats that overlap a glob.
- Commands that require exactly one input use the crate-private single-input path and reject multi-match globs.

## Output Contracts

- Human table output is rendered per source in the command layer because that is where source headers belong.
- Machine-readable output must not include human source headers.
- Structured aggregate output is limited to JSON, JSONL, and CSV.
- Single-file structured output keeps the historical single-file shape.
- Multi-file structured metadata output includes a `file` field.
- `head` and `tail` structured output may combine batches only when data schemas are compatible; custom Arrow schema and top-level field metadata do not make otherwise identical fields incompatible, but Arrow extension-type metadata remains schema-significant.
- Scan results retain their Arrow schema even when they contain no record batches. Non-empty eager and streaming batches use that same normalized schema, empty CSV scans therefore keep headers, and empty inputs participate in schema validation.
- Nested Parquet leaves use their full dotted column paths and effective nullability across parent groups.
- Stats results distinguish complete metadata from unavailable or partial metadata. Unknown counts and bounds are not rendered as authoritative zeroes.
- Stats aggregation respects logical unsigned, decimal, and `FLOAT16` ordering across row groups; decimal JSON bounds use exact scaled strings. Bounds require exact metadata and a supported type-defined column order, so legacy, unknown, and undefined orders do not produce authoritative bounds.
- Missing statistics null counts remain unknown rather than being coerced to zero.
- Non-finite stats bounds use explicit JSON strings because JSON has no native NaN or infinity values.
- `count` intentionally prints plain text counts instead of using the structured output system.

## Streaming Query Contract

- `execute_query` is the public foundation for projection and future query execution.
- Query planning validates options and reads every source's Parquet metadata before returning, but record batches are decoded only as `QueryStream` advances.
- `Projection::Columns` selects top-level Arrow columns, pushes that selection into the Parquet reader, and preserves the caller's requested column order.
- Empty projections, duplicate projected columns, unknown columns, and zero batch sizes are typed planning errors.
- Batch size and row limit apply independently to each dataset source. Source order and repeated explicit inputs are preserved through `QueryBatch::source_index`.
- `QueryStream::sources` retains every projected source schema, including empty sources. Structured multi-file consumers call `compatible_schema` before writing output.
- A decoding failure terminates and fuses the stream. The query layer returns typed data and errors; it does not render output or weaken the existing atomic-write boundary.

## Safe Write Contract

- Generated output reserves a new same-directory temporary file without truncating an existing path, writes through that reserved file handle, and renames it into place only after the writer successfully finishes and buffered data is flushed.
- Temporary paths are internal implementation details and must not determine output format.
- User-requested output paths determine format inference and user-facing write errors.
- Failed reads, unsupported formats, schema mismatches, and writer failures must not truncate an existing output file.
- Merge compatibility follows the same data-schema rule and does not reject otherwise identical fields because producer metadata differs; different Arrow extension types remain incompatible.

## Error Contract

- A downstream consumer closing standard output early is a normal CLI pipeline termination: the CLI exits successfully without printing an error, while library callers receive the typed `ParqkitError::BrokenPipe` variant.
- Library errors use `ParqkitError`.
- File read errors should carry path context and be classified into typed variants where practical.
- File write errors should report the user-requested path, not an internal temporary path.
- Machine-readable stdout should stay empty when a command fails before producing a complete valid payload.

## Future Query Foundation

- Filter and expression features should extend `execute_query` and its Arrow batches, not rendered text.
- Output contracts should remain independent from query parsing.
- New query features should not weaken dataset validation, safe writes, or machine-readable output guarantees.

## Non-Goals For The Foundation Phase

- Do not add new user-facing query syntax yet.
- Do not expose public APIs just because command code needs a helper.
- Do not make output formatting responsible for dataset/source decisions.
- Do not infer behavior from temporary paths or rendered strings.
