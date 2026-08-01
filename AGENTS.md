# Parqkit Agent Guide

## Scope

These instructions apply to the entire repository.

Parqkit is a Rust 2021 CLI and library for inspecting, streaming, converting, and merging Parquet data through Arrow. Keep the library core typed and predictable while the project grows toward jq-like querying.

## Start Here

Before changing code:

1. Inspect `git status --short --branch` and preserve unrelated user changes.
2. Read `README.md`, `Cargo.toml`, and the relevant source modules.
3. Read `docs/core-contracts.md` before changing public APIs, dataset behavior, query execution, output formats, errors, or file writes.
4. Inspect existing tests for the affected contract before implementing it.

Do not fetch, pull, switch branches, commit, push, install packages, or update dependencies unless the user requests it or it is clearly required by the task.

## Architecture

- `src/lib.rs`: public exports and hidden CLI entrypoint.
- `src/api.rs`: typed public library operations; do not print or parse CLI arguments here.
- `src/model.rs`: public result and query types.
- `src/query.rs`: pull-based streaming query execution and Parquet projection pushdown.
- `src/dataset/`: input validation, glob expansion, ordering, and deduplication.
- `src/engine/`: Arrow and Parquet reads, metadata, statistics, and merge execution.
- `src/commands/`: CLI orchestration and per-source presentation decisions.
- `src/output/`: table, JSON, JSONL, and CSV rendering.
- `src/atomic_output.rs`: same-directory temporary output and commit-by-rename.
- `tests/core.rs`: public library and foundation contracts.
- `tests/cli.rs`: user-visible CLI and output contracts.
- `tests/stress.rs` and `stress/`: edge, load, benchmark, and chaos coverage.

Keep dependencies flowing from CLI orchestration toward typed APIs and engines. Public library code must not depend on command or output modules.

## Core Invariants

- Preserve typed `ParqkitError` variants and path context.
- Preserve explicit repeated inputs while sorting and deduplicating glob matches according to the documented dataset rules.
- Never mix human source headers into machine-readable output.
- Validate multi-source schema compatibility before structured output.
- Retain schemas for empty scans and empty streaming-query sources.
- Keep conversion and merge writes atomic: failures must not truncate existing output.
- Determine output formats and user-facing errors from requested paths, never temporary paths.
- Keep query execution streaming and batch-bounded. Push projection and limits into the Parquet reader when supported.
- Extend `execute_query` for future filters and expressions; do not query rendered text.
- Do not expose CLI helpers as public APIs without a deliberately designed typed contract.

## Rust Standards

The crate forbids unsafe code and denies common shortcuts through `Cargo.toml` lints. Production code must not use `unwrap`, `expect`, `panic!`, `todo!`, `unimplemented!`, or `dbg!`.

- Prefer checked numeric conversions and checked arithmetic for file metadata.
- Add `Debug` implementations to new public types.
- Preserve source-path context when mapping Arrow, Parquet, and I/O failures.
- Keep batches bounded and avoid collecting complete datasets unless the existing API explicitly promises eager results.
- Use comments for non-obvious contracts and invariants, not line-by-line narration.

## Required Validation

Run focused tests while iterating, then mirror CI before handoff:

```bash
cargo build --locked --all-targets --all-features
cargo fmt --check
cargo clippy --locked --all-targets --all-features
cargo test --locked --all-targets --all-features
```

Run `cargo doc --locked --no-deps --all-features` when public APIs or rustdoc change.

The large-load stress tests are intentionally ignored by default. Run ignored stress tests only when the task affects performance, memory behavior, concurrency, or large-file handling and the required fixtures/resources are available.

## Test Expectations

- Add public API and contract coverage to `tests/core.rs`.
- Add CLI parsing, stdout/stderr, and output-shape coverage to `tests/cli.rs`.
- Add focused unit tests beside private helpers when that produces clearer failure localization.
- Cover empty inputs, zero rows, incompatible schemas, repeated sources, missing metadata, and preservation of existing output when relevant.
- Avoid weakening assertions merely to make a test pass.

## Local Tooling

The development environment is Arch Linux with Bash. Prefer the installed Rust CLI tools:

- `eza` for listings
- `fd` for file discovery
- `rg` for text search
- `bat` for file viewing
- `sd` for simple mechanical replacement
- `just` when a `Justfile` exists
- `tokei` for code statistics
- `hyperfine` for benchmarks
- `procs` for process inspection

Use `apply_patch` for source edits. Do not create or overwrite files with shell redirection when a patch is sufficient.

## Documentation And Handoff

Update `README.md` for user-facing behavior and `docs/core-contracts.md` for foundation invariants. In the final handoff, report the behavioral outcome, important files changed, validation performed, ignored checks, and current Git branch/worktree state.
