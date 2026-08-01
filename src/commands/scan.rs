//! Head and tail commands

use crate::api;
use crate::cli::args::{HeadArgs, TailArgs};
use crate::dataset::Dataset;
use crate::{commands, output, ParqkitError, Result, ScanKind, ScanOptions, ScanResult};
use std::sync::Arc;

pub fn run_head(args: HeadArgs) -> Result<()> {
    run_scan(
        args.inputs,
        ScanKind::Head,
        args.rows,
        args.output.into(),
        args.quiet,
    )
}

pub fn run_tail(args: TailArgs) -> Result<()> {
    run_scan(
        args.inputs,
        ScanKind::Tail,
        args.rows,
        args.output.into(),
        args.quiet,
    )
}

fn run_scan(
    inputs: Vec<std::path::PathBuf>,
    kind: ScanKind,
    rows: usize,
    output_format: crate::output::OutputFormat,
    quiet: bool,
) -> Result<()> {
    let dataset = Dataset::from_inputs(inputs)?;
    let results = api::scan(&dataset, kind, ScanOptions { rows })?;

    if let Some(structured_output) = output_format.structured() {
        validate_compatible_schemas(&results)?;
        let schema = results
            .first()
            .map(|result| Arc::clone(&result.schema))
            .ok_or(ParqkitError::NoInputFiles)?;
        let batches = results
            .into_iter()
            .flat_map(|result| result.batches)
            .collect::<Vec<_>>();
        output::write_structured_batches(structured_output, quiet, &schema, &batches)?;
    } else {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet)?;
            output::write_table_batches(quiet, &result.batches)?;
        }
    }

    Ok(())
}

fn validate_compatible_schemas(results: &[ScanResult]) -> Result<()> {
    let Some(first) = results.first() else {
        return Ok(());
    };

    for result in results.iter().skip(1) {
        if result.schema.as_ref() != first.schema.as_ref() {
            return Err(ParqkitError::SchemaMismatch {
                file1: first.path.display().to_string(),
                file2: result.path.display().to_string(),
                details: "Cannot combine scan results with different schemas for structured output"
                    .to_string(),
            });
        }
    }

    Ok(())
}
