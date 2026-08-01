//! Schema display command

use crate::api;
use crate::cli::args::SchemaArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: SchemaArgs) -> Result<()> {
    let SchemaArgs {
        inputs,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output_format: output::OutputFormat = output.into();
    let results = api::schema(&dataset)?;

    if let Some(structured_output) = output_format.structured() {
        output::write_schema_results(structured_output, quiet, &results)?;
    } else {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet)?;
            output::write_schema_table(quiet, &result.columns)?;
        }
    }

    Ok(())
}
