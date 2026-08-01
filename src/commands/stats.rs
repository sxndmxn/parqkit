//! Column statistics command

use crate::api;
use crate::cli::args::StatsArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: StatsArgs) -> Result<()> {
    let StatsArgs {
        inputs,
        column,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output_format: output::OutputFormat = output.into();
    let results = api::stats(&dataset, column.as_deref())?;

    if let Some(structured_output) = output_format.structured() {
        output::write_stats_results(structured_output, quiet, &results)?;
    } else {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet)?;
            output::write_stats_table(quiet, &result.rows)?;
        }
    }

    Ok(())
}
