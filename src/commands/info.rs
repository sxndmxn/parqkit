//! File metadata command

use crate::api;
use crate::cli::args::InfoArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let InfoArgs {
        inputs,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output_format: output::OutputFormat = output.into();
    let infos = api::info(&dataset)?;
    if let Some(structured_output) = output_format.structured() {
        output::write_file_infos(structured_output, quiet, &infos)
    } else {
        for info in &infos {
            commands::print_source_header(&dataset, info.path(), quiet)?;
            output::write_file_info(quiet, info)?;
        }
        Ok(())
    }
}
