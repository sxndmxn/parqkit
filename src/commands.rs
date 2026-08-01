use crate::cli::args::Command;
use crate::dataset::Dataset;
use crate::Result;
use std::io::Write;
use std::path::Path;

mod convert;
mod count;
mod info;
mod merge;
mod scan;
mod schema;
mod stats;

pub(crate) fn run(command: Command) -> Result<()> {
    match command {
        Command::Schema(args) => schema::run(args),
        Command::Head(args) => scan::run_head(args),
        Command::Tail(args) => scan::run_tail(args),
        Command::Count(args) => count::run(args),
        Command::Stats(args) => stats::run(args),
        Command::Convert(args) => convert::run(args),
        Command::Merge(args) => merge::run(args),
        Command::Info(args) => info::run(args),
    }
}

fn print_source_header(dataset: &Dataset, path: &Path, quiet: bool) -> Result<()> {
    if dataset.is_multi_source() && !quiet {
        writeln!(std::io::stdout().lock(), "==> {} <==", path.display())?;
    }
    Ok(())
}
