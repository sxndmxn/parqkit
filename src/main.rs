//! Parqkit command-line entry point.

use parqkit::{ParqkitError, Result};
use std::io::Write;

fn main() {
    if let Err(err) = run() {
        if matches!(err, ParqkitError::BrokenPipe) {
            return;
        }
        let _ignored = writeln!(std::io::stderr().lock(), "error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    parqkit::run_cli()
}
