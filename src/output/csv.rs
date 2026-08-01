//! CSV output formatting

use crate::Result;
use arrow::array::RecordBatch;
use arrow::csv::{Writer, WriterBuilder};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

pub fn write_batches<W: Write>(
    mut writer: W,
    batches: &[RecordBatch],
    include_header: bool,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    for (index, batch) in batches.iter().enumerate() {
        let mut csv_writer = WriterBuilder::new()
            .with_header(include_header && index == 0)
            .build(&mut writer);
        csv_writer.write(batch)?;
    }

    writer.flush()?;
    Ok(())
}

pub struct BatchFileWriter {
    writer: Writer<File>,
    schema: SchemaRef,
    wrote_batch: bool,
}

impl BatchFileWriter {
    pub fn create(file: File, schema: SchemaRef) -> Self {
        Self {
            writer: WriterBuilder::new().with_header(true).build(file),
            schema,
            wrote_batch: false,
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> std::result::Result<(), ArrowError> {
        self.writer.write(batch)?;
        self.wrote_batch = true;
        Ok(())
    }

    pub fn finish(&mut self) -> std::result::Result<(), ArrowError> {
        if !self.wrote_batch {
            self.writer
                .write(&RecordBatch::new_empty(Arc::clone(&self.schema)))?;
        }
        Ok(())
    }
}
