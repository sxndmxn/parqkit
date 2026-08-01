//! JSON and JSONL output formatting

use crate::Result;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::json::writer::{JsonArray, LineDelimited, Writer};
use arrow::json::WriterBuilder;
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

pub fn write_json<W: Write>(writer: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(writer);

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    writer.into_inner().flush()?;
    Ok(())
}

pub fn write_jsonl<W: Write>(writer: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(writer);

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    writer.into_inner().flush()?;
    Ok(())
}

pub fn write_value<W: Write, T: Serialize + ?Sized>(mut writer: W, value: &T) -> Result<()> {
    serde_json::to_writer_pretty(&mut writer, value)?;
    writeln!(writer)?;
    writer.flush()?;
    Ok(())
}

pub fn write_json_lines<W: Write, T: Serialize>(mut writer: W, values: &[T]) -> Result<()> {
    for value in values {
        serde_json::to_writer(&mut writer, value)?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}

pub struct JsonBatchFileWriter {
    writer: Writer<BufWriter<File>, JsonArray>,
}

impl JsonBatchFileWriter {
    pub fn create(file: File) -> Self {
        let writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(BufWriter::new(file));
        Self { writer }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> std::result::Result<(), ArrowError> {
        self.writer.write(batch)
    }

    pub fn finish(mut self) -> std::result::Result<(), ArrowError> {
        self.writer.finish()?;
        self.writer.into_inner().flush()?;
        Ok(())
    }
}

pub struct JsonlBatchFileWriter {
    writer: Writer<BufWriter<File>, LineDelimited>,
}

impl JsonlBatchFileWriter {
    pub fn create(file: File) -> Self {
        let writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(BufWriter::new(file));
        Self { writer }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> std::result::Result<(), ArrowError> {
        self.writer.write(batch)
    }

    pub fn finish(mut self) -> std::result::Result<(), ArrowError> {
        self.writer.finish()?;
        self.writer.into_inner().flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct FlushErrorWriter;

    impl Write for FlushErrorWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("flush failed"))
        }
    }

    #[test]
    fn propagates_final_flush_errors() {
        let result = write_json(FlushErrorWriter, &[]);
        assert!(result.is_err());
    }
}
