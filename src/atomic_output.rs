use crate::{ParqkitError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_OUTPUT_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_TEMP_OUTPUT_ATTEMPTS: usize = 1_000;

#[derive(Debug)]
pub(crate) struct PendingOutput {
    target_path: PathBuf,
    temp_path: PathBuf,
    temp_file: Option<File>,
    committed: bool,
}

impl PendingOutput {
    pub fn new(target_path: &Path) -> Result<Self> {
        let (temp_path, temp_file) = reserve_temp_file(target_path, &TEMP_OUTPUT_COUNTER)?;

        Ok(Self {
            target_path: target_path.to_path_buf(),
            temp_path,
            temp_file: Some(temp_file),
            committed: false,
        })
    }

    pub fn take_file(&mut self) -> Result<File> {
        self.temp_file.take().ok_or_else(|| {
            ParqkitError::write_error(&self.target_path, "temporary output file is already in use")
        })
    }

    #[cfg(test)]
    pub fn path(&self) -> &Path {
        &self.temp_path
    }

    pub fn commit(mut self) -> Result<()> {
        drop(self.temp_file.take());
        fs::rename(&self.temp_path, &self.target_path)
            .map_err(|error| ParqkitError::write_error(&self.target_path, error))?;
        self.committed = true;
        Ok(())
    }
}

fn reserve_temp_file(target_path: &Path, counter: &AtomicU64) -> Result<(PathBuf, File)> {
    let file_name = target_path.file_name().ok_or_else(|| {
        ParqkitError::write_error(target_path, "output path must include a file name")
    })?;
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));

    for _ in 0..MAX_TEMP_OUTPUT_ATTEMPTS {
        let suffix = counter.fetch_add(1, Ordering::Relaxed);
        let temp_file_name = format!(
            ".{}.tmp.{}.{}",
            file_name.to_string_lossy(),
            std::process::id(),
            suffix
        );
        let temp_path = parent.join(temp_file_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(ParqkitError::write_error(target_path, error)),
        }
    }

    Err(ParqkitError::write_error(
        target_path,
        format!(
            "could not reserve a unique temporary output file after {MAX_TEMP_OUTPUT_ATTEMPTS} attempts"
        ),
    ))
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.committed {
            drop(self.temp_file.take());
            let _ignored = fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(extension: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ParqkitError::output_error)?
            .as_nanos();
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(std::env::temp_dir().join(format!("parqkit_output_{unique}_{counter}.{extension}")))
    }

    #[test]
    fn pending_output_commits_temp_file_to_target() -> Result<()> {
        let target_path = temp_path("txt")?;
        let mut pending_output = PendingOutput::new(&target_path)?;
        pending_output.take_file()?.write_all(b"replacement")?;

        pending_output.commit()?;

        assert_eq!(fs::read(&target_path)?, b"replacement");
        fs::remove_file(target_path)?;
        Ok(())
    }

    #[test]
    fn pending_output_replaces_existing_target_on_commit() -> Result<()> {
        let target_path = temp_path("txt")?;
        fs::write(&target_path, b"original")?;
        let mut pending_output = PendingOutput::new(&target_path)?;
        pending_output.take_file()?.write_all(b"replacement")?;

        pending_output.commit()?;

        assert_eq!(fs::read(&target_path)?, b"replacement");
        fs::remove_file(target_path)?;
        Ok(())
    }

    #[test]
    fn pending_output_removes_temp_file_when_dropped() -> Result<()> {
        let target_path = temp_path("txt")?;
        let temp_path = {
            let mut pending_output = PendingOutput::new(&target_path)?;
            let temp_path = pending_output.path().to_path_buf();
            pending_output.take_file()?.write_all(b"partial")?;
            temp_path
        };

        assert!(!temp_path.exists());
        assert!(!target_path.exists());
        Ok(())
    }

    #[test]
    fn pending_output_temp_path_does_not_preserve_target_extension() -> Result<()> {
        let target_path = temp_path("jsonl")?;
        let pending_output = PendingOutput::new(&target_path)?;
        let temp_file_name = pending_output
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ParqkitError::output_error("temp path should have a file name"))?;

        assert!(!temp_file_name.ends_with(".jsonl"));
        Ok(())
    }

    #[test]
    fn pending_output_does_not_clobber_existing_temp_files() -> Result<()> {
        let target_path = temp_path("txt")?;
        let counter = AtomicU64::new(42);
        let target_name = target_path
            .file_name()
            .ok_or_else(|| ParqkitError::output_error("target should have a file name"))?;
        let occupied_path = target_path.with_file_name(format!(
            ".{}.tmp.{}.42",
            target_name.to_string_lossy(),
            std::process::id()
        ));
        fs::write(&occupied_path, b"keep me")?;

        let (reserved_path, reserved_file) = reserve_temp_file(&target_path, &counter)?;
        drop(reserved_file);

        assert_ne!(reserved_path, occupied_path);
        assert_eq!(fs::read(&occupied_path)?, b"keep me");
        fs::remove_file(reserved_path)?;
        fs::remove_file(occupied_path)?;
        Ok(())
    }
}
