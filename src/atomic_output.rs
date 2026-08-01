use crate::{ParqkitError, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_OUTPUT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct PendingOutput {
    target_path: PathBuf,
    temp_path: PathBuf,
    committed: bool,
}

impl PendingOutput {
    pub fn new(target_path: &Path) -> Result<Self> {
        let file_name = target_path.file_name().ok_or_else(|| {
            ParqkitError::write_error(target_path, "output path must include a file name")
        })?;
        let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
        let counter = TEMP_OUTPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_file_name = format!(
            ".{}.tmp.{}.{}",
            file_name.to_string_lossy(),
            std::process::id(),
            counter
        );

        Ok(Self {
            target_path: target_path.to_path_buf(),
            temp_path: parent.join(temp_file_name),
            committed: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.temp_path
    }

    pub fn commit(mut self) -> Result<()> {
        fs::rename(&self.temp_path, &self.target_path)
            .map_err(|error| ParqkitError::write_error(&self.target_path, error))?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ignored = fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let pending_output = PendingOutput::new(&target_path)?;
        fs::write(pending_output.path(), b"replacement")?;

        pending_output.commit()?;

        assert_eq!(fs::read(&target_path)?, b"replacement");
        fs::remove_file(target_path)?;
        Ok(())
    }

    #[test]
    fn pending_output_replaces_existing_target_on_commit() -> Result<()> {
        let target_path = temp_path("txt")?;
        fs::write(&target_path, b"original")?;
        let pending_output = PendingOutput::new(&target_path)?;
        fs::write(pending_output.path(), b"replacement")?;

        pending_output.commit()?;

        assert_eq!(fs::read(&target_path)?, b"replacement");
        fs::remove_file(target_path)?;
        Ok(())
    }

    #[test]
    fn pending_output_removes_temp_file_when_dropped() -> Result<()> {
        let target_path = temp_path("txt")?;
        let temp_path = {
            let pending_output = PendingOutput::new(&target_path)?;
            let temp_path = pending_output.path().to_path_buf();
            fs::write(&temp_path, b"partial")?;
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
}
