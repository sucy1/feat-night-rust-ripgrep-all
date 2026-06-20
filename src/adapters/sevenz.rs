use crate::{
    adapted_iter::AdaptedFilesIterBox,
    adapters::{AdaptInfo, AdapterMeta, FileAdapter, GetMetadata},
    matching::{FastFileMatcher, FileMatcher},
    print_bytes,
};
use anyhow::{Context, Result, format_err};
use async_stream::stream;
use async_trait::async_trait;
use lazy_static::lazy_static;
use log::*;
use std::path::PathBuf;

use super::custom::map_exe_error;

static EXTENSIONS: &[&str] = &["7z", "7z001"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "7z".to_owned(),
        version: 1,
        description: "Uses the 7z command to extract and search within 7z archives".to_owned(),
        recurses: true,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false
    };
}

#[derive(Clone)]
pub struct SevenzAdapter {
    cmd_name: String,
}

impl Default for SevenzAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SevenzAdapter {
    pub fn new() -> Self {
        Self {
            cmd_name: "7z".to_owned(),
        }
    }

    #[doc(hidden)]
    pub fn with_cmd(cmd_name: impl Into<String>) -> Self {
        Self {
            cmd_name: cmd_name.into(),
        }
    }

    #[doc(hidden)]
    pub fn cmd_name(&self) -> &str {
        &self.cmd_name
    }

    pub async fn check_available(&self) -> Result<()> {
        let output = tokio::process::Command::new(&self.cmd_name)
            .arg("--help")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| {
                map_exe_error(
                    e,
                    &self.cmd_name,
                    "Install p7zip or 7-zip to search 7z archives.",
                )
            })?;
        if !output.status.success() {
            return Err(format_err!(
                "{} command failed. Is 7-zip/p7zip installed?",
                self.cmd_name
            ));
        }
        Ok(())
    }

    async fn list_entries(&self, archive_path: &PathBuf) -> Result<Vec<String>> {
        let output = tokio::process::Command::new(&self.cmd_name)
            .args(["l", "-slt", "-ba"])
            .arg(archive_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| {
                map_exe_error(
                    e,
                    &self.cmd_name,
                    "Install p7zip or 7-zip to search 7z archives.",
                )
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!("{} list failed: {}", self.cmd_name, stderr));
        }
        let listing = String::from_utf8_lossy(&output.stdout);
        let mut entries = Vec::new();
        for line in listing.lines() {
            let line = line.trim();
            if let Some(path) = line.strip_prefix("Path = ") {
                let path = path.trim();
                if !path.ends_with('/') && !path.is_empty() {
                    entries.push(path.to_string());
                }
            }
        }
        Ok(entries)
    }
}

impl GetMetadata for SevenzAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl FileAdapter for SevenzAdapter {
    async fn adapt(
        &self,
        ai: AdaptInfo,
        _detection_reason: &FileMatcher,
    ) -> Result<AdaptedFilesIterBox> {
        let AdaptInfo {
            filepath_hint,
            is_real_file,
            line_prefix,
            archive_recursion_depth,
            postprocess,
            config,
            ..
        } = ai;

        if !is_real_file {
            let msg = format!(
                "7z adapter: skipping {} because it is not a real file on disk",
                filepath_hint.display()
            );
            warn!("{}", msg);
            eprintln!("rga warning: {}", msg);
            return Ok(Box::pin(tokio_stream::empty()));
        }

        if let Err(e) = self.check_available().await {
            let msg = format!("7z adapter disabled: {}", e);
            warn!("{}", msg);
            eprintln!("rga warning: {}", msg);
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let entries = self.list_entries(&filepath_hint).await?;
        if entries.is_empty() {
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let tmp_dir = tempfile::tempdir().context("failed to create temp dir for 7z extraction")?;

        let extract_output = tokio::process::Command::new(&self.cmd_name)
            .args(["x", "-y"])
            .arg(format!("-o{}", tmp_dir.path().display()))
            .arg(&filepath_hint)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| {
                map_exe_error(
                    e,
                    &self.cmd_name,
                    "Install p7zip or 7-zip to search 7z archives.",
                )
            })?;
        if !extract_output.status.success() {
            let stderr = String::from_utf8_lossy(&extract_output.stderr);
            return Err(format_err!("{} extract failed: {}", self.cmd_name, stderr));
        }

        let cmd_name = self.cmd_name.clone();
        let s = stream! {
            let tmp_dir = tmp_dir;
            let tmp_dir_path = tmp_dir.path().to_path_buf();
            for entry_path_str in &entries {
                let full_path = tmp_dir_path.join(entry_path_str);
                debug!(
                    "{}{}|{}: reading from extracted 7z via {}",
                    line_prefix,
                    filepath_hint.display(),
                    entry_path_str,
                    cmd_name
                );
                match tokio::fs::File::open(&full_path).await {
                    Ok(file) => {
                        let metadata = file.metadata().await;
                        let size_str = match &metadata {
                            Ok(m) => print_bytes(m.len() as f64),
                            Err(_) => "?".to_string(),
                        };
                        debug!(
                            "{}{}|{}: {}",
                            line_prefix,
                            filepath_hint.display(),
                            entry_path_str,
                            size_str
                        );
                        let new_line_prefix = format!("{}{}: ", line_prefix, entry_path_str);
                        let fname = PathBuf::from(entry_path_str);
                        yield Ok(AdaptInfo {
                            filepath_hint: fname,
                            is_real_file: false,
                            inp: Box::pin(file),
                            line_prefix: new_line_prefix,
                            archive_recursion_depth: archive_recursion_depth + 1,
                            postprocess,
                            config: config.clone(),
                        });
                    }
                    Err(e) => {
                        let msg = format!("failed to open extracted file {}: {}", full_path.display(), e);
                        warn!("{}", msg);
                        eprintln!("rga warning: {}", msg);
                    }
                }
            }
        };

        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preproc::loop_adapt;
    use crate::test_utils::{adapted_to_vec, simple_adapt_info, simple_fs_adapt_info, test_data_dir};
    use anyhow::Result;
    use std::path::PathBuf;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_7z_not_real_file_skips() -> Result<()> {
        let adapter = SevenzAdapter::new();
        let content = b"test content".to_vec();
        let (a, d) = simple_adapt_info(
            &PathBuf::from("test.7z"),
            Box::pin(std::io::Cursor::new(content)),
        );
        let result = adapter.adapt(a, &d).await;
        assert!(result.is_ok());
        let iter = result.unwrap();
        let output = adapted_to_vec(iter).await?;
        assert_eq!(output, Vec::<u8>::new());
        Ok(())
    }

    #[tokio::test]
    async fn test_7z_missing_cmd_returns_empty_with_warning() -> Result<()> {
        let adapter = SevenzAdapter::with_cmd("nonexistent_7z_cmd_xyz_12345");

        let check_result = adapter.check_available().await;
        assert!(check_result.is_err());
        let err_msg = format!("{}", check_result.unwrap_err());
        assert!(
            err_msg.contains("nonexistent_7z_cmd_xyz_12345"),
            "error should mention the missing command name, got: {}",
            err_msg
        );
        assert!(
            err_msg.contains("p7zip") || err_msg.contains("7-zip") || err_msg.contains("7zip"),
            "error should hint at installing 7z/p7zip, got: {}",
            err_msg
        );

        let test_dir = test_data_dir();
        let test_7z = test_dir.join("test.7z");
        if !test_7z.exists() {
            eprintln!("Skipping part of test: test.7z not found in test data dir");
            return Ok(());
        }
        let (a, d) = simple_fs_adapt_info(&test_7z).await?;
        let result = adapter.adapt(a, &d).await;
        assert!(result.is_ok());
        let iter = result.unwrap();
        let output = adapted_to_vec(iter).await?;
        assert_eq!(
            output,
            Vec::<u8>::new(),
            "with missing command, adapt() must return empty stream"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_7z_real_file() -> Result<()> {
        let test_dir = test_data_dir();
        let test_7z = test_dir.join("test.7z");
        if !test_7z.exists() {
            eprintln!("Skipping 7z test: test.7z not found in test data dir");
            return Ok(());
        }
        let (a, d) = simple_fs_adapt_info(&test_7z).await?;
        let adapter = SevenzAdapter::new();
        let r = loop_adapt(&adapter, d, a).await?;
        let o = adapted_to_vec(r).await?;
        let output = String::from_utf8(o)?;
        assert!(
            output.contains("hello"),
            "Expected 'hello' in 7z output, got: {}",
            output
        );
        Ok(())
    }
}
