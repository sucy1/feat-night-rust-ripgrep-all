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

#[derive(Default, Clone)]
pub struct SevenzAdapter;

impl SevenzAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for SevenzAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

async fn check_7z_available() -> Result<()> {
    let output = tokio::process::Command::new("7z")
        .arg("--help")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(|e| map_exe_error(e, "7z", "Install p7zip or 7-zip to search 7z archives."))?;
    if !output.status.success() {
        return Err(format_err!("7z command failed. Is 7-zip/p7zip installed?"));
    }
    Ok(())
}

async fn list_7z_entries(archive_path: &PathBuf) -> Result<Vec<String>> {
    let output = tokio::process::Command::new("7z")
        .args(["l", "-slt", "-ba"])
        .arg(archive_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(|e| map_exe_error(e, "7z", "Install p7zip or 7-zip to search 7z archives."))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format_err!("7z list failed: {}", stderr));
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
            warn!(
                "7z adapter: skipping {} because it is not a real file on disk",
                filepath_hint.display()
            );
            return Ok(Box::pin(tokio_stream::empty()));
        }

        if let Err(e) = check_7z_available().await {
            warn!("7z adapter disabled: {}", e);
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let entries = list_7z_entries(&filepath_hint).await?;
        if entries.is_empty() {
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let tmp_dir = tempfile::tempdir().context("failed to create temp dir for 7z extraction")?;
        let tmp_dir_path = tmp_dir.path().to_path_buf();

        let extract_output = tokio::process::Command::new("7z")
            .args(["x", "-y"])
            .arg(format!("-o{}", tmp_dir_path.display()))
            .arg(&filepath_hint)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| map_exe_error(e, "7z", "Install p7zip or 7-zip to search 7z archives."))?;
        if !extract_output.status.success() {
            let stderr = String::from_utf8_lossy(&extract_output.stderr);
            return Err(format_err!("7z extract failed: {}", stderr));
        }

        let s = stream! {
            for entry_path_str in &entries {
                let full_path = tmp_dir_path.join(entry_path_str);
                debug!(
                    "{}{}|{}: reading from extracted 7z",
                    line_prefix,
                    filepath_hint.display(),
                    entry_path_str
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
                        warn!("failed to open extracted file {}: {}", full_path.display(), e);
                    }
                }
            }
            if let Err(e) = tokio::fs::remove_dir_all(&tmp_dir_path).await {
                warn!("failed to clean up temp dir {}: {}", tmp_dir_path.display(), e);
            }
        };

        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{preproc::loop_adapt, test_utils::*};
    use anyhow::Result;

    #[tokio::test]
    async fn test_7z_not_available() -> Result<()> {
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
