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

static EXTENSIONS: &[&str] = &["rar", "rar5"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "rar".to_owned(),
        version: 1,
        description: "Uses unrar (or 7z as fallback) to extract and search within RAR archives"
            .to_owned(),
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
pub struct RarAdapter;

impl RarAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for RarAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

enum RarTool {
    Unrar,
    SevenZ,
}

async fn detect_rar_tool() -> Option<RarTool> {
    if tokio::process::Command::new("unrar")
        .arg("--help")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .is_ok_and(|o| o.status.success())
    {
        return Some(RarTool::Unrar);
    }
    if tokio::process::Command::new("7z")
        .arg("--help")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .is_ok_and(|o| o.status.success())
    {
        return Some(RarTool::SevenZ);
    }
    None
}

async fn list_rar_entries_unrar(archive_path: &PathBuf) -> Result<Vec<String>> {
    let output = tokio::process::Command::new("unrar")
        .args(["l", "-inul"])
        .arg(archive_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(|e| map_exe_error(e, "unrar", "Install unrar to search RAR archives."))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format_err!("unrar list failed: {}", stderr));
    }
    let listing = String::from_utf8_lossy(&output.stdout);
    let mut entries = Vec::new();
    let mut in_file_list = false;
    for line in listing.lines() {
        let line = line.trim();
        if line.starts_with("-----------") {
            in_file_list = !in_file_list;
            continue;
        }
        if in_file_list && !line.is_empty() {
            let parts: Vec<&str> = line.splitn(5, ' ').collect();
            if let Some(filename) = parts.last() {
                let filename = filename.trim();
                if !filename.is_empty() {
                    entries.push(filename.to_string());
                }
            }
        }
    }
    Ok(entries)
}

async fn list_rar_entries_7z(archive_path: &PathBuf) -> Result<Vec<String>> {
    let output = tokio::process::Command::new("7z")
        .args(["l", "-slt", "-ba"])
        .arg(archive_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(|e| map_exe_error(e, "7z", "Install 7z/p7zip to search RAR archives."))?;
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
impl FileAdapter for RarAdapter {
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
                "rar adapter: skipping {} because it is not a real file on disk",
                filepath_hint.display()
            );
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let tool = match detect_rar_tool().await {
            Some(t) => t,
            None => {
                warn!(
                    "rar adapter disabled: neither unrar nor 7z is available. \
                     Install unrar or p7zip to search RAR archives."
                );
                return Ok(Box::pin(tokio_stream::empty()));
            }
        };

        match tool {
            RarTool::Unrar => adapt_with_unrar(
                filepath_hint,
                line_prefix,
                archive_recursion_depth,
                postprocess,
                config,
            )
            .await,
            RarTool::SevenZ => adapt_with_7z(
                filepath_hint,
                line_prefix,
                archive_recursion_depth,
                postprocess,
                config,
            )
            .await,
        }
    }
}

async fn adapt_with_unrar(
    filepath_hint: PathBuf,
    line_prefix: String,
    archive_recursion_depth: i32,
    postprocess: bool,
    config: crate::config::RgaConfig,
) -> Result<AdaptedFilesIterBox> {
    let entries = list_rar_entries_unrar(&filepath_hint).await?;
    if entries.is_empty() {
        return Ok(Box::pin(tokio_stream::empty()));
    }

    let s = stream! {
        for entry_path_str in &entries {
            debug!(
                "{}{}|{}: extracting from RAR via unrar",
                line_prefix,
                filepath_hint.display(),
                entry_path_str
            );
            let mut child = match tokio::process::Command::new("unrar")
                .args(["p", "-inul"])
                .arg(&filepath_hint)
                .arg(entry_path_str)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    warn!("failed to spawn unrar for {}: {}", entry_path_str, e);
                    continue;
                }
            };
            let stdout = child.stdout.take().expect("stdout is piped");
            let new_line_prefix = format!("{}{}: ", line_prefix, entry_path_str);
            let fname = PathBuf::from(entry_path_str.clone());
            let config_clone = config.clone();
            let entry_for_log = entry_path_str.clone();
            let fph_for_log = filepath_hint.display().to_string();
            let lp_for_log = line_prefix.clone();
            yield Ok(AdaptInfo {
                filepath_hint: fname,
                is_real_file: false,
                inp: Box::pin(stdout),
                line_prefix: new_line_prefix,
                archive_recursion_depth: archive_recursion_depth + 1,
                postprocess,
                config: config_clone,
            });
            match child.wait().await {
                Ok(status) => {
                    if !status.success() {
                        debug!(
                            "{}{}|{}: unrar exited with status {}",
                            lp_for_log,
                            fph_for_log,
                            entry_for_log,
                            status
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        "{}{}|{}: unrar wait error: {}",
                        lp_for_log,
                        fph_for_log,
                        entry_for_log,
                        e
                    );
                }
            }
        }
    };

    Ok(Box::pin(s))
}

async fn adapt_with_7z(
    filepath_hint: PathBuf,
    line_prefix: String,
    archive_recursion_depth: i32,
    postprocess: bool,
    config: crate::config::RgaConfig,
) -> Result<AdaptedFilesIterBox> {
    let entries = list_rar_entries_7z(&filepath_hint).await?;
    if entries.is_empty() {
        return Ok(Box::pin(tokio_stream::empty()));
    }

    let tmp_dir =
        tempfile::tempdir().context("failed to create temp dir for RAR extraction via 7z")?;
    let tmp_dir_path = tmp_dir.path().to_path_buf();

    let extract_output = tokio::process::Command::new("7z")
        .args(["x", "-y"])
        .arg(format!("-o{}", tmp_dir_path.display()))
        .arg(&filepath_hint)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .map_err(|e| map_exe_error(e, "7z", "Install p7zip to search RAR archives."))?;
    if !extract_output.status.success() {
        let stderr = String::from_utf8_lossy(&extract_output.stderr);
        return Err(format_err!("7z extract failed: {}", stderr));
    }

    let s = stream! {
        for entry_path_str in &entries {
            let full_path = tmp_dir_path.join(entry_path_str);
            debug!(
                "{}{}|{}: reading from extracted RAR via 7z",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{preproc::loop_adapt, test_utils::*};
    use anyhow::Result;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_rar_not_real_file() -> Result<()> {
        let adapter = RarAdapter::new();
        let content = b"test content".to_vec();
        let (a, d) = simple_adapt_info(
            &PathBuf::from("test.rar"),
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
    async fn test_rar_real_file() -> Result<()> {
        let test_dir = test_data_dir();
        let test_rar = test_dir.join("test.rar");
        if !test_rar.exists() {
            eprintln!("Skipping RAR test: test.rar not found in test data dir");
            return Ok(());
        }
        let (a, d) = simple_fs_adapt_info(&test_rar).await?;
        let adapter = RarAdapter::new();
        let r = loop_adapt(&adapter, d, a).await?;
        let o = adapted_to_vec(r).await?;
        let output = String::from_utf8(o)?;
        assert!(
            output.contains("hello"),
            "Expected 'hello' in RAR output, got: {}",
            output
        );
        Ok(())
    }
}
