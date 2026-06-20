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

#[derive(Clone)]
pub struct RarAdapter {
    unrar_cmd: String,
    sevenz_cmd: String,
}

impl Default for RarAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl RarAdapter {
    pub fn new() -> Self {
        Self {
            unrar_cmd: "unrar".to_owned(),
            sevenz_cmd: "7z".to_owned(),
        }
    }

    #[doc(hidden)]
    pub fn with_cmds(unrar_cmd: impl Into<String>, sevenz_cmd: impl Into<String>) -> Self {
        Self {
            unrar_cmd: unrar_cmd.into(),
            sevenz_cmd: sevenz_cmd.into(),
        }
    }

    #[doc(hidden)]
    pub fn unrar_cmd(&self) -> &str {
        &self.unrar_cmd
    }

    #[doc(hidden)]
    pub fn sevenz_cmd(&self) -> &str {
        &self.sevenz_cmd
    }

    async fn is_cmd_available(&self, cmd: &str) -> bool {
        if let Ok(output) = tokio::process::Command::new(cmd)
            .arg("--help")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
        {
            output.status.success()
        } else {
            false
        }
    }

    pub async fn detect_tool(&self) -> Option<&'static str> {
        if self.is_cmd_available(&self.unrar_cmd).await {
            Some("unrar")
        } else if self.is_cmd_available(&self.sevenz_cmd).await {
            Some("7z")
        } else {
            None
        }
    }

    async fn list_entries_unrar(&self, archive_path: &PathBuf) -> Result<Vec<String>> {
        let output = tokio::process::Command::new(&self.unrar_cmd)
            .args(["l", "-inul"])
            .arg(archive_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| map_exe_error(e, &self.unrar_cmd, "Install unrar to search RAR archives."))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!("{} list failed: {}", self.unrar_cmd, stderr));
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

    async fn list_entries_7z(&self, archive_path: &PathBuf) -> Result<Vec<String>> {
        let output = tokio::process::Command::new(&self.sevenz_cmd)
            .args(["l", "-slt", "-ba"])
            .arg(archive_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| map_exe_error(e, &self.sevenz_cmd, "Install 7z/p7zip to search RAR archives."))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!("{} list failed: {}", self.sevenz_cmd, stderr));
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

impl GetMetadata for RarAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
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
            let msg = format!(
                "rar adapter: skipping {} because it is not a real file on disk",
                filepath_hint.display()
            );
            warn!("{}", msg);
            eprintln!("rga warning: {}", msg);
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let tool = match self.detect_tool().await {
            Some("unrar") => "unrar",
            Some("7z") => "7z",
            _ => {
                let msg = format!(
                    "rar adapter disabled: neither {} nor {} is available. \
                     Install unrar or p7zip to search RAR archives.",
                    self.unrar_cmd, self.sevenz_cmd
                );
                warn!("{}", msg);
                eprintln!("rga warning: {}", msg);
                return Ok(Box::pin(tokio_stream::empty()));
            }
        };

        match tool {
            "unrar" => self
                .adapt_with_unrar(
                    filepath_hint,
                    line_prefix,
                    archive_recursion_depth,
                    postprocess,
                    config,
                )
                .await,
            "7z" => self
                .adapt_with_7z(
                    filepath_hint,
                    line_prefix,
                    archive_recursion_depth,
                    postprocess,
                    config,
                )
                .await,
            _ => unreachable!(),
        }
    }
}

impl RarAdapter {
    async fn adapt_with_unrar(
        &self,
        filepath_hint: PathBuf,
        line_prefix: String,
        archive_recursion_depth: i32,
        postprocess: bool,
        config: crate::config::RgaConfig,
    ) -> Result<AdaptedFilesIterBox> {
        let entries = self.list_entries_unrar(&filepath_hint).await?;
        if entries.is_empty() {
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let unrar_cmd = self.unrar_cmd.clone();
        let s = stream! {
            for entry_path_str in &entries {
                debug!(
                    "{}{}|{}: extracting from RAR via {}",
                    line_prefix,
                    filepath_hint.display(),
                    entry_path_str,
                    unrar_cmd
                );
                let mut child = match tokio::process::Command::new(&unrar_cmd)
                    .args(["p", "-inul"])
                    .arg(&filepath_hint)
                    .arg(entry_path_str)
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                {
                    Ok(c) => c,
                    Err(e) => {
                        let msg = format!("failed to spawn {} for {}: {}", unrar_cmd, entry_path_str, e);
                        warn!("{}", msg);
                        eprintln!("rga warning: {}", msg);
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
                let unrar_for_log = unrar_cmd.clone();
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
                                "{}{}|{}: {} exited with status {}",
                                lp_for_log,
                                fph_for_log,
                                entry_for_log,
                                unrar_for_log,
                                status
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            "{}{}|{}: {} wait error: {}",
                            lp_for_log,
                            fph_for_log,
                            entry_for_log,
                            unrar_for_log,
                            e
                        );
                    }
                }
            }
        };

        Ok(Box::pin(s))
    }

    async fn adapt_with_7z(
        &self,
        filepath_hint: PathBuf,
        line_prefix: String,
        archive_recursion_depth: i32,
        postprocess: bool,
        config: crate::config::RgaConfig,
    ) -> Result<AdaptedFilesIterBox> {
        let entries = self.list_entries_7z(&filepath_hint).await?;
        if entries.is_empty() {
            return Ok(Box::pin(tokio_stream::empty()));
        }

        let tmp_dir =
            tempfile::tempdir().context("failed to create temp dir for RAR extraction via 7z")?;

        let extract_output = tokio::process::Command::new(&self.sevenz_cmd)
            .args(["x", "-y"])
            .arg(format!("-o{}", tmp_dir.path().display()))
            .arg(&filepath_hint)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| map_exe_error(e, &self.sevenz_cmd, "Install p7zip to search RAR archives."))?;
        if !extract_output.status.success() {
            let stderr = String::from_utf8_lossy(&extract_output.stderr);
            return Err(format_err!("{} extract failed: {}", self.sevenz_cmd, stderr));
        }

        let sevenz_cmd = self.sevenz_cmd.clone();
        let s = stream! {
            let tmp_dir = tmp_dir;
            let tmp_dir_path = tmp_dir.path().to_path_buf();
            for entry_path_str in &entries {
                let full_path = tmp_dir_path.join(entry_path_str);
                debug!(
                    "{}{}|{}: reading from extracted RAR via {}",
                    line_prefix,
                    filepath_hint.display(),
                    entry_path_str,
                    sevenz_cmd
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
    async fn test_rar_not_real_file_skips() -> Result<()> {
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
    async fn test_rar_both_cmds_missing_returns_empty_with_warning() -> Result<()> {
        let adapter = RarAdapter::with_cmds(
            "nonexistent_unrar_cmd_xyz_98765",
            "nonexistent_7z_cmd_xyz_98765",
        );

        let tool = adapter.detect_tool().await;
        assert!(
            tool.is_none(),
            "detect_tool() must return None when both commands are missing"
        );

        let test_dir = test_data_dir();
        let test_rar = test_dir.join("test.rar");
        if test_rar.exists() {
            let (a, d) = simple_fs_adapt_info(&test_rar).await?;
            let result = adapter.adapt(a, &d).await;
            assert!(result.is_ok());
            let iter = result.unwrap();
            let output = adapted_to_vec(iter).await?;
            assert_eq!(
                output,
                Vec::<u8>::new(),
                "with both commands missing, adapt() must return empty stream"
            );
        }

        let test_7z = test_dir.join("test.7z");
        if test_7z.exists() {
            let (a, d) = simple_fs_adapt_info(&test_7z).await?;
            let result = adapter.adapt(a, &d).await;
            assert!(result.is_ok());
            let iter = result.unwrap();
            let output = adapted_to_vec(iter).await?;
            assert_eq!(
                output,
                Vec::<u8>::new(),
                "with both commands missing, adapt() must return empty stream even for 7z-shaped input"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rar_only_unrar_missing_falls_back_to_7z() -> Result<()> {
        let adapter = RarAdapter::with_cmds(
            "nonexistent_unrar_cmd_abc_111",
            "7z",
        );
        let tool = adapter.detect_tool().await;
        match tool {
            Some("7z") => {}
            other => {
                eprintln!("Skipping fallback test: 7z not actually available (detected {:?})", other);
                return Ok(());
            }
        }
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
