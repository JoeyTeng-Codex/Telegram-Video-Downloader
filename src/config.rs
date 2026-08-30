use std::path::{Path, PathBuf};
use std::{env, fs};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
#[cfg(any(target_os = "macos", target_os = "ios"))]
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub downloads: DownloadsConfig,
    #[serde(default)]
    pub tools: ToolsConfig,
    #[serde(default)]
    pub pdf: PdfConfig,
    #[serde(default)]
    pub video: VideoConfig,
    #[serde(default)]
    pub bilibili: BilibiliConfig,
    #[serde(default)]
    pub bot: BotConfig,
    #[serde(skip)]
    project_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramConfig {
    pub token: String,
    #[serde(default)]
    pub allowed_chat_ids: Vec<i64>,
    #[serde(default)]
    pub allow_all_chats: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DownloadsConfig {
    #[serde(default = "default_video_dir")]
    pub video_dir: PathBuf,
    #[serde(default = "default_pdf_dir")]
    pub pdf_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolsConfig {
    #[serde(default = "default_bbdown")]
    pub bbdown: PathBuf,
    #[serde(default = "default_yt_dlp")]
    pub yt_dlp: PathBuf,
    #[serde(default = "default_uv")]
    pub uv: PathBuf,
    #[serde(default = "default_pdf_helper")]
    pub pdf_helper: PathBuf,
    #[serde(default = "default_chrome")]
    pub chrome: PathBuf,
    #[serde(default = "default_ffmpeg")]
    pub ffmpeg: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PdfConfig {
    #[serde(default = "default_auto_pdf_domains")]
    pub auto_domains: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VideoConfig {
    #[serde(default = "default_subtitle_languages")]
    pub subtitle_languages: Vec<String>,
    #[serde(default = "default_true")]
    pub write_nfo: bool,
    #[serde(default = "default_true")]
    pub keep_sidecars: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BilibiliConfig {
    #[serde(default)]
    pub extra_args: Vec<String>,
    #[serde(default)]
    pub global_args: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub plan_args: Vec<String>,
    #[serde(default)]
    pub download_args: Vec<String>,
    #[serde(default)]
    pub playurl_mode: Option<String>,
    #[serde(default)]
    pub restricted_area: Option<String>,
    #[serde(default)]
    pub restricted_area_proxies: Vec<String>,
    #[serde(default)]
    pub restricted_api_proxies: Vec<String>,
    #[serde(default = "default_bilibili_danmaku_formats")]
    pub danmaku_formats: Vec<String>,
    #[serde(default)]
    pub danmaku: BilibiliDanmakuConfig,
    #[serde(default)]
    pub auth: BilibiliAuthConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BilibiliDanmakuConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BilibiliAuthConfig {
    #[serde(default = "default_bilibili_auth_state_path")]
    pub state_path: PathBuf,
    #[serde(default = "default_bilibili_credential_file")]
    pub credential_file: PathBuf,
    #[serde(default)]
    pub credential_profile: Option<String>,
    #[serde(default = "default_bilibili_login_timeout_seconds")]
    pub login_timeout_seconds: u64,
    #[serde(default = "default_bilibili_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BotConfig {
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_poll_timeout_seconds")]
    pub poll_timeout_seconds: u64,
    #[serde(default = "default_progress_update_seconds")]
    pub progress_update_seconds: u64,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_command_idle_timeout_seconds")]
    pub command_idle_timeout_seconds: u64,
}

impl AppConfig {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            telegram: TelegramConfig {
                token: "test-token".to_string(),
                allowed_chat_ids: vec![123456789],
                allow_all_chats: false,
            },
            downloads: DownloadsConfig::default(),
            tools: ToolsConfig::default(),
            pdf: PdfConfig::default(),
            video: VideoConfig::default(),
            bilibili: BilibiliConfig::default(),
            bot: BotConfig::default(),
            project_dir: PathBuf::from("."),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config_path = fs::canonicalize(path)
            .with_context(|| format!("failed to resolve config file {}", path.display()))?;
        let project_dir = config_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self::from_toml_str(&content, project_dir)
    }

    pub fn ensure_runtime_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.downloads.video_dir).with_context(|| {
            format!(
                "failed to create video download directory {}",
                self.downloads.video_dir.display()
            )
        })?;
        fs::create_dir_all(&self.downloads.pdf_dir).with_context(|| {
            format!(
                "failed to create pdf download directory {}",
                self.downloads.pdf_dir.display()
            )
        })?;
        Ok(())
    }

    pub fn resolve_project_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.project_dir.join(path)
        }
    }

    fn from_toml_str(content: &str, project_dir: PathBuf) -> Result<Self> {
        let mut config: Self = toml::from_str(content).context("failed to parse config TOML")?;
        config.project_dir = project_dir;
        config.expand_config_paths();
        config.validate()?;
        Ok(config)
    }

    fn expand_config_paths(&mut self) {
        let video_dir = expand_home_path(&self.downloads.video_dir);
        self.downloads.video_dir = self.resolve_project_path(&video_dir);
        let pdf_dir = expand_home_path(&self.downloads.pdf_dir);
        self.downloads.pdf_dir = self.resolve_project_path(&pdf_dir);
        self.tools.bbdown = expand_home_path(&self.tools.bbdown);
        self.tools.yt_dlp = expand_home_path(&self.tools.yt_dlp);
        self.tools.uv = expand_home_path(&self.tools.uv);
        self.tools.pdf_helper = expand_home_path(&self.tools.pdf_helper);
        self.tools.chrome = expand_home_path(&self.tools.chrome);
        self.tools.ffmpeg = expand_home_path(&self.tools.ffmpeg);
        let state_path = expand_home_path(&self.bilibili.auth.state_path);
        self.bilibili.auth.state_path = self.resolve_project_path(&state_path);
        let credential_file = expand_home_path(&self.bilibili.auth.credential_file);
        self.bilibili.auth.credential_file = self.resolve_project_path(&credential_file);
    }

    fn validate(&self) -> Result<()> {
        if self.telegram.token.trim().is_empty() {
            bail!("telegram.token must not be empty");
        }
        if !self.telegram.allow_all_chats && self.telegram.allowed_chat_ids.is_empty() {
            bail!("telegram.allowed_chat_ids must not be empty unless allow_all_chats is true");
        }
        if self.bot.concurrency == 0 {
            bail!("bot.concurrency must be at least 1");
        }
        if self.bot.poll_timeout_seconds == 0 {
            bail!("bot.poll_timeout_seconds must be at least 1");
        }
        if self.bot.progress_update_seconds == 0 {
            bail!("bot.progress_update_seconds must be at least 1");
        }
        if self.bot.command_timeout_seconds == 0 {
            bail!("bot.command_timeout_seconds must be at least 1");
        }
        if self.bot.command_idle_timeout_seconds == 0 {
            bail!("bot.command_idle_timeout_seconds must be at least 1");
        }
        if self.bilibili.auth.login_timeout_seconds == 0 {
            bail!("bilibili.auth.login_timeout_seconds must be at least 1");
        }
        if self.bilibili.auth.poll_interval_seconds == 0 {
            bail!("bilibili.auth.poll_interval_seconds must be at least 1");
        }
        if self.bilibili.auth.poll_interval_seconds >= self.bilibili.auth.login_timeout_seconds {
            bail!(
                "bilibili.auth.poll_interval_seconds must be less than bilibili.auth.login_timeout_seconds"
            );
        }
        if let Some(profile) = &self.bilibili.auth.credential_profile
            && profile.trim().is_empty()
        {
            bail!("bilibili.auth.credential_profile must not be empty when set");
        }
        ensure_auth_state_path_is_not_symlink(&self.bilibili.auth.state_path)?;
        ensure_distinct_auth_paths(
            &self.bilibili.auth.state_path,
            &self.bilibili.auth.credential_file,
        )?;
        if let Some(playurl_mode) = &self.bilibili.playurl_mode
            && !matches!(playurl_mode.as_str(), "web" | "tv" | "app")
        {
            bail!("bilibili.playurl_mode must be one of web, tv, or app");
        }
        if let Some(area) = &self.bilibili.restricted_area
            && !matches!(area.as_str(), "cn" | "th" | "hk" | "tw")
        {
            bail!("bilibili.restricted_area must be one of cn, th, hk, or tw");
        }
        if self.bilibili.danmaku_formats.is_empty() {
            bail!("bilibili.danmaku_formats must not be empty");
        }
        for format in &self.bilibili.danmaku_formats {
            if !matches!(format.as_str(), "xml" | "ass") {
                bail!("bilibili.danmaku_formats entries must be xml or ass");
            }
        }
        Ok(())
    }
}

fn ensure_auth_state_path_is_not_symlink(state_path: &Path) -> Result<()> {
    match fs::symlink_metadata(state_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => bail!(
            "bilibili.auth.state_path must not be a symbolic link: {}",
            state_path.display()
        ),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to inspect bilibili.auth.state_path: {}",
                state_path.display()
            )
        }),
    }
}

fn ensure_distinct_auth_paths(state_path: &Path, credential_file: &Path) -> Result<()> {
    let normalized_state = normalize_path_for_comparison(state_path)?;
    let normalized_credential = normalize_path_for_comparison(credential_file)?;
    let cleanup_files = legacy_auth_cleanup_files(state_path);
    let cleanup_dir = legacy_auth_cleanup_dir(state_path);
    let normalized_cleanup_dir = normalize_path_for_comparison(&cleanup_dir)?;
    let aliases_cleanup_file = cleanup_files.iter().try_fold(false, |aliased, path| {
        Ok::<_, anyhow::Error>(
            aliased
                || auth_paths_equal(
                    &normalize_path_for_comparison(path)?,
                    &normalized_credential,
                )
                || existing_paths_share_identity(path, credential_file)?,
        )
    })?;
    let inside_cleanup_dir = auth_path_starts_with(&normalized_credential, &normalized_cleanup_dir);
    let aliases_cleanup_dir_entry =
        existing_file_alias_in_directory(credential_file, &cleanup_dir)?;
    if auth_paths_equal(&normalized_state, &normalized_credential)
        || aliases_cleanup_file
        || inside_cleanup_dir
        || aliases_cleanup_dir_entry
    {
        bail!(
            "bilibili.auth.state_path and bilibili.auth.credential_file must refer to distinct files, and credential_file must be outside legacy auth cleanup paths"
        );
    }
    Ok(())
}

fn legacy_auth_cleanup_files(state_path: &Path) -> [PathBuf; 3] {
    [
        state_path.to_path_buf(),
        path_with_suffix(state_path, ".bbdown.config"),
        path_with_suffix(state_path, ".bbdown.config.json"),
    ]
}

fn legacy_auth_cleanup_dir(state_path: &Path) -> PathBuf {
    path_with_suffix(state_path, ".bbdown.config.d")
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn existing_file_alias_in_directory(file: &Path, directory: &Path) -> Result<bool> {
    if metadata_if_present(file)?.is_none() {
        return Ok(false);
    }
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to inspect legacy auth cleanup directory {}",
                    directory.display()
                )
            });
        }
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_file() && existing_paths_share_identity(file, &entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn normalize_path_for_comparison(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to resolve the current directory for auth path validation")?
            .join(path)
    };
    let mut ancestor = absolute.clone();
    let mut missing = Vec::new();
    loop {
        match fs::canonicalize(&ancestor) {
            Ok(mut canonical) => {
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                return Ok(lexical_normalize(&canonical));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let name = ancestor.file_name().with_context(|| {
                    format!(
                        "failed to find an existing ancestor for auth path {}",
                        path.display()
                    )
                })?;
                missing.push(name.to_os_string());
                ancestor.pop();
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to resolve auth path for comparison: {}",
                        path.display()
                    )
                });
            }
        }
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                normalized.push(component.as_os_str());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(name) => normalized.push(name),
        }
    }
    normalized
}

fn auth_paths_equal(first: &Path, second: &Path) -> bool {
    let mut first = first.components();
    let mut second = second.components();
    loop {
        match (first.next(), second.next()) {
            (Some(first), Some(second)) if auth_path_components_equal(first, second) => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn auth_path_starts_with(path: &Path, prefix: &Path) -> bool {
    let mut path = path.components();
    prefix.components().all(|prefix_component| {
        path.next()
            .is_some_and(|component| auth_path_components_equal(component, prefix_component))
    })
}

fn auth_path_components_equal(
    first: std::path::Component<'_>,
    second: std::path::Component<'_>,
) -> bool {
    if first == second {
        return true;
    }

    match (first.as_os_str().to_str(), second.as_os_str().to_str()) {
        (Some(first), Some(second)) => {
            auth_path_component_comparison_key(first) == auth_path_component_comparison_key(second)
        }
        _ => false,
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn auth_path_component_comparison_key(value: &str) -> String {
    value.nfd().flat_map(char::to_lowercase).collect()
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn auth_path_component_comparison_key(value: &str) -> String {
    value.to_lowercase()
}

fn existing_paths_share_identity(first: &Path, second: &Path) -> Result<bool> {
    let first_metadata = metadata_if_present(first)?;
    let second_metadata = metadata_if_present(second)?;
    let (Some(first_metadata), Some(second_metadata)) = (first_metadata, second_metadata) else {
        return Ok(false);
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Ok(first_metadata.dev() == second_metadata.dev()
            && first_metadata.ino() == second_metadata.ino())
    }
    #[cfg(not(unix))]
    {
        Ok(false)
    }
}

fn metadata_if_present(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)
            .with_context(|| format!("failed to inspect auth path identity: {}", path.display())),
    }
}

impl TelegramConfig {
    pub fn is_chat_allowed(&self, chat_id: i64) -> bool {
        self.allow_all_chats || self.allowed_chat_ids.contains(&chat_id)
    }
}

impl Default for DownloadsConfig {
    fn default() -> Self {
        Self {
            video_dir: default_video_dir(),
            pdf_dir: default_pdf_dir(),
        }
    }
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            bbdown: default_bbdown(),
            yt_dlp: default_yt_dlp(),
            uv: default_uv(),
            pdf_helper: default_pdf_helper(),
            chrome: default_chrome(),
            ffmpeg: default_ffmpeg(),
        }
    }
}

impl Default for PdfConfig {
    fn default() -> Self {
        Self {
            auto_domains: default_auto_pdf_domains(),
        }
    }
}

impl Default for VideoConfig {
    fn default() -> Self {
        Self {
            subtitle_languages: default_subtitle_languages(),
            write_nfo: true,
            keep_sidecars: true,
        }
    }
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            poll_timeout_seconds: default_poll_timeout_seconds(),
            progress_update_seconds: default_progress_update_seconds(),
            command_timeout_seconds: default_command_timeout_seconds(),
            command_idle_timeout_seconds: default_command_idle_timeout_seconds(),
        }
    }
}

impl Default for BilibiliConfig {
    fn default() -> Self {
        Self {
            extra_args: Vec::new(),
            global_args: Vec::new(),
            plan_args: Vec::new(),
            download_args: Vec::new(),
            playurl_mode: None,
            restricted_area: None,
            restricted_area_proxies: Vec::new(),
            restricted_api_proxies: Vec::new(),
            danmaku_formats: default_bilibili_danmaku_formats(),
            danmaku: BilibiliDanmakuConfig::default(),
            auth: BilibiliAuthConfig::default(),
        }
    }
}

impl Default for BilibiliDanmakuConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl Default for BilibiliAuthConfig {
    fn default() -> Self {
        Self {
            state_path: default_bilibili_auth_state_path(),
            credential_file: default_bilibili_credential_file(),
            credential_profile: None,
            login_timeout_seconds: default_bilibili_login_timeout_seconds(),
            poll_interval_seconds: default_bilibili_poll_interval_seconds(),
        }
    }
}

fn default_video_dir() -> PathBuf {
    home_path(&["Movies", "Downloads"], "video-downloads")
}

fn default_pdf_dir() -> PathBuf {
    home_path(&["Documents", "Downloads"], "pdf-downloads")
}

fn default_bbdown() -> PathBuf {
    PathBuf::from("bbdown")
}

fn default_yt_dlp() -> PathBuf {
    PathBuf::from("yt-dlp")
}

fn default_uv() -> PathBuf {
    PathBuf::from("uv")
}

fn default_pdf_helper() -> PathBuf {
    PathBuf::from("scripts/pdf_helper.py")
}

fn default_chrome() -> PathBuf {
    PathBuf::from("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
}

fn default_ffmpeg() -> PathBuf {
    PathBuf::from("ffmpeg")
}

fn home_path(parts: &[&str], fallback: &str) -> PathBuf {
    let Some(mut path) = home_dir() else {
        return PathBuf::from(fallback);
    };

    for part in parts {
        path.push(part);
    }
    path
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
}

fn expand_home_path(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(home) = home_dir() else {
        return path.to_path_buf();
    };

    let value = text.as_ref();
    if matches!(value, "~" | "$HOME" | "${HOME}") {
        return home;
    }

    for prefix in ["~/", "$HOME/", "${HOME}/"] {
        if let Some(suffix) = value.strip_prefix(prefix) {
            return home.join(suffix);
        }
    }

    path.to_path_buf()
}

fn default_concurrency() -> usize {
    2
}

fn default_poll_timeout_seconds() -> u64 {
    50
}

fn default_progress_update_seconds() -> u64 {
    5
}

fn default_command_timeout_seconds() -> u64 {
    7200
}

fn default_command_idle_timeout_seconds() -> u64 {
    300
}

fn default_auto_pdf_domains() -> Vec<String> {
    vec!["mp.weixin.qq.com".to_string()]
}

fn default_subtitle_languages() -> Vec<String> {
    ["zh-Hans", "zh-Hant", "zh", "zh-CN", "zh-TW", "en", "ja"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_true() -> bool {
    true
}

fn default_bilibili_danmaku_formats() -> Vec<String> {
    vec!["xml".to_string(), "ass".to_string()]
}

fn default_bilibili_auth_state_path() -> PathBuf {
    home_path(
        &[
            ".local",
            "state",
            "telegram-video-downloader",
            "bilibili-auth.json",
        ],
        "bilibili-auth.json",
    )
}

fn default_bilibili_credential_file() -> PathBuf {
    home_path(
        &[
            ".local",
            "state",
            "telegram-video-downloader",
            "bbdown-credentials.json",
        ],
        "bbdown-credentials.json",
    )
}

fn default_bilibili_login_timeout_seconds() -> u64 {
    180
}

fn default_bilibili_poll_interval_seconds() -> u64 {
    2
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;

    struct CurrentDirGuard {
        original: PathBuf,
    }

    impl CurrentDirGuard {
        fn change_to(path: &Path) -> Self {
            let original = env::current_dir().expect("current dir should be available");
            env::set_current_dir(path).expect("current dir should change");
            Self { original }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.original);
        }
    }

    fn temp_test_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        env::temp_dir().join(format!("telegram-video-downloader-config-{label}-{nanos}"))
    }

    #[test]
    fn loads_defaults() {
        let home = home_dir().expect("HOME should be set during tests");
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.downloads.video_dir,
            home.join("Movies").join("Downloads")
        );
        assert_eq!(
            config.downloads.pdf_dir,
            home.join("Documents").join("Downloads")
        );
        assert_eq!(config.tools.bbdown, PathBuf::from("bbdown"));
        assert_eq!(config.tools.yt_dlp, PathBuf::from("yt-dlp"));
        assert_eq!(
            config.tools.pdf_helper,
            PathBuf::from("scripts/pdf_helper.py")
        );
        assert_eq!(config.tools.ffmpeg, PathBuf::from("ffmpeg"));
        assert_eq!(config.bot.concurrency, 2);
        assert_eq!(config.bot.poll_timeout_seconds, 50);
        assert_eq!(config.bot.progress_update_seconds, 5);
        assert_eq!(config.bot.command_timeout_seconds, 7200);
        assert_eq!(config.bot.command_idle_timeout_seconds, 300);
        assert_eq!(config.pdf.auto_domains, vec!["mp.weixin.qq.com"]);
        assert_eq!(
            config.video.subtitle_languages,
            vec!["zh-Hans", "zh-Hant", "zh", "zh-CN", "zh-TW", "en", "ja"]
        );
        assert!(config.video.write_nfo);
        assert!(config.video.keep_sidecars);
        assert!(config.bilibili.extra_args.is_empty());
        assert!(config.bilibili.global_args.is_empty());
        assert!(config.bilibili.plan_args.is_empty());
        assert!(config.bilibili.download_args.is_empty());
        assert_eq!(config.bilibili.playurl_mode, None);
        assert_eq!(config.bilibili.restricted_area, None);
        assert!(config.bilibili.restricted_area_proxies.is_empty());
        assert!(config.bilibili.restricted_api_proxies.is_empty());
        assert_eq!(config.bilibili.danmaku_formats, vec!["xml", "ass"]);
        assert!(config.bilibili.danmaku.enabled);
        assert_eq!(
            config.bilibili.auth.state_path,
            home.join(".local")
                .join("state")
                .join("telegram-video-downloader")
                .join("bilibili-auth.json")
        );
        assert_eq!(
            config.bilibili.auth.credential_file,
            home.join(".local")
                .join("state")
                .join("telegram-video-downloader")
                .join("bbdown-credentials.json")
        );
        assert_eq!(config.bilibili.auth.credential_profile, None);
        assert_eq!(config.bilibili.auth.login_timeout_seconds, 180);
        assert_eq!(config.bilibili.auth.poll_interval_seconds, 2);
    }

    #[test]
    fn preserves_explicit_bilibili_multi_thread_setting() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili]
            extra_args = ["--video-ascending", "--skip-mux", "--multi-thread", "true"]
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.bilibili.extra_args,
            vec!["--video-ascending", "--skip-mux", "--multi-thread", "true"]
        );
    }

    #[test]
    fn rejects_zero_concurrency() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bot]
            concurrency = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero concurrency should fail");

        assert!(err.to_string().contains("bot.concurrency"));
    }

    #[test]
    fn rejects_zero_command_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bot]
            command_timeout_seconds = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero command timeout should fail");

        assert!(err.to_string().contains("bot.command_timeout_seconds"));
    }

    #[test]
    fn rejects_zero_bilibili_auth_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            login_timeout_seconds = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero auth timeout should fail");

        assert!(
            err.to_string()
                .contains("bilibili.auth.login_timeout_seconds")
        );
    }

    #[test]
    fn rejects_bilibili_auth_poll_interval_at_or_above_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            login_timeout_seconds = 5
            poll_interval_seconds = 5
            "#,
            PathBuf::from("."),
        )
        .expect_err("slow auth polling should fail");

        assert!(
            err.to_string()
                .contains("bilibili.auth.poll_interval_seconds")
        );
    }

    #[test]
    fn loads_disabled_bilibili_danmaku_config() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.danmaku]
            enabled = false
            "#,
            PathBuf::from("."),
        )
        .expect("disabled danmaku config should parse");

        assert!(!config.bilibili.danmaku.enabled);
    }

    #[test]
    fn resolves_relative_project_path() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.resolve_project_path(Path::new("scripts/pdf_helper.py")),
            PathBuf::from("/tmp/project/scripts/pdf_helper.py")
        );
    }

    #[test]
    fn resolves_relative_bilibili_auth_state_path_to_project_dir() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            state_path = "state/bilibili-auth.json"
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.bilibili.auth.state_path,
            PathBuf::from("/tmp/project/state/bilibili-auth.json")
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_bilibili_auth_state_path_leaf_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_test_dir("auth-state-leaf-symlink");
        fs::create_dir_all(&root).expect("auth root should create");
        let target = root.join("legacy-auth-target.json");
        let state = root.join("bilibili-auth.json");
        fs::write(&target, b"legacy-auth").expect("legacy auth target should write");
        symlink(&target, &state).expect("legacy auth symlink should create");
        let config = format!(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            state_path = "{}"
            credential_file = "{}"
            "#,
            state.display(),
            root.join("credentials.json").display()
        );

        let error = AppConfig::from_toml_str(&config, root.clone())
            .expect_err("leaf symlink auth state should fail validation");

        assert!(
            error
                .to_string()
                .contains("bilibili.auth.state_path must not be a symbolic link")
        );
        assert!(target.is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_relative_and_absolute_auth_path_aliases() {
        let root = temp_test_dir("auth-relative-absolute-alias");
        fs::create_dir_all(root.join("auth")).expect("auth dir should create");
        let credential = root.join("auth/shared.json");
        let config = format!(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            state_path = "auth/shared.json"
            credential_file = "{}"
            "#,
            credential.display()
        );

        let error = AppConfig::from_toml_str(&config, root.clone())
            .expect_err("aliased auth paths should fail validation");

        assert!(error.to_string().contains("must refer to distinct files"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_case_only_missing_auth_path_aliases() {
        let root = temp_test_dir("auth-case-only-alias");
        fs::create_dir_all(&root).expect("auth root should create");
        let state = root.join("Auth.json");
        let credential = root.join("auth.json");

        let error = ensure_distinct_auth_paths(&state, &credential)
            .expect_err("case-only auth paths should fail validation");

        assert!(error.to_string().contains("must refer to distinct files"));
        assert!(!state.exists());
        assert!(!credential.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn rejects_unicode_normalization_equivalent_missing_auth_paths() {
        let root = temp_test_dir("auth-unicode-normalization-alias");
        fs::create_dir_all(&root).expect("auth root should create");
        let state = root.join("caf\u{e9}.json");
        let credential = root.join("cafe\u{301}.json");

        let error = ensure_distinct_auth_paths(&state, &credential)
            .expect_err("normalization-equivalent auth paths should fail validation");

        assert!(error.to_string().contains("must refer to distinct files"));
        assert!(!state.exists());
        assert!(!credential.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_auth_paths_through_symlinked_parent_aliases() {
        use std::os::unix::fs::symlink;

        let root = temp_test_dir("auth-symlink-parent-alias");
        let real = root.join("real");
        fs::create_dir_all(&real).expect("real auth dir should create");
        symlink(&real, root.join("alias")).expect("auth dir alias should create");

        let error = ensure_distinct_auth_paths(
            &real.join("credentials.json"),
            &root.join("alias/credentials.json"),
        )
        .expect_err("symlinked parent aliases should fail validation");

        assert!(error.to_string().contains("must refer to distinct files"));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_missing_auth_alias_after_symlink_and_parent_components() {
        use std::os::unix::fs::symlink;

        let root = temp_test_dir("auth-symlink-parent-components");
        let safe = root.join("safe");
        let alias = root.join("alias");
        fs::create_dir_all(safe.join("child")).expect("symlink target should create");
        fs::create_dir_all(&alias).expect("alias directory should create");
        symlink(safe.join("child"), alias.join("jump")).expect("path alias should create");
        let state = safe.join("auth.json");
        let credential = alias.join("jump/../auth.json");

        let error = ensure_distinct_auth_paths(&state, &credential)
            .expect_err("filesystem-resolved parent components should expose the alias");

        assert!(error.to_string().contains("must refer to distinct files"));
        assert!(!state.exists());
        assert!(!credential.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_existing_auth_paths_with_the_same_inode() {
        let root = temp_test_dir("auth-hardlink-alias");
        fs::create_dir_all(&root).expect("auth root should create");
        let state = root.join("state.json");
        let credential = root.join("credentials.json");
        fs::write(&state, "{}").expect("auth state should write");
        fs::hard_link(&state, &credential).expect("auth hard link should create");

        let error = ensure_distinct_auth_paths(&state, &credential)
            .expect_err("same-inode auth paths should fail validation");

        assert!(error.to_string().contains("must refer to distinct files"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_credential_file_at_legacy_cleanup_paths() {
        let root = temp_test_dir("auth-legacy-cleanup-paths");
        fs::create_dir_all(&root).expect("auth root should create");
        let state = root.join("state.json");

        for credential in [
            path_with_suffix(&state, ".bbdown.config"),
            path_with_suffix(&state, ".bbdown.config.json"),
            path_with_suffix(&state, ".bbdown.config.d").join("credentials.json"),
        ] {
            let error = ensure_distinct_auth_paths(&state, &credential)
                .expect_err("legacy cleanup target must not hold credentials");
            assert!(error.to_string().contains("legacy auth cleanup paths"));
        }

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_credential_hardlink_to_legacy_cleanup_directory_entry() {
        let root = temp_test_dir("auth-legacy-cleanup-hardlink");
        let state = root.join("state.json");
        let cleanup_dir = path_with_suffix(&state, ".bbdown.config.d");
        fs::create_dir_all(&cleanup_dir).expect("cleanup directory should create");
        let stale = cleanup_dir.join("stale.config");
        let credential = root.join("credentials.json");
        fs::write(&stale, "secret").expect("stale file should write");
        fs::hard_link(&stale, &credential).expect("credential hard link should create");

        let error = ensure_distinct_auth_paths(&state, &credential)
            .expect_err("cleanup-directory hard link must be rejected");

        assert!(error.to_string().contains("legacy auth cleanup paths"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn load_resolves_relative_config_and_auth_state_to_absolute_paths() {
        let root = temp_test_dir("relative-load");
        fs::create_dir_all(&root).expect("temp config dir should be created");
        fs::write(
            root.join("config.toml"),
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [downloads]
            video_dir = "relative-videos"
            pdf_dir = "relative-pdfs"

            [bilibili.auth]
            state_path = "state/bilibili-auth.json"
            "#,
        )
        .expect("config should be written");
        let expected_root = fs::canonicalize(&root).expect("temp config dir should canonicalize");
        let guard = CurrentDirGuard::change_to(&root);

        let config = AppConfig::load(Path::new("config.toml")).expect("config should load");

        assert!(config.bilibili.auth.state_path.is_absolute());
        assert_eq!(
            config.downloads.video_dir,
            expected_root.join("relative-videos")
        );
        assert_eq!(
            config.downloads.pdf_dir,
            expected_root.join("relative-pdfs")
        );
        assert_eq!(
            config.bilibili.auth.state_path,
            expected_root.join("state/bilibili-auth.json")
        );
        drop(guard);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn expands_home_paths() {
        let home = home_dir().expect("HOME should be set during tests");
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [downloads]
            video_dir = "~/Movies/Bot"
            pdf_dir = "$HOME/Documents/Bot"

            [tools]
            bbdown = "${HOME}/.dotnet/tools/BBDown"

            [bilibili.auth]
            state_path = "~/Library/Application Support/Bot/bilibili-auth.json"
            credential_file = "$HOME/Library/Application Support/Bot/bbdown-credentials.json"
            "#,
            PathBuf::from("."),
        )
        .expect("config should parse");

        assert_eq!(config.downloads.video_dir, home.join("Movies").join("Bot"));
        assert_eq!(config.downloads.pdf_dir, home.join("Documents").join("Bot"));
        assert_eq!(
            config.tools.bbdown,
            home.join(".dotnet").join("tools").join("BBDown")
        );
        assert_eq!(
            config.bilibili.auth.state_path,
            home.join("Library")
                .join("Application Support")
                .join("Bot")
                .join("bilibili-auth.json")
        );
        assert_eq!(
            config.bilibili.auth.credential_file,
            home.join("Library")
                .join("Application Support")
                .join("Bot")
                .join("bbdown-credentials.json")
        );
    }

    #[test]
    fn requires_chat_allowlist_by_default() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            "#,
            PathBuf::from("."),
        )
        .expect_err("missing allowlist should fail");

        assert!(err.to_string().contains("telegram.allowed_chat_ids"));
    }

    #[test]
    fn checks_allowed_chat_ids() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allowed_chat_ids = [10, 20]
            "#,
            PathBuf::from("."),
        )
        .expect("config should parse");

        assert!(config.telegram.is_chat_allowed(10));
        assert!(!config.telegram.is_chat_allowed(30));
    }
}
