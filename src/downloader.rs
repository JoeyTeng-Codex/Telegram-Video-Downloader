#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use bbdown_core::{
    DownloadFileKind, DownloadMode, DownloadProgressEvent, DownloadProgressSink, DownloadReport,
};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{Instant, sleep_until, timeout as tokio_timeout};
use tracing::info;

use crate::bilibili_auth;
use crate::bilibili_core;
use crate::config::AppConfig;
use crate::router::{BilibiliSelection, JobRequest};

static VIDEO_OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(1);
static OVERWRITE_BACKUP_COUNTER: AtomicU64 = AtomicU64::new(1);
const VIDEO_STAGING_DIR_NAME: &str = ".telegram-video-downloader-staging";
const BILIBILI_FFMPEG_CONCAT_FILE_PREFIX: &str = ".telegram-video-downloader-ffmpeg-concat";
const OVERWRITE_BACKUP_DIR_PREFIX: &str = ".telegram-video-downloader-overwrite";
const VIDEO_SIDECAR_EXTENSIONS: &[&str] = &[
    "nfo",
    "json",
    "description",
    "jpg",
    "jpeg",
    "png",
    "webp",
    "srt",
    "vtt",
    "ass",
    "xml",
];
const OUTPUT_CLOSE_GRACE: Duration = Duration::from_secs(2);
const OUTPUT_ABORT_GRACE: Duration = Duration::from_secs(3);
const BILIBILI_METADATA_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
const BILIBILI_METADATA_PROBE_AFTER_DUPLICATE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(unix)]
type CommandProcessGroup = Option<libc::pid_t>;
#[cfg(not(unix))]
type CommandProcessGroup = Option<()>;

#[derive(Debug, Clone)]
pub struct JobReport {
    pub saved_location: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgress {
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoDuplicateAction {
    Overwrite,
    KeepBoth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoDuplicate {
    pub identity: VideoIdentity,
    pub existing_videos: Vec<PathBuf>,
}

impl VideoDuplicate {
    pub fn describe_existing_videos(&self, limit: usize) -> String {
        let mut lines = self
            .existing_videos
            .iter()
            .take(limit)
            .map(|path| format!("- {}", path.display()))
            .collect::<Vec<_>>();
        if self.existing_videos.len() > limit {
            lines.push(format!(
                "- ... and {} more",
                self.existing_videos.len() - limit
            ));
        }
        lines.join("\n")
    }

    pub fn allows_overwrite_for(&self, job: &JobRequest) -> bool {
        if self.overwrite_target().is_none() {
            return false;
        }

        match (job, self.identity.provider) {
            (JobRequest::Youtube { .. }, VideoProvider::Youtube) => true,
            (JobRequest::Bilibili { selection, .. }, VideoProvider::Bilibili) => {
                !matches!(selection, Some(BilibiliSelection::All))
                    && is_bilibili_entry_identity(&self.identity.id)
                    && self.existing_videos.first().is_some_and(|video| {
                        metadata_sidecars_match_identity(video, &self.identity)
                    })
            }
            _ => false,
        }
    }

    fn overwrite_target(&self) -> Option<&PathBuf> {
        (self.existing_videos.len() == 1
            && (self.identity.provider == VideoProvider::Youtube
                || is_bilibili_entry_identity(&self.identity.id)))
        .then(|| &self.existing_videos[0])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VideoIdentity {
    pub provider: VideoProvider,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum VideoProvider {
    Bilibili,
    Youtube,
}

impl VideoProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bilibili => "bilibili",
            Self::Youtube => "youtube",
        }
    }
}

fn is_bilibili_entry_identity(id: &str) -> bool {
    ["cid", "ep"].into_iter().any(|prefix| {
        id.strip_prefix(prefix).is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedPrimaryMediaKind {
    Video,
    VideoOrAudio,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub activity_dir: Option<PathBuf>,
    pub cleanup_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubtitleSource {
    Manual,
    Automatic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtitlePlan {
    pub source: Option<SubtitleSource>,
    pub languages: Vec<String>,
}

impl SubtitlePlan {
    fn none() -> Self {
        Self {
            source: None,
            languages: Vec::new(),
        }
    }

    fn describe(&self) -> String {
        match &self.source {
            Some(SubtitleSource::Manual) => {
                format!("Subtitles: manual {}", self.languages.join(","))
            }
            Some(SubtitleSource::Automatic) => {
                format!("Subtitles: automatic {}", self.languages.join(","))
            }
            None => "Subtitles: no preferred subtitles found".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct YoutubeMetadata {
    id: Option<String>,
    title: Option<String>,
    description: Option<String>,
    uploader: Option<String>,
    channel: Option<String>,
    upload_date: Option<String>,
    webpage_url: Option<String>,
    #[serde(default)]
    subtitles: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    automatic_captions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
struct BilibiliMetadata {
    title: Option<String>,
    uploader_url: Option<String>,
    publish_date: Option<String>,
    id: Option<String>,
    resolved_id: Option<String>,
    aid: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliDownloadPlan {
    title: String,
    #[serde(default)]
    entries: Vec<BilibiliDownloadEntry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliDownloadEntry {
    index: u32,
    aid: u64,
    bvid: Option<String>,
    cid: u64,
    epid: Option<u64>,
    title: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliDownloadReport {
    title: String,
    output_dir: PathBuf,
    #[serde(default)]
    entries: Vec<BilibiliEntryDownloadReport>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliEntryDownloadReport {
    index: u32,
    title: String,
    #[serde(default)]
    files: Vec<BilibiliDownloadedFile>,
    mux: Option<BilibiliMuxReport>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliDownloadedFile {
    kind: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BilibiliMuxReport {
    output_path: PathBuf,
}

impl From<&bbdown_core::DownloadPlan> for BilibiliDownloadPlan {
    fn from(plan: &bbdown_core::DownloadPlan) -> Self {
        Self {
            title: plan.title.clone(),
            entries: plan
                .entries
                .iter()
                .map(|entry| BilibiliDownloadEntry {
                    index: entry.index,
                    aid: entry.aid,
                    bvid: entry.bvid.clone(),
                    cid: entry.cid,
                    epid: entry.epid,
                    title: entry.title.clone(),
                })
                .collect(),
        }
    }
}

impl From<&DownloadReport> for BilibiliDownloadReport {
    fn from(report: &DownloadReport) -> Self {
        Self {
            title: report.title.clone(),
            output_dir: report.output_dir.clone(),
            entries: report
                .entries
                .iter()
                .map(|entry| BilibiliEntryDownloadReport {
                    index: entry.index,
                    title: entry.title.clone(),
                    files: entry
                        .files
                        .iter()
                        .map(|file| BilibiliDownloadedFile {
                            kind: bilibili_file_kind(&file.kind).to_string(),
                            path: file.path.clone(),
                        })
                        .collect(),
                    mux: entry.mux.as_ref().map(|mux| BilibiliMuxReport {
                        output_path: mux.output_path.clone(),
                    }),
                })
                .collect(),
        }
    }
}

pub async fn run_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    match job {
        JobRequest::Bilibili { url, selection } => {
            run_bilibili_job(config, url, *selection, progress).await
        }
        JobRequest::Youtube { url } => run_youtube_job(config, url, progress).await,
        JobRequest::Pdf { .. } => run_simple_job(config, job, progress).await,
    }
}

pub async fn run_job_with_duplicate_action(
    config: &AppConfig,
    job: &JobRequest,
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    if !matches!(
        job,
        JobRequest::Bilibili { .. } | JobRequest::Youtube { .. }
    ) {
        return run_job(config, job, progress).await;
    }
    if matches!(action, VideoDuplicateAction::Overwrite) && !duplicate.allows_overwrite_for(job) {
        bail!("overwrite requires one exact video or Bilibili entry match");
    }

    run_staged_video_job(config, job, action, duplicate, progress).await
}

pub async fn run_video_job_staged_keep_both(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let Some(identity) = fallback_video_identity(job) else {
        return run_job(config, job, progress).await;
    };
    let duplicate = VideoDuplicate {
        identity,
        existing_videos: Vec::new(),
    };
    run_staged_video_job(
        config,
        job,
        VideoDuplicateAction::KeepBoth,
        &duplicate,
        progress,
    )
    .await
}

pub async fn find_video_duplicate_with_probe(
    config: &AppConfig,
    job: &JobRequest,
) -> Result<Option<VideoDuplicate>> {
    let index = scan_video_identity_index(config, job).await?;
    let mut identities = video_identity(job).into_iter().collect::<Vec<_>>();
    let direct_overwrite_identities = identities
        .iter()
        .filter(|identity| identity_is_overwrite_safe(identity))
        .cloned()
        .collect::<Vec<_>>();
    let direct_duplicate =
        find_video_duplicate_in_index(&index, &identities, &direct_overwrite_identities);
    let mut overwrite_identities = Vec::new();

    if let JobRequest::Bilibili { url, selection } = job {
        let probe_timeout = if direct_duplicate.is_some() {
            BILIBILI_METADATA_PROBE_AFTER_DUPLICATE_TIMEOUT
        } else {
            BILIBILI_METADATA_PROBE_TIMEOUT
        };
        match probe_bilibili_plan(config, url, *selection, probe_timeout).await {
            Ok(plan) => {
                push_bilibili_plan_identities(&mut identities, &plan);
                overwrite_identities = bilibili_plan_overwrite_identities(&plan);
            }
            Err(err) if identities.is_empty() && direct_duplicate.is_none() => {
                return Err(err).with_context(|| {
                    format!("failed to probe Bilibili plan for duplicate check: {url}")
                });
            }
            Err(err) => {
                info!(
                    url = %url,
                    error = %err,
                    "Bilibili plan probe skipped during duplicate check"
                );
            }
        }
    } else {
        return Ok(direct_duplicate);
    }

    match find_video_duplicate_in_index(&index, &identities, &overwrite_identities) {
        Some(duplicate) => Ok(Some(duplicate)),
        None => Ok(direct_duplicate),
    }
}

async fn scan_video_identity_index(
    config: &AppConfig,
    job: &JobRequest,
) -> Result<VideoIdentityIndex> {
    let scan_config = config.clone();
    let scan_job = job.clone();
    tokio::task::spawn_blocking(move || build_video_identity_index(&scan_config, &scan_job))
        .await
        .context("duplicate scan task failed")?
}

fn find_video_duplicate_for_identities(
    config: &AppConfig,
    job: &JobRequest,
    identities: Vec<VideoIdentity>,
) -> Result<Option<VideoDuplicate>> {
    if identities.is_empty() {
        return Ok(None);
    }

    let index = build_video_identity_index(config, job)?;
    Ok(find_video_duplicate_in_index(&index, &identities, &[]))
}

#[derive(Debug, Default)]
struct VideoIdentityIndex {
    videos_by_identity: BTreeMap<VideoIdentity, Vec<PathBuf>>,
    overwrite_videos_by_identity: BTreeMap<VideoIdentity, Vec<PathBuf>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentityIndexReadPolicy {
    BestEffort,
    Strict,
}

impl VideoIdentityIndex {
    fn insert(&mut self, identity: VideoIdentity, video: &Path) {
        insert_identity_path(&mut self.videos_by_identity, identity, video);
    }

    fn insert_overwrite_evidence(&mut self, identity: VideoIdentity, video: &Path) {
        self.insert(identity.clone(), video);
        insert_identity_path(&mut self.overwrite_videos_by_identity, identity, video);
    }

    fn videos(&self, identity: &VideoIdentity) -> &[PathBuf] {
        identity_paths(&self.videos_by_identity, identity)
    }

    fn overwrite_videos(&self, identity: &VideoIdentity) -> &[PathBuf] {
        identity_paths(&self.overwrite_videos_by_identity, identity)
    }
}

fn insert_identity_path(
    index: &mut BTreeMap<VideoIdentity, Vec<PathBuf>>,
    identity: VideoIdentity,
    video: &Path,
) {
    let videos = index.entry(identity).or_default();
    if !videos.iter().any(|existing| existing == video) {
        videos.push(video.to_path_buf());
    }
}

fn identity_paths<'a>(
    index: &'a BTreeMap<VideoIdentity, Vec<PathBuf>>,
    identity: &VideoIdentity,
) -> &'a [PathBuf] {
    index.get(identity).map(Vec::as_slice).unwrap_or_default()
}

fn build_video_identity_index(config: &AppConfig, job: &JobRequest) -> Result<VideoIdentityIndex> {
    let primary_media_kind = staged_primary_media_kind(config, job)?;
    build_video_identity_index_in_dir(
        &duplicate_scan_video_dir(config, job),
        primary_media_kind,
        IdentityIndexReadPolicy::BestEffort,
    )
}

fn build_video_identity_index_in_dir(
    root: &Path,
    primary_media_kind: StagedPrimaryMediaKind,
    read_policy: IdentityIndexReadPolicy,
) -> Result<VideoIdentityIndex> {
    let media_files = list_primary_media_files(root, primary_media_kind)?;
    let mut index = VideoIdentityIndex::default();
    for video in media_files {
        index_video_identities(&mut index, &video, read_policy)?;
    }
    Ok(index)
}

fn find_video_duplicate_in_index(
    index: &VideoIdentityIndex,
    identities: &[VideoIdentity],
    overwrite_identities: &[VideoIdentity],
) -> Option<VideoDuplicate> {
    let mut exact_identity = None;
    let mut exact_videos = BTreeSet::new();
    for identity in overwrite_identities {
        let videos = index.overwrite_videos(identity);
        if !videos.is_empty() && exact_identity.is_none() {
            exact_identity = Some(identity.clone());
        }
        for video in videos {
            exact_videos.insert(video.clone());
        }
    }
    if exact_videos.len() == 1
        && let Some(identity) = exact_identity
    {
        return Some(VideoDuplicate {
            identity,
            existing_videos: exact_videos.into_iter().collect(),
        });
    }

    let mut matched_identity = None;
    let mut seen_videos = BTreeSet::new();
    let mut existing_videos = Vec::new();
    for identity in identities {
        let videos = index.videos(identity);
        for video in videos {
            if seen_videos.insert(video.clone()) {
                existing_videos.push(video.clone());
            }
        }
        if !videos.is_empty() && matched_identity.is_none() {
            matched_identity = Some(identity.clone());
        }
    }

    matched_identity.map(|identity| VideoDuplicate {
        identity,
        existing_videos,
    })
}

fn duplicate_scan_video_dir(config: &AppConfig, _job: &JobRequest) -> PathBuf {
    config.downloads.video_dir.clone()
}

async fn run_simple_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let spec = command_spec(config, job)?;
    let output = run_command(config, &spec, progress.clone()).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let saved_location = last_nonempty_line(&stdout)
        .ok_or_else(|| anyhow!("pdf helper finished without printing output path"))?
        .to_string();
    Ok(JobReport {
        saved_location,
        details: tail_lines(&stderr, 6),
    })
}

async fn run_bilibili_job(
    config: &AppConfig,
    url: &str,
    selection: Option<BilibiliSelection>,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let _guard = video_output_lock("Bilibili download", progress.as_ref()).await;
    run_bilibili_job_locked(config, url, selection, progress).await
}

async fn run_bilibili_job_locked(
    config: &AppConfig,
    url: &str,
    selection: Option<BilibiliSelection>,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    sync_bilibili_rust_credentials(config)?;
    let mut options = bilibili_core::download_options(config)?;
    let mux_locally = matches!(options.mode, DownloadMode::All)
        && matches!(options.mux, bbdown_core::MuxOptions::Ffmpeg { .. });
    if mux_locally {
        options = options.with_mux(bbdown_core::MuxOptions::Disabled);
    }
    let client = bilibili_core::client(config)?;
    let core_plan = probe_bilibili_plan_with_mode(
        &client,
        url,
        selection,
        options.mode,
        BILIBILI_METADATA_PROBE_TIMEOUT,
    )
    .await?;
    let plan = BilibiliDownloadPlan::from(&core_plan);
    let progress_reporter = BilibiliCoreProgress::new(progress.clone());
    let command_started_at = SystemTime::now();
    let core_report = tokio_timeout(
        Duration::from_secs(config.bot.command_timeout_seconds),
        client.download_plan_with_progress(&core_plan, options, &progress_reporter),
    )
    .await
    .with_context(|| {
        format!(
            "Bilibili direct download timed out after {} seconds",
            config.bot.command_timeout_seconds
        )
    })??;
    let mut report = BilibiliDownloadReport::from(&core_report);
    let output_dir = bilibili_core::output_dir(config);
    if mux_locally {
        mux_bilibili_report_media(
            config,
            &output_dir,
            &mut report,
            command_started_at,
            progress.clone(),
        )
        .await?;
    }
    cleanup_bilibili_mux_input_files(&output_dir, &report)?;
    let primary_videos = bilibili_report_primary_media(&output_dir, &report);
    let mut details = vec![format!(
        "BBDown-rust crate: {} entr{}",
        report.entries.len(),
        if report.entries.len() == 1 {
            "y"
        } else {
            "ies"
        }
    )];
    if !report.title.trim().is_empty() {
        details.push(format!("Title: {}", report.title));
    }
    if config.video.write_nfo {
        match write_bilibili_nfos(&output_dir, url, &plan, &report) {
            Ok(created_nfos) if !created_nfos.is_empty() => {
                details.push(format!("NFO: {}", join_paths(&created_nfos)));
            }
            Ok(_) => {}
            Err(err) => details.push(format!("NFO skipped: {err}")),
        }
    }

    Ok(JobReport {
        saved_location: if primary_videos.is_empty() {
            resolve_command_output_path(&output_dir, &report.output_dir)
                .display()
                .to_string()
        } else if primary_videos.len() == 1 {
            primary_videos[0].display().to_string()
        } else {
            join_paths(&primary_videos)
        },
        details: nonempty_join(details),
    })
}

#[derive(Clone)]
struct BilibiliCoreProgress {
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
}

impl BilibiliCoreProgress {
    fn new(progress: Option<mpsc::UnboundedSender<JobProgress>>) -> Self {
        Self { progress }
    }
}

impl DownloadProgressSink for BilibiliCoreProgress {
    fn on_download_progress(&self, event: &DownloadProgressEvent) {
        let message = match event {
            DownloadProgressEvent::PlanStarted {
                title, entry_count, ..
            } => format!("BBDown-rust: planning {title} ({entry_count} entries)"),
            DownloadProgressEvent::EntryStarted { index, title, .. } => {
                format!("BBDown-rust: entry {index} started - {title}")
            }
            DownloadProgressEvent::FileStarted {
                kind,
                path,
                attempt,
                max_attempts,
                ..
            } => format!(
                "BBDown-rust: {} started ({attempt}/{max_attempts}) - {}",
                bilibili_file_kind(kind),
                path.display()
            ),
            DownloadProgressEvent::FileCompleted {
                kind,
                path,
                total_bytes,
                ..
            } => format!(
                "BBDown-rust: {} completed ({}) - {}",
                bilibili_file_kind(kind),
                human_bytes(*total_bytes),
                path.display()
            ),
            DownloadProgressEvent::MuxStarted { output_path, .. } => {
                format!("BBDown-rust: mux started - {}", output_path.display())
            }
            DownloadProgressEvent::MuxCompleted { output_path, .. } => {
                format!("BBDown-rust: mux completed - {}", output_path.display())
            }
            DownloadProgressEvent::EntryCompleted {
                index,
                title,
                file_count,
                ..
            } => format!("BBDown-rust: entry {index} completed - {title} ({file_count} files)"),
            DownloadProgressEvent::PlanCompleted {
                title, entry_count, ..
            } => format!("BBDown-rust: completed {title} ({entry_count} entries)"),
            _ => return,
        };
        send_progress(self.progress.as_ref(), message);
    }
}

fn bilibili_report_primary_media(cwd: &Path, report: &BilibiliDownloadReport) -> Vec<PathBuf> {
    let mut media = Vec::new();
    for entry in &report.entries {
        if let Some(mux) = &entry.mux {
            push_unique_path(
                &mut media,
                resolve_command_output_path(cwd, &mux.output_path),
            );
            continue;
        }

        let video_files = entry
            .files
            .iter()
            .filter(|file| matches!(file.kind.as_str(), "video" | "flv_segment"))
            .map(|file| resolve_command_output_path(cwd, &file.path))
            .collect::<Vec<_>>();
        if video_files.is_empty() {
            for file in entry.files.iter().filter(|file| file.kind == "audio") {
                push_unique_path(&mut media, resolve_command_output_path(cwd, &file.path));
            }
        } else {
            for path in video_files {
                push_unique_path(&mut media, path);
            }
        }
    }
    media
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BilibiliMediaInput {
    kind: String,
    path: PathBuf,
}

#[derive(Debug)]
struct OwnedTemporaryFile {
    path: PathBuf,
}

impl OwnedTemporaryFile {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for OwnedTemporaryFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

async fn mux_bilibili_report_media(
    config: &AppConfig,
    cwd: &Path,
    report: &mut BilibiliDownloadReport,
    since: SystemTime,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<()> {
    let report_title = report.title.clone();
    for entry in &mut report.entries {
        if entry.mux.is_some() {
            continue;
        }
        let media_inputs = bilibili_entry_media_inputs(cwd, entry);
        if media_inputs.is_empty() {
            continue;
        }
        let entry_dir = media_inputs[0].path.parent().unwrap_or(cwd).to_path_buf();
        let title = if entry.title.trim().is_empty() {
            report_title.as_str()
        } else {
            entry.title.as_str()
        };
        let output_path = unique_bilibili_mux_output_path(&entry_dir, title, "mp4", since);
        let (spec, concat_file) =
            bilibili_local_mux_command_spec(config, &media_inputs, &entry_dir, &output_path)?;
        let output_result = run_command(config, &spec, progress.clone()).await;
        drop(concat_file);
        let output_result = match output_result {
            Ok(output_result) => output_result,
            Err(err) => {
                let _ = fs::remove_file(&output_path);
                return Err(err);
            }
        };
        if !output_result.status.success() {
            let _ = fs::remove_file(&output_path);
            bail!(
                "{} exited with status {}\n{}",
                spec.program.display(),
                output_result.status,
                summarize_output(
                    &String::from_utf8_lossy(&output_result.stdout),
                    &String::from_utf8_lossy(&output_result.stderr)
                )
            );
        }
        if !output_path.is_file() {
            bail!(
                "Bilibili mux finished without creating {}",
                output_path.display()
            );
        }
        entry.mux = Some(BilibiliMuxReport { output_path });
    }
    Ok(())
}

fn bilibili_entry_media_inputs(
    cwd: &Path,
    entry: &BilibiliEntryDownloadReport,
) -> Vec<BilibiliMediaInput> {
    entry
        .files
        .iter()
        .filter(|file| matches!(file.kind.as_str(), "video" | "audio" | "flv_segment"))
        .map(|file| BilibiliMediaInput {
            kind: file.kind.clone(),
            path: resolve_command_output_path(cwd, &file.path),
        })
        .collect()
}

fn bilibili_local_mux_command_spec(
    config: &AppConfig,
    media_inputs: &[BilibiliMediaInput],
    entry_dir: &Path,
    output: &Path,
) -> Result<(CommandSpec, Option<OwnedTemporaryFile>)> {
    let mut args = vec![
        "-hide_banner".to_string(),
        "-y".to_string(),
        "-nostdin".to_string(),
    ];
    let concat_file = if only_bilibili_flv_segments(media_inputs) {
        let concat_file = create_bilibili_concat_file(
            entry_dir,
            ffmpeg_concat_file_list(media_inputs).as_bytes(),
        )?;
        args.extend([
            "-f".to_string(),
            "concat".to_string(),
            "-safe".to_string(),
            "0".to_string(),
            "-i".to_string(),
            command_path_arg(concat_file.path()),
        ]);
        Some(concat_file)
    } else {
        for media_input in media_inputs {
            args.push("-i".to_string());
            args.push(command_path_arg(&media_input.path));
        }
        for index in 0..media_inputs.len() {
            args.push("-map".to_string());
            args.push(format!("{index}:0"));
        }
        None
    };
    args.extend([
        "-c".to_string(),
        "copy".to_string(),
        "-movflags".to_string(),
        "+faststart".to_string(),
        command_path_arg(output),
    ]);
    Ok((
        CommandSpec {
            program: config.tools.ffmpeg.clone(),
            args,
            cwd: entry_dir.to_path_buf(),
            activity_dir: Some(entry_dir.to_path_buf()),
            cleanup_paths: Vec::new(),
        },
        concat_file,
    ))
}

fn create_bilibili_concat_file(entry_dir: &Path, contents: &[u8]) -> Result<OwnedTemporaryFile> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..128 {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = entry_dir.join(format!(
            "{BILIBILI_FFMPEG_CONCAT_FILE_PREFIX}-{}-{stamp:x}-{counter:x}.txt",
            std::process::id()
        ));
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to create Bilibili ffmpeg concat list {}",
                        path.display()
                    )
                });
            }
        };
        if let Err(err) = file.write_all(contents) {
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(err).with_context(|| {
                format!(
                    "failed to write Bilibili ffmpeg concat list {}",
                    path.display()
                )
            });
        }
        return Ok(OwnedTemporaryFile { path });
    }
    bail!("failed to allocate a unique Bilibili ffmpeg concat list")
}

fn only_bilibili_flv_segments(media_inputs: &[BilibiliMediaInput]) -> bool {
    !media_inputs.is_empty() && media_inputs.iter().all(|input| input.kind == "flv_segment")
}

fn ffmpeg_concat_file_list(media_inputs: &[BilibiliMediaInput]) -> String {
    media_inputs
        .iter()
        .map(|input| {
            format!(
                "file '{}'\n",
                command_path_arg(&input.path).replace('\'', "'\\''")
            )
        })
        .collect()
}

fn cleanup_bilibili_mux_input_files(cwd: &Path, report: &BilibiliDownloadReport) -> Result<()> {
    for entry in &report.entries {
        if entry.mux.is_none() {
            continue;
        }
        for file in entry
            .files
            .iter()
            .filter(|file| matches!(file.kind.as_str(), "video" | "audio" | "flv_segment"))
        {
            let path = resolve_command_output_path(cwd, &file.path);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to remove raw Bilibili input {}", path.display())
                    });
                }
            }
        }
    }
    Ok(())
}

fn resolve_command_output_path(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

fn bilibili_file_kind(kind: &DownloadFileKind) -> &'static str {
    match kind {
        DownloadFileKind::Video => "video",
        DownloadFileKind::Audio => "audio",
        DownloadFileKind::FlvSegment => "flv_segment",
        DownloadFileKind::Cover => "cover",
        DownloadFileKind::Subtitle => "subtitle",
        DownloadFileKind::Danmaku => "danmaku",
        DownloadFileKind::DanmakuAss => "danmaku_ass",
        _ => "unknown",
    }
}

fn write_bilibili_nfos(
    cwd: &Path,
    source_url: &str,
    plan: &BilibiliDownloadPlan,
    report: &BilibiliDownloadReport,
) -> Result<Vec<PathBuf>> {
    let mut nfos = Vec::new();
    for report_entry in &report.entries {
        let Some(primary_media) = bilibili_entry_primary_media(cwd, report_entry) else {
            continue;
        };
        if !primary_media.is_file()
            || (!is_video_file(&primary_media) && !is_audio_file(&primary_media))
        {
            continue;
        }
        let plan_entry = plan
            .entries
            .iter()
            .find(|entry| entry.index == report_entry.index);
        let Some(ids) = bilibili_nfo_ids(plan_entry) else {
            continue;
        };
        let title = plan_entry
            .map(|entry| entry.title.as_str())
            .filter(|title| !title.trim().is_empty())
            .or_else(|| {
                (!report_entry.title.trim().is_empty()).then_some(report_entry.title.as_str())
            })
            .or_else(|| (!plan.title.trim().is_empty()).then_some(plan.title.as_str()));
        let alternate_unique_ids = ids
            .alternate_unique_ids
            .iter()
            .map(|(unique_id_type, unique_id)| (unique_id_type.as_str(), unique_id.as_str()))
            .collect::<Vec<_>>();
        let nfo_path = write_nfo_for_media(
            &primary_media,
            &MediaNfo {
                title,
                plot: None,
                unique_id_type: ids.primary_unique_id_type.as_str(),
                unique_id: ids.primary_unique_id.as_str(),
                alternate_unique_ids,
                source_url,
                studio: Some("Bilibili"),
                premiered: None,
            },
        )?;
        nfos.push(nfo_path);
    }
    Ok(nfos)
}

fn bilibili_entry_primary_media(
    cwd: &Path,
    entry: &BilibiliEntryDownloadReport,
) -> Option<PathBuf> {
    if let Some(mux) = &entry.mux {
        return Some(resolve_command_output_path(cwd, &mux.output_path));
    }
    entry
        .files
        .iter()
        .find(|file| matches!(file.kind.as_str(), "video" | "flv_segment"))
        .or_else(|| entry.files.iter().find(|file| file.kind == "audio"))
        .map(|file| resolve_command_output_path(cwd, &file.path))
}

#[derive(Debug)]
struct BilibiliNfoIds {
    primary_unique_id_type: String,
    primary_unique_id: String,
    alternate_unique_ids: Vec<(String, String)>,
}

fn bilibili_nfo_ids(entry: Option<&BilibiliDownloadEntry>) -> Option<BilibiliNfoIds> {
    let entry = entry?;
    let (primary_unique_id_type, primary_unique_id) =
        if let Some(bvid) = entry.bvid.as_deref().filter(|bvid| !bvid.trim().is_empty()) {
            ("bilibili".to_string(), bvid.to_string())
        } else if let Some(epid) = entry.epid {
            ("bilibili-epid".to_string(), format!("ep{epid}"))
        } else {
            ("bilibili-aid".to_string(), format!("av{}", entry.aid))
        };

    let mut alternate_unique_ids = Vec::new();
    push_unique_nfo_id(
        &mut alternate_unique_ids,
        "bilibili-aid",
        format!("av{}", entry.aid),
        &primary_unique_id_type,
        &primary_unique_id,
    );
    push_unique_nfo_id(
        &mut alternate_unique_ids,
        "bilibili-cid",
        format!("cid{}", entry.cid),
        &primary_unique_id_type,
        &primary_unique_id,
    );
    if let Some(epid) = entry.epid {
        push_unique_nfo_id(
            &mut alternate_unique_ids,
            "bilibili-epid",
            format!("ep{epid}"),
            &primary_unique_id_type,
            &primary_unique_id,
        );
    }

    Some(BilibiliNfoIds {
        primary_unique_id_type,
        primary_unique_id,
        alternate_unique_ids,
    })
}

fn push_unique_nfo_id(
    ids: &mut Vec<(String, String)>,
    unique_id_type: &str,
    unique_id: String,
    primary_unique_id_type: &str,
    primary_unique_id: &str,
) {
    if unique_id_type == primary_unique_id_type && unique_id == primary_unique_id {
        return;
    }
    if ids.iter().any(|(existing_type, existing_id)| {
        existing_type == unique_id_type && existing_id == &unique_id
    }) {
        return;
    }
    ids.push((unique_id_type.to_string(), unique_id));
}

fn bilibili_mux_candidates(
    config: &AppConfig,
    metadata: &BilibiliMetadata,
    created_videos: Vec<PathBuf>,
    since: SystemTime,
    video_only: bool,
) -> Result<Vec<PathBuf>> {
    let mut candidates = created_videos;
    if let Some(aid) = metadata.aid.as_deref() {
        let aid_dir = config.downloads.video_dir.join(aid);
        if aid_dir.is_dir() {
            for video in list_video_files(&aid_dir)? {
                let audio = video.with_extension("m4a");
                let stream_modified = modified_since(&video, since)
                    || (!video_only && audio.is_file() && modified_since(&audio, since));
                let has_required_streams = video_only || audio.is_file();
                if has_required_streams && stream_modified && !candidates.contains(&video) {
                    candidates.push(video);
                }
            }
        }
    }
    Ok(candidates)
}

fn modified_since(path: &Path, since: SystemTime) -> bool {
    path.metadata()
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|modified| modified >= since)
}

async fn merge_bilibili_streams(
    config: &AppConfig,
    videos: &[PathBuf],
    metadata: &BilibiliMetadata,
    video_only: bool,
    command_started_at: SystemTime,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<Vec<PathBuf>> {
    let mut merged = Vec::new();
    for video in videos {
        let audio = video.with_extension("m4a");
        if !audio.is_file() {
            if !video_only {
                bail!(
                    "Bilibili post-processing failed: expected audio stream {}",
                    audio.display()
                );
            }
            move_bilibili_danmaku_sidecars_with_extra_dirs(
                video,
                video,
                command_started_at,
                &[config.downloads.video_dir.as_path()],
            )?;
            cleanup_bilibili_json_danmaku_sidecars(video, video, command_started_at)?;
            merged.push(video.clone());
            continue;
        }

        let title = metadata
            .title
            .as_deref()
            .or_else(|| video.file_stem().and_then(|stem| stem.to_str()))
            .unwrap_or("bilibili");
        let output = unique_bilibili_mux_output_path(
            &config.downloads.video_dir,
            title,
            "mp4",
            command_started_at,
        );
        let spec = ffmpeg_mux_command_spec(config, video, &audio, &output);
        let output_result = run_command(config, &spec, progress.clone()).await?;
        if !output_result.status.success() {
            bail!(
                "{} exited with status {}\n{}",
                spec.program.display(),
                output_result.status,
                summarize_output(
                    &String::from_utf8_lossy(&output_result.stdout),
                    &String::from_utf8_lossy(&output_result.stderr)
                )
            );
        }

        move_bilibili_danmaku_sidecars(video, &output, command_started_at)?;
        cleanup_bilibili_json_danmaku_sidecars(video, &output, command_started_at)?;
        let _ = fs::remove_file(video);
        let _ = fs::remove_file(&audio);
        merged.push(output);
    }
    Ok(merged)
}

fn move_bilibili_danmaku_sidecars(
    source_video: &Path,
    output_video: &Path,
    since: SystemTime,
) -> Result<Vec<PathBuf>> {
    move_bilibili_danmaku_sidecars_with_extra_dirs(source_video, output_video, since, &[])
}

fn move_bilibili_danmaku_sidecars_with_extra_dirs(
    source_video: &Path,
    output_video: &Path,
    since: SystemTime,
    extra_directories: &[&Path],
) -> Result<Vec<PathBuf>> {
    let mut moved = Vec::new();
    for extension in ["xml", "ass"] {
        let destination = output_video.with_extension(extension);
        if destination.is_file() && modified_since(&destination, since) {
            remove_direct_bilibili_danmaku_duplicate(source_video, extension, &destination, since)?;
            continue;
        }
        let alternate_sources = current_bilibili_danmaku_sidecars(
            output_video,
            extension,
            since,
            source_video,
            extra_directories,
        )?;
        let Some(source) = select_bilibili_danmaku_source(
            output_video,
            source_video,
            extension,
            alternate_sources,
        ) else {
            continue;
        };
        if source == destination {
            continue;
        }
        if destination.exists() {
            fs::remove_file(&destination).with_context(|| {
                format!(
                    "failed to replace stale Bilibili danmaku sidecar {}",
                    destination.display()
                )
            })?;
        }
        fs::rename(&source, &destination).with_context(|| {
            format!(
                "failed to move Bilibili danmaku sidecar {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        remove_matching_bilibili_json_danmaku_sidecar(&source, since)?;
        moved.push(destination);
    }
    Ok(moved)
}

fn remove_matching_bilibili_json_danmaku_sidecar(sidecar: &Path, since: SystemTime) -> Result<()> {
    let candidate = sidecar.with_extension("json");
    if candidate
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".info.json"))
    {
        return Ok(());
    }
    if candidate.is_file() && modified_since(&candidate, since) {
        fs::remove_file(&candidate).with_context(|| {
            format!(
                "failed to remove matching Bilibili JSON danmaku sidecar {}",
                candidate.display()
            )
        })?;
    }
    Ok(())
}

fn remove_direct_bilibili_danmaku_duplicate(
    source_video: &Path,
    extension: &str,
    destination: &Path,
    since: SystemTime,
) -> Result<()> {
    let source = source_video.with_extension(extension);
    if source != destination && source.is_file() && modified_since(&source, since) {
        fs::remove_file(&source).with_context(|| {
            format!(
                "failed to remove duplicate Bilibili danmaku sidecar {}",
                source.display()
            )
        })?;
    }
    Ok(())
}

fn cleanup_bilibili_json_danmaku_sidecars(
    source_video: &Path,
    output_video: &Path,
    since: SystemTime,
) -> Result<()> {
    for candidate in current_bilibili_json_danmaku_sidecars(output_video, since, source_video)? {
        fs::remove_file(&candidate).with_context(|| {
            format!(
                "failed to remove Bilibili JSON danmaku sidecar {}",
                candidate.display()
            )
        })?;
    }
    Ok(())
}

fn cleanup_bilibili_download_outputs(videos: &[PathBuf], since: SystemTime) -> Result<()> {
    for video in videos {
        move_bilibili_danmaku_sidecars(video, video, since)?;
        cleanup_bilibili_json_danmaku_sidecars(video, video, since)?;
    }
    Ok(())
}

fn current_bilibili_json_danmaku_sidecars(
    output_video: &Path,
    since: SystemTime,
    source_video: &Path,
) -> Result<Vec<PathBuf>> {
    let source = source_video.with_extension("json");
    let output = output_video.with_extension("json");
    let mut candidates = BTreeSet::new();
    for candidate in [&source, &output] {
        if candidate.is_file() && modified_since(candidate, since) {
            candidates.insert(candidate.clone());
        }
    }

    if let Some(base_candidate) = base_bilibili_json_danmaku_sidecar(output_video)
        && base_candidate != source
        && base_candidate != output
    {
        let has_current_primary = sidecar_has_current_primary_media(&base_candidate, since)?;
        if !has_current_primary
            && base_candidate.is_file()
            && modified_since(&base_candidate, since)
        {
            candidates.insert(base_candidate);
        }
    }

    Ok(candidates.into_iter().collect())
}

fn current_bilibili_danmaku_sidecars(
    output_video: &Path,
    extension: &str,
    since: SystemTime,
    source_video: &Path,
    extra_directories: &[&Path],
) -> Result<Vec<PathBuf>> {
    let mut candidates = BTreeSet::new();
    let source = source_video.with_extension(extension);
    if source.is_file() && modified_since(&source, since) {
        candidates.insert(source.clone());
    }
    let mut directories = BTreeSet::new();
    for directory in [source_video.parent(), output_video.parent()]
        .into_iter()
        .flatten()
    {
        directories.insert(directory.to_path_buf());
    }
    for directory in extra_directories {
        directories.insert((*directory).to_path_buf());
    }
    for directory in directories {
        collect_current_bilibili_danmaku_sidecars_in_dir(
            &directory,
            extension,
            since,
            &mut candidates,
        )?;
    }
    candidates
        .into_iter()
        .filter_map(|candidate| {
            if candidate == source {
                return Some(Ok(candidate));
            }
            match sidecar_has_current_primary_media(&candidate, since) {
                Ok(true) => None,
                Ok(false) => Some(Ok(candidate)),
                Err(err) => Some(Err(err)),
            }
        })
        .collect::<Result<Vec<_>>>()
}

fn sidecar_has_current_primary_media(sidecar: &Path, since: SystemTime) -> Result<bool> {
    let Some(parent) = sidecar.parent() else {
        return Ok(false);
    };
    let Some(sidecar_stem) = sidecar.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(false);
    };
    let entries =
        fs::read_dir(parent).with_context(|| format!("failed to read {}", parent.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file()
            && (is_video_file(&path) || is_audio_file(&path))
            && path.file_stem().and_then(|stem| stem.to_str()) == Some(sidecar_stem)
            && modified_since(&path, since)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn base_bilibili_json_danmaku_sidecar(output_video: &Path) -> Option<PathBuf> {
    let parent = output_video.parent().unwrap_or_else(|| Path::new("."));
    let output_stem = output_video.file_stem()?.to_str()?;
    let base_stem = stem_without_unique_suffix(output_stem);
    (base_stem != output_stem).then(|| parent.join(format!("{base_stem}.json")))
}

fn collect_current_bilibili_danmaku_sidecars_in_dir(
    directory: &Path,
    extension: &str,
    since: SystemTime,
    candidates: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", directory.display()));
        }
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file()
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(extension))
            && modified_since(&path, since)
        {
            candidates.insert(path);
        }
    }
    Ok(())
}

fn select_bilibili_danmaku_source(
    output_video: &Path,
    source_video: &Path,
    extension: &str,
    candidates: Vec<PathBuf>,
) -> Option<PathBuf> {
    if candidates.is_empty() {
        return None;
    }
    let direct_source = source_video.with_extension(extension);
    if let Some(candidate) = candidates
        .iter()
        .find(|candidate| **candidate == direct_source)
    {
        return Some(candidate.clone());
    }
    let output_stem = output_video.file_stem()?.to_str()?;
    if let Some(candidate) = candidates
        .iter()
        .find(|candidate| candidate.file_stem().and_then(|stem| stem.to_str()) == Some(output_stem))
    {
        return Some(candidate.clone());
    }
    let base_stem = stem_without_unique_suffix(output_stem);
    if let Some(candidate) = candidates
        .iter()
        .find(|candidate| candidate.file_stem().and_then(|stem| stem.to_str()) == Some(base_stem))
    {
        return Some(candidate.clone());
    }
    if candidates.len() == 1 {
        return candidates.into_iter().next();
    }
    None
}

fn stem_without_unique_suffix(stem: &str) -> &str {
    let Some(stripped) = stem.strip_suffix(')') else {
        return stem;
    };
    let Some((base, suffix)) = stripped.rsplit_once(" (") else {
        return stem;
    };
    if suffix.parse::<usize>().is_ok_and(|index| index >= 2) {
        base
    } else {
        stem
    }
}

async fn run_youtube_job(
    config: &AppConfig,
    url: &str,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let metadata = fetch_youtube_metadata(config, url, progress.clone()).await?;
    let subtitle_plan = select_subtitles(&metadata, &config.video.subtitle_languages);
    let _guard = video_output_lock("YouTube download", progress.as_ref()).await;
    run_youtube_job_locked(config, url, metadata, subtitle_plan, progress).await
}

async fn run_youtube_job_locked(
    config: &AppConfig,
    url: &str,
    metadata: YoutubeMetadata,
    subtitle_plan: SubtitlePlan,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let spec = youtube_download_command_spec(config, url, &subtitle_plan);
    let output = run_command(config, &spec, progress).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let saved_location = last_nonempty_line(&stdout)
        .filter(|line| Path::new(line).is_absolute())
        .map(str::to_string)
        .unwrap_or_else(|| config.downloads.video_dir.display().to_string());

    let mut details = vec![subtitle_plan.describe(), tail_lines(&stderr, 6)];
    if config.video.write_nfo {
        let video_path = Path::new(&saved_location);
        if video_path.is_absolute() && video_path.is_file() && is_video_file(video_path) {
            let title = metadata
                .title
                .as_deref()
                .or_else(|| video_path.file_stem()?.to_str());
            let source_url = metadata.webpage_url.as_deref().unwrap_or(url);
            let studio = metadata.uploader.as_deref().or(metadata.channel.as_deref());
            let premiered = metadata.upload_date.as_deref().and_then(format_yt_date);
            match write_nfo_for_media(
                video_path,
                &MediaNfo {
                    title,
                    plot: metadata.description.as_deref(),
                    unique_id_type: "youtube",
                    unique_id: metadata.id.as_deref().unwrap_or(url),
                    alternate_unique_ids: Vec::new(),
                    source_url,
                    studio,
                    premiered: premiered.as_deref(),
                },
            ) {
                Ok(nfo_path) => details.push(format!("NFO: {}", nfo_path.display())),
                Err(err) => details.push(format!("NFO skipped: {err}")),
            }
        }
    }

    Ok(JobReport {
        saved_location,
        details: nonempty_join(details),
    })
}

async fn run_staged_video_job(
    config: &AppConfig,
    job: &JobRequest,
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let _guard = video_output_lock("Staged video download", progress.as_ref()).await;
    let final_dir = config.downloads.video_dir.clone();
    let primary_media_kind = staged_primary_media_kind(config, job)?;
    let staging_dir = create_video_staging_dir(&final_dir)?;
    let _staging_cleanup = RemoveDirOnDrop::new(staging_dir.clone());
    copy_bbdown_config_for_staging(&final_dir, &staging_dir)?;
    send_progress(
        progress.as_ref(),
        format!("staging: downloading into {}", staging_dir.display()),
    );

    let mut staging_config = config.clone();
    staging_config.downloads.video_dir = staging_dir.clone();
    preserve_bilibili_config_paths_for_staging(&mut staging_config, &final_dir);
    let result = match job {
        JobRequest::Bilibili { url, selection } => {
            run_bilibili_job_locked(&staging_config, url, *selection, progress.clone()).await
        }
        JobRequest::Youtube { url } => {
            let metadata = fetch_youtube_metadata(&staging_config, url, progress.clone()).await;
            match metadata {
                Ok(metadata) => {
                    let subtitle_plan =
                        select_subtitles(&metadata, &staging_config.video.subtitle_languages);
                    run_youtube_job_locked(
                        &staging_config,
                        url,
                        metadata,
                        subtitle_plan,
                        progress.clone(),
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        }
        JobRequest::Pdf { .. } => run_job(config, job, progress.clone()).await,
    };

    let report = match result {
        Ok(report) => report,
        Err(err) => return Err(err),
    };

    let staged_files = collect_regular_files(&staging_dir)?
        .into_iter()
        .filter(|path| !is_staging_support_file(&staging_dir, path))
        .collect::<Vec<_>>();
    if staged_files.is_empty() {
        bail!(
            "staged video download finished but no output files were found in {}",
            staging_dir.display()
        );
    }
    let staged_media = staged_files
        .iter()
        .filter(|path| is_primary_media_file(path, primary_media_kind))
        .cloned()
        .collect::<Vec<_>>();
    let artifact_only = bilibili_downloads_artifacts_only(config, job)?;
    if staged_media.is_empty() && !artifact_only {
        bail!(
            "staged video download finished but no primary media files were found in {}",
            staging_dir.display()
        );
    }

    let moved_files = if staged_media.is_empty() && artifact_only {
        move_staged_artifact_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            action,
            duplicate,
            primary_media_kind,
        )
    } else {
        move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            action,
            duplicate,
            primary_media_kind,
        )
    }
    .with_context(|| format!("failed to move staged files from {}", staging_dir.display()))?;
    send_progress(
        progress.as_ref(),
        format!("staging: moved {} file(s)", moved_files.len()),
    );

    let saved_location = if moved_files.len() == 1 {
        moved_files[0].display().to_string()
    } else {
        join_paths(&moved_files)
    };
    let details = nonempty_join(vec![
        remove_staging_detail_lines(&report.details, &staging_dir),
        format!("Moved: {}", join_paths(&moved_files)),
    ]);
    Ok(JobReport {
        saved_location,
        details,
    })
}

fn staged_primary_media_kind(
    config: &AppConfig,
    job: &JobRequest,
) -> Result<StagedPrimaryMediaKind> {
    match job {
        JobRequest::Bilibili { .. } if bilibili_downloads_audio_only(config)? => {
            Ok(StagedPrimaryMediaKind::VideoOrAudio)
        }
        _ => Ok(StagedPrimaryMediaKind::Video),
    }
}

fn remove_staging_detail_lines(details: &str, staging_dir: &Path) -> String {
    let staging_marker = staging_dir.display().to_string();
    details
        .lines()
        .filter(|line| !line.contains(&staging_marker))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn command_spec(config: &AppConfig, job: &JobRequest) -> Result<CommandSpec> {
    match job {
        JobRequest::Bilibili { .. } => {
            bail!("Bilibili downloads use the bbdown-core crate API and do not have a command spec")
        }
        JobRequest::Youtube { url } => Ok(youtube_download_command_spec(
            config,
            url,
            &SubtitlePlan::none(),
        )),
        JobRequest::Pdf { url } => Ok(pdf_command_spec(config, url)),
    }
}

fn fallback_video_identity(job: &JobRequest) -> Option<VideoIdentity> {
    video_identity(job).or_else(|| match job {
        JobRequest::Bilibili { .. } => Some(VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: "unknown".to_string(),
        }),
        JobRequest::Youtube { .. } => Some(VideoIdentity {
            provider: VideoProvider::Youtube,
            id: "unknown".to_string(),
        }),
        JobRequest::Pdf { .. } => None,
    })
}

fn sync_bilibili_rust_credentials(config: &AppConfig) -> Result<()> {
    bilibili_auth::sync_bbdown_rust_credentials_from_state(
        &config.bilibili.auth.state_path,
        &config.bilibili.auth.credential_file,
        config.bilibili.auth.credential_profile.as_deref(),
    )?;
    Ok(())
}

pub fn bilibili_metadata_command_spec(config: &AppConfig, url: &str) -> Result<CommandSpec> {
    let (extra_args, explicit_config_path) = bilibili_extra_args_without_config_file(config);
    let base_config_path = bbdown_base_config_path(config, explicit_config_path.as_deref());
    let base_config_lines =
        read_bbdown_base_config_lines(base_config_path.as_deref(), explicit_config_path.is_some())?;
    let safe_config_lines = filter_bilibili_metadata_config_lines(&base_config_lines);
    let config_path = bilibili_auth::ensure_isolated_bbdown_config_file_with_lines(
        &config.bilibili.auth.state_path,
        &safe_config_lines,
    )?;
    let mut args = vec![url.to_string(), "--only-show-info".to_string()];
    args.extend(filter_bilibili_metadata_args(&extra_args));
    args.extend([
        "--config-file".to_string(),
        config_path.display().to_string(),
    ]);

    Ok(CommandSpec {
        program: config.tools.bbdown.clone(),
        args,
        cwd: config.downloads.video_dir.clone(),
        activity_dir: None,
        cleanup_paths: vec![config_path],
    })
}

fn bilibili_extra_args_without_config_file(config: &AppConfig) -> (Vec<String>, Option<PathBuf>) {
    split_bilibili_extra_args(&config.bilibili.extra_args)
}

fn read_bbdown_config_lines(path: &Path) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read BBDown config {}", path.display()))?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect())
}

fn read_bbdown_base_config_lines(path: Option<&Path>, required: bool) -> Result<Vec<String>> {
    match path {
        Some(path) if required || path.exists() => read_bbdown_config_lines(path),
        _ => Ok(Vec::new()),
    }
}

fn filter_bilibili_metadata_args(args: &[String]) -> Vec<String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if is_bilibili_metadata_ignored_arg(arg) {
            index += 1;
            if args
                .get(index)
                .is_some_and(|value| !value.starts_with('-') || parse_bool_token(value).is_some())
            {
                index += 1;
            }
        } else {
            filtered.push(arg.clone());
            index += 1;
        }
    }
    filtered
}

fn filter_bilibili_metadata_config_lines(lines: &[String]) -> Vec<String> {
    let mut filtered = Vec::with_capacity(lines.len());
    let mut index = 0;
    while index < lines.len() {
        let line = &lines[index];
        let (ignored, consumes_next) = bilibili_metadata_config_line_ignored(line);
        if ignored {
            index += 1;
            if consumes_next
                && lines.get(index).is_some_and(|value| {
                    !value.starts_with('-') || parse_bool_token(value).is_some()
                })
            {
                index += 1;
            }
        } else {
            filtered.push(line.clone());
            index += 1;
        }
    }
    filtered
}

fn bilibili_metadata_config_line_ignored(line: &str) -> (bool, bool) {
    let trimmed = line.trim();
    const FLAGS: &[&str] = &[
        "--audio-only",
        "--download-danmaku",
        "--only-show-info",
        "--save-archives-to-file",
        "--skip-ai",
        "--skip-mux",
        "--video-only",
        "-dd",
        "-info",
    ];

    for flag in FLAGS {
        if trimmed == *flag {
            return (true, true);
        }
        if trimmed.strip_prefix(flag).is_some_and(|suffix| {
            suffix.starts_with('=')
                || suffix.starts_with(':')
                || suffix.chars().next().is_some_and(char::is_whitespace)
        }) {
            return (true, false);
        }
    }
    (false, false)
}

fn is_bilibili_metadata_ignored_arg(arg: &str) -> bool {
    const FLAGS: &[&str] = &[
        "--audio-only",
        "--download-danmaku",
        "--only-show-info",
        "--save-archives-to-file",
        "--skip-ai",
        "--skip-mux",
        "--video-only",
        "-dd",
        "-info",
    ];

    FLAGS.iter().any(|flag| {
        arg == *flag
            || arg
                .strip_prefix(flag)
                .is_some_and(|suffix| suffix.starts_with('=') || suffix.starts_with(':'))
    })
}

fn parse_bool_token(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

fn bilibili_downloads_audio_only(config: &AppConfig) -> Result<bool> {
    Ok(bilibili_core::download_mode_from_config(config)? == DownloadMode::AudioOnly)
}

fn bilibili_downloads_artifacts_only(config: &AppConfig, job: &JobRequest) -> Result<bool> {
    if !matches!(job, JobRequest::Bilibili { .. }) {
        return Ok(false);
    }
    Ok(matches!(
        bilibili_core::download_mode_from_config(config)?,
        DownloadMode::SubtitleOnly | DownloadMode::DanmakuOnly | DownloadMode::CoverOnly
    ))
}

fn has_bilibili_only_value(args: &[String], expected: &str) -> bool {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--only" {
            if args.get(index + 1).is_some_and(|value| value == expected) {
                return true;
            }
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--only=")
            && value == expected
        {
            return true;
        }
        index += 1;
    }
    false
}

fn split_bilibili_extra_args(extra_args: &[String]) -> (Vec<String>, Option<PathBuf>) {
    let mut filtered = Vec::with_capacity(extra_args.len());
    let mut config_path = None;
    let mut index = 0;
    while index < extra_args.len() {
        let arg = &extra_args[index];
        if arg == "--config-file" {
            if let Some(value) = extra_args.get(index + 1) {
                config_path = Some(PathBuf::from(value));
                index += 2;
            } else {
                filtered.push(arg.clone());
                index += 1;
            }
        } else if let Some(value) = arg.strip_prefix("--config-file=") {
            config_path = Some(PathBuf::from(value));
            index += 1;
        } else {
            filtered.push(arg.clone());
            index += 1;
        }
    }
    (filtered, config_path)
}

fn preserve_bilibili_config_paths_for_staging(config: &mut AppConfig, final_video_dir: &Path) {
    let mut args = Vec::with_capacity(config.bilibili.extra_args.len());
    let mut index = 0;
    while index < config.bilibili.extra_args.len() {
        let arg = &config.bilibili.extra_args[index];
        if arg == "--config-file" {
            args.push(arg.clone());
            if let Some(value) = config.bilibili.extra_args.get(index + 1) {
                args.push(
                    absolute_bbdown_config_path(final_video_dir, Path::new(value))
                        .display()
                        .to_string(),
                );
                index += 2;
            } else {
                index += 1;
            }
        } else if let Some(value) = arg.strip_prefix("--config-file=") {
            args.push(format!(
                "--config-file={}",
                absolute_bbdown_config_path(final_video_dir, Path::new(value)).display()
            ));
            index += 1;
        } else {
            args.push(arg.clone());
            index += 1;
        }
    }
    config.bilibili.extra_args = args;
}

fn bbdown_base_config_path(
    config: &AppConfig,
    explicit_config_path: Option<&Path>,
) -> Option<PathBuf> {
    explicit_config_path
        .map(|path| resolve_bbdown_config_path(&config.downloads.video_dir, path))
        .or_else(|| {
            let default_path = config.downloads.video_dir.join("BBDown.config");
            default_path.exists().then_some(default_path)
        })
}

fn resolve_bbdown_config_path(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn absolute_bbdown_config_path(cwd: &Path, path: &Path) -> PathBuf {
    let resolved = resolve_bbdown_config_path(cwd, path);
    absolute_process_path(&resolved)
}

fn absolute_process_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

pub fn youtube_metadata_command_spec(config: &AppConfig, url: &str) -> CommandSpec {
    CommandSpec {
        program: config.tools.yt_dlp.clone(),
        args: vec![
            "--dump-json".to_string(),
            "--skip-download".to_string(),
            "--no-playlist".to_string(),
            url.to_string(),
        ],
        cwd: config.downloads.video_dir.clone(),
        activity_dir: None,
        cleanup_paths: Vec::new(),
    }
}

pub fn youtube_download_command_spec(
    config: &AppConfig,
    url: &str,
    subtitle_plan: &SubtitlePlan,
) -> CommandSpec {
    let mut args = vec![
        "--no-playlist".to_string(),
        "-P".to_string(),
        ".".to_string(),
        "--merge-output-format".to_string(),
        "mkv".to_string(),
        "--remux-video".to_string(),
        "mkv".to_string(),
        "--embed-thumbnail".to_string(),
        "--embed-metadata".to_string(),
        "--embed-chapters".to_string(),
        "--embed-info-json".to_string(),
        "--convert-thumbnails".to_string(),
        "jpg".to_string(),
        "--print".to_string(),
        "after_move:filepath".to_string(),
    ];

    if config.video.keep_sidecars {
        args.extend([
            "--write-info-json".to_string(),
            "--write-description".to_string(),
            "--write-thumbnail".to_string(),
        ]);
    }

    match &subtitle_plan.source {
        Some(SubtitleSource::Manual) => args.push("--write-subs".to_string()),
        Some(SubtitleSource::Automatic) => args.push("--write-auto-subs".to_string()),
        None => {}
    }

    if !subtitle_plan.languages.is_empty() {
        args.extend([
            "--sub-langs".to_string(),
            subtitle_plan.languages.join(","),
            "--sub-format".to_string(),
            "srt/vtt/best".to_string(),
            "--convert-subs".to_string(),
            "srt".to_string(),
            "--embed-subs".to_string(),
        ]);
    }

    args.push(url.to_string());

    CommandSpec {
        program: config.tools.yt_dlp.clone(),
        args,
        cwd: config.downloads.video_dir.clone(),
        activity_dir: Some(config.downloads.video_dir.clone()),
        cleanup_paths: Vec::new(),
    }
}

pub fn pdf_command_spec(config: &AppConfig, url: &str) -> CommandSpec {
    CommandSpec {
        program: config.tools.uv.clone(),
        args: vec![
            "run".to_string(),
            "python".to_string(),
            config
                .resolve_project_path(&config.tools.pdf_helper)
                .display()
                .to_string(),
            "--url".to_string(),
            url.to_string(),
            "--output-dir".to_string(),
            config.downloads.pdf_dir.display().to_string(),
            "--chrome".to_string(),
            config.tools.chrome.display().to_string(),
        ],
        cwd: config.resolve_project_path(Path::new(".")),
        activity_dir: Some(config.downloads.pdf_dir.clone()),
        cleanup_paths: Vec::new(),
    }
}

fn ffmpeg_mux_command_spec(
    config: &AppConfig,
    video: &Path,
    audio: &Path,
    output: &Path,
) -> CommandSpec {
    CommandSpec {
        program: config.tools.ffmpeg.clone(),
        args: vec![
            "-hide_banner".to_string(),
            "-y".to_string(),
            "-i".to_string(),
            command_path_arg(video),
            "-i".to_string(),
            command_path_arg(audio),
            "-map".to_string(),
            "0:v:0".to_string(),
            "-map".to_string(),
            "1:a:0".to_string(),
            "-c".to_string(),
            "copy".to_string(),
            "-movflags".to_string(),
            "+faststart".to_string(),
            command_path_arg(output),
        ],
        cwd: config.downloads.video_dir.clone(),
        activity_dir: Some(config.downloads.video_dir.clone()),
        cleanup_paths: Vec::new(),
    }
}

fn command_path_arg(path: &Path) -> String {
    if path.is_absolute() {
        return path.display().to_string();
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(path)
        .display()
        .to_string()
}

fn command_progress_name(spec: &CommandSpec) -> String {
    let command_name = spec
        .program
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or("command")
        .to_string();
    let lower_name = command_name.to_ascii_lowercase();
    if lower_name.contains("yt-dlp")
        && spec.args.iter().any(|arg| arg == "--dump-json")
        && spec.args.iter().any(|arg| arg == "--skip-download")
    {
        format!("{command_name} metadata")
    } else {
        command_name
    }
}

async fn fetch_youtube_metadata(
    config: &AppConfig,
    url: &str,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<YoutubeMetadata> {
    let spec = youtube_metadata_command_spec(config, url);
    let output = run_command(config, &spec, progress).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let json = last_nonempty_line(&stdout).ok_or_else(|| anyhow!("yt-dlp returned no metadata"))?;
    serde_json::from_str(json).context("failed to parse yt-dlp metadata JSON")
}

async fn probe_bilibili_plan(
    config: &AppConfig,
    url: &str,
    selection: Option<BilibiliSelection>,
    timeout: Duration,
) -> Result<BilibiliDownloadPlan> {
    sync_bilibili_rust_credentials(config)?;
    let client = bilibili_core::client(config)?;
    let mode = bilibili_core::download_mode_from_config(config)?;
    let plan = probe_bilibili_plan_with_mode(&client, url, selection, mode, timeout).await?;
    Ok(BilibiliDownloadPlan::from(&plan))
}

async fn probe_bilibili_plan_with_mode(
    client: &bbdown_core::BiliClient,
    url: &str,
    selection: Option<BilibiliSelection>,
    mode: DownloadMode,
    timeout: Duration,
) -> Result<bbdown_core::DownloadPlan> {
    tokio_timeout(
        timeout,
        client.plan_download_with_mode(url, bilibili_core::selection(selection), mode),
    )
    .await
    .context("Bilibili plan probe timed out")?
    .context("failed to probe Bilibili plan with bbdown-core")
}

fn parse_bilibili_plan(stdout: &str) -> Result<BilibiliDownloadPlan> {
    parse_bilibili_json_output(stdout, "BBDown-rust plan JSON")
}

fn parse_bilibili_download_report(stdout: &str) -> Result<BilibiliDownloadReport> {
    parse_bilibili_json_output(stdout, "BBDown-rust download report JSON")
}

fn parse_bilibili_json_output<T>(stdout: &str, label: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        bail!("{label} was empty");
    }
    if let Ok(value) = serde_json::from_str(trimmed) {
        return Ok(value);
    }
    let Some(start) = trimmed.find('{') else {
        bail!("{label} did not contain a JSON object");
    };
    let Some(end) = trimmed.rfind('}') else {
        bail!("{label} did not contain a complete JSON object");
    };
    serde_json::from_str(&trimmed[start..=end]).with_context(|| format!("failed to parse {label}"))
}

async fn probe_bilibili_metadata(
    config: &AppConfig,
    url: &str,
    timeout: Duration,
) -> Result<BilibiliMetadata> {
    let spec = bilibili_metadata_command_spec(config, url)?;
    let mut probe_config = config.clone();
    probe_config.bot.command_timeout_seconds = probe_config
        .bot
        .command_timeout_seconds
        .min(timeout.as_secs().max(1));
    let output = run_command(&probe_config, &spec, None).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    Ok(parse_bilibili_metadata(url, &format!("{stdout}\n{stderr}")))
}

async fn video_output_lock(
    job_label: &str,
    progress: Option<&mpsc::UnboundedSender<JobProgress>>,
) -> MutexGuard<'static, ()> {
    let lock = VIDEO_OUTPUT_LOCK.get_or_init(|| Mutex::new(()));
    match lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            send_progress(
                progress,
                format!("{job_label}: waiting for video output slot"),
            );
            let guard = lock.lock().await;
            send_progress(progress, format!("{job_label}: video output slot acquired"));
            guard
        }
    }
}

fn send_progress(progress: Option<&mpsc::UnboundedSender<JobProgress>>, message: String) {
    if let Some(progress) = progress {
        let _ = progress.send(JobProgress { message });
    }
}

#[derive(Debug)]
struct CommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug)]
struct CommandCleanup {
    paths: Vec<PathBuf>,
}

impl CommandCleanup {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

impl Drop for CommandCleanup {
    fn drop(&mut self) {
        for path in &self.paths {
            bilibili_auth::release_bbdown_config_file(path);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandStream {
    Stdout,
    Stderr,
}

#[derive(Debug)]
struct CommandChunk {
    stream: CommandStream,
    bytes: Vec<u8>,
}

async fn run_command(
    config: &AppConfig,
    spec: &CommandSpec,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<CommandOutput> {
    let _cleanup = CommandCleanup::new(spec.cleanup_paths.clone());
    let mut file_activity = match &spec.activity_dir {
        Some(activity_dir) => match FileActivityTracker::new(activity_dir).await {
            Ok(tracker) => Some(tracker),
            Err(err) => {
                info!(
                    command = %spec.program.display(),
                    activity_dir = %activity_dir.display(),
                    error = %err,
                    "file activity tracking disabled"
                );
                None
            }
        },
        None => {
            info!(command = %spec.program.display(), "file activity tracking disabled");
            None
        }
    };

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to run {}", spec.program.display()))?;
    let process_group = command_process_group(&child);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture {} stdout", spec.program.display()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture {} stderr", spec.program.display()))?;

    let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel();
    let stdout_handle = tokio::spawn(read_command_stream(
        stdout,
        CommandStream::Stdout,
        chunk_tx.clone(),
    ));
    let stderr_handle = tokio::spawn(read_command_stream(stderr, CommandStream::Stderr, chunk_tx));

    let total_timeout = Duration::from_secs(config.bot.command_timeout_seconds);
    let idle_timeout = Duration::from_secs(config.bot.command_idle_timeout_seconds);
    let started_at = Instant::now();
    let total_deadline = started_at + total_timeout;
    let mut last_activity_at = started_at;
    let progress_interval = Duration::from_secs(config.bot.progress_update_seconds);
    let activity_poll_interval = file_activity_poll_interval(progress_interval, idle_timeout);
    let mut next_activity_poll = started_at + activity_poll_interval;
    let mut progress_tracker =
        ProgressTracker::new(command_progress_name(spec), progress_interval, progress);

    let mut output_closed = false;
    let status = loop {
        let idle_deadline = last_activity_at + idle_timeout;
        tokio::select! {
            maybe_chunk = chunk_rx.recv(), if !output_closed => {
                match maybe_chunk {
                    Some(chunk) => {
                        last_activity_at = Instant::now();
                        progress_tracker.observe(chunk.stream, &chunk.bytes);
                    }
                    None => output_closed = true,
                }
            }
            wait_result = child.wait() => {
                break wait_result
                    .with_context(|| format!("failed to wait for {}", spec.program.display()))?;
            }
            _ = sleep_until(total_deadline) => {
                terminate_command_tree(&mut child, process_group).await;
                let (stdout, stderr) =
                    collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
                bail!(
                    "{} timed out after {}s\n{}",
                    spec.program.display(),
                    config.bot.command_timeout_seconds,
                    summarize_output(&String::from_utf8_lossy(&stdout), &String::from_utf8_lossy(&stderr))
                );
            }
            _ = sleep_until(idle_deadline) => {
                terminate_command_tree(&mut child, process_group).await;
                let (stdout, stderr) =
                    collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
                bail!(
                    "{} had no output or file activity for {}s\n{}",
                    spec.program.display(),
                    config.bot.command_idle_timeout_seconds,
                    summarize_output(&String::from_utf8_lossy(&stdout), &String::from_utf8_lossy(&stderr))
                );
            }
            _ = sleep_until(next_activity_poll), if file_activity.is_some() => {
                next_activity_poll = Instant::now() + activity_poll_interval;
                let tracker = file_activity.as_mut().expect("guarded by is_some");
                match tracker.poll().await {
                    Ok(report) => {
                        if report.changed_since_previous_poll {
                            last_activity_at = Instant::now();
                        }
                        progress_tracker.emit_file_activity(report);
                    }
                    Err(err) => {
                        info!(
                            command = %spec.program.display(),
                            activity_dir = %tracker.root.display(),
                            error = %err,
                            "file activity tracking stopped"
                        );
                        file_activity = None;
                    }
                }
            }
        }
    };

    let (stdout, stderr) =
        collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
}

fn file_activity_poll_interval(progress_interval: Duration, idle_timeout: Duration) -> Duration {
    let half_idle_timeout = idle_timeout / 2;
    progress_interval.min(if half_idle_timeout.is_zero() {
        idle_timeout
    } else {
        half_idle_timeout
    })
}

fn command_process_group(child: &tokio::process::Child) -> CommandProcessGroup {
    #[cfg(unix)]
    {
        child.id().map(|id| id as libc::pid_t)
    }

    #[cfg(not(unix))]
    {
        let _ = child;
        None
    }
}

async fn terminate_command_tree(
    child: &mut tokio::process::Child,
    process_group: CommandProcessGroup,
) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group {
        signal_process_group(process_group_id, libc::SIGTERM);
        let direct_child_exited = tokio_timeout(Duration::from_secs(5), child.wait())
            .await
            .is_ok();
        signal_process_group(process_group_id, libc::SIGKILL);
        if !direct_child_exited {
            let _ = child.wait().await;
        }
        return;
    }

    let _ = child.kill().await;
}

#[cfg(unix)]
fn signal_process_group(process_group_id: libc::pid_t, signal: libc::c_int) {
    unsafe {
        libc::kill(-process_group_id, signal);
    }
}

fn force_terminate_process_group(process_group: CommandProcessGroup) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group {
        signal_process_group(process_group_id, libc::SIGKILL);
    }

    #[cfg(not(unix))]
    {
        let _ = process_group;
    }
}

async fn read_command_stream<R>(
    mut reader: R,
    stream: CommandStream,
    progress: mpsc::UnboundedSender<CommandChunk>,
) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let size = reader.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        let bytes = buffer[..size].to_vec();
        output.extend_from_slice(&bytes);
        let _ = progress.send(CommandChunk { stream, bytes });
    }
    Ok(output)
}

async fn collect_stream_outputs(
    mut stdout_handle: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    mut stderr_handle: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    process_group: CommandProcessGroup,
) -> (Vec<u8>, Vec<u8>) {
    let close_deadline = Instant::now() + OUTPUT_CLOSE_GRACE;
    let mut abort_deadline = close_deadline + OUTPUT_ABORT_GRACE;
    let mut did_terminate_group = false;
    let mut stdout = None;
    let mut stderr = None;

    loop {
        if stdout.is_some() && stderr.is_some() {
            break;
        }

        tokio::select! {
            result = &mut stdout_handle, if stdout.is_none() => {
                stdout = Some(join_stream_output(result));
            }
            result = &mut stderr_handle, if stderr.is_none() => {
                stderr = Some(join_stream_output(result));
            }
            _ = sleep_until(close_deadline), if !did_terminate_group => {
                force_terminate_process_group(process_group);
                did_terminate_group = true;
                abort_deadline = Instant::now() + OUTPUT_ABORT_GRACE;
            }
            _ = sleep_until(abort_deadline), if did_terminate_group => {
                if stdout.is_none() {
                    stdout_handle.abort();
                    stdout = Some(b"stdout reader did not close after process termination".to_vec());
                }
                if stderr.is_none() {
                    stderr_handle.abort();
                    stderr = Some(b"stderr reader did not close after process termination".to_vec());
                }
            }
        }
    }

    (
        stdout.expect("stdout is set before loop exits"),
        stderr.expect("stderr is set before loop exits"),
    )
}

fn join_stream_output(result: Result<std::io::Result<Vec<u8>>, tokio::task::JoinError>) -> Vec<u8> {
    match result {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => format!("failed to read command output: {err}").into_bytes(),
        Err(err) => format!("failed to join command output reader: {err}").into_bytes(),
    }
}

struct FileActivityTracker {
    root: PathBuf,
    baseline: FileActivitySnapshot,
    last_changed_file_count: usize,
    last_changed_size: u64,
    started_at: Instant,
    last_poll_at: Instant,
    last_change_at: Option<Instant>,
}

struct FileActivitySnapshot {
    files: BTreeMap<PathBuf, u64>,
    direct_dirs: BTreeSet<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
struct FileActivityReport {
    changed_file_count: usize,
    changed_size: u64,
    speed_bytes_per_second: f64,
    elapsed: Duration,
    last_change_age: Option<Duration>,
    changed_since_previous_poll: bool,
}

impl FileActivityTracker {
    async fn new(root: &Path) -> Result<Self> {
        let root = root.to_path_buf();
        let baseline = collect_file_activity(root.clone(), None).await?;
        let now = Instant::now();
        Ok(Self {
            root,
            baseline,
            last_changed_file_count: 0,
            last_changed_size: 0,
            started_at: now,
            last_poll_at: now,
            last_change_at: None,
        })
    }

    async fn poll(&mut self) -> Result<FileActivityReport> {
        let current =
            collect_file_activity(self.root.clone(), Some(self.baseline.direct_dirs.clone()))
                .await?;
        let now = Instant::now();
        let changed = current
            .files
            .iter()
            .filter(|(path, size)| self.baseline.files.get(*path) != Some(*size));
        let mut changed_file_count = 0;
        let mut changed_size = 0;
        for (_, size) in changed {
            changed_file_count += 1;
            changed_size += size;
        }

        let changed_since_previous_poll = changed_file_count != self.last_changed_file_count
            || changed_size != self.last_changed_size;
        let delta_bytes = changed_size.saturating_sub(self.last_changed_size);
        let poll_elapsed = now.saturating_duration_since(self.last_poll_at);
        let speed_bytes_per_second = if poll_elapsed.is_zero() {
            0.0
        } else {
            delta_bytes as f64 / poll_elapsed.as_secs_f64()
        };

        if changed_since_previous_poll {
            self.last_change_at = Some(now);
        }

        self.last_changed_file_count = changed_file_count;
        self.last_changed_size = changed_size;
        self.last_poll_at = now;

        Ok(FileActivityReport {
            changed_file_count,
            changed_size,
            speed_bytes_per_second,
            elapsed: now.saturating_duration_since(self.started_at),
            last_change_age: self
                .last_change_at
                .map(|last_change_at| now.saturating_duration_since(last_change_at)),
            changed_since_previous_poll,
        })
    }
}

async fn collect_file_activity(
    root: PathBuf,
    baseline_direct_dirs: Option<BTreeSet<PathBuf>>,
) -> Result<FileActivitySnapshot> {
    tokio::task::spawn_blocking(move || collect_file_activity_blocking(&root, baseline_direct_dirs))
        .await
        .context("failed to join file activity scan")?
}

fn collect_file_activity_blocking(
    root: &Path,
    baseline_direct_dirs: Option<BTreeSet<PathBuf>>,
) -> Result<FileActivitySnapshot> {
    let mut files = BTreeMap::new();
    let mut direct_dirs = BTreeSet::new();
    if !root.exists() {
        return Ok(FileActivitySnapshot { files, direct_dirs });
    }

    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(FileActivitySnapshot { files, direct_dirs });
        }
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", root.display())),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            files.insert(path, metadata.len());
        } else if file_type.is_dir() {
            direct_dirs.insert(path.clone());
            if should_scan_activity_dir(&path, baseline_direct_dirs.as_ref()) {
                collect_file_sizes_recursive(&path, &mut files)?;
            }
        }
    }

    Ok(FileActivitySnapshot { files, direct_dirs })
}

fn should_scan_activity_dir(path: &Path, baseline_direct_dirs: Option<&BTreeSet<PathBuf>>) -> bool {
    baseline_direct_dirs.is_some_and(|baseline| !baseline.contains(path))
        || is_likely_bilibili_aid_dir(path)
}

fn is_likely_bilibili_aid_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| !name.is_empty() && name.chars().all(|ch| ch.is_ascii_digit()))
}

fn collect_file_sizes_recursive(root: &Path, files: &mut BTreeMap<PathBuf, u64>) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", root.display())),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            files.insert(path, metadata.len());
        } else if file_type.is_dir() {
            collect_file_sizes_recursive(&path, files)?;
        }
    }
    Ok(())
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{} B", bytes as u64)
    }
}

fn human_rate(bytes_per_second: f64) -> String {
    let bytes_per_second = bytes_per_second.max(0.0).round() as u64;
    format!("{}/s", human_bytes(bytes_per_second))
}

fn format_duration_compact(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 60 * 60 {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    } else {
        format!(
            "{}h {:02}m {:02}s",
            seconds / 3600,
            (seconds % 3600) / 60,
            seconds % 60
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressStage {
    Metadata,
    Resolving,
    Downloading,
    Video,
    Audio,
    Downloaded,
    Merging,
    Finalizing,
    Running,
}

impl ProgressStage {
    fn initial_for(command_name: &str) -> Self {
        let command_name = command_name.to_ascii_lowercase();
        if command_name.contains("metadata") {
            Self::Metadata
        } else if command_name.contains("bbdown") {
            Self::Resolving
        } else if command_name.contains("yt-dlp") {
            Self::Downloading
        } else if command_name.contains("ffmpeg") {
            Self::Merging
        } else {
            Self::Running
        }
    }

    fn update_from_text(self, command_name: &str, text: &str) -> Self {
        let command_name = command_name.to_ascii_lowercase();
        let lower_text = text.to_ascii_lowercase();

        if self == Self::Metadata {
            Self::Metadata
        } else if command_name.contains("bbdown") {
            if text.contains("混流") || text.contains("合并") || lower_text.contains("mux") {
                Self::Merging
            } else if text.contains("开始下载") && (text.contains("音频") || text.contains("音轨"))
            {
                Self::Audio
            } else if text.contains("开始下载") && text.contains("视频") {
                Self::Video
            } else if text.contains("任务完成")
                || text.contains("下载任务完成")
                || lower_text.contains("download finished")
            {
                Self::Downloaded
            } else {
                self
            }
        } else if command_name.contains("yt-dlp") {
            if lower_text.contains("[download]") {
                Self::Downloading
            } else if lower_text.contains("ffmpeg") || lower_text.contains("merging") {
                Self::Merging
            } else if lower_text.contains("embedding") || lower_text.contains("metadata") {
                Self::Finalizing
            } else {
                self
            }
        } else if command_name.contains("ffmpeg") {
            Self::Merging
        } else {
            self
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Metadata => "resolving metadata",
            Self::Resolving => "resolving metadata",
            Self::Downloading => "downloading media",
            Self::Video => "downloading video",
            Self::Audio => "downloading audio",
            Self::Downloaded => "download complete",
            Self::Merging => "muxing media",
            Self::Finalizing => "finalizing",
            Self::Running => "running",
        }
    }

    fn done_label(self) -> &'static str {
        match self {
            Self::Metadata => "-",
            Self::Resolving | Self::Running => "-",
            Self::Downloading => "resolve",
            Self::Video => "resolve",
            Self::Audio => "resolve, video",
            Self::Downloaded => "resolve, video, audio",
            Self::Merging => "download",
            Self::Finalizing => "download, mux",
        }
    }

    fn todo_label(self) -> &'static str {
        match self {
            Self::Metadata => "download, embed, move",
            Self::Resolving => "video, audio, mux, move",
            Self::Downloading => "metadata, embed, move",
            Self::Video => "audio, mux, move",
            Self::Audio => "mux, move",
            Self::Downloaded => "mux, move",
            Self::Merging => "move",
            Self::Finalizing => "move",
            Self::Running => "finish",
        }
    }
}

struct ProgressTracker {
    command_name: String,
    min_interval: Duration,
    next_send_at: Instant,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
    last_message: Option<String>,
    last_output: Option<String>,
    last_file_activity: Option<FileActivityReport>,
    stage: ProgressStage,
}

impl ProgressTracker {
    fn new(
        command_name: String,
        min_interval: Duration,
        progress: Option<mpsc::UnboundedSender<JobProgress>>,
    ) -> Self {
        Self {
            stage: ProgressStage::initial_for(&command_name),
            command_name,
            min_interval,
            next_send_at: Instant::now(),
            progress,
            last_message: None,
            last_output: None,
            last_file_activity: None,
        }
    }

    fn observe(&mut self, stream: CommandStream, bytes: &[u8]) {
        if self.progress.is_none() {
            return;
        }

        let text = normalize_terminal_text(&String::from_utf8_lossy(bytes));
        self.stage = self.stage.update_from_text(&self.command_name, &text);
        let Some(message) = summarize_progress_chunk(&self.command_name, stream, &text) else {
            return;
        };

        self.last_output = Some(message);
        let now = Instant::now();
        self.emit_current(now);
    }

    fn emit_file_activity(&mut self, report: FileActivityReport) {
        if self.progress.is_none() {
            return;
        }

        self.last_file_activity = Some(report);
        self.emit_current(Instant::now());
    }

    fn emit_current(&mut self, now: Instant) {
        let Some(progress) = &self.progress else {
            return;
        };

        if now < self.next_send_at {
            return;
        }
        let message = self.current_message();
        if self.last_message.as_ref() == Some(&message) {
            return;
        }

        self.send(progress.clone(), message, now);
    }

    fn current_message(&self) -> String {
        let mut lines = vec![
            format!("{}: {}", self.command_name, self.stage.label()),
            format!("Done: {}", self.stage.done_label()),
            format!("Todo: {}", self.stage.todo_label()),
        ];

        if let Some(report) = self.last_file_activity {
            lines.push(format!(
                "Files: {} changed, {} written",
                report.changed_file_count,
                human_bytes(report.changed_size)
            ));
            lines.push(format!(
                "Speed: {}",
                human_rate(report.speed_bytes_per_second)
            ));
            lines.push(format!(
                "Elapsed: {}",
                format_duration_compact(report.elapsed)
            ));
            let last_change = report
                .last_change_age
                .map(|age| format!("{} ago", format_duration_compact(age)))
                .unwrap_or_else(|| "none yet".to_string());
            lines.push(format!("Last file change: {last_change}"));
        }

        if let Some(output) = &self.last_output {
            lines.push(format!("Last output: {output}"));
        }

        lines.join("\n")
    }

    fn send(
        &mut self,
        progress: mpsc::UnboundedSender<JobProgress>,
        message: String,
        now: Instant,
    ) {
        let message = redact_sensitive_output(&message);
        self.last_message = Some(message.clone());
        self.next_send_at = now + self.min_interval;
        info!(command = %self.command_name, message = %message, "command progress");
        let _ = progress.send(JobProgress { message });
    }
}

fn summarize_progress_chunk(
    command_name: &str,
    stream: CommandStream,
    text: &str,
) -> Option<String> {
    if let Some(percent) = extract_last_percent(text) {
        return Some(format!("{command_name}: {percent}%"));
    }

    let line = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| !line.starts_with("Response:"))
        .filter(|line| !line.starts_with('{'))
        .filter(|line| !line.contains("baseUrl"))
        .rfind(|line| line.chars().count() <= 180)?;

    let normalized = line
        .trim_start_matches(|ch: char| ch == '-' || ch.is_ascii_whitespace())
        .to_string();
    if normalized.is_empty() {
        return None;
    }

    let stream_name = match stream {
        CommandStream::Stdout => "stdout",
        CommandStream::Stderr => "stderr",
    };
    Some(format!("{command_name} {stream_name}: {normalized}"))
}

fn normalize_terminal_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch == '\r' || ch == '\n' {
            normalized.push('\n');
        } else if ch.is_control() {
            normalized.push(' ');
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

fn extract_last_percent(text: &str) -> Option<u8> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut last = None;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }

        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }

        if index < bytes.len() && bytes[index] == b'.' {
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }

        if index < bytes.len()
            && bytes[index] == b'%'
            && let Ok(value) = text[start..index].parse::<f64>()
            && (0.0..=100.0).contains(&value)
        {
            last = Some(value.floor() as u8);
        }
    }
    last
}

fn select_subtitles(metadata: &YoutubeMetadata, preferred_languages: &[String]) -> SubtitlePlan {
    let manual = select_matching_languages(&metadata.subtitles, preferred_languages);
    if !manual.is_empty() {
        return SubtitlePlan {
            source: Some(SubtitleSource::Manual),
            languages: manual,
        };
    }

    let automatic = select_matching_languages(&metadata.automatic_captions, preferred_languages);
    if !automatic.is_empty() {
        return SubtitlePlan {
            source: Some(SubtitleSource::Automatic),
            languages: automatic,
        };
    }

    SubtitlePlan::none()
}

fn select_matching_languages(
    available: &BTreeMap<String, serde_json::Value>,
    preferred_languages: &[String],
) -> Vec<String> {
    let mut selected = Vec::new();
    for preferred in preferred_languages {
        for language in available.keys() {
            if language_matches(preferred, language) && !selected.contains(language) {
                selected.push(language.clone());
            }
        }
    }
    selected
}

fn language_matches(preferred: &str, available: &str) -> bool {
    let preferred = preferred.to_ascii_lowercase();
    let available = available.to_ascii_lowercase();
    available == preferred
        || available
            .strip_prefix(&preferred)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn parse_bilibili_metadata(url: &str, stdout: &str) -> BilibiliMetadata {
    let mut metadata = BilibiliMetadata {
        id: bilibili_id_from_url(url),
        ..BilibiliMetadata::default()
    };

    for line in stdout.lines() {
        if let Some((_, title)) = line.split_once("视频标题:") {
            metadata.title = Some(title.trim().to_string());
        } else if let Some((_, aid)) = line.split_once("获取aid结束:") {
            metadata.aid = Some(aid.trim().to_string());
        } else if let Some((_, video_url)) = line.split_once("视频URL:") {
            if let Some(resolved_id) = bilibili_id_from_url(video_url.trim()) {
                if metadata.id.is_none() {
                    metadata.id = Some(resolved_id.clone());
                }
                metadata.resolved_id = Some(resolved_id);
            }
        } else if let Some((_, published)) = line.split_once("发布时间:") {
            let published = published.trim();
            metadata.publish_date = published.get(..10).map(str::to_string);
        } else if let Some((_, uploader_url)) = line.split_once("UP主页:") {
            metadata.uploader_url = Some(uploader_url.trim().to_string());
        }
    }

    metadata
}

fn push_bilibili_metadata_identities(
    identities: &mut Vec<VideoIdentity>,
    metadata: &BilibiliMetadata,
) {
    for id in [
        metadata.id.as_deref(),
        metadata.resolved_id.as_deref(),
        metadata.aid.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter(|id| !id.trim().is_empty())
    {
        push_unique_video_identity(
            identities,
            VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: id.to_string(),
            },
        );
    }
    if let Some(aid) = metadata.aid.as_deref().filter(|aid| !aid.trim().is_empty()) {
        push_unique_video_identity(
            identities,
            VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: format!("av{}", aid.trim_start_matches("av")),
            },
        );
    }
}

fn push_bilibili_plan_identities(identities: &mut Vec<VideoIdentity>, plan: &BilibiliDownloadPlan) {
    for entry in &plan.entries {
        if let Some(bvid) = entry.bvid.as_deref().filter(|bvid| !bvid.trim().is_empty()) {
            push_unique_video_identity(
                identities,
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: bvid.to_string(),
                },
            );
        }
        for id in [format!("av{}", entry.aid), format!("cid{}", entry.cid)] {
            push_unique_video_identity(
                identities,
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id,
                },
            );
        }
        if let Some(epid) = entry.epid {
            push_unique_video_identity(
                identities,
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: format!("ep{epid}"),
                },
            );
        }
    }
}

fn bilibili_plan_overwrite_identities(plan: &BilibiliDownloadPlan) -> Vec<VideoIdentity> {
    let [entry] = plan.entries.as_slice() else {
        return Vec::new();
    };
    let mut identities = Vec::new();
    if entry.cid != 0 {
        identities.push(VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: format!("cid{}", entry.cid),
        });
    }
    if let Some(epid) = entry.epid {
        identities.push(VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: format!("ep{epid}"),
        });
    }
    identities
}

fn identity_is_overwrite_safe(identity: &VideoIdentity) -> bool {
    match identity.provider {
        VideoProvider::Youtube => true,
        VideoProvider::Bilibili => is_bilibili_entry_identity(&identity.id),
    }
}

fn push_unique_video_identity(identities: &mut Vec<VideoIdentity>, identity: VideoIdentity) {
    if identities
        .iter()
        .any(|existing| existing.provider == identity.provider && existing.id == identity.id)
    {
        return;
    }
    identities.push(identity);
}

fn bilibili_id_from_url(raw_url: &str) -> Option<String> {
    let url = url::Url::parse(raw_url).ok()?;
    url.path_segments()?
        .find(|segment| {
            segment.starts_with("BV")
                || segment.starts_with("bv")
                || segment.starts_with("av")
                || segment.starts_with("ep")
        })
        .map(str::to_string)
}

fn video_identity(job: &JobRequest) -> Option<VideoIdentity> {
    match job {
        JobRequest::Bilibili { url, .. } => bilibili_id_from_url(url).map(|id| VideoIdentity {
            provider: VideoProvider::Bilibili,
            id,
        }),
        JobRequest::Youtube { url } => youtube_id_from_url(url).map(|id| VideoIdentity {
            provider: VideoProvider::Youtube,
            id,
        }),
        JobRequest::Pdf { .. } => None,
    }
}

fn youtube_id_from_url(raw_url: &str) -> Option<String> {
    let url = url::Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if host == "youtu.be" {
        return url
            .path_segments()?
            .find(|segment| !segment.is_empty())
            .map(str::to_string);
    }

    if !domain_or_subdomain(&host, "youtube.com")
        && !domain_or_subdomain(&host, "youtube-nocookie.com")
    {
        return None;
    }

    if let Some(video_id) = url
        .query_pairs()
        .find(|(key, _)| key == "v")
        .map(|(_, value)| value.to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(video_id);
    }

    let mut segments = url.path_segments()?;
    match segments.next()? {
        "embed" | "shorts" | "live" => segments
            .next()
            .filter(|segment| !segment.is_empty())
            .map(str::to_string),
        _ => None,
    }
}

fn domain_or_subdomain(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn index_video_identities(
    index: &mut VideoIdentityIndex,
    video: &Path,
    read_policy: IdentityIndexReadPolicy,
) -> Result<()> {
    index_video_filename_identities(index, video);

    for path in metadata_sidecar_paths(video) {
        index_metadata_sidecar(index, video, &path, &path, read_policy)?;
    }

    Ok(())
}

fn index_video_filename_identities(index: &mut VideoIdentityIndex, video: &Path) {
    for id in video_file_identity_ids(video) {
        for provider in [VideoProvider::Bilibili, VideoProvider::Youtube] {
            let identity = VideoIdentity {
                provider,
                id: id.clone(),
            };
            if provider == VideoProvider::Youtube {
                index.insert_overwrite_evidence(identity, video);
            } else {
                index.insert(identity, video);
            }
        }
    }
}

fn index_metadata_sidecar(
    index: &mut VideoIdentityIndex,
    video: &Path,
    logical_path: &Path,
    content_path: &Path,
    read_policy: IdentityIndexReadPolicy,
) -> Result<()> {
    let content = match fs::read_to_string(content_path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) if matches!(read_policy, IdentityIndexReadPolicy::BestEffort) => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "overwrite identity metadata is unreadable: {}",
                    logical_path.display()
                )
            });
        }
    };
    let identities = match metadata_sidecar_identities(logical_path, &content) {
        Ok(identities) => identities,
        Err(_) if matches!(read_policy, IdentityIndexReadPolicy::BestEffort) => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "overwrite identity metadata is invalid: {}",
                    logical_path.display()
                )
            });
        }
    };
    for identity in identities {
        index.insert_overwrite_evidence(identity, video);
    }

    Ok(())
}

fn metadata_sidecars_match_identity(video: &Path, identity: &VideoIdentity) -> bool {
    metadata_sidecar_paths(video).into_iter().any(|path| {
        fs::read_to_string(&path)
            .ok()
            .and_then(|content| metadata_sidecar_identities(&path, &content).ok())
            .is_some_and(|identities| identities.contains(identity))
    })
}

fn video_file_identity_ids(path: &Path) -> Vec<String> {
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Vec::new();
    };
    let mut ids = vec![stem.to_string()];
    for (open, close) in [('[', ']'), ('(', ')')] {
        if let Some(id) = trailing_delimited_identity(stem, open, close)
            && !ids.iter().any(|existing| existing == id)
        {
            ids.push(id.to_string());
        }
    }
    ids
}

fn trailing_delimited_identity(stem: &str, open: char, close: char) -> Option<&str> {
    let without_close = stem.strip_suffix(close)?;
    let start = without_close.rfind(open)? + open.len_utf8();
    let identity = &without_close[start..];
    (!identity.is_empty()).then_some(identity)
}

fn metadata_sidecar_identities(path: &Path, content: &str) -> Result<Vec<VideoIdentity>> {
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".info.json"))
    {
        let metadata = serde_json::from_str::<serde_json::Value>(content)
            .context("failed to parse info JSON")?;
        return Ok(info_json_identities(&metadata));
    }

    if path.extension().and_then(|extension| extension.to_str()) == Some("nfo") {
        return Ok(nfo_identities(content));
    }

    Ok(Vec::new())
}

fn info_json_identities(metadata: &serde_json::Value) -> Vec<VideoIdentity> {
    let Some(id) = json_string_field(metadata, "id").filter(|id| !id.trim().is_empty()) else {
        return Vec::new();
    };
    let mut providers = BTreeSet::new();
    for value in ["extractor", "extractor_key", "ie_key"]
        .into_iter()
        .filter_map(|key| json_string_field(metadata, key))
    {
        let value = value.to_ascii_lowercase();
        for provider in [VideoProvider::Bilibili, VideoProvider::Youtube] {
            if value.contains(provider.as_str()) {
                providers.insert(provider);
            }
        }
    }
    if let Some(provider) = json_string_field(metadata, "webpage_url").and_then(provider_from_url) {
        providers.insert(provider);
    }
    providers
        .into_iter()
        .map(|provider| VideoIdentity {
            provider,
            id: id.to_string(),
        })
        .collect()
}

fn json_string_field<'a>(metadata: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    metadata.get(key)?.as_str()
}

fn provider_from_url(raw_url: &str) -> Option<VideoProvider> {
    let url = url::Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if domain_or_subdomain(&host, "bilibili.com")
        || domain_or_subdomain(&host, "bilibili.tv")
        || host == "b23.tv"
    {
        Some(VideoProvider::Bilibili)
    } else if domain_or_subdomain(&host, "youtube.com")
        || domain_or_subdomain(&host, "youtube-nocookie.com")
        || host == "youtu.be"
    {
        Some(VideoProvider::Youtube)
    } else {
        None
    }
}

fn nfo_identities(content: &str) -> Vec<VideoIdentity> {
    let mut identities = Vec::new();
    for chunk in content.split("<uniqueid").skip(1) {
        let Some((tag, rest)) = chunk.split_once('>') else {
            continue;
        };
        let tag = tag.to_ascii_lowercase();
        let Some(unique_id_type) = ['"', '\'']
            .into_iter()
            .find_map(|quote| uniqueid_tag_type(&tag, quote))
        else {
            continue;
        };
        let Some(provider) = unique_id_type_provider(unique_id_type) else {
            continue;
        };
        let Some((value, _)) = rest.split_once("</uniqueid>") else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        push_unique_video_identity(
            &mut identities,
            VideoIdentity {
                provider,
                id: value.to_string(),
            },
        );
        if provider == VideoProvider::Bilibili
            && matches!(unique_id_type, "bilibili-aid" | "bilibili_aid")
            && value.bytes().all(|byte| byte.is_ascii_digit())
        {
            push_unique_video_identity(
                &mut identities,
                VideoIdentity {
                    provider,
                    id: format!("av{value}"),
                },
            );
        }
    }
    identities
}

fn unique_id_type_provider(unique_id_type: &str) -> Option<VideoProvider> {
    [VideoProvider::Bilibili, VideoProvider::Youtube]
        .into_iter()
        .find(|provider| {
            unique_id_type == provider.as_str()
                || unique_id_type
                    .strip_prefix(provider.as_str())
                    .is_some_and(|suffix| {
                        matches!(
                            suffix,
                            "-aid" | "_aid" | "-cid" | "_cid" | "-epid" | "_epid"
                        )
                    })
        })
}

fn uniqueid_tag_type(tag: &str, quote: char) -> Option<&str> {
    let (_, rest) = tag.split_once(&format!("type={quote}"))?;
    let (value, _) = rest.split_once(quote)?;
    Some(value)
}

fn metadata_sidecar_paths(video: &Path) -> Vec<PathBuf> {
    ["nfo", "info.json"]
        .into_iter()
        .map(|extension| video.with_extension(extension))
        .collect()
}

fn list_video_files(root: &Path) -> Result<BTreeSet<PathBuf>> {
    list_primary_media_files(root, StagedPrimaryMediaKind::Video)
}

fn list_primary_media_files(
    root: &Path,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    collect_primary_media_files(root, primary_media_kind, &mut files)?;
    Ok(files)
}

fn collect_primary_media_files(
    path: &Path,
    primary_media_kind: StagedPrimaryMediaKind,
    files: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == VIDEO_STAGING_DIR_NAME)
            {
                continue;
            }
            collect_primary_media_files(&path, primary_media_kind, files)?;
        } else if file_type.is_file() && is_primary_media_file(&path, primary_media_kind) {
            files.insert(path);
        }
    }

    Ok(())
}

fn is_video_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "mkv" | "mp4" | "m4v" | "webm" | "mov" | "avi" | "flv" | "m4s"
            )
        })
        .unwrap_or(false)
}

fn is_audio_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "m4a" | "mp3" | "aac" | "flac" | "ogg" | "opus" | "wav"
            )
        })
        .unwrap_or(false)
}

fn is_primary_media_file(path: &Path, kind: StagedPrimaryMediaKind) -> bool {
    is_video_file(path)
        || (matches!(kind, StagedPrimaryMediaKind::VideoOrAudio) && is_audio_file(path))
}

fn create_video_staging_dir(final_dir: &Path) -> Result<PathBuf> {
    let parent = final_dir.join(VIDEO_STAGING_DIR_NAME);
    fs::create_dir_all(&parent)
        .with_context(|| format!("failed to create staging directory {}", parent.display()))?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for index in 0..1000 {
        let candidate = parent.join(format!("job-{}-{nanos}-{index}", std::process::id()));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to create {}", candidate.display()));
            }
        }
    }
    bail!(
        "failed to allocate a unique staging directory under {}",
        parent.display()
    )
}

fn copy_bbdown_config_for_staging(final_dir: &Path, staging_dir: &Path) -> Result<()> {
    let source = final_dir.join("BBDown.config");
    if !source.is_file() {
        return Ok(());
    }
    let destination = staging_dir.join("BBDown.config");
    fs::copy(&source, &destination).with_context(|| {
        format!(
            "failed to copy {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn is_staging_support_file(staging_dir: &Path, path: &Path) -> bool {
    path == staging_dir.join("BBDown.config")
}

fn collect_regular_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_regular_files_recursive(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_regular_files_recursive(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_regular_files_recursive(&path, files)?;
        } else if file_type.is_file() {
            files.push(path);
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct MoveStep {
    source: PathBuf,
    destination: PathBuf,
}

#[derive(Debug)]
struct FileBackup {
    original: PathBuf,
    backup: PathBuf,
}

#[derive(Debug)]
struct AcquiredOverwrite {
    target: PathBuf,
    backups: Vec<FileBackup>,
    backup_dir: PathBuf,
    target_restored: bool,
}

impl AcquiredOverwrite {
    fn target(&self) -> &Path {
        &self.target
    }

    fn backup_for(&self, original: &Path) -> Option<&Path> {
        self.backups
            .iter()
            .find(|backup| backup.original == original)
            .map(|backup| backup.backup.as_path())
    }

    fn restore_original(&mut self, original: &Path) -> Result<()> {
        let index = self
            .backups
            .iter()
            .position(|backup| backup.original == original)
            .with_context(|| format!("overwrite backup is missing for {}", original.display()))?;
        let backup = self.backups.remove(index);
        if let Err(err) = restore_file_backup(&backup) {
            self.backups.insert(index, backup);
            return Err(err);
        }
        if original == self.target {
            self.target_restored = true;
        }
        Ok(())
    }

    fn restore_unreplaced(&mut self, replaced_destinations: &BTreeSet<PathBuf>) -> Result<()> {
        let target = self.target.clone();
        self.restore_original(&target)?;

        let untouched = self
            .backups
            .iter()
            .filter(|backup| !replaced_destinations.contains(backup.original.as_path()))
            .map(|backup| backup.original.clone())
            .collect::<Vec<_>>();
        for original in untouched {
            self.restore_original(&original)?;
        }
        Ok(())
    }

    fn restore(self) -> Result<()> {
        if self.target_restored {
            restore_remaining_backups(&self.backups, &self.backup_dir, &self.target)
        } else {
            restore_backups(&self.backups, &self.backup_dir, &self.target)
        }
    }

    fn commit(self) -> Result<()> {
        remove_backups(&self.backups, &self.backup_dir)
    }
}

#[derive(Debug)]
struct RemoveDirOnDrop {
    path: PathBuf,
}

impl RemoveDirOnDrop {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for RemoveDirOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn acquire_and_validate_overwrite_target(
    final_dir: &Path,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<AcquiredOverwrite> {
    // Protect the semantic identity of the acquired object; timestamps are not identity evidence.
    let target = duplicate
        .overwrite_target()
        .context("overwrite target is not an exact unique match")?
        .clone();
    if !target.starts_with(final_dir) {
        bail!(
            "overwrite target is outside the configured video directory: {}",
            target.display()
        );
    }

    let mut artifacts = existing_video_artifacts(&target)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    artifacts.remove(&target);
    artifacts.extend(metadata_sidecar_paths(&target));
    let backup_parent = target.parent().unwrap_or(final_dir);
    let backup_dir = create_overwrite_backup_dir(backup_parent)?;
    let mut backups = Vec::new();

    if let Err(err) = acquire_overwrite_path(&target, true, &backup_dir, &mut backups) {
        if backups.is_empty() {
            let _ = fs::remove_dir(&backup_dir);
            return Err(err);
        }
        return Err(rollback_acquired_overwrite(
            err,
            AcquiredOverwrite {
                target,
                backups,
                backup_dir,
                target_restored: false,
            },
        ));
    }
    for artifact in artifacts {
        if let Err(err) = acquire_overwrite_path(&artifact, false, &backup_dir, &mut backups) {
            return Err(rollback_acquired_overwrite(
                err,
                AcquiredOverwrite {
                    target,
                    backups,
                    backup_dir,
                    target_restored: false,
                },
            ));
        }
    }

    let acquired = AcquiredOverwrite {
        target,
        backups,
        backup_dir,
        target_restored: false,
    };
    if let Err(err) =
        validate_acquired_overwrite(final_dir, duplicate, primary_media_kind, &acquired)
    {
        return Err(rollback_acquired_overwrite(err, acquired));
    }
    Ok(acquired)
}

fn create_overwrite_backup_dir(parent: &Path) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..128 {
        let counter = OVERWRITE_BACKUP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            "{OVERWRITE_BACKUP_DIR_PREFIX}-{}-{stamp:x}-{counter:x}",
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to create overwrite backup directory {}",
                        path.display()
                    )
                });
            }
        }
    }
    bail!("failed to allocate a unique overwrite backup directory")
}

fn acquire_overwrite_path(
    original: &Path,
    required: bool,
    backup_dir: &Path,
    backups: &mut Vec<FileBackup>,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(original) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => return Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            bail!("overwrite target is missing: {}", original.display());
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to inspect overwrite path {}", original.display())
            });
        }
    };
    if !metadata.file_type().is_file() {
        if is_identity_metadata_path(original) {
            bail!(
                "overwrite identity metadata is unreadable: {}",
                original.display()
            );
        }
        bail!(
            "overwrite path is no longer a regular file: {}",
            original.display()
        );
    }

    let backup = backup_dir.join(format!("{:04x}", backups.len()));
    fs::rename(original, &backup).with_context(|| {
        format!(
            "failed to acquire overwrite path {} as {}",
            original.display(),
            backup.display()
        )
    })?;
    backups.push(FileBackup {
        original: original.to_path_buf(),
        backup: backup.clone(),
    });
    let acquired_metadata = fs::symlink_metadata(&backup).with_context(|| {
        format!(
            "failed to inspect acquired overwrite path {}",
            backup.display()
        )
    })?;
    if !acquired_metadata.file_type().is_file() {
        bail!(
            "acquired overwrite path is not a regular file: {}",
            original.display()
        );
    }
    Ok(())
}

fn validate_acquired_overwrite(
    final_dir: &Path,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
    acquired: &AcquiredOverwrite,
) -> Result<()> {
    let target_backup = acquired
        .backup_for(acquired.target())
        .context("acquired overwrite target backup is missing")?;
    let metadata = fs::symlink_metadata(target_backup).with_context(|| {
        format!(
            "failed to inspect acquired overwrite target {}",
            target_backup.display()
        )
    })?;
    if !metadata.file_type().is_file()
        || !is_primary_media_file(acquired.target(), primary_media_kind)
    {
        bail!(
            "acquired overwrite target is not a regular primary media file: {}",
            acquired.target().display()
        );
    }

    let mut acquired_index = VideoIdentityIndex::default();
    index_video_filename_identities(&mut acquired_index, acquired.target());
    for logical_path in metadata_sidecar_paths(acquired.target()) {
        if let Some(content_path) = acquired.backup_for(&logical_path) {
            index_metadata_sidecar(
                &mut acquired_index,
                acquired.target(),
                &logical_path,
                content_path,
                IdentityIndexReadPolicy::Strict,
            )?;
        } else if path_entry_exists(&logical_path)? {
            bail!(
                "overwrite identity metadata changed during acquisition: {}",
                logical_path.display()
            );
        }
    }
    match acquired_index.overwrite_videos(&duplicate.identity) {
        [current_target] if current_target == acquired.target() => {}
        [] => bail!(
            "acquired overwrite target identity no longer matches {}:{}: {}",
            duplicate.identity.provider.as_str(),
            duplicate.identity.id,
            acquired.target().display()
        ),
        _ => bail!(
            "acquired overwrite identity {}:{} is ambiguous",
            duplicate.identity.provider.as_str(),
            duplicate.identity.id
        ),
    }

    if path_entry_exists(acquired.target())? {
        bail!(
            "overwrite target path was recreated during acquisition: {}",
            acquired.target().display()
        );
    }
    let live_index = build_video_identity_index_in_dir(
        final_dir,
        primary_media_kind,
        IdentityIndexReadPolicy::Strict,
    )
    .context("failed to rebuild the media identity index after acquiring overwrite target")?;
    let live_targets = live_index.overwrite_videos(&duplicate.identity);
    if !live_targets.is_empty() {
        bail!(
            "overwrite identity {}:{} is now ambiguous across {} files",
            duplicate.identity.provider.as_str(),
            duplicate.identity.id,
            live_targets.len() + 1
        );
    }
    Ok(())
}

fn path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn is_identity_metadata_path(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("nfo")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".info.json"))
}

fn rollback_acquired_overwrite(error: anyhow::Error, acquired: AcquiredOverwrite) -> anyhow::Error {
    match acquired.restore() {
        Ok(()) => error,
        Err(restore_error) => {
            anyhow!("{error:#}\nfailed to restore acquired overwrite files: {restore_error:#}")
        }
    }
}

fn move_staged_video_files(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let staged_media_count = staged_files
        .iter()
        .filter(|path| is_primary_media_file(path, primary_media_kind))
        .count();
    let acquisition = if matches!(action, VideoDuplicateAction::Overwrite) {
        if staged_media_count != 1 {
            bail!("overwrite requires exactly one staged primary media file");
        }
        Some(acquire_and_validate_overwrite_target(
            final_dir,
            duplicate,
            primary_media_kind,
        )?)
    } else {
        None
    };
    let overwrite_target = acquisition.as_ref().map(AcquiredOverwrite::target);

    let move_result = move_staged_video_files_inner(
        staging_dir,
        final_dir,
        staged_files,
        overwrite_target,
        primary_media_kind,
    );
    match move_result {
        Ok(moved_videos) => {
            if let Some(acquisition) = acquisition {
                acquisition
                    .commit()
                    .context("video overwrite succeeded but old-file cleanup failed")?;
            }
            Ok(moved_videos)
        }
        Err(err) => Err(match acquisition {
            Some(acquisition) => rollback_acquired_overwrite(err, acquisition),
            None => err,
        }),
    }
}

fn move_staged_video_files_inner(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    overwrite_target: Option<&Path>,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let plan = staged_move_plan(
        staging_dir,
        final_dir,
        staged_files,
        overwrite_target,
        primary_media_kind,
    )?;
    execute_move_plan(plan, primary_media_kind)
}

fn move_staged_artifact_files(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let mut acquisition = if matches!(action, VideoDuplicateAction::Overwrite) {
        Some(acquire_and_validate_overwrite_target(
            final_dir,
            duplicate,
            primary_media_kind,
        )?)
    } else {
        None
    };
    let overwrite_target = acquisition.as_ref().map(AcquiredOverwrite::target);
    let plan_result =
        staged_artifact_move_plan(staging_dir, final_dir, staged_files, overwrite_target);
    let plan = match plan_result {
        Ok(plan) => plan,
        Err(err) => {
            return Err(match acquisition {
                Some(acquired) => rollback_acquired_overwrite(err, acquired),
                None => err,
            });
        }
    };
    let moved_pairs = plan
        .iter()
        .map(|step| (step.source.clone(), step.destination.clone()))
        .collect::<Vec<_>>();
    let replaced_destinations = moved_pairs
        .iter()
        .map(|(_, destination)| destination.clone())
        .collect::<BTreeSet<_>>();
    let move_result = execute_artifact_move_plan(plan);
    match move_result {
        Ok(moved_files) => {
            if let Some(mut acquired) = acquisition.take() {
                if let Err(err) = acquired.restore_unreplaced(&replaced_destinations) {
                    rollback_moves(&moved_pairs);
                    return Err(rollback_acquired_overwrite(err, acquired));
                }
                acquired
                    .commit()
                    .context("artifact overwrite succeeded but old-sidecar cleanup failed")?;
            }
            Ok(moved_files)
        }
        Err(err) => Err(match acquisition {
            Some(acquired) => rollback_acquired_overwrite(err, acquired),
            None => err,
        }),
    }
}

fn staged_artifact_move_plan(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    overwrite_video: Option<&Path>,
) -> Result<Vec<MoveStep>> {
    let mut reserved = BTreeSet::new();
    let mut steps = Vec::with_capacity(staged_files.len());
    for source in staged_files {
        let preferred = overwrite_video
            .and_then(|video| artifact_overwrite_destination(source, video))
            .unwrap_or_else(|| relative_destination(staging_dir, final_dir, source));
        let destination = if overwrite_video.is_some() {
            if reserved.contains(&preferred) {
                bail!(
                    "multiple staged artifacts map to overwrite destination {}",
                    preferred.display()
                );
            }
            preferred
        } else {
            unique_path_avoiding(preferred, &reserved)
        };
        reserved.insert(destination.clone());
        steps.push(MoveStep {
            source: source.clone(),
            destination,
        });
    }
    Ok(steps)
}

fn artifact_overwrite_destination(source: &Path, target_video: &Path) -> Option<PathBuf> {
    if !is_known_video_sidecar(source) {
        return None;
    }
    if let Some(suffix) = unbound_bilibili_sidecar_suffix(source) {
        return sidecar_destination_for_target_video(target_video, &suffix);
    }
    let extension = source.extension()?.to_str()?;
    let source_stem = source.file_stem()?.to_str()?;
    let target_stem = target_video.file_stem()?.to_str()?;
    let suffix = source_stem
        .find('.')
        .map(|index| &source_stem[index..])
        .unwrap_or("");
    Some(
        target_video
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{target_stem}{suffix}.{extension}")),
    )
}

fn execute_artifact_move_plan(plan: Vec<MoveStep>) -> Result<Vec<PathBuf>> {
    let mut moved = Vec::new();
    for step in plan {
        if step.destination.exists() {
            rollback_moves(&moved);
            bail!(
                "destination already exists while moving staged artifact: {}",
                step.destination.display()
            );
        }
        if let Some(parent) = step.destination.parent()
            && !parent.as_os_str().is_empty()
            && let Err(err) = fs::create_dir_all(parent)
        {
            rollback_moves(&moved);
            return Err(err).with_context(|| format!("failed to create {}", parent.display()));
        }
        if let Err(err) = fs::rename(&step.source, &step.destination) {
            rollback_moves(&moved);
            return Err(err).with_context(|| {
                format!(
                    "failed to move {} to {}",
                    step.source.display(),
                    step.destination.display()
                )
            });
        }
        moved.push((step.source, step.destination));
    }
    Ok(moved
        .into_iter()
        .map(|(_, destination)| destination)
        .collect())
}

fn staged_move_plan(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    overwrite_target: Option<&Path>,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<MoveStep>> {
    let mut reserved = BTreeSet::new();
    let staged_videos = staged_files
        .iter()
        .filter(|path| is_primary_media_file(path, primary_media_kind))
        .collect::<Vec<_>>();
    if overwrite_target.is_some() && staged_videos.len() != 1 {
        bail!("overwrite requires exactly one staged primary media file");
    }
    let mut video_destinations = Vec::with_capacity(staged_videos.len());
    for staged_video in &staged_videos {
        let preferred = match overwrite_target {
            Some(existing_video) => overwrite_video_destination(existing_video, staged_video),
            None => unique_primary_media_path_avoiding(
                relative_destination(staging_dir, final_dir, staged_video),
                &reserved,
            ),
        };
        if reserved.contains(&preferred) {
            bail!(
                "multiple staged media files map to overwrite destination {}",
                preferred.display()
            );
        }
        reserved.insert(preferred.clone());
        video_destinations.push((*staged_video, preferred));
    }

    let mut steps = Vec::with_capacity(staged_files.len());

    for source in staged_files {
        let destination = if let Some((_, destination)) = video_destinations
            .iter()
            .find(|(staged_video, _)| source == *staged_video)
        {
            destination.clone()
        } else if let Some(preferred) = sidecar_destination_for_best_primary(
            source,
            video_destinations
                .iter()
                .map(|(staged_video, destination)| (*staged_video, destination.as_path())),
        ) {
            if overwrite_target.is_some() {
                if reserved.contains(&preferred) {
                    bail!(
                        "multiple staged files map to overwrite destination {}",
                        preferred.display()
                    );
                }
                preferred
            } else {
                unique_path_avoiding(preferred, &reserved)
            }
        } else {
            unique_path_avoiding(
                relative_destination(staging_dir, final_dir, source),
                &reserved,
            )
        };
        reserved.insert(destination.clone());
        steps.push(MoveStep {
            source: source.clone(),
            destination,
        });
    }

    Ok(steps)
}

fn sidecar_destination_for_best_primary<'a>(
    source: &Path,
    video_destinations: impl Iterator<Item = (&'a PathBuf, &'a Path)>,
) -> Option<PathBuf> {
    let video_destinations = video_destinations.collect::<Vec<_>>();
    if let Some(destination) = video_destinations
        .iter()
        .filter_map(|(staged_video, video_destination)| {
            let suffix = sidecar_suffix_for_video(source, staged_video)?;
            let stem_len = staged_video.file_stem()?.to_str()?.len();
            let destination = sidecar_destination_for_target_video(video_destination, &suffix)?;
            Some((stem_len, destination))
        })
        .max_by_key(|(stem_len, _)| *stem_len)
        .map(|(_, destination)| destination)
    {
        return Some(destination);
    }

    unbound_bilibili_sidecar_destination_for_single_primary(source, &video_destinations)
}

fn unbound_bilibili_sidecar_destination_for_single_primary<'a>(
    source: &Path,
    video_destinations: &[(&'a PathBuf, &'a Path)],
) -> Option<PathBuf> {
    let suffix = unbound_bilibili_sidecar_suffix(source)?;
    let matching_videos = video_destinations
        .iter()
        .filter(|(staged_video, _)| staged_video.parent() == source.parent())
        .collect::<Vec<_>>();
    if matching_videos.len() != 1 {
        return None;
    }
    let (_, video_destination) = matching_videos[0];
    sidecar_destination_for_target_video(video_destination, &suffix)
}

fn relative_destination(staging_dir: &Path, final_dir: &Path, source: &Path) -> PathBuf {
    source
        .strip_prefix(staging_dir)
        .map(|relative| final_dir.join(relative))
        .unwrap_or_else(|_| {
            final_dir.join(
                source
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("download")),
            )
        })
}

fn overwrite_video_destination(existing_video: &Path, staged_video: &Path) -> PathBuf {
    match staged_video
        .extension()
        .and_then(|extension| extension.to_str())
    {
        Some(extension)
            if existing_video
                .extension()
                .and_then(|existing| existing.to_str())
                .is_none_or(|existing| !existing.eq_ignore_ascii_case(extension)) =>
        {
            existing_video.with_extension(extension)
        }
        _ => existing_video.to_path_buf(),
    }
}

fn sidecar_destination_for_target_video(target_video: &Path, suffix: &str) -> Option<PathBuf> {
    let target_stem = target_video.file_stem()?.to_str()?;
    Some(
        target_video
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{target_stem}{suffix}")),
    )
}

fn sidecar_suffix_for_video(sidecar: &Path, video: &Path) -> Option<String> {
    if sidecar == video || is_video_file(sidecar) {
        return None;
    }
    if sidecar.parent() != video.parent() {
        return None;
    }
    let sidecar_name = sidecar.file_name()?.to_str()?;
    let video_stem = video.file_stem()?.to_str()?;
    sidecar_name
        .strip_prefix(video_stem)
        .filter(|suffix| suffix.starts_with('.'))
        .map(str::to_string)
}

fn unbound_bilibili_sidecar_suffix(path: &Path) -> Option<String> {
    if !is_known_video_sidecar(path) {
        return None;
    }
    let name = path.file_name()?.to_str()?;
    let extension = path.extension()?.to_str()?;
    let lower_name = name.to_ascii_lowercase();
    if lower_name == "danmaku.xml" || lower_name == "danmaku.ass" {
        return Some(format!(".{extension}"));
    }
    if lower_name.starts_with("subtitle-") {
        return Some(format!(".{name}"));
    }
    if lower_name.starts_with("cover-")
        && ["jpg", "jpeg", "png", "webp"]
            .iter()
            .any(|known| extension.eq_ignore_ascii_case(known))
    {
        return Some(format!(".cover.{extension}"));
    }
    None
}

fn unique_path_avoiding(candidate: PathBuf, reserved: &BTreeSet<PathBuf>) -> PathBuf {
    if !candidate.exists() && !reserved.contains(&candidate) {
        return candidate;
    }
    let parent = candidate.parent().unwrap_or_else(|| Path::new("."));
    let stem = candidate
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("download");
    let extension = candidate
        .extension()
        .and_then(|extension| extension.to_str());
    for index in 2.. {
        let file_name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let next = parent.join(file_name);
        if !next.exists() && !reserved.contains(&next) {
            return next;
        }
    }
    unreachable!("unbounded loop returns once it finds a unique path")
}

fn unique_primary_media_path_avoiding(candidate: PathBuf, reserved: &BTreeSet<PathBuf>) -> PathBuf {
    if !candidate.exists() && !reserved.contains(&candidate) && !output_sidecar_exists(&candidate) {
        return candidate;
    }
    let parent = candidate.parent().unwrap_or_else(|| Path::new("."));
    let stem = candidate
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("download");
    let extension = candidate
        .extension()
        .and_then(|extension| extension.to_str());
    for index in 2.. {
        let file_name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let next = parent.join(file_name);
        if !next.exists() && !reserved.contains(&next) && !output_sidecar_exists(&next) {
            return next;
        }
    }
    unreachable!("unbounded loop returns once it finds a unique path")
}

fn execute_move_plan(
    plan: Vec<MoveStep>,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let mut moved = Vec::new();
    let mut moved_videos = Vec::new();
    for step in plan {
        if step.destination.exists() {
            rollback_moves(&moved);
            bail!(
                "destination already exists while moving staged file: {}",
                step.destination.display()
            );
        }
        if let Some(parent) = step.destination.parent()
            && !parent.as_os_str().is_empty()
            && let Err(err) = fs::create_dir_all(parent)
        {
            rollback_moves(&moved);
            return Err(err).with_context(|| format!("failed to create {}", parent.display()));
        }
        if let Err(err) = fs::rename(&step.source, &step.destination) {
            rollback_moves(&moved);
            return Err(err).with_context(|| {
                format!(
                    "failed to move {} to {}",
                    step.source.display(),
                    step.destination.display()
                )
            });
        }
        if is_primary_media_file(&step.destination, primary_media_kind) {
            moved_videos.push(step.destination.clone());
        }
        moved.push((step.source, step.destination));
    }
    Ok(moved_videos)
}

fn existing_video_artifacts(video: &Path) -> Result<Vec<PathBuf>> {
    let mut artifacts = vec![video.to_path_buf()];
    let Some(parent) = video.parent() else {
        return Ok(artifacts);
    };
    let Some(stem) = video.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(artifacts);
    };
    let mut entries = Vec::new();
    let mut primary_stems = BTreeSet::new();
    for entry in
        fs::read_dir(parent).with_context(|| format!("failed to read {}", parent.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if (is_video_file(&path) || is_audio_file(&path))
            && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            primary_stems.insert(stem.to_string());
        }
        entries.push(path);
    }
    let prefix = format!("{stem}.");
    let video_is_only_primary = primary_stems.len() == 1 && primary_stems.contains(stem);
    for path in entries {
        if path == video {
            continue;
        }
        if (is_known_video_sidecar(&path)
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
            && best_primary_stem_for_sidecar(&path, &primary_stems).as_deref() == Some(stem))
            || (video_is_only_primary && unbound_bilibili_sidecar_suffix(&path).is_some())
        {
            artifacts.push(path);
        }
    }
    Ok(artifacts)
}

fn best_primary_stem_for_sidecar(path: &Path, primary_stems: &BTreeSet<String>) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    primary_stems
        .iter()
        .filter(|stem| name.starts_with(&format!("{stem}.")))
        .max_by_key(|stem| stem.len())
        .cloned()
}

fn is_known_video_sidecar(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            VIDEO_SIDECAR_EXTENSIONS
                .iter()
                .any(|known| extension.eq_ignore_ascii_case(known))
        })
}

fn rollback_moves(moved: &[(PathBuf, PathBuf)]) {
    for (source, destination) in moved.iter().rev() {
        if destination.exists() && !source.exists() {
            let _ = fs::rename(destination, source);
        }
    }
}

fn restore_file_backup(backup: &FileBackup) -> Result<()> {
    if !path_entry_exists(&backup.backup)? {
        bail!("overwrite backup is missing: {}", backup.backup.display());
    }
    if path_entry_exists(&backup.original)? {
        bail!(
            "restore destination is occupied; retained backup {} for {}",
            backup.backup.display(),
            backup.original.display()
        );
    }
    fs::rename(&backup.backup, &backup.original).with_context(|| {
        format!(
            "failed to restore overwrite backup {} to {}",
            backup.backup.display(),
            backup.original.display()
        )
    })
}

fn restore_backups(backups: &[FileBackup], backup_dir: &Path, target: &Path) -> Result<()> {
    let mut failures = Vec::new();
    let target_backup = backups
        .iter()
        .find(|backup| backup.original == target)
        .context("overwrite target backup is missing during restore")?;
    if let Err(err) = restore_file_backup(target_backup) {
        bail!("{err:#}");
    }
    for backup in backups
        .iter()
        .rev()
        .filter(|backup| backup.original != target)
    {
        if let Err(err) = restore_file_backup(backup) {
            failures.push(format!("{err:#}"));
        }
    }
    if failures.is_empty() {
        match fs::remove_dir(backup_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => failures.push(format!(
                "failed to remove overwrite backup directory {}: {err}",
                backup_dir.display()
            )),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn restore_remaining_backups(
    backups: &[FileBackup],
    backup_dir: &Path,
    restored_target: &Path,
) -> Result<()> {
    let target_metadata = fs::symlink_metadata(restored_target).with_context(|| {
        format!(
            "failed to inspect restored overwrite target {}",
            restored_target.display()
        )
    })?;
    if !target_metadata.file_type().is_file() {
        bail!(
            "restored overwrite target is no longer a regular file: {}",
            restored_target.display()
        );
    }

    let mut failures = Vec::new();
    for backup in backups.iter().rev() {
        if let Err(err) = restore_file_backup(backup) {
            failures.push(format!("{err:#}"));
        }
    }
    if failures.is_empty() {
        match fs::remove_dir(backup_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => failures.push(format!(
                "failed to remove overwrite backup directory {}: {err}",
                backup_dir.display()
            )),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn remove_backups(backups: &[FileBackup], backup_dir: &Path) -> Result<()> {
    let mut failures = Vec::new();
    for backup in backups {
        if backup.backup.parent() != Some(backup_dir) {
            failures.push(format!(
                "refused to remove overwrite backup outside owned directory: {}",
                backup.backup.display()
            ));
            continue;
        }
        match fs::remove_file(&backup.backup) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => failures.push(format!(
                "failed to remove overwrite backup {}: {err}",
                backup.backup.display()
            )),
        }
    }
    if failures.is_empty() {
        match fs::remove_dir(backup_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => failures.push(format!(
                "failed to remove overwrite backup directory {}: {err}",
                backup_dir.display()
            )),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn unique_bilibili_mux_output_path(
    root: &Path,
    title: &str,
    extension: &str,
    since: SystemTime,
) -> PathBuf {
    let stem = safe_file_stem(title);
    let mut candidate = root.join(format!("{stem}.{extension}"));
    let mut index = 2;
    while candidate.exists() || stale_output_sidecar_exists(&candidate, since) {
        candidate = root.join(format!("{stem} ({index}).{extension}"));
        index += 1;
    }
    candidate
}

fn output_sidecar_exists(output: &Path) -> bool {
    output_sidecar_exists_matching(output, |_| true)
}

fn stale_output_sidecar_exists(output: &Path, since: SystemTime) -> bool {
    output_sidecar_exists_matching(output, |path| !modified_since(path, since))
}

fn output_sidecar_exists_matching(output: &Path, include: impl Fn(&Path) -> bool) -> bool {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let Some(output_stem) = output.file_stem().and_then(|stem| stem.to_str()) else {
        return false;
    };
    let Ok(entries) = fs::read_dir(parent) else {
        return false;
    };
    let paths = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().ok()?.is_file() {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let mut primary_stems = BTreeSet::from([output_stem.to_string()]);
    for path in &paths {
        if (is_video_file(path) || is_audio_file(path))
            && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            primary_stems.insert(stem.to_string());
        }
    }

    paths.into_iter().any(|path| {
        if path == output
            || is_video_file(&path)
            || !is_known_video_sidecar(&path)
            || !include(&path)
        {
            return false;
        }
        best_primary_stem_for_sidecar(&path, &primary_stems).as_deref() == Some(output_stem)
    })
}

fn safe_file_stem(title: &str) -> String {
    let sanitized = title
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();
    if sanitized.is_empty() {
        "bilibili".to_string()
    } else {
        sanitized
    }
}

struct MediaNfo<'a> {
    title: Option<&'a str>,
    plot: Option<&'a str>,
    unique_id_type: &'a str,
    unique_id: &'a str,
    alternate_unique_ids: Vec<(&'a str, &'a str)>,
    source_url: &'a str,
    studio: Option<&'a str>,
    premiered: Option<&'a str>,
}

fn write_nfo_for_media(video_path: &Path, nfo: &MediaNfo<'_>) -> Result<PathBuf> {
    let title = nfo
        .title
        .or_else(|| video_path.file_stem().and_then(|stem| stem.to_str()))
        .unwrap_or("Untitled");
    let nfo_path = video_path.with_extension("nfo");
    fs::write(&nfo_path, render_nfo(title, nfo))
        .with_context(|| format!("failed to write {}", nfo_path.display()))?;
    Ok(nfo_path)
}

fn render_nfo(title: &str, nfo: &MediaNfo<'_>) -> String {
    let mut content =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
    content.push_str(&format!("  <title>{}</title>\n", xml_escape(title)));
    content.push_str(&format!(
        "  <uniqueid type=\"{}\" default=\"true\">{}</uniqueid>\n",
        xml_escape(nfo.unique_id_type),
        xml_escape(nfo.unique_id)
    ));
    for (unique_id_type, unique_id) in &nfo.alternate_unique_ids {
        content.push_str(&format!(
            "  <uniqueid type=\"{}\">{}</uniqueid>\n",
            xml_escape(unique_id_type),
            xml_escape(unique_id)
        ));
    }
    content.push_str(&format!(
        "  <trailer>{}</trailer>\n",
        xml_escape(nfo.source_url)
    ));

    if let Some(plot) = nfo.plot.filter(|plot| !plot.trim().is_empty()) {
        content.push_str(&format!("  <plot>{}</plot>\n", xml_escape(plot.trim())));
    }
    if let Some(studio) = nfo.studio.filter(|studio| !studio.trim().is_empty()) {
        content.push_str(&format!(
            "  <studio>{}</studio>\n",
            xml_escape(studio.trim())
        ));
    }
    if let Some(premiered) = nfo
        .premiered
        .filter(|premiered| !premiered.trim().is_empty())
    {
        content.push_str(&format!(
            "  <premiered>{}</premiered>\n",
            xml_escape(premiered.trim())
        ));
        if let Some(year) = premiered.get(..4) {
            content.push_str(&format!("  <year>{}</year>\n", xml_escape(year)));
        }
    }

    content.push_str("</movie>\n");
    content
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn format_yt_date(upload_date: &str) -> Option<String> {
    if upload_date.len() == 8 {
        Some(format!(
            "{}-{}-{}",
            &upload_date[0..4],
            &upload_date[4..6],
            &upload_date[6..8]
        ))
    } else {
        None
    }
}

fn summarize_output(stdout: &str, stderr: &str) -> String {
    let stderr_tail = tail_lines(&redact_sensitive_output(stderr), 10);
    let stdout_tail = tail_lines(&redact_sensitive_output(stdout), 10);
    match (stderr_tail.is_empty(), stdout_tail.is_empty()) {
        (true, true) => "no command output captured".to_string(),
        (false, true) => format!("stderr:\n{stderr_tail}"),
        (true, false) => format!("stdout:\n{stdout_tail}"),
        (false, false) => format!("stderr:\n{stderr_tail}\nstdout:\n{stdout_tail}"),
    }
}

fn last_nonempty_line(text: &str) -> Option<&str> {
    text.lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
}

fn tail_lines(text: &str, max_lines: usize) -> String {
    let redacted = redact_sensitive_output(text);
    let lines: Vec<_> = redacted
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

fn redact_sensitive_output(text: &str) -> String {
    let mut redacted = redact_flag_line_values(text, "--cookie", "<redacted Bilibili cookie>");
    redacted = redact_bilibili_cookie_lines(&redacted);
    for name in BILIBILI_COOKIE_NAMES {
        redacted = redact_cookie_pair_values(&redacted, name, "<redacted>");
    }
    redact_bilibili_qrcode_urls(&redacted)
}

const BILIBILI_COOKIE_NAMES: &[&str] = &[
    "SESSDATA",
    "bili_jct",
    "DedeUserID",
    "DedeUserID__ckMd5",
    "sid",
    "buvid3",
    "buvid4",
    "b_nut",
    "ac_time_value",
];

fn redact_flag_line_values(text: &str, flag: &str, replacement: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find(flag) {
        let absolute_start = text.len() - rest.len() + index;
        let before = text[..absolute_start].chars().next_back();
        let after_index = index + flag.len();
        let after = rest[after_index..].chars().next();
        let is_token_start = before.is_none_or(char::is_whitespace);
        let is_flag = after.is_some_and(|ch| ch == '=' || ch.is_whitespace());
        if !is_token_start || !is_flag {
            output.push_str(&rest[..after_index]);
            rest = &rest[after_index..];
            continue;
        }

        output.push_str(&rest[..index]);
        output.push_str(flag);
        let separator = after.expect("is_flag requires a separator");
        if separator == '=' {
            output.push('=');
            output.push_str(replacement);
            let value_start = after_index + 1;
            let value_end = rest[value_start..]
                .find(['\r', '\n'])
                .map_or(rest.len(), |offset| value_start + offset);
            rest = &rest[value_end..];
        } else {
            output.push_str(&rest[after_index..after_index + separator.len_utf8()]);
            output.push_str(replacement);
            let value_start = after_index + separator.len_utf8();
            let value_end = rest[value_start..]
                .find(['\r', '\n'])
                .map_or(rest.len(), |offset| value_start + offset);
            rest = &rest[value_end..];
        }
    }
    output.push_str(rest);
    output
}

fn redact_cookie_pair_values(text: &str, name: &str, replacement: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut rest = text;
    let prefix = format!("{name}=");
    while let Some(index) = rest.find(&prefix) {
        redacted.push_str(&rest[..index]);
        redacted.push_str(&prefix);
        redacted.push_str(replacement);
        let value_start = index + prefix.len();
        let value_end = rest[value_start..]
            .find(|ch: char| {
                ch == ';' || ch == '&' || ch.is_ascii_whitespace() || ch == '"' || ch == '\''
            })
            .map_or(rest.len(), |offset| value_start + offset);
        rest = &rest[value_end..];
    }
    redacted.push_str(rest);
    redacted
}

fn redact_bilibili_cookie_lines(text: &str) -> String {
    text.lines()
        .map(|line| {
            if is_bilibili_cookie_line(line) {
                "<redacted Bilibili cookie line>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_bilibili_cookie_line(line: &str) -> bool {
    if !line.contains(';') {
        return false;
    }
    let has_known_cookie = BILIBILI_COOKIE_NAMES
        .iter()
        .any(|name| line.contains(&format!("{name}=")));
    if !has_known_cookie {
        return false;
    }
    line.split(';')
        .filter(|part| part.trim().contains('='))
        .take(2)
        .count()
        >= 2
}

fn redact_bilibili_qrcode_urls(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.contains("passport.bilibili.com") && line.contains("qrcode_key=") {
                "<redacted Bilibili login QR URL>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn nonempty_join(lines: Vec<String>) -> String {
    lines
        .into_iter()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::env;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use crate::config::AppConfig;

    use super::*;

    fn test_config() -> AppConfig {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut config = AppConfig::load(&manifest_dir.join("config.example.toml"))
            .expect("example config should parse");
        config.bilibili.auth.state_path =
            temp_test_dir("telegram-video-downloader-test-auth-missing").join("auth.json");
        config
    }

    fn test_home() -> PathBuf {
        env::var_os("HOME")
            .map(PathBuf::from)
            .expect("HOME should be set during tests")
    }

    fn write_bilibili_identity_nfo(video: &Path, identity: &str) {
        fs::write(
            video.with_extension("nfo"),
            format!("<movie><uniqueid type=\"bilibili-cid\">{identity}</uniqueid></movie>"),
        )
        .expect("Bilibili identity NFO should write");
    }

    fn overwrite_backup_dirs(root: &Path) -> Vec<PathBuf> {
        fs::read_dir(root)
            .expect("test directory should read")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(OVERWRITE_BACKUP_DIR_PREFIX))
            })
            .map(|entry| entry.path())
            .collect()
    }

    fn command_config_path(spec: &CommandSpec) -> Option<PathBuf> {
        spec.args
            .iter()
            .position(|arg| arg == "--config-file")
            .and_then(|index| spec.args.get(index + 1))
            .map(PathBuf::from)
    }

    fn metadata_with_subtitles() -> YoutubeMetadata {
        YoutubeMetadata {
            subtitles: BTreeMap::from([
                ("en".to_string(), serde_json::json!([])),
                ("ja".to_string(), serde_json::json!([])),
                ("fr".to_string(), serde_json::json!([])),
            ]),
            automatic_captions: BTreeMap::from([
                ("zh-Hans".to_string(), serde_json::json!([])),
                ("en".to_string(), serde_json::json!([])),
            ]),
            ..YoutubeMetadata::default()
        }
    }

    fn find_video_duplicate_without_probe(
        config: &AppConfig,
        job: &JobRequest,
    ) -> Result<Option<VideoDuplicate>> {
        let identities = video_identity(job).into_iter().collect();
        find_video_duplicate_for_identities(config, job, identities)
    }

    #[test]
    fn extracts_video_identity_from_supported_urls() {
        assert_eq!(
            video_identity(&JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M&t=47s".to_string()
            }),
            Some(VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string()
            })
        );
        assert_eq!(
            video_identity(&JobRequest::Youtube {
                url: "https://youtu.be/PHH1wTDF-1M?t=47".to_string()
            }),
            Some(VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string()
            })
        );
        assert_eq!(
            video_identity(&JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/".to_string(),
                selection: None,
            }),
            Some(VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV12TRrBcEP8".to_string()
            })
        );
        assert_eq!(
            video_identity(&JobRequest::Bilibili {
                url: "https://b23.tv/abc".to_string(),
                selection: None,
            }),
            None
        );
        assert_eq!(
            youtube_id_from_url("https://notyoutube.com/watch?v=PHH1wTDF-1M"),
            None
        );
    }

    #[test]
    fn unresolved_bilibili_urls_keep_staging_fallback_identity() {
        for url in [
            "https://b23.tv/abc",
            "https://www.bilibili.com/bangumi/play/ss12345",
            "https://www.bilibili.com/bangumi/media/md12345",
            "https://www.bilibili.tv/en/play/123/456",
        ] {
            assert_eq!(
                fallback_video_identity(&JobRequest::Bilibili {
                    url: url.to_string(),
                    selection: Some(BilibiliSelection::Latest),
                }),
                Some(VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "unknown".to_string(),
                }),
                "{url} should still run through staged keep-both"
            );
        }
    }

    #[test]
    fn finds_duplicate_video_from_filename_and_sidecar_metadata() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-detection");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let youtube_path = video_dir.join("Example [PHH1wTDF-1M].mkv");
        fs::write(&youtube_path, "video").expect("youtube file should write");
        fs::write(
            video_dir.join("Example [PHH1wTDF-1M].trailer.mp4"),
            "trailer",
        )
        .expect("related trailer should write");
        fs::write(video_dir.join("Example [PHH1wTDF-1M].part2.mkv"), "part2")
            .expect("related part should write");
        let bilibili_path = video_dir.join("bilibili-title.mp4");
        fs::write(&bilibili_path, "video").expect("bilibili file should write");
        fs::write(
            bilibili_path.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili\">BV12TRrBcEP8</uniqueid></movie>",
        )
        .expect("nfo should write");

        let youtube_duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed")
        .expect("youtube duplicate should be found");
        assert_eq!(youtube_duplicate.existing_videos, vec![youtube_path]);

        let bilibili_duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/".to_string(),
                selection: None,
            },
        )
        .expect("duplicate scan should succeed")
        .expect("bilibili duplicate should be found");
        assert_eq!(bilibili_duplicate.existing_videos, vec![bilibili_path]);

        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn finds_bilibili_duplicate_from_resolved_aid_sidecar() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-aid");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let bilibili_path = video_dir.join("bilibili-title.mp4");
        fs::write(&bilibili_path, "video").expect("bilibili file should write");
        fs::write(
            bilibili_path.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili-aid\">116539978154171</uniqueid></movie>",
        )
        .expect("nfo should write");

        let duplicate = find_video_duplicate_for_identities(
            &config,
            &JobRequest::Bilibili {
                url: "https://b23.tv/Jt1mZiL".to_string(),
                selection: None,
            },
            vec![
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "BV12TRrBcEP8".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "116539978154171".to_string(),
                },
            ],
        )
        .expect("duplicate scan should succeed")
        .expect("bilibili aid duplicate should be found");

        assert_eq!(duplicate.identity.id, "116539978154171");
        assert_eq!(duplicate.existing_videos, vec![bilibili_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn finds_bilibili_duplicate_from_av_identity_and_legacy_bare_aid_sidecar() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-av-legacy-aid");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let bilibili_path = video_dir.join("bilibili-title.mp4");
        fs::write(&bilibili_path, "video").expect("bilibili file should write");
        fs::write(
            bilibili_path.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili-aid\">116539978154171</uniqueid></movie>",
        )
        .expect("nfo should write");

        let duplicate = find_video_duplicate_for_identities(
            &config,
            &JobRequest::Bilibili {
                url: "https://b23.tv/Jt1mZiL".to_string(),
                selection: None,
            },
            vec![VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "av116539978154171".to_string(),
            }],
        )
        .expect("duplicate scan should succeed")
        .expect("bilibili av identity should match legacy aid sidecar");

        assert_eq!(duplicate.identity.id, "av116539978154171");
        assert_eq!(duplicate.existing_videos, vec![bilibili_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn aggregates_bilibili_duplicates_across_aliases() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-aliases");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let bvid_path = video_dir.join("Title [BV12TRrBcEP8].mp4");
        fs::write(&bvid_path, "video").expect("bvid video should write");
        let aid_path = video_dir.join("Title from aid.mp4");
        fs::write(&aid_path, "video").expect("aid video should write");
        fs::write(
            aid_path.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili-aid\">116539978154171</uniqueid></movie>",
        )
        .expect("aid nfo should write");

        let duplicate = find_video_duplicate_for_identities(
            &config,
            &JobRequest::Bilibili {
                url: "https://b23.tv/Jt1mZiL".to_string(),
                selection: None,
            },
            vec![
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "BV12TRrBcEP8".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "116539978154171".to_string(),
                },
            ],
        )
        .expect("duplicate scan should succeed")
        .expect("bilibili duplicate should be found");

        assert_eq!(duplicate.identity.id, "BV12TRrBcEP8");
        assert_eq!(duplicate.existing_videos, vec![bvid_path, aid_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn filename_only_bilibili_entry_ids_never_authorize_overwrite() {
        for id in ["cid123", "ep123"] {
            let mut config = test_config();
            let video_dir = temp_test_dir(&format!("duplicate-bilibili-filename-only-{id}"));
            config.downloads.video_dir = video_dir.clone();
            let existing = video_dir.join(format!("{id}.mp4"));
            fs::write(&existing, "unrelated-video").expect("video should write");
            let job = JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ep123".to_string(),
                selection: Some(BilibiliSelection::Latest),
            };
            let identity = VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: id.to_string(),
            };

            let index = build_video_identity_index(&config, &job).expect("index should build");
            let duplicate = find_video_duplicate_in_index(
                &index,
                std::slice::from_ref(&identity),
                std::slice::from_ref(&identity),
            )
            .expect("filename should still produce a duplicate prompt");

            assert_eq!(duplicate.existing_videos, vec![existing]);
            assert!(index.overwrite_videos(&identity).is_empty());
            assert!(!duplicate.allows_overwrite_for(&job));
            let _ = fs::remove_dir_all(video_dir);
        }
    }

    #[test]
    fn bilibili_overwrite_requires_the_same_entry_identity() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-cross-entry");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let first_entry = video_dir.join("Part 1.mp4");
        fs::write(&first_entry, "part-1").expect("first entry should write");
        fs::write(
            first_entry.with_extension("nfo"),
            r#"<movie>
            <uniqueid type="bilibili">BV123</uniqueid>
            <uniqueid type="bilibili-cid">cid111</uniqueid>
            </movie>"#,
        )
        .expect("first entry nfo should write");
        let job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV123?p=2".to_string(),
            selection: None,
        };
        let plan = BilibiliDownloadPlan {
            title: "Multi-part video".to_string(),
            entries: vec![BilibiliDownloadEntry {
                index: 2,
                aid: 123,
                bvid: Some("BV123".to_string()),
                cid: 222,
                epid: Some(333),
                title: "Part 2".to_string(),
            }],
        };
        let mut identities = video_identity(&job).into_iter().collect::<Vec<_>>();
        push_bilibili_plan_identities(&mut identities, &plan);
        let overwrite_identities = bilibili_plan_overwrite_identities(&plan);

        let index = build_video_identity_index(&config, &job).expect("index should build");
        let broad_duplicate =
            find_video_duplicate_in_index(&index, &identities, &overwrite_identities)
                .expect("shared BVID should still prompt for a duplicate");
        assert_eq!(broad_duplicate.identity.id, "BV123");
        assert_eq!(broad_duplicate.existing_videos, vec![first_entry.clone()]);
        assert!(!broad_duplicate.allows_overwrite_for(&job));

        let second_entry = video_dir.join("Part 2.mp4");
        fs::write(&second_entry, "part-2").expect("second entry should write");
        fs::write(
            second_entry.with_extension("nfo"),
            r#"<movie>
            <uniqueid type="bilibili">BV123</uniqueid>
            <uniqueid type="bilibili-cid">cid222</uniqueid>
            <uniqueid type="bilibili-epid">ep333</uniqueid>
            </movie>"#,
        )
        .expect("second entry nfo should write");
        let index = build_video_identity_index(&config, &job).expect("index should rebuild");
        let exact_duplicate =
            find_video_duplicate_in_index(&index, &identities, &overwrite_identities)
                .expect("same cid should identify the exact duplicate");
        assert_eq!(exact_duplicate.identity.id, "cid222");
        assert_eq!(exact_duplicate.existing_videos, vec![second_entry.clone()]);
        assert!(exact_duplicate.allows_overwrite_for(&job));

        let conflicting_entry = video_dir.join("Conflicting episode identity.mp4");
        fs::write(&conflicting_entry, "conflict").expect("conflicting entry should write");
        fs::write(
            conflicting_entry.with_extension("nfo"),
            r#"<movie>
            <uniqueid type="bilibili-epid">ep333</uniqueid>
            </movie>"#,
        )
        .expect("conflicting entry nfo should write");
        let index = build_video_identity_index(&config, &job).expect("index should rebuild");
        let ambiguous_duplicate =
            find_video_duplicate_in_index(&index, &identities, &overwrite_identities)
                .expect("conflicting entry identities should still prompt");
        assert!(!ambiguous_duplicate.allows_overwrite_for(&job));
        assert!(ambiguous_duplicate.existing_videos.contains(&second_entry));
        assert!(
            ambiguous_duplicate
                .existing_videos
                .contains(&conflicting_entry)
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_identity_index_does_not_reread_sidecars_for_each_query() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-identity-index");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let video = video_dir.join("Indexed.mp4");
        let nfo = video.with_extension("nfo");
        fs::write(&video, "video").expect("video should write");
        fs::write(
            &nfo,
            r#"<movie>
            <uniqueid type="bilibili">BV123</uniqueid>
            <uniqueid type="bilibili-cid">cid456</uniqueid>
            </movie>"#,
        )
        .expect("nfo should write");
        let job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV123".to_string(),
            selection: None,
        };
        let index = build_video_identity_index(&config, &job).expect("index should build");
        fs::remove_file(nfo).expect("sidecar should be removable after indexing");

        for id in ["BV123", "cid456"] {
            let identity = VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: id.to_string(),
            };
            assert_eq!(index.videos(&identity), std::slice::from_ref(&video));
        }
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn youtube_duplicate_detection_keeps_id_case_sensitive() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-youtube-id-case");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let video_path = video_dir.join("Example [abcdef12345].mkv");
        fs::write(&video_path, "video").expect("video file should write");
        fs::write(
            video_path.with_extension("nfo"),
            r#"<movie><uniqueid type="youtube">abcdef12345</uniqueid></movie>"#,
        )
        .expect("nfo should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=ABCDEF12345".to_string(),
            },
        )
        .expect("duplicate scan should succeed");

        assert_eq!(duplicate, None);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_detection_uses_structured_info_json_identity() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-info-json");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let video_path = video_dir.join("Unrelated title.mkv");
        fs::write(&video_path, "video").expect("video file should write");
        fs::write(
            video_path.with_extension("info.json"),
            serde_json::json!({
                "id": "PHH1wTDF-1M",
                "extractor": "youtube",
                "webpage_url": "https://www.youtube.com/watch?v=PHH1wTDF-1M"
            })
            .to_string(),
        )
        .expect("info json should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed")
        .expect("info json duplicate should be found");

        assert_eq!(duplicate.existing_videos, vec![video_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_detection_ignores_description_references() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-description-reference");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let video_path = video_dir.join("Unrelated title.mkv");
        fs::write(&video_path, "video").expect("video file should write");
        fs::write(
            video_path.with_extension("description"),
            "This video mentions https://www.youtube.com/watch?v=PHH1wTDF-1M",
        )
        .expect("description should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed");

        assert_eq!(duplicate, None);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_detection_ignores_info_json_free_text_references() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-info-json-free-text");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let video_path = video_dir.join("Unrelated title.mkv");
        fs::write(&video_path, "video").expect("video file should write");
        fs::write(
            video_path.with_extension("info.json"),
            serde_json::json!({
                "id": "different-id",
                "extractor": "youtube",
                "description": "<uniqueid type='youtube'>PHH1wTDF-1M</uniqueid>"
            })
            .to_string(),
        )
        .expect("info json should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed");

        assert_eq!(duplicate, None);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_detection_ignores_bare_id_filename_suffixes() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bare-suffix");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        fs::write(video_dir.join("Unrelated PHH1wTDF-1M.mkv"), "video")
            .expect("video file should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed");

        assert_eq!(duplicate, None);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn bilibili_audio_only_duplicate_detection_includes_audio_primary_files() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-audio-only");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        config.bilibili.extra_args = vec!["--audio-only".to_string()];
        let audio_path = video_dir.join("Example [BV12TRrBcEP8].m4a");
        fs::write(&audio_path, "audio").expect("audio file should write");

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/".to_string(),
                selection: None,
            },
        )
        .expect("duplicate scan should succeed")
        .expect("audio-only duplicate should be found");

        assert_eq!(duplicate.existing_videos, vec![audio_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn writes_bilibili_nfo_for_audio_only_primary_media() {
        let video_dir = temp_test_dir("bilibili-audio-only-nfo");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        let audio_path = video_dir.join("Episode One.m4a");
        fs::write(&audio_path, "audio").expect("audio file should write");
        let plan = BilibiliDownloadPlan {
            title: "Bangumi".to_string(),
            entries: vec![BilibiliDownloadEntry {
                index: 1,
                aid: 1_556_453_868,
                bvid: Some("BV12TRrBcEP8".to_string()),
                cid: 987_654_321,
                epid: Some(123_456),
                title: "Episode One".to_string(),
            }],
        };
        let report = BilibiliDownloadReport {
            title: "Bangumi".to_string(),
            output_dir: video_dir.clone(),
            entries: vec![BilibiliEntryDownloadReport {
                index: 1,
                title: "Episode One".to_string(),
                files: vec![BilibiliDownloadedFile {
                    kind: "audio".to_string(),
                    path: PathBuf::from("Episode One.m4a"),
                }],
                mux: None,
            }],
        };

        let nfos = write_bilibili_nfos(
            &video_dir,
            "https://www.bilibili.com/bangumi/play/ep123456",
            &plan,
            &report,
        )
        .expect("audio-only nfo should write");
        let nfo_path = audio_path.with_extension("nfo");
        let nfo = fs::read_to_string(&nfo_path).expect("audio nfo should exist");

        assert_eq!(nfos, vec![nfo_path]);
        assert!(
            nfo.contains("<uniqueid type=\"bilibili\" default=\"true\">BV12TRrBcEP8</uniqueid>")
        );
        assert!(nfo.contains("<uniqueid type=\"bilibili-cid\">cid987654321</uniqueid>"));
        assert!(nfo.contains("<uniqueid type=\"bilibili-epid\">ep123456</uniqueid>"));
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_detection_ignores_staging_directory() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-staging-ignore");
        let staging_dir = video_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(staging_dir.join("Example [PHH1wTDF-1M].mkv"), "video")
            .expect("staged file should write");
        config.downloads.video_dir = video_dir.clone();

        let duplicate = find_video_duplicate_without_probe(
            &config,
            &JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
            },
        )
        .expect("duplicate scan should succeed");

        assert_eq!(duplicate, None);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn keep_both_moves_staged_video_to_unique_path() {
        let final_dir = temp_test_dir("keep-both-final");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Example [PHH1wTDF-1M].mkv");
        fs::write(&existing, "old").expect("existing file should write");
        fs::write(existing.with_extension("nfo"), "old-nfo").expect("old nfo should write");
        fs::write(existing.with_extension("info.json"), "old-json").expect("old json should write");
        let staged = staging_dir.join("Example [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new").expect("staged file should write");
        fs::write(staged.with_extension("nfo"), "new-nfo").expect("new nfo should write");
        fs::write(staged.with_extension("info.json"), "new-json").expect("new json should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        let kept = final_dir.join("Example [PHH1wTDF-1M] (2).mkv");
        assert_eq!(moved, vec![kept.clone()]);
        assert_eq!(
            fs::read_to_string(existing).expect("old file should remain"),
            "old"
        );
        assert_eq!(
            fs::read_to_string(kept).expect("new file should move"),
            "new"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Example [PHH1wTDF-1M] (2).nfo"))
                .expect("new nfo should follow kept video basename"),
            "new-nfo"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Example [PHH1wTDF-1M] (2).info.json"))
                .expect("new info json should follow kept video basename"),
            "new-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_avoids_existing_sidecar_stem_for_primary_media() {
        let final_dir = temp_test_dir("keep-both-existing-sidecar-stem");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Example.xml"), "old-xml").expect("old xml should write");
        let staged = staging_dir.join("Example.mkv");
        fs::write(&staged, "new-video").expect("staged video should write");
        fs::write(staged.with_extension("xml"), "new-xml").expect("staged xml should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        let kept = final_dir.join("Example (2).mkv");
        assert_eq!(moved, vec![kept.clone()]);
        assert_eq!(
            fs::read_to_string(final_dir.join("Example.xml")).expect("old xml should remain"),
            "old-xml"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Example (2).xml"))
                .expect("new xml should follow kept video basename"),
            "new-xml"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_avoids_existing_compound_sidecar_stem_for_primary_media() {
        let final_dir = temp_test_dir("keep-both-existing-compound-sidecar-stem");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Example.info.json"), "old-json").expect("old json should write");
        let staged = staging_dir.join("Example.mkv");
        fs::write(&staged, "new-video").expect("staged video should write");
        fs::write(staged.with_extension("info.json"), "new-json")
            .expect("staged info json should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        let kept = final_dir.join("Example (2).mkv");
        assert_eq!(moved, vec![kept.clone()]);
        assert_eq!(
            fs::read_to_string(kept).expect("new video should avoid compound sidecar stem"),
            "new-video"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Example (2).info.json"))
                .expect("compound sidecar should follow kept video basename"),
            "new-json"
        );
        assert!(!final_dir.join("Example.mkv").exists());
        assert_eq!(
            fs::read_to_string(final_dir.join("Example.info.json"))
                .expect("old compound sidecar should remain"),
            "old-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_moves_secondary_sidecars_with_their_video() {
        let final_dir = temp_test_dir("keep-both-secondary-sidecar");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Part 2.mkv"), "old-part2").expect("old part2 should write");
        let staged_part1 = staging_dir.join("Part 1.mkv");
        fs::write(&staged_part1, "new-part1").expect("part1 should write");
        fs::write(staged_part1.with_extension("nfo"), "new-part1-nfo")
            .expect("part1 nfo should write");
        let staged_part2 = staging_dir.join("Part 2.mkv");
        fs::write(&staged_part2, "new-part2").expect("part2 should write");
        fs::write(staged_part2.with_extension("nfo"), "new-part2-nfo")
            .expect("part2 nfo should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        assert!(moved.contains(&final_dir.join("Part 1.mkv")));
        assert!(moved.contains(&final_dir.join("Part 2 (2).mkv")));
        assert_eq!(
            fs::read_to_string(final_dir.join("Part 2 (2).nfo"))
                .expect("part2 nfo should follow renamed video"),
            "new-part2-nfo"
        );
        assert!(
            !final_dir.join("Part 2.nfo").exists(),
            "new sidecar should not attach to the old colliding video"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_uses_most_specific_primary_for_dot_prefixed_sidecars() {
        let final_dir = temp_test_dir("keep-both-dot-prefix-sidecar");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Movie.part2.mkv"), "old-part2").expect("old part2 should write");
        let staged_movie = staging_dir.join("Movie.mkv");
        fs::write(&staged_movie, "new-movie").expect("movie should write");
        let staged_part2 = staging_dir.join("Movie.part2.mkv");
        fs::write(&staged_part2, "new-part2").expect("part2 should write");
        fs::write(staged_part2.with_extension("nfo"), "new-part2-nfo")
            .expect("part2 nfo should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        assert!(moved.contains(&final_dir.join("Movie.mkv")));
        assert!(moved.contains(&final_dir.join("Movie.part2 (2).mkv")));
        assert_eq!(
            fs::read_to_string(final_dir.join("Movie.part2 (2).nfo"))
                .expect("part2 nfo should follow the most specific renamed video"),
            "new-part2-nfo"
        );
        assert!(
            !final_dir.join("Movie.nfo").exists(),
            "part2 sidecar should not attach to the shorter Movie stem"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_ignores_sidecars_bound_to_longer_primary_stem() {
        let final_dir = temp_test_dir("keep-both-longer-primary-sidecar-stem");
        fs::create_dir_all(&final_dir).expect("final dir should create");
        fs::write(final_dir.join("Movie.trailer.mp4"), "trailer")
            .expect("trailer video should write");
        fs::write(final_dir.join("Movie.trailer.nfo"), "trailer-nfo")
            .expect("trailer nfo should write");

        let output =
            unique_primary_media_path_avoiding(final_dir.join("Movie.mp4"), &BTreeSet::new());

        assert_eq!(output, final_dir.join("Movie.mp4"));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_keeps_recursive_same_stem_sidecars_with_same_directory_primary() {
        let final_dir = temp_test_dir("keep-both-recursive-same-stem-sidecar");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        let staged_a_dir = staging_dir.join("a");
        let staged_b_dir = staging_dir.join("b");
        fs::create_dir_all(&staged_a_dir).expect("staging a dir should create");
        fs::create_dir_all(&staged_b_dir).expect("staging b dir should create");
        let staged_a = staged_a_dir.join("Movie.mkv");
        let staged_b = staged_b_dir.join("Movie.mkv");
        fs::write(&staged_a, "new-a").expect("movie a should write");
        fs::write(staged_a.with_extension("nfo"), "new-a-nfo").expect("movie a nfo should write");
        fs::write(&staged_b, "new-b").expect("movie b should write");
        fs::write(staged_b.with_extension("nfo"), "new-b-nfo").expect("movie b nfo should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should move");

        assert!(moved.contains(&final_dir.join("a/Movie.mkv")));
        assert!(moved.contains(&final_dir.join("b/Movie.mkv")));
        assert_eq!(
            fs::read_to_string(final_dir.join("a/Movie.nfo"))
                .expect("movie a nfo should stay with movie a"),
            "new-a-nfo"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("b/Movie.nfo"))
                .expect("movie b nfo should stay with movie b"),
            "new-b-nfo"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_moves_audio_only_primary_file() {
        let final_dir = temp_test_dir("keep-both-audio-only");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Episode.m4a"), "old-audio").expect("old audio should write");
        let staged_audio = staging_dir.join("Episode.m4a");
        fs::write(&staged_audio, "new-audio").expect("audio should write");
        fs::write(staged_audio.with_extension("nfo"), "new-nfo").expect("nfo should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::VideoOrAudio,
        )
        .expect("staged files should move");

        let kept = final_dir.join("Episode (2).m4a");
        assert_eq!(moved, vec![kept.clone()]);
        assert_eq!(
            fs::read_to_string(kept).expect("new audio should move"),
            "new-audio"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Episode (2).nfo"))
                .expect("nfo should follow kept audio basename"),
            "new-nfo"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn artifact_only_staging_moves_outputs_without_primary_media() {
        let final_dir = temp_test_dir("artifact-only-staging");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Episode.xml"), "old-xml").expect("old xml should write");
        fs::write(staging_dir.join("Episode.xml"), "new-xml").expect("xml should write");
        fs::write(staging_dir.join("Episode.cover.jpg"), "cover").expect("cover should write");
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: Vec::new(),
        };

        let moved = move_staged_artifact_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("artifact files should move");

        let kept_xml = final_dir.join("Episode (2).xml");
        let cover = final_dir.join("Episode.cover.jpg");
        assert_eq!(moved, vec![cover.clone(), kept_xml.clone()]);
        assert_eq!(
            fs::read_to_string(final_dir.join("Episode.xml")).expect("old xml should remain"),
            "old-xml"
        );
        assert_eq!(
            fs::read_to_string(kept_xml).expect("new xml should move uniquely"),
            "new-xml"
        );
        assert_eq!(
            fs::read_to_string(cover).expect("cover should move"),
            "cover"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn artifact_only_overwrite_replaces_existing_video_sidecars() {
        let final_dir = temp_test_dir("artifact-only-overwrite");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Existing Title.mkv");
        fs::write(&existing, "video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let nfo = existing.with_extension("nfo");
        let description = existing.with_extension("description");
        let subtitle = final_dir.join("Existing Title.zh-Hans.srt");
        fs::write(&description, "old-description").expect("old description should write");
        fs::write(&subtitle, "old-subtitle").expect("old subtitle should write");
        fs::write(existing.with_extension("xml"), "old-xml").expect("old xml should write");
        fs::write(final_dir.join("Existing Title.cover.jpg"), "old-cover")
            .expect("old cover should write");
        fs::write(staging_dir.join("Downloaded.xml"), "new-xml").expect("xml should write");
        fs::write(staging_dir.join("Downloaded.cover.jpg"), "new-cover")
            .expect("cover should write");
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };

        let moved = move_staged_artifact_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("artifact files should overwrite sidecars");

        let xml = existing.with_extension("xml");
        let cover = final_dir.join("Existing Title.cover.jpg");
        assert_eq!(moved, vec![cover.clone(), xml.clone()]);
        assert_eq!(
            fs::read_to_string(xml).expect("xml should be replaced"),
            "new-xml"
        );
        assert_eq!(
            fs::read_to_string(cover).expect("cover should be replaced"),
            "new-cover"
        );
        assert_eq!(
            fs::read_to_string(existing).expect("video should remain"),
            "video"
        );
        assert!(
            fs::read_to_string(nfo)
                .expect("identity nfo should remain")
                .contains("cid123")
        );
        assert_eq!(
            fs::read_to_string(description).expect("description should remain"),
            "old-description"
        );
        assert_eq!(
            fs::read_to_string(subtitle).expect("subtitle should remain"),
            "old-subtitle"
        );
        assert!(
            fs::read_dir(&final_dir)
                .expect("final dir should scan")
                .all(|entry| !entry
                    .expect("directory entry should read")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(OVERWRITE_BACKUP_DIR_PREFIX)),
            "overwrite backup directory should be removed"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn artifact_only_overwrite_revalidation_rejects_changed_identity() {
        let final_dir = temp_test_dir("artifact-only-overwrite-revalidation");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Existing Title.mkv");
        fs::write(&existing, "video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid999");
        let existing_xml = existing.with_extension("xml");
        fs::write(&existing_xml, "old-xml").expect("old xml should write");
        let staged_xml = staging_dir.join("Downloaded.xml");
        fs::write(&staged_xml, "new-xml").expect("staged xml should write");
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };

        let error = move_staged_artifact_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("artifact overwrite must revalidate the existing video identity");

        assert!(error.to_string().contains("identity no longer matches"));
        assert_eq!(
            fs::read_to_string(existing).expect("existing video should remain"),
            "video"
        );
        assert_eq!(
            fs::read_to_string(existing_xml).expect("old artifact should remain"),
            "old-xml"
        );
        assert_eq!(
            fs::read_to_string(staged_xml).expect("staged artifact should remain after rejection"),
            "new-xml"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keep_both_moves_bilibili_segment_primary_files() {
        let final_dir = temp_test_dir("keep-both-bilibili-segments");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        fs::write(final_dir.join("Episode.m4s"), "old-segment").expect("old segment should write");
        let staged_m4s = staging_dir.join("Episode.m4s");
        fs::write(&staged_m4s, "new-segment").expect("m4s segment should write");
        fs::write(staged_m4s.with_extension("nfo"), "new-nfo").expect("nfo should write");
        let staged_flv = staging_dir.join("Episode Part 2.flv");
        fs::write(&staged_flv, "new-flv").expect("flv segment should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: Vec::new(),
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::KeepBoth,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged segment files should move");

        let kept_m4s = final_dir.join("Episode (2).m4s");
        let kept_flv = final_dir.join("Episode Part 2.flv");
        assert_eq!(moved.len(), 2);
        assert!(moved.contains(&kept_m4s));
        assert!(moved.contains(&kept_flv));
        assert_eq!(
            fs::read_to_string(kept_m4s).expect("new m4s segment should move"),
            "new-segment"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Episode (2).nfo"))
                .expect("nfo should follow kept segment basename"),
            "new-nfo"
        );
        assert_eq!(
            fs::read_to_string(kept_flv).expect("new flv segment should move"),
            "new-flv"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_replaces_existing_video_and_sidecar() {
        let final_dir = temp_test_dir("overwrite-final");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old Title [PHH1wTDF-1M].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        fs::write(existing.with_extension("nfo"), "old-nfo").expect("old nfo should write");
        fs::write(existing.with_extension("xml"), "old-xml").expect("old xml should write");
        let unrelated_video = final_dir.join("Old Title [PHH1wTDF-1M].trailer.mp4");
        fs::write(&unrelated_video, "trailer").expect("unrelated video should write");
        let unrelated_part = final_dir.join("Old Title [PHH1wTDF-1M].part2.mkv");
        fs::write(&unrelated_part, "part2").expect("unrelated part should write");
        fs::write(unrelated_part.with_extension("nfo"), "part2-nfo")
            .expect("unrelated part nfo should write");
        let staged = staging_dir.join("New Title [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        fs::write(staged.with_extension("nfo"), "new-nfo").expect("new nfo should write");
        fs::write(staged.with_extension("xml"), "new-xml").expect("new xml should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should overwrite existing files");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(
            fs::read_to_string(&existing).expect("video should be replaced"),
            "new-video"
        );
        assert_eq!(
            fs::read_to_string(existing.with_extension("nfo")).expect("nfo should be replaced"),
            "new-nfo"
        );
        assert_eq!(
            fs::read_to_string(existing.with_extension("xml")).expect("xml should be replaced"),
            "new-xml"
        );
        assert_eq!(
            fs::read_to_string(unrelated_video).expect("unrelated video should remain"),
            "trailer"
        );
        assert_eq!(
            fs::read_to_string(unrelated_part).expect("unrelated part should remain"),
            "part2"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Old Title [PHH1wTDF-1M].part2.nfo"))
                .expect("unrelated part nfo should remain"),
            "part2-nfo"
        );
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_replaces_bare_bilibili_danmaku_sidecar() {
        let final_dir = temp_test_dir("overwrite-bare-bilibili-danmaku");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old Title [BV123].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        fs::write(final_dir.join("danmaku.xml"), "old-danmaku")
            .expect("old bare danmaku should write");
        let staged = staging_dir.join("New Title [BV123].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        fs::write(staging_dir.join("danmaku.xml"), "new-danmaku")
            .expect("new bare danmaku should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should overwrite existing files");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(
            fs::read_to_string(&existing).expect("video should be replaced"),
            "new-video"
        );
        assert_eq!(
            fs::read_to_string(existing.with_extension("xml"))
                .expect("bare danmaku should follow overwritten video basename"),
            "new-danmaku"
        );
        assert!(
            !final_dir.join("danmaku.xml").exists(),
            "stale bare danmaku should be removed during overwrite backup cleanup"
        );
        assert!(
            !final_dir.join("danmaku (2).xml").exists(),
            "new bare danmaku should not be moved as an unrelated relative artifact"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_replaces_unbound_bilibili_subtitle_sidecar() {
        let final_dir = temp_test_dir("overwrite-unbound-bilibili-subtitle");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old Title [BV123].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        fs::write(final_dir.join("subtitle-zh-01-old.ass"), "old-subtitle")
            .expect("old unbound subtitle should write");
        let staged = staging_dir.join("New Title [BV123].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        fs::write(staging_dir.join("subtitle-zh-01-new.ass"), "new-subtitle")
            .expect("new unbound subtitle should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should overwrite existing files");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(
            fs::read_to_string(existing.with_file_name("Old Title [BV123].subtitle-zh-01-new.ass"))
                .expect("subtitle should follow overwritten video basename"),
            "new-subtitle"
        );
        assert!(
            !final_dir.join("subtitle-zh-01-old.ass").exists(),
            "stale unbound subtitle should be removed during overwrite backup cleanup"
        );
        assert!(
            !final_dir.join("subtitle-zh-01-new.ass").exists(),
            "new unbound subtitle should not be moved as an unrelated relative artifact"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_replaces_unbound_bilibili_cover_sidecar() {
        let final_dir = temp_test_dir("overwrite-unbound-bilibili-cover");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old Title [BV123].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        fs::write(final_dir.join("cover-image-old.jpg"), "old-cover")
            .expect("old unbound cover should write");
        let staged = staging_dir.join("New Title [BV123].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        fs::write(staging_dir.join("cover-image-new.jpg"), "new-cover")
            .expect("new unbound cover should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should overwrite existing files");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(
            fs::read_to_string(existing.with_file_name("Old Title [BV123].cover.jpg"))
                .expect("cover should follow overwritten video basename"),
            "new-cover"
        );
        assert!(
            !final_dir.join("cover-image-old.jpg").exists(),
            "stale unbound cover should be removed during overwrite backup cleanup"
        );
        assert!(
            !final_dir.join("cover-image-new.jpg").exists(),
            "new unbound cover should not be moved as an unrelated relative artifact"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn moves_bilibili_danmaku_sidecars_after_mux() {
        let final_dir = temp_test_dir("bilibili-danmaku-sidecars");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("xml"), "danmaku-xml")
            .expect("source xml should write");
        fs::write(source_video.with_extension("ass"), "danmaku-ass")
            .expect("source ass should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("danmaku sidecars should move");

        assert_eq!(
            moved,
            vec![
                output_video.with_extension("xml"),
                output_video.with_extension("ass")
            ]
        );
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("output xml should exist"),
            "danmaku-xml"
        );
        assert_eq!(
            fs::read_to_string(output_video.with_extension("ass"))
                .expect("output ass should exist"),
            "danmaku-ass"
        );
        assert!(!source_video.with_extension("xml").exists());
        assert!(!source_video.with_extension("ass").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn moves_bilibili_danmaku_sidecars_over_stale_output_sidecar() {
        let final_dir = temp_test_dir("bilibili-danmaku-stale-sidecar");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("xml"), "old-xml").expect("stale xml should write");
        std::thread::sleep(Duration::from_millis(20));
        let since = SystemTime::now();
        std::thread::sleep(Duration::from_millis(20));
        fs::write(source_video.with_extension("xml"), "new-xml").expect("source xml should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, since)
            .expect("stale danmaku sidecar should be replaced");

        assert_eq!(moved, vec![output_video.with_extension("xml")]);
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("output xml should exist"),
            "new-xml"
        );
        assert!(!source_video.with_extension("xml").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn moves_root_danmaku_sidecars_to_unique_mux_output() {
        let final_dir = temp_test_dir("bilibili-danmaku-unique-output");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(final_dir.join("Final Title.xml"), "root-xml").expect("root xml should write");
        fs::write(final_dir.join("Final Title.ass"), "root-ass").expect("root ass should write");
        let output_video = final_dir.join("Final Title (2).mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("root danmaku sidecars should move");

        assert_eq!(
            moved,
            vec![
                output_video.with_extension("xml"),
                output_video.with_extension("ass")
            ]
        );
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("unique output xml should exist"),
            "root-xml"
        );
        assert_eq!(
            fs::read_to_string(output_video.with_extension("ass"))
                .expect("unique output ass should exist"),
            "root-ass"
        );
        assert!(!final_dir.join("Final Title.xml").exists());
        assert!(!final_dir.join("Final Title.ass").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn moves_single_custom_danmaku_sidecar_to_mux_output() {
        let final_dir = temp_test_dir("bilibili-danmaku-custom-output");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(final_dir.join("Custom Pattern.xml"), "custom-xml")
            .expect("custom xml should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("single custom danmaku sidecar should move");

        assert_eq!(moved, vec![output_video.with_extension("xml")]);
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("output xml should exist"),
            "custom-xml"
        );
        assert!(!final_dir.join("Custom Pattern.xml").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn ignores_nested_unrelated_danmaku_sidecars_after_mux() {
        let final_dir = temp_test_dir("bilibili-danmaku-nested-unrelated");
        let stream_dir = final_dir.join("1556453868");
        let unrelated_dir = final_dir.join("unrelated").join("nested");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        fs::create_dir_all(&unrelated_dir).expect("unrelated dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let unrelated = unrelated_dir.join("Only Candidate.xml");
        fs::write(&unrelated, "unrelated-xml").expect("unrelated xml should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("unrelated nested sidecars should be ignored");

        assert!(moved.is_empty());
        assert_eq!(
            fs::read_to_string(&unrelated).expect("unrelated xml should remain"),
            "unrelated-xml"
        );
        assert!(!output_video.with_extension("xml").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn prefers_raw_stream_danmaku_sidecar_among_multiple_candidates() {
        let final_dir = temp_test_dir("bilibili-danmaku-raw-priority");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("xml"), "part-1-xml")
            .expect("source xml should write");
        fs::write(stream_dir.join("Part 2.xml"), "part-2-xml").expect("part 2 xml should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("raw danmaku sidecar should move");

        assert_eq!(moved, vec![output_video.with_extension("xml")]);
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("output xml should exist"),
            "part-1-xml"
        );
        assert!(!source_video.with_extension("xml").exists());
        assert!(
            stream_dir.join("Part 2.xml").exists(),
            "unmatched sibling sidecar should remain"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn keeps_current_root_danmaku_sidecar_after_mux() {
        let final_dir = temp_test_dir("bilibili-danmaku-current-root-sidecar");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("xml"), "raw-xml").expect("source xml should write");
        fs::write(stream_dir.join("Part 2.xml"), "part-2-xml").expect("part 2 xml should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("xml"), "current-xml")
            .expect("current xml should write");

        let moved =
            move_bilibili_danmaku_sidecars(&source_video, &output_video, SystemTime::UNIX_EPOCH)
                .expect("current root sidecar should be kept");

        assert!(moved.is_empty());
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("output xml should exist"),
            "current-xml"
        );
        assert!(
            !source_video.with_extension("xml").exists(),
            "duplicate raw sidecar should be removed when the root sidecar is from the same download"
        );
        assert!(
            stream_dir.join("Part 2.xml").exists(),
            "sibling sidecars should remain for later mux outputs"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn does_not_reassign_bound_root_danmaku_sidecar_to_later_mux_output() {
        let final_dir = temp_test_dir("bilibili-danmaku-bound-root-sidecar");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 2.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let first_output = final_dir.join("Final Title.mp4");
        fs::write(&first_output, "first-output").expect("first output should write");
        fs::write(first_output.with_extension("xml"), "first-xml").expect("first xml should write");
        let second_output = final_dir.join("Final Title (2).mp4");
        fs::write(&second_output, "second-output").expect("second output should write");

        let moved =
            move_bilibili_danmaku_sidecars(&source_video, &second_output, SystemTime::UNIX_EPOCH)
                .expect("bound root sidecar should not be reassigned");

        assert!(moved.is_empty());
        assert_eq!(
            fs::read_to_string(first_output.with_extension("xml"))
                .expect("first xml should remain bound"),
            "first-xml"
        );
        assert!(!second_output.with_extension("xml").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn removes_current_json_danmaku_sidecars_after_mux() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-cleanup");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("json"), "raw-json")
            .expect("source json should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("json"), "root-json")
            .expect("root json should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("current json danmaku sidecars should be removed");

        assert!(!source_video.with_extension("json").exists());
        assert!(!output_video.with_extension("json").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn preserves_unrelated_current_json_during_json_danmaku_cleanup() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-unrelated-cleanup");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let state_json = final_dir.join("state.json");
        fs::write(&state_json, "state-json").expect("state json should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("json danmaku cleanup should succeed");

        assert_eq!(
            fs::read_to_string(state_json).expect("state json should remain"),
            "state-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn removes_current_root_json_danmaku_sidecar_for_unique_mux_output() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-unique-cleanup");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let root_json = final_dir.join("Final Title.json");
        fs::write(&root_json, "root-json").expect("root json should write");
        let output_video = final_dir.join("Final Title (2).mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("root json danmaku sidecar should be removed");

        assert!(!root_json.exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn preserves_bound_root_json_danmaku_sidecar_for_later_mux_output() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-bound-root");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 2.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let first_output = final_dir.join("Final Title.mp4");
        fs::write(&first_output, "first-output").expect("first output should write");
        let root_json = first_output.with_extension("json");
        fs::write(&root_json, "first-json").expect("root json should write");
        let second_output = final_dir.join("Final Title (2).mp4");
        fs::write(&second_output, "second-output").expect("second output should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &second_output, UNIX_EPOCH)
            .expect("bound root json sidecar should be preserved");

        assert_eq!(
            fs::read_to_string(root_json).expect("bound root json should remain"),
            "first-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn preserves_info_json_sidecars_during_json_danmaku_cleanup() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-info-json");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        let info_json = final_dir.join("Final Title.info.json");
        fs::write(&info_json, "info-json").expect("info json should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("json danmaku cleanup should succeed");

        assert_eq!(
            fs::read_to_string(info_json).expect("info json should remain"),
            "info-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn removes_current_json_danmaku_when_video_stem_ends_with_info() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-info-stem");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.info.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("json"), "raw-json")
            .expect("source json should write");
        let output_video = final_dir.join("Final Title.info.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("json"), "root-json")
            .expect("root json should write");
        let info_json = final_dir.join("Final Title.info.info.json");
        fs::write(&info_json, "info-json").expect("info json should write");

        cleanup_bilibili_json_danmaku_sidecars(&source_video, &output_video, UNIX_EPOCH)
            .expect("json danmaku cleanup should succeed");

        assert!(!source_video.with_extension("json").exists());
        assert!(!output_video.with_extension("json").exists());
        assert_eq!(
            fs::read_to_string(info_json).expect("info json should remain"),
            "info-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn cleanup_bilibili_download_outputs_removes_json_without_mux() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-no-mux-cleanup");
        fs::create_dir_all(&final_dir).expect("final dir should create");
        let video = final_dir.join("Final Title.mp4");
        fs::write(&video, "video").expect("video should write");
        fs::write(video.with_extension("xml"), "xml").expect("xml should write");
        fs::write(video.with_extension("ass"), "ass").expect("ass should write");
        fs::write(video.with_extension("json"), "json").expect("json should write");

        cleanup_bilibili_download_outputs(std::slice::from_ref(&video), UNIX_EPOCH)
            .expect("non-mux output cleanup should succeed");

        assert!(video.with_extension("xml").exists());
        assert!(video.with_extension("ass").exists());
        assert!(!video.with_extension("json").exists());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn preserves_stale_json_danmaku_sidecars_after_mux() {
        let final_dir = temp_test_dir("bilibili-json-danmaku-stale");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("json"), "old-raw-json")
            .expect("source json should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("json"), "old-root-json")
            .expect("root json should write");

        cleanup_bilibili_json_danmaku_sidecars(
            &source_video,
            &output_video,
            SystemTime::now() + Duration::from_secs(1),
        )
        .expect("stale json danmaku sidecars should remain");

        assert_eq!(
            fs::read_to_string(source_video.with_extension("json"))
                .expect("old raw json should remain"),
            "old-raw-json"
        );
        assert_eq!(
            fs::read_to_string(output_video.with_extension("json"))
                .expect("old root json should remain"),
            "old-root-json"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn preserves_existing_root_danmaku_sidecar_without_replacement() {
        let final_dir = temp_test_dir("bilibili-danmaku-stale-root-sidecar");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");
        fs::write(output_video.with_extension("xml"), "old-xml").expect("old xml should write");

        let moved = move_bilibili_danmaku_sidecars(
            &source_video,
            &output_video,
            SystemTime::now() + Duration::from_secs(1),
        )
        .expect("existing root sidecar should remain");

        assert!(moved.is_empty());
        assert_eq!(
            fs::read_to_string(output_video.with_extension("xml"))
                .expect("existing xml should remain"),
            "old-xml"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn ignores_stale_raw_danmaku_sidecar_without_replacement() {
        let final_dir = temp_test_dir("bilibili-danmaku-stale-raw-sidecar");
        let stream_dir = final_dir.join("1556453868");
        fs::create_dir_all(&stream_dir).expect("stream dir should create");
        let source_video = stream_dir.join("Part 1.mp4");
        fs::write(&source_video, "video").expect("source video should write");
        fs::write(source_video.with_extension("xml"), "old-raw-xml")
            .expect("old raw xml should write");
        let output_video = final_dir.join("Final Title.mp4");
        fs::write(&output_video, "merged").expect("output video should write");

        let moved = move_bilibili_danmaku_sidecars(
            &source_video,
            &output_video,
            SystemTime::now() + Duration::from_secs(1),
        )
        .expect("stale raw sidecar should be ignored");

        assert!(moved.is_empty());
        assert!(!output_video.with_extension("xml").exists());
        assert_eq!(
            fs::read_to_string(source_video.with_extension("xml"))
                .expect("stale raw xml should remain"),
            "old-raw-xml"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn bilibili_mux_output_avoids_stale_root_danmaku_sidecars_without_replacement() {
        let final_dir = temp_test_dir("bilibili-output-current-sidecar");
        fs::create_dir_all(&final_dir).expect("final dir should create");
        fs::write(final_dir.join("Final Title.xml"), "current-xml")
            .expect("current xml should write");

        let output = unique_bilibili_mux_output_path(
            &final_dir,
            "Final Title",
            "mp4",
            SystemTime::now() + Duration::from_secs(1),
        );

        assert_eq!(output, final_dir.join("Final Title (2).mp4"));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn bilibili_mux_output_uses_current_root_danmaku_sidecar_stem() {
        let final_dir = temp_test_dir("bilibili-output-current-sidecar-stem");
        fs::create_dir_all(&final_dir).expect("final dir should create");
        fs::write(final_dir.join("Final Title.xml"), "current-xml")
            .expect("current xml should write");

        let output = unique_bilibili_mux_output_path(&final_dir, "Final Title", "mp4", UNIX_EPOCH);

        assert_eq!(output, final_dir.join("Final Title.mp4"));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn bilibili_metadata_command_is_info_only_without_activity_tracking() {
        let mut config = test_config();
        let video_dir = temp_test_dir("bilibili-metadata-empty-config");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        fs::write(video_dir.join("BBDown.config"), "--save-archives-to-file\n")
            .expect("default BBDown config should write");
        config.downloads.video_dir = video_dir.clone();
        config.bilibili.auth.state_path = video_dir.join("missing-auth.json");
        let spec = bilibili_metadata_command_spec(&config, "https://b23.tv/Jt1mZiL")
            .expect("Bilibili metadata command should build");

        assert_eq!(spec.args[0], "https://b23.tv/Jt1mZiL");
        assert_eq!(spec.args[1], "--only-show-info");
        let config_path = command_config_path(&spec).expect("config file arg should be present");
        assert_eq!(fs::read_to_string(&config_path).unwrap(), "");
        assert_eq!(spec.activity_dir, None);
        assert_eq!(spec.cleanup_paths, vec![config_path.clone()]);
        bilibili_auth::release_bbdown_config_file(&config_path);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn bilibili_metadata_command_ignores_download_args() {
        let mut config = test_config();
        config.bilibili.extra_args = vec![
            "--only-show-info=false".to_string(),
            "-info".to_string(),
            "false".to_string(),
            "--skip-mux".to_string(),
        ];

        let spec = bilibili_metadata_command_spec(&config, "https://b23.tv/Jt1mZiL")
            .expect("Bilibili metadata command should build");

        assert!(!spec.args.contains(&"--only-show-info=false".to_string()));
        assert!(
            !spec
                .args
                .windows(2)
                .any(|args| args[0] == "-info" && args[1] == "false")
        );
        assert_eq!(
            spec.args
                .iter()
                .filter(|arg| arg.as_str() == "--only-show-info")
                .count(),
            1
        );
        assert!(!spec.args.contains(&"--skip-mux".to_string()));
    }

    #[test]
    fn bilibili_metadata_command_ignores_archive_writing_flags() {
        let mut config = test_config();
        config.bilibili.extra_args = vec![
            "--save-archives-to-file".to_string(),
            "true".to_string(),
            "--save-archives-to-file=false".to_string(),
            "--skip-mux".to_string(),
        ];

        let spec = bilibili_metadata_command_spec(&config, "https://b23.tv/Jt1mZiL")
            .expect("Bilibili metadata command should build");

        assert!(
            !spec
                .args
                .iter()
                .any(|arg| arg.starts_with("--save-archives-to-file"))
        );
        assert!(!spec.args.contains(&"true".to_string()));
        assert!(spec.args.contains(&"--only-show-info".to_string()));
        assert!(!spec.args.contains(&"--skip-mux".to_string()));
    }

    #[test]
    fn overwrite_rejects_ambiguous_duplicate_videos() {
        let final_dir = temp_test_dir("overwrite-unmapped-duplicates");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let first_existing = final_dir.join("First [PHH1wTDF-1M].mkv");
        fs::write(&first_existing, "old-first").expect("first existing should write");
        let second_existing = final_dir.join("Second [PHH1wTDF-1M].mkv");
        fs::write(&second_existing, "old-second").expect("second existing should write");
        fs::write(second_existing.with_extension("nfo"), "old-second-nfo")
            .expect("second nfo should write");
        let staged = staging_dir.join("New [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![first_existing.clone(), second_existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let error = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("ambiguous duplicates must not select an overwrite target by position");

        assert!(error.to_string().contains("exact unique match"));
        assert_eq!(
            fs::read_to_string(first_existing).expect("first existing should remain"),
            "old-first"
        );
        assert_eq!(
            fs::read_to_string(second_existing).expect("second existing should remain"),
            "old-second"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Second [PHH1wTDF-1M].nfo"))
                .expect("second nfo should remain"),
            "old-second-nfo"
        );
        assert_eq!(
            fs::read_to_string(staged).expect("staged file should remain after rejection"),
            "new-video"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidation_rejects_missing_target() {
        let final_dir = temp_test_dir("overwrite-revalidation-missing");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let missing = final_dir.join("Missing [PHH1wTDF-1M].mkv");
        let staged = staging_dir.join("New [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![missing],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let error = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("a missing overwrite target must be rejected");

        assert!(error.to_string().contains("overwrite target is missing"));
        assert_eq!(
            fs::read_to_string(staged).expect("staged file should remain after rejection"),
            "new-video"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidation_rejects_unreadable_identity_metadata() {
        let final_dir = temp_test_dir("overwrite-revalidation-unreadable");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old [PHH1wTDF-1M].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        fs::create_dir(existing.with_extension("nfo"))
            .expect("unreadable NFO fixture should create");
        let staged = staging_dir.join("New [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let error = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("unreadable identity metadata must be rejected");

        assert!(format!("{error:#}").contains("overwrite identity metadata is unreadable"));
        assert_eq!(
            fs::read_to_string(existing).expect("existing file should remain"),
            "old-video"
        );
        assert_eq!(
            fs::read_to_string(staged).expect("staged file should remain after rejection"),
            "new-video"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidation_rejects_changed_identity() {
        let final_dir = temp_test_dir("overwrite-revalidation-changed-identity");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Old [BV123].mkv");
        fs::write(&existing, "old-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid999");
        let staged = staging_dir.join("New [BV123].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let error = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("a changed overwrite identity must be rejected");

        assert!(error.to_string().contains("identity no longer matches"));
        assert_eq!(
            fs::read_to_string(existing).expect("existing file should remain"),
            "old-video"
        );
        assert_eq!(
            fs::read_to_string(staged).expect("staged file should remain after rejection"),
            "new-video"
        );
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidation_rejects_new_identity_ambiguity() {
        let final_dir = temp_test_dir("overwrite-revalidation-new-ambiguity");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("First [BV123].mkv");
        fs::write(&existing, "old-first").expect("first existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let second = final_dir.join("Second [BV123].mkv");
        fs::write(&second, "old-second").expect("second existing file should write");
        write_bilibili_identity_nfo(&second, "cid123");
        let staged = staging_dir.join("New [BV123].mkv");
        fs::write(&staged, "new-video").expect("staged file should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let error = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("new overwrite identity ambiguity must be rejected");

        assert!(
            error
                .to_string()
                .contains("is now ambiguous across 2 files")
        );
        assert_eq!(
            fs::read_to_string(existing).expect("first existing file should remain"),
            "old-first"
        );
        assert_eq!(
            fs::read_to_string(second).expect("second existing file should remain"),
            "old-second"
        );
        assert_eq!(
            fs::read_to_string(staged).expect("staged file should remain after rejection"),
            "new-video"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn acquired_overwrite_keeps_owned_backups_when_target_is_recreated() {
        let final_dir = temp_test_dir("overwrite-acquired-target-recreated");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };

        let acquired = acquire_and_validate_overwrite_target(
            &final_dir,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("overwrite target should be acquired");
        let backup_dir = acquired.backup_dir.clone();
        let backup_paths = acquired
            .backups
            .iter()
            .map(|backup| backup.backup.clone())
            .collect::<Vec<_>>();
        fs::write(&existing, "replacement-video").expect("replacement should write");

        let error = acquired
            .restore()
            .expect_err("occupied target must block restoration");

        assert!(
            error
                .to_string()
                .contains("restore destination is occupied")
        );
        assert_eq!(
            fs::read_to_string(&existing).expect("replacement should remain"),
            "replacement-video"
        );
        assert!(!existing.with_extension("nfo").exists());
        assert!(backup_dir.is_dir());
        assert!(backup_paths.iter().all(|path| path.is_file()));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_uses_most_specific_primary_for_existing_sidecars() {
        let final_dir = temp_test_dir("overwrite-dot-prefix-sidecar");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let sibling = final_dir.join("Movie.mkv");
        fs::write(&sibling, "sibling-video").expect("sibling video should write");
        fs::write(sibling.with_extension("nfo"), "sibling-nfo").expect("sibling nfo should write");
        let existing_part2 = final_dir.join("Movie.part2.mkv");
        fs::write(&existing_part2, "old-part2").expect("existing part2 should write");
        fs::write(
            existing_part2.with_extension("nfo"),
            r#"<movie><uniqueid type="youtube">PHH1wTDF-1M</uniqueid></movie>"#,
        )
        .expect("existing part2 nfo should write");
        let staged_part2 = staging_dir.join("New Movie.part2.mkv");
        fs::write(&staged_part2, "new-part2").expect("staged part2 should write");
        fs::write(staged_part2.with_extension("nfo"), "new-part2-nfo")
            .expect("staged part2 nfo should write");
        let duplicate = VideoDuplicate {
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![existing_part2.clone()],
        };
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");

        let moved = move_staged_video_files(
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect("staged files should overwrite existing part2 files");

        assert_eq!(moved, vec![existing_part2.clone()]);
        assert_eq!(
            fs::read_to_string(&existing_part2).expect("part2 should be replaced"),
            "new-part2"
        );
        assert_eq!(
            fs::read_to_string(existing_part2.with_extension("nfo"))
                .expect("part2 nfo should be replaced"),
            "new-part2-nfo"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Movie.nfo")).expect("sibling nfo should remain"),
            "sibling-nfo"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn removes_staging_paths_from_report_details() {
        let staging_dir = PathBuf::from("/tmp/videos/.telegram-video-downloader-staging/job-1");
        let details = [
            "Subtitles: manual en",
            "NFO: /tmp/videos/.telegram-video-downloader-staging/job-1/video.nfo",
            "stderr tail",
        ]
        .join("\n");

        assert_eq!(
            remove_staging_detail_lines(&details, &staging_dir),
            "Subtitles: manual en\nstderr tail"
        );
    }

    #[test]
    fn bilibili_jobs_do_not_build_cli_command_specs() {
        let config = test_config();
        let error = command_spec(
            &config,
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string(),
                selection: None,
            },
        )
        .expect_err("Bilibili command spec should not exist");

        assert!(error.to_string().contains("bbdown-core crate API"));
    }

    #[test]
    fn builds_bilibili_direct_download_options_from_config() {
        let mut config = test_config();
        let video_dir = temp_test_dir("bilibili-direct-options");
        config.downloads.video_dir = video_dir.clone();
        config.tools.ffmpeg = PathBuf::from("/opt/bin/ffmpeg");
        config.bilibili.danmaku.enabled = false;
        config.bilibili.download_args = vec!["--only".to_string(), "audio".to_string()];

        let options =
            bilibili_core::download_options(&config).expect("Bilibili direct options should build");

        assert_eq!(options.output_dir, video_dir);
        assert_eq!(options.mode, DownloadMode::AudioOnly);
        assert!(!options.include_danmaku);
        assert!(!options.sidecars.danmaku);
        assert!(!options.media_hosts.allow_pcdn);
        assert_eq!(
            options.download_idle_timeout,
            Some(Duration::from_secs(config.bot.command_idle_timeout_seconds))
        );
        assert!(
            matches!(options.mux, bbdown_core::MuxOptions::Ffmpeg { ref binary } if binary == &PathBuf::from("/opt/bin/ffmpeg"))
        );

        let mut relative_config = test_config();
        relative_config.downloads.video_dir = PathBuf::from("relative-videos");
        let relative_options = bilibili_core::download_options(&relative_config)
            .expect("relative Bilibili direct options should build");
        assert_eq!(
            relative_options.output_dir,
            PathBuf::from("relative-videos")
        );
    }

    #[test]
    fn duplicate_scan_uses_configured_video_dir_for_all_providers() {
        let root = temp_test_dir("relative-download-dir-config");
        fs::create_dir_all(&root).expect("temp config dir should create");
        fs::write(
            root.join("config.toml"),
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [downloads]
            video_dir = "relative-videos"
            "#,
        )
        .expect("config should write");
        let config = AppConfig::load(&root.join("config.toml")).expect("config should load");
        let expected = fs::canonicalize(&root)
            .expect("temp config dir should canonicalize")
            .join("relative-videos");

        assert_eq!(
            duplicate_scan_video_dir(
                &config,
                &JobRequest::Bilibili {
                    url: "https://www.bilibili.com/video/BV123".to_string(),
                    selection: None,
                },
            ),
            expected
        );
        assert_eq!(
            duplicate_scan_video_dir(
                &config,
                &JobRequest::Youtube {
                    url: "https://youtu.be/PHH1wTDF-1M".to_string(),
                },
            ),
            expected
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parses_bilibili_plan_and_pushes_episode_identities() {
        let plan = parse_bilibili_plan(
            r#"
            {"title":"Season","entries":[{"index":1,"aid":123,"bvid":"BV123","cid":456,"epid":789,"title":"Episode 1"}]}
            "#,
        )
        .expect("plan JSON should parse");
        let mut identities = Vec::new();
        push_bilibili_plan_identities(&mut identities, &plan);
        let ids = identities
            .into_iter()
            .map(|identity| identity.id)
            .collect::<Vec<_>>();

        assert!(ids.contains(&"BV123".to_string()));
        assert!(ids.contains(&"av123".to_string()));
        assert!(ids.contains(&"cid456".to_string()));
        assert!(ids.contains(&"ep789".to_string()));
        assert!(!ids.contains(&"123".to_string()));
        assert!(!ids.contains(&"789".to_string()));
    }

    #[test]
    fn parses_bilibili_download_report_and_selects_primary_media() {
        let report = parse_bilibili_download_report(
            r#"
            {"title":"Season","output_dir":".","entries":[{"index":1,"title":"Episode 1","directory":"Episode 1","files":[{"kind":"video","path":"Episode 1/video.m4s"},{"kind":"audio","path":"Episode 1/audio.m4s"}],"mux":{"output_path":"Episode 1/Episode 1.mkv"}}]}
            "#,
        )
        .expect("download report JSON should parse");
        let primary_media = bilibili_report_primary_media(Path::new("/tmp/videos"), &report);

        assert_eq!(
            primary_media,
            vec![PathBuf::from("/tmp/videos/Episode 1/Episode 1.mkv")]
        );
    }

    #[test]
    fn cleanup_bilibili_mux_input_files_removes_raw_streams_only() {
        let root = temp_test_dir("cleanup-bilibili-mux-inputs");
        let entry_dir = root.join("Episode 1");
        fs::create_dir_all(&entry_dir).expect("entry dir should create");
        let video = entry_dir.join("video.m4s");
        let audio = entry_dir.join("audio.m4s");
        let mux = entry_dir.join("Episode 1.mkv");
        let danmaku = entry_dir.join("Episode 1.xml");
        let concat = entry_dir.join("ffmpeg-concat.txt");
        fs::write(&video, "video").expect("video should write");
        fs::write(&audio, "audio").expect("audio should write");
        fs::write(&mux, "mux").expect("mux should write");
        fs::write(&danmaku, "danmaku").expect("danmaku should write");
        fs::write(&concat, "user-owned").expect("user concat file should write");
        let report = parse_bilibili_download_report(
            r#"
            {"title":"Season","output_dir":".","entries":[{"index":1,"title":"Episode 1","directory":"Episode 1","files":[{"kind":"video","path":"Episode 1/video.m4s"},{"kind":"audio","path":"Episode 1/audio.m4s"},{"kind":"danmaku","path":"Episode 1/Episode 1.xml"}],"mux":{"output_path":"Episode 1/Episode 1.mkv"}}]}
            "#,
        )
        .expect("download report JSON should parse");

        cleanup_bilibili_mux_input_files(&root, &report).expect("raw inputs should clean up");

        assert!(!video.exists());
        assert!(!audio.exists());
        assert_eq!(
            fs::read_to_string(concat).expect("user concat file should remain"),
            "user-owned"
        );
        assert!(mux.exists());
        assert!(danmaku.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn staging_support_files_only_include_managed_config() {
        let staging_dir = PathBuf::from("/tmp/staging");

        assert!(is_staging_support_file(
            &staging_dir,
            &staging_dir.join("BBDown.config")
        ));
        assert!(!is_staging_support_file(
            &staging_dir,
            &staging_dir.join("ffmpeg-concat.txt")
        ));
        assert!(!is_staging_support_file(
            &staging_dir,
            &staging_dir.join("Episode 1.mkv")
        ));
    }

    #[test]
    fn audio_only_downloads_use_audio_primary_media_detection() {
        let mut config = test_config();
        config.bilibili.download_args = vec!["--only".to_string(), "audio".to_string()];
        assert!(bilibili_downloads_audio_only(&config).expect("audio-only check should not fail"));

        let mut legacy_config = test_config();
        legacy_config.bilibili.extra_args = vec!["--audio-only".to_string()];
        assert!(
            bilibili_downloads_audio_only(&legacy_config)
                .expect("legacy audio-only check should not fail")
        );

        assert!(has_bilibili_only_value(
            &["--only=audio".to_string()],
            "audio"
        ));
        assert!(!has_bilibili_only_value(
            &["--only".to_string(), "video".to_string()],
            "audio"
        ));
    }

    #[test]
    fn artifact_only_bilibili_modes_skip_primary_media_requirement() {
        let mut config = test_config();
        let job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV123".to_string(),
            selection: None,
        };

        for mode in ["subtitle", "danmaku", "cover"] {
            config.bilibili.download_args = vec!["--only".to_string(), mode.to_string()];
            assert!(
                bilibili_downloads_artifacts_only(&config, &job)
                    .expect("artifact-only check should succeed"),
                "{mode} should be artifact-only"
            );
        }

        for mode in ["all", "audio", "video"] {
            config.bilibili.download_args = vec!["--only".to_string(), mode.to_string()];
            assert!(
                !bilibili_downloads_artifacts_only(&config, &job)
                    .expect("artifact-only check should succeed"),
                "{mode} should require primary media"
            );
        }

        config.bilibili.download_args = vec!["--only".to_string(), "danmaku".to_string()];
        assert!(
            !bilibili_downloads_artifacts_only(
                &config,
                &JobRequest::Youtube {
                    url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
                },
            )
            .expect("non-Bilibili check should succeed")
        );
    }

    #[test]
    fn builds_youtube_download_command_without_repeating_relative_output_dir() {
        let mut config = test_config();
        config.downloads.video_dir = PathBuf::from("downloads");
        let spec =
            youtube_download_command_spec(&config, "https://youtu.be/abc", &SubtitlePlan::none());

        assert_eq!(spec.cwd, PathBuf::from("downloads"));
        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["-P".to_string(), ".".to_string()])
        );
        assert!(!spec.args.contains(&"downloads".to_string()));
    }

    #[test]
    fn builds_ffmpeg_mux_command() {
        let config = test_config();
        let spec = ffmpeg_mux_command_spec(
            &config,
            Path::new("/tmp/video.mp4"),
            Path::new("/tmp/audio.m4a"),
            Path::new("/tmp/output.mp4"),
        );

        assert_eq!(spec.program, PathBuf::from("ffmpeg"));
        for expected in ["-i", "/tmp/video.mp4", "/tmp/audio.m4a", "-c", "copy"] {
            assert!(
                spec.args.contains(&expected.to_string()),
                "missing {expected}"
            );
        }
    }

    #[test]
    fn builds_bilibili_local_dash_mux_command() {
        let mut config = test_config();
        config.tools.ffmpeg = PathBuf::from("/opt/bin/ffmpeg");
        let entry_dir = temp_test_dir("bilibili-local-dash-mux");
        let user_concat = entry_dir.join("ffmpeg-concat.txt");
        fs::write(&user_concat, "user-owned").expect("user concat file should write");
        let inputs = vec![
            BilibiliMediaInput {
                kind: "video".to_string(),
                path: entry_dir.join("video.m4s"),
            },
            BilibiliMediaInput {
                kind: "audio".to_string(),
                path: entry_dir.join("audio.m4s"),
            },
        ];
        let output = entry_dir.join("Episode.mp4");

        let (spec, concat_file) =
            bilibili_local_mux_command_spec(&config, &inputs, &entry_dir, &output)
                .expect("dash mux spec should build");

        assert!(concat_file.is_none());
        assert_eq!(spec.program, PathBuf::from("/opt/bin/ffmpeg"));
        assert_eq!(spec.cwd, entry_dir);
        assert!(spec.args.contains(&"-nostdin".to_string()));
        assert!(spec.args.windows(2).any(|args| args == ["-map", "0:0"]));
        assert!(spec.args.windows(2).any(|args| args == ["-map", "1:0"]));
        assert!(!spec.args.windows(2).any(|args| args == ["-f", "concat"]));
        assert_eq!(
            fs::read_to_string(user_concat).expect("user concat file should remain"),
            "user-owned"
        );
        let _ = fs::remove_dir_all(entry_dir);
    }

    #[test]
    fn builds_bilibili_local_flv_concat_mux_command() {
        let config = test_config();
        let entry_dir = temp_test_dir("bilibili-local-flv-mux");
        let user_concat = entry_dir.join("ffmpeg-concat.txt");
        fs::write(&user_concat, "user-owned").expect("user concat file should write");
        let inputs = vec![
            BilibiliMediaInput {
                kind: "flv_segment".to_string(),
                path: entry_dir.join("segment-001.flv"),
            },
            BilibiliMediaInput {
                kind: "flv_segment".to_string(),
                path: entry_dir.join("segment-002.flv"),
            },
        ];
        let output = entry_dir.join("Episode.mp4");

        let (spec, concat_file) =
            bilibili_local_mux_command_spec(&config, &inputs, &entry_dir, &output)
                .expect("flv mux spec should build");

        let concat_file = concat_file.expect("flv mux should create concat list");
        let concat_path = concat_file.path().to_path_buf();
        let concat = fs::read_to_string(&concat_path).expect("concat list should read");
        assert!(concat.contains("segment-001.flv"));
        assert!(concat.contains("segment-002.flv"));
        assert_ne!(concat_path, user_concat);
        assert!(
            concat_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(BILIBILI_FFMPEG_CONCAT_FILE_PREFIX))
        );
        assert!(spec.args.windows(2).any(|args| args == ["-f", "concat"]));
        let concat_arg = command_path_arg(&concat_path);
        assert!(
            spec.args
                .windows(2)
                .any(|args| args[0] == "-i" && args[1] == concat_arg)
        );
        drop(concat_file);
        assert!(!concat_path.exists());
        assert_eq!(
            fs::read_to_string(user_concat).expect("user concat file should remain"),
            "user-owned"
        );
        let _ = fs::remove_dir_all(entry_dir);
    }

    #[test]
    fn builds_youtube_metadata_command() {
        let config = test_config();
        let spec = youtube_metadata_command_spec(&config, "https://youtu.be/abc");

        assert_eq!(spec.program, PathBuf::from("yt-dlp"));
        assert!(spec.args.contains(&"--dump-json".to_string()));
        assert!(spec.args.contains(&"--skip-download".to_string()));
        assert!(spec.args.contains(&"--no-playlist".to_string()));
        assert_eq!(spec.activity_dir, None);
    }

    #[test]
    fn builds_youtube_download_command_with_metadata_sidecars() {
        let config = test_config();
        let subtitle_plan = SubtitlePlan {
            source: Some(SubtitleSource::Manual),
            languages: vec!["en".to_string(), "ja".to_string()],
        };
        let spec = youtube_download_command_spec(&config, "https://youtu.be/abc", &subtitle_plan);

        assert_eq!(spec.program, PathBuf::from("yt-dlp"));
        for expected in [
            "--merge-output-format",
            "mkv",
            "--remux-video",
            "--embed-thumbnail",
            "--embed-metadata",
            "--embed-chapters",
            "--embed-info-json",
            "--write-info-json",
            "--write-description",
            "--write-thumbnail",
            "--write-subs",
            "--sub-langs",
            "en,ja",
            "--embed-subs",
            "after_move:filepath",
        ] {
            assert!(
                spec.args.contains(&expected.to_string()),
                "missing {expected}"
            );
        }
        assert_eq!(spec.cwd, test_home().join("Movies").join("Downloads"));
        assert!(
            spec.args
                .windows(2)
                .any(|args| args == ["-P".to_string(), ".".to_string()])
        );
    }

    #[test]
    fn builds_pdf_command_with_uv() {
        let config = test_config();
        let spec = command_spec(
            &config,
            &JobRequest::Pdf {
                url: "https://example.com".to_string(),
            },
        )
        .expect("PDF command should build");

        assert_eq!(spec.program, PathBuf::from("uv"));
        assert_eq!(spec.args[0], "run");
        assert_eq!(spec.args[1], "python");
        assert!(
            spec.args
                .iter()
                .any(|arg| arg.ends_with("scripts/pdf_helper.py"))
        );
        assert!(spec.args.contains(&"--chrome".to_string()));
    }

    #[test]
    fn selects_manual_subtitles_before_automatic() {
        let plan = select_subtitles(
            &metadata_with_subtitles(),
            &["zh-Hans".to_string(), "en".to_string(), "ja".to_string()],
        );

        assert_eq!(
            plan,
            SubtitlePlan {
                source: Some(SubtitleSource::Manual),
                languages: vec!["en".to_string(), "ja".to_string()]
            }
        );
    }

    #[test]
    fn falls_back_to_automatic_subtitles() {
        let metadata = YoutubeMetadata {
            automatic_captions: BTreeMap::from([
                ("zh-Hans".to_string(), serde_json::json!([])),
                ("en".to_string(), serde_json::json!([])),
            ]),
            ..YoutubeMetadata::default()
        };

        let plan = select_subtitles(&metadata, &["zh".to_string(), "en".to_string()]);

        assert_eq!(
            plan,
            SubtitlePlan {
                source: Some(SubtitleSource::Automatic),
                languages: vec!["zh-Hans".to_string(), "en".to_string()]
            }
        );
    }

    #[test]
    fn parses_bilibili_metadata() {
        let metadata = parse_bilibili_metadata(
            "https://www.bilibili.com/video/BV12TRrBcEP8/",
            "[2026] - 获取aid结束: 1556453868\n[2026] - 视频标题: Workout\n[2026] - 发布时间: 2026-05-05 05:24:12 +01:00\n[2026] - UP主页: https://space.bilibili.com/604003146",
        );

        assert_eq!(metadata.title.as_deref(), Some("Workout"));
        assert_eq!(metadata.publish_date.as_deref(), Some("2026-05-05"));
        assert_eq!(
            metadata.uploader_url.as_deref(),
            Some("https://space.bilibili.com/604003146")
        );
        assert_eq!(metadata.id.as_deref(), Some("BV12TRrBcEP8"));
        assert_eq!(metadata.aid.as_deref(), Some("1556453868"));
    }

    #[test]
    fn parses_bilibili_metadata_resolved_video_url() {
        let metadata = parse_bilibili_metadata(
            "https://b23.tv/Jt1mZiL",
            "[2026] - 视频URL: https://www.bilibili.com/video/BV12TRrBcEP8/\n[2026] - 获取aid结束: 116539978154171",
        );

        assert_eq!(metadata.id.as_deref(), Some("BV12TRrBcEP8"));
        assert_eq!(metadata.resolved_id.as_deref(), Some("BV12TRrBcEP8"));
        assert_eq!(metadata.aid.as_deref(), Some("116539978154171"));
    }

    #[test]
    fn preserves_bilibili_av_and_resolved_bv_identities() {
        let metadata = parse_bilibili_metadata(
            "https://www.bilibili.com/video/av1556453868/",
            "[2026] - 视频URL: https://www.bilibili.com/video/BV12TRrBcEP8/\n[2026] - 获取aid结束: 1556453868",
        );
        let mut identities = Vec::new();

        push_bilibili_metadata_identities(&mut identities, &metadata);

        assert_eq!(
            identities,
            vec![
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "av1556453868".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "BV12TRrBcEP8".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "1556453868".to_string(),
                },
            ]
        );
    }

    #[test]
    fn adds_bilibili_av_alias_from_resolved_aid() {
        let metadata = parse_bilibili_metadata(
            "https://b23.tv/Jt1mZiL",
            "[2026] - 视频URL: https://www.bilibili.com/video/BV12TRrBcEP8/\n[2026] - 获取aid结束: 1556453868",
        );
        let mut identities = Vec::new();

        push_bilibili_metadata_identities(&mut identities, &metadata);

        assert_eq!(
            identities,
            vec![
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "BV12TRrBcEP8".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "1556453868".to_string(),
                },
                VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: "av1556453868".to_string(),
                },
            ]
        );
    }

    #[test]
    fn finds_bilibili_duplicate_from_resolved_av_sidecar() {
        let mut config = test_config();
        let video_dir = temp_test_dir("duplicate-bilibili-av");
        fs::create_dir_all(&video_dir).expect("video dir should create");
        config.downloads.video_dir = video_dir.clone();
        let bilibili_path = video_dir.join("bilibili-title.mp4");
        fs::write(&bilibili_path, "video").expect("bilibili file should write");
        fs::write(
            bilibili_path.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili\">av1556453868</uniqueid></movie>",
        )
        .expect("nfo should write");
        let metadata = parse_bilibili_metadata(
            "https://b23.tv/Jt1mZiL",
            "[2026] - 视频URL: https://www.bilibili.com/video/BV12TRrBcEP8/\n[2026] - 获取aid结束: 1556453868",
        );
        let mut identities = Vec::new();
        push_bilibili_metadata_identities(&mut identities, &metadata);

        let duplicate = find_video_duplicate_for_identities(
            &config,
            &JobRequest::Bilibili {
                url: "https://b23.tv/Jt1mZiL".to_string(),
                selection: None,
            },
            identities,
        )
        .expect("duplicate scan should succeed")
        .expect("bilibili av duplicate should be found");

        assert_eq!(duplicate.identity.id, "av1556453868");
        assert_eq!(duplicate.existing_videos, vec![bilibili_path]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bilibili_metadata_probe_uses_controlled_command_timeout() {
        let mut config = test_config();
        let root = temp_test_dir("bilibili-probe-timeout");
        fs::create_dir_all(&root).expect("probe root should create");
        let fake_bbdown = root.join("fake-bbdown.sh");
        fs::write(&fake_bbdown, "#!/bin/sh\nsleep 30\n").expect("fake BBDown should write");
        let mut permissions = fs::metadata(&fake_bbdown)
            .expect("fake BBDown metadata should exist")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_bbdown, permissions).expect("fake BBDown should be executable");
        config.tools.bbdown = fake_bbdown;
        config.downloads.video_dir = root.clone();
        config.bilibili.auth.state_path = root.join("missing-auth.json");
        config.bot.command_timeout_seconds = 1;
        config.bot.command_idle_timeout_seconds = 30;

        let error = tokio_timeout(
            Duration::from_secs(8),
            probe_bilibili_metadata(
                &config,
                "https://b23.tv/Jt1mZiL",
                BILIBILI_METADATA_PROBE_TIMEOUT,
            ),
        )
        .await
        .expect("probe should return through run_command timeout")
        .expect_err("probe should fail on controlled timeout");

        let message = error.to_string();
        assert!(message.contains("timed out after 1s"), "{message}");
        assert!(!message.contains("metadata probe timed out"), "{message}");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn finds_bilibili_mux_candidates_in_aid_directory() {
        let root = temp_test_dir("mux-candidates");
        let aid_dir = root.join("1556453868");
        fs::create_dir_all(&aid_dir).expect("aid dir should be created");
        let since = SystemTime::now();
        let video = aid_dir.join("1556453868.P1.1625322228.mp4");
        fs::write(&video, b"video").expect("video should be written");
        fs::write(aid_dir.join("1556453868.P1.1625322228.m4a"), b"audio")
            .expect("audio should be written");
        let mut config = test_config();
        config.downloads.video_dir = root.clone();
        let metadata = BilibiliMetadata {
            aid: Some("1556453868".to_string()),
            ..BilibiliMetadata::default()
        };

        let candidates = bilibili_mux_candidates(&config, &metadata, Vec::new(), since, false)
            .expect("candidates should scan");

        assert_eq!(candidates, vec![video]);
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn finds_video_only_bilibili_candidates_in_aid_directory() {
        let root = temp_test_dir("video-only-mux-candidates");
        let aid_dir = root.join("1556453868");
        fs::create_dir_all(&aid_dir).expect("aid dir should be created");
        let since = SystemTime::now();
        let video = aid_dir.join("1556453868.P1.1625322228.mp4");
        fs::write(&video, b"video").expect("video should be written");
        let mut config = test_config();
        config.downloads.video_dir = root.clone();
        let metadata = BilibiliMetadata {
            aid: Some("1556453868".to_string()),
            ..BilibiliMetadata::default()
        };

        let candidates = bilibili_mux_candidates(&config, &metadata, Vec::new(), since, true)
            .expect("video-only candidates should scan");

        assert_eq!(candidates, vec![video]);
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn video_only_bilibili_cleanup_removes_json_danmaku_without_audio() {
        let root = temp_test_dir("video-only-json-danmaku-cleanup");
        let aid_dir = root.join("1556453868");
        fs::create_dir_all(&aid_dir).expect("aid dir should be created");
        let video = aid_dir.join("Part 1.mp4");
        fs::write(&video, b"video").expect("video should be written");
        fs::write(video.with_extension("xml"), b"xml").expect("xml should be written");
        fs::write(video.with_extension("json"), b"json").expect("json should be written");
        let custom_json = aid_dir.join("Custom Pattern.json");
        fs::write(&custom_json, b"custom-json").expect("custom json should be written");
        let mut config = test_config();
        config.downloads.video_dir = root.clone();

        let merged = merge_bilibili_streams(
            &config,
            std::slice::from_ref(&video),
            &BilibiliMetadata::default(),
            true,
            UNIX_EPOCH,
            None,
        )
        .await
        .expect("video-only stream should be processed without audio");

        assert_eq!(merged, vec![video.clone()]);
        assert!(video.with_extension("xml").exists());
        assert!(!video.with_extension("json").exists());
        assert!(custom_json.exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn video_only_bilibili_cleanup_moves_root_danmaku_without_audio() {
        let root = temp_test_dir("video-only-root-danmaku-cleanup");
        let aid_dir = root.join("1556453868");
        fs::create_dir_all(&aid_dir).expect("aid dir should be created");
        let video = aid_dir.join("Part 1.mp4");
        fs::write(&video, b"video").expect("video should be written");
        let root_xml = root.join("Custom Pattern.xml");
        let root_ass = root.join("Custom Pattern.ass");
        let root_json = root.join("Custom Pattern.json");
        let state_json = root.join("state.json");
        fs::write(&root_xml, b"root-xml").expect("root xml should be written");
        fs::write(&root_ass, b"root-ass").expect("root ass should be written");
        fs::write(&root_json, b"root-json").expect("root json should be written");
        fs::write(&state_json, b"state-json").expect("state json should be written");
        let mut config = test_config();
        config.downloads.video_dir = root.clone();

        let merged = merge_bilibili_streams(
            &config,
            std::slice::from_ref(&video),
            &BilibiliMetadata::default(),
            true,
            UNIX_EPOCH,
            None,
        )
        .await
        .expect("video-only stream should move root danmaku sidecars");

        assert_eq!(merged, vec![video.clone()]);
        assert_eq!(
            fs::read_to_string(video.with_extension("xml")).expect("video xml should exist"),
            "root-xml"
        );
        assert_eq!(
            fs::read_to_string(video.with_extension("ass")).expect("video ass should exist"),
            "root-ass"
        );
        assert!(!root_xml.exists());
        assert!(!root_ass.exists());
        assert!(!root_json.exists());
        assert_eq!(
            fs::read_to_string(state_json).expect("state json should remain"),
            "state-json"
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn renders_nfo_with_xml_escaping() {
        let nfo = render_nfo(
            "A & B",
            &MediaNfo {
                title: Some("A & B"),
                plot: Some("x < y"),
                unique_id_type: "youtube",
                unique_id: "id",
                alternate_unique_ids: vec![("youtube-alt", "alt & id")],
                source_url: "https://example.com/?a=1&b=2",
                studio: Some("Studio"),
                premiered: Some("2026-05-17"),
            },
        );

        assert!(nfo.contains("<title>A &amp; B</title>"));
        assert!(nfo.contains("<uniqueid type=\"youtube-alt\">alt &amp; id</uniqueid>"));
        assert!(nfo.contains("<plot>x &lt; y</plot>"));
        assert!(nfo.contains("<year>2026</year>"));
    }

    #[test]
    fn extracts_latest_terminal_percent() {
        assert_eq!(
            extract_last_percent("[-----]  12% \u{0008}\u{0008}[###--]  87%"),
            Some(87)
        );
        assert_eq!(
            extract_last_percent("[download] 42.3% of 1.00MiB"),
            Some(42)
        );
        assert_eq!(
            extract_last_percent("[download] 100.0% of 1.00MiB"),
            Some(100)
        );
        assert_eq!(extract_last_percent("no progress"), None);
    }

    #[test]
    fn summarizes_command_progress_percent() {
        assert_eq!(
            summarize_progress_chunk("BBDown", CommandStream::Stdout, "  42% | - 1.2 MB/s"),
            Some("BBDown: 42%".to_string())
        );
    }

    #[test]
    fn summarizes_short_command_lines() {
        assert_eq!(
            summarize_progress_chunk("BBDown", CommandStream::Stdout, "开始合并音视频...\n"),
            Some("BBDown stdout: 开始合并音视频...".to_string())
        );
    }

    #[test]
    fn redacts_bilibili_cookie_values_from_command_output() {
        let summary = summarize_output(
            "safe stdout\n--cookie SESSDATA=secret%2Cvalue; bili_jct=csrf; ac_time_value=token\n",
            "debug: SESSDATA=secret&bili_jct=csrf\nsafe stderr",
        );

        assert!(summary.contains("safe stdout"));
        assert!(summary.contains("safe stderr"));
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("csrf"));
        assert!(!summary.contains("token"));
        assert!(summary.contains("SESSDATA=<redacted>"));
        assert!(summary.contains("bili_jct=<redacted>"));
        assert!(summary.contains("--cookie <redacted Bilibili cookie>"));
    }

    #[test]
    fn redacts_bilibili_cookie_values_from_progress() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(
            CommandStream::Stdout,
            b"Debug: --cookie SESSDATA=secret; bili_jct=csrf; ac_time_value=token",
        );

        let message = rx.try_recv().expect("progress should be sent").message;
        assert!(!message.contains("secret"));
        assert!(!message.contains("csrf"));
        assert!(!message.contains("token"));
        assert!(message.contains("--cookie <redacted Bilibili cookie>"));
    }

    #[test]
    fn redacts_multiline_bilibili_cookie_flag_values() {
        let redacted = redact_sensitive_output(
            "config:\n--cookie\nSESSDATA=secret; bili_jct=csrf; ac_time_value=token\nsafe",
        );

        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("csrf"));
        assert!(!redacted.contains("token"));
        assert!(redacted.contains("--cookie\n<redacted Bilibili cookie>"));
        assert!(redacted.contains("safe"));
    }

    #[test]
    fn redacts_unknown_bilibili_cookie_pairs_from_cookie_lines() {
        let redacted = redact_sensitive_output(
            "Cookie: SESSDATA=secret; bili_jct=csrf; ac_time_value=token; unknown_cookie=value\nsafe",
        );

        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("csrf"));
        assert!(!redacted.contains("token"));
        assert!(!redacted.contains("value"));
        assert!(redacted.contains("<redacted Bilibili cookie line>"));
        assert!(redacted.contains("safe"));
    }

    #[test]
    fn redacts_standalone_bilibili_session_cookie_pairs() {
        let redacted = redact_sensitive_output("debug ac_time_value=token safe");

        assert!(!redacted.contains("token"));
        assert!(redacted.contains("ac_time_value=<redacted>"));
        assert!(redacted.contains("safe"));
    }

    #[test]
    fn formats_file_activity_bytes() {
        assert_eq!(human_bytes(42), "42 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(2 * 1024 * 1024), "2.0 MiB");
        assert_eq!(human_rate(0.0), "0 B/s");
        assert_eq!(human_rate(1_572_864.0), "1.5 MiB/s");
        assert_eq!(format_duration_compact(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration_compact(Duration::from_secs(65)), "1m 05s");
        assert_eq!(
            format_duration_compact(Duration::from_secs(3661)),
            "1h 01m 01s"
        );
    }

    #[test]
    fn keeps_file_activity_polling_ahead_of_idle_timeout() {
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(30), Duration::from_secs(300)),
            Duration::from_secs(30)
        );
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(600), Duration::from_secs(300)),
            Duration::from_secs(150)
        );
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(30), Duration::from_secs(1)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn throttles_percent_progress_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("yt-dlp".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, b"[download] 1.0%");
        let first = rx.try_recv().unwrap().message;
        assert!(first.contains("yt-dlp: downloading media"));
        assert!(first.contains("Done: resolve"));
        assert!(first.contains("Todo: metadata, embed, move"));
        assert!(first.contains("Last output: yt-dlp: 1%"));

        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        assert!(rx.try_recv().is_err());

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        let second = rx.try_recv().unwrap().message;
        assert!(second.contains("Last output: yt-dlp: 2%"));
    }

    #[test]
    fn tracks_youtube_metadata_as_resolving() {
        let config = test_config();
        let spec = youtube_metadata_command_spec(&config, "https://youtu.be/abc");
        let command_name = command_progress_name(&spec);
        assert_eq!(command_name, "yt-dlp metadata");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker = ProgressTracker::new(command_name, Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, b"[download] 1.0%");
        let first = rx.try_recv().unwrap().message;
        assert!(first.contains("yt-dlp metadata: resolving metadata"));
        assert!(first.contains("Done: -"));
        assert!(first.contains("Todo: download, embed, move"));
        assert!(!first.contains("downloading media"));
    }

    #[test]
    fn throttles_file_activity_progress_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.emit_file_activity(FileActivityReport {
            changed_file_count: 1,
            changed_size: 1024 * 1024,
            speed_bytes_per_second: 1024.0 * 1024.0,
            elapsed: Duration::from_secs(60),
            last_change_age: Some(Duration::ZERO),
            changed_since_previous_poll: true,
        });
        let first = rx.try_recv().unwrap().message;
        assert!(first.contains("BBDown: resolving metadata"));
        assert!(first.contains("Done: -"));
        assert!(first.contains("Todo: video, audio, mux, move"));
        assert!(first.contains("Files: 1 changed, 1.0 MiB written"));
        assert!(first.contains("Speed: 1.0 MiB/s"));
        assert!(first.contains("Elapsed: 1m 00s"));
        assert!(first.contains("Last file change: 0s ago"));

        tracker.emit_file_activity(FileActivityReport {
            changed_file_count: 1,
            changed_size: 2 * 1024 * 1024,
            speed_bytes_per_second: 1024.0 * 1024.0,
            elapsed: Duration::from_secs(61),
            last_change_age: Some(Duration::ZERO),
            changed_since_previous_poll: true,
        });
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn tracks_bbdown_stage_from_output() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, "开始下载P1视频\n".as_bytes());
        let first = rx.try_recv().unwrap().message;
        assert!(first.contains("BBDown: downloading video"));
        assert!(first.contains("Done: resolve"));
        assert!(first.contains("Todo: audio, mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "下载P1视频完毕\n".as_bytes());
        let video_done = rx.try_recv().unwrap().message;
        assert!(video_done.contains("BBDown: downloading video"));
        assert!(video_done.contains("Done: resolve"));
        assert!(video_done.contains("Todo: audio, mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "开始下载P1音频\n".as_bytes());
        let second = rx.try_recv().unwrap().message;
        assert!(second.contains("BBDown: downloading audio"));
        assert!(second.contains("Done: resolve, video"));
        assert!(second.contains("Todo: mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "任务完成\n".as_bytes());
        let third = rx.try_recv().unwrap().message;
        assert!(third.contains("BBDown: download complete"));
        assert!(third.contains("Done: resolve, video, audio"));
        assert!(third.contains("Todo: mux, move"));
    }

    #[tokio::test]
    async fn reports_only_contended_video_output_lock_waits() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let guard = video_output_lock("Bilibili download", Some(&tx)).await;
        assert!(rx.try_recv().is_err());
        drop(guard);

        let held_guard = VIDEO_OUTPUT_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .await;
        let waiter = tokio::spawn(async move {
            let _guard = video_output_lock("Bilibili download", Some(&tx)).await;
        });

        assert_eq!(
            rx.recv().await.expect("waiting progress should be sent"),
            JobProgress {
                message: "Bilibili download: waiting for video output slot".to_string()
            }
        );

        drop(held_guard);
        assert_eq!(
            rx.recv().await.expect("acquired progress should be sent"),
            JobProgress {
                message: "Bilibili download: video output slot acquired".to_string()
            }
        );
        waiter.await.expect("waiter should finish");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tracks_files_in_direct_subdirectories() {
        let root = temp_test_dir("file-activity");
        let existing = root.join("existing");
        let existing_aid = root.join("1556453868");
        fs::create_dir_all(&existing).expect("existing dir should be created");
        fs::create_dir_all(&existing_aid).expect("existing aid dir should be created");
        fs::write(existing.join("old.part"), b"old").expect("existing file should be written");
        fs::write(existing_aid.join("old.part"), b"old")
            .expect("existing aid file should be written");
        let mut tracker = FileActivityTracker::new(&root)
            .await
            .expect("tracker should initialize");

        fs::write(existing.join("old.part"), b"changed").expect("existing file should change");
        fs::write(existing_aid.join("old.part"), b"changed")
            .expect("existing aid file should change");
        let report = tracker.poll().await.expect("poll should work");
        assert_eq!(report.changed_file_count, 1);
        assert_eq!(report.changed_size, 7);
        assert!(report.changed_since_previous_poll);

        let created = root.join("created");
        fs::create_dir_all(&created).expect("new dir should be created");
        fs::write(created.join("new.part"), b"new bytes").expect("new file should be written");
        let report = tracker.poll().await.expect("poll should work");

        assert_eq!(report.changed_file_count, 2);
        assert_eq!(report.changed_size, 16);
        assert!(report.changed_since_previous_poll);

        let stable_report = tracker.poll().await.expect("poll should work");
        assert_eq!(stable_report.changed_file_count, 2);
        assert_eq!(stable_report.changed_size, 16);
        assert!(!stable_report.changed_since_previous_poll);
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_command_refreshes_file_activity_without_output() {
        let root = temp_test_dir("silent-file-activity");
        let output = root.join("out.part");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 10;
        config.bot.command_idle_timeout_seconds = 10;
        config.bot.progress_update_seconds = 1;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "printf data > \"$0\"; sleep 3".to_string(),
                output.display().to_string(),
            ],
            cwd: root.clone(),
            activity_dir: Some(root.clone()),
            cleanup_paths: Vec::new(),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();

        let result = tokio_timeout(
            Duration::from_secs(8),
            run_command(&config, &spec, Some(tx)),
        )
        .await
        .expect("silent file activity command should not hang")
        .expect("silent file activity command should succeed");

        assert!(result.stdout.is_empty());
        assert!(result.stderr.is_empty());
        let mut messages = Vec::new();
        while let Ok(progress) = rx.try_recv() {
            messages.push(progress.message);
        }
        let file_activity_messages = messages
            .iter()
            .filter(|message| message.contains("Files: 1 changed, 4 B written"))
            .count();
        assert!(
            file_activity_messages >= 2,
            "expected repeated fixed-interval file activity updates, got {messages:?}"
        );
        assert!(messages.iter().any(|message| message.contains("Speed:")));
        assert!(messages.iter().any(|message| message.contains("Elapsed:")));
        assert!(
            messages
                .iter()
                .any(|message| message.contains("Last file change:"))
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_terminates_descendant_processes() {
        let root = temp_test_dir("process-group");
        let pid_file = root.join("child.pid");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 2;
        config.bot.command_idle_timeout_seconds = 30;
        config.bot.progress_update_seconds = 1;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "sleep 30 & echo $! > \"$0\"; wait".to_string(),
                pid_file.display().to_string(),
            ],
            cwd: root.clone(),
            activity_dir: Some(root.clone()),
            cleanup_paths: Vec::new(),
        };

        let result = run_command(&config, &spec, None).await;

        assert!(result.is_err());
        let pid = fs::read_to_string(&pid_file)
            .expect("child pid should be written")
            .trim()
            .parse::<libc::pid_t>()
            .expect("child pid should parse");
        for _ in 0..20 {
            if !process_exists(pid) {
                let _ = fs::remove_dir_all(&root);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = fs::remove_dir_all(&root);
        panic!("descendant process {pid} survived command timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_child_exit_does_not_hang_on_background_pipe_holder() {
        let root = temp_test_dir("background-pipe");
        let pid_file = root.join("child.pid");
        let cleanup_file = root.join("command-secret.txt");
        fs::write(&cleanup_file, b"secret").expect("cleanup file should be written");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 30;
        config.bot.command_idle_timeout_seconds = 30;
        config.bot.progress_update_seconds = 1;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "sleep 30 & echo $! > \"$0\"; exit 0".to_string(),
                pid_file.display().to_string(),
            ],
            cwd: root.clone(),
            activity_dir: Some(root.clone()),
            cleanup_paths: vec![cleanup_file.clone()],
        };

        let result = tokio_timeout(Duration::from_secs(8), run_command(&config, &spec, None))
            .await
            .expect("run_command should not hang on inherited pipes");

        result.expect("direct child exit status should be successful");
        assert!(!cleanup_file.exists());
        let pid = fs::read_to_string(&pid_file)
            .expect("child pid should be written")
            .trim()
            .parse::<libc::pid_t>()
            .expect("child pid should parse");
        for _ in 0..20 {
            if !process_exists(pid) {
                let _ = fs::remove_dir_all(&root);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = fs::remove_dir_all(&root);
        panic!("background pipe holder {pid} survived command collection");
    }

    fn temp_test_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "telegram-video-downloader-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("test temp dir should be created");
        root
    }

    #[cfg(unix)]
    fn process_exists(pid: libc::pid_t) -> bool {
        (unsafe { libc::kill(pid, 0) == 0 })
            || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    #[test]
    fn tails_nonempty_lines() {
        assert_eq!(tail_lines("a\n\nb\nc\n", 2), "b\nc");
    }
}
