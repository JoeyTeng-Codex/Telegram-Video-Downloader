#![cfg_attr(not(test), allow(dead_code))]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use bbdown_core::{
    DownloadFileKind, DownloadMode, DownloadProgressEvent, DownloadProgressSink, DownloadReport,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio::time::{Instant, sleep, sleep_until, timeout as tokio_timeout};
use tracing::info;

use crate::bilibili_auth;
use crate::bilibili_core;
use crate::config::AppConfig;
use crate::router::{BilibiliSelection, JobRequest};
use crate::safe_fs::{
    BoundDirectory, BoundEntry, BoundFile, EntryIdentity, RootedFs, identity_for_open_file,
};

static VIDEO_OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
#[cfg(unix)]
static BILIBILI_WORKER_PROCESS: AtomicBool = AtomicBool::new(false);
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(1);
static OVERWRITE_BACKUP_COUNTER: AtomicU64 = AtomicU64::new(1);
const VIDEO_STAGING_DIR_NAME: &str = ".telegram-video-downloader-staging";
const VIDEO_STAGING_INITIALIZING_DIR_PREFIX: &str = ".initializing-job-";
const VIDEO_STAGING_OWNER_FILE_NAME: &str = ".owner.json";
const VIDEO_STAGING_OWNER_VERSION: u32 = 1;
const VIDEO_STAGING_OWNER_LIMIT: usize = 1024;
const VIDEO_STAGING_PUBLICATION_MANIFEST_NAME: &str = ".publication.json";
const VIDEO_STAGING_PUBLICATION_MANIFEST_VERSION: u32 = 1;
const VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT: usize = 512 * 1024;
const VIDEO_STAGING_PUBLICATION_MAX_STEPS: usize = 4096;
const VIDEO_STAGING_RETENTION_FILE_NAME: &str = ".retained.json";
const VIDEO_STAGING_RETENTION_VERSION: u32 = 1;
const VIDEO_STAGING_RETENTION_LIMIT: usize = 4096;
const VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON: &str =
    "download completed; automatic publication has not finished";
const BILIBILI_WORKER_REQUEST_FILE_NAME: &str = ".bilibili-worker.json";
const BILIBILI_WORKER_REQUEST_VERSION: u32 = 2;
const BILIBILI_WORKER_REQUEST_LIMIT: usize = 1024 * 1024;
const BILIBILI_STAGING_CONFIG_LIMIT: usize = 1024 * 1024;
const VIDEO_OUTPUT_LOCK_FILE_NAME: &str = ".telegram-video-downloader.lock";
const VIDEO_CONTROL_DIR_NAME: &str = ".telegram-video-downloader-control";
const VIDEO_CONTROL_INITIALIZING_DIR_PREFIX: &str =
    ".telegram-video-downloader-control.initializing";
const VIDEO_CONTROL_OWNER_FILE_NAME: &str = "owner.json";
const VIDEO_CONTROL_OWNER_VERSION: u32 = 1;
const VIDEO_CONTROL_OWNER_LIMIT: usize = 1024;
const LEGACY_VIDEO_RECOVERY_STATE_FILE_NAME: &str = ".telegram-video-downloader.recovery";
const VIDEO_RECOVERY_STATE_FILE_NAME: &str = "recovery-state";
const VIDEO_RECOVERY_STATE_TEMP_FILE_NAME: &str = "recovery-state.next";
const VIDEO_RECOVERY_STATE_CLEAN: u8 = b'C';
const VIDEO_RECOVERY_STATE_DIRTY: u8 = b'D';
const BILIBILI_FFMPEG_CONCAT_FILE_PREFIX: &str = ".telegram-video-downloader-ffmpeg-concat";
const BILIBILI_MUX_STAGING_DIR_PREFIX: &str = ".telegram-video-downloader-mux";
const BILIBILI_MUX_RECOVERY_MANIFEST_NAME: &str = "manifest.json";
const BILIBILI_MUX_RECOVERY_MANIFEST_TEMP_NAME: &str = "manifest.next.json";
const BILIBILI_MUX_RECOVERY_ANCHOR_NAME: &str = "output.anchor";
const BILIBILI_MUX_RECOVERY_MANIFEST_VERSION: u32 = 1;
const BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT: usize = 16 * 1024;
#[cfg(unix)]
const BILIBILI_MUX_FD_BASE: i32 = 64;
#[cfg(unix)]
const BILIBILI_WORKER_LIVENESS_FD: i32 = BILIBILI_MUX_FD_BASE - 1;
#[cfg(unix)]
const COMMAND_DESCENDANT_FENCE_FD: i32 = BILIBILI_MUX_FD_BASE - 2;
const OVERWRITE_BACKUP_DIR_PREFIX: &str = ".telegram-video-downloader-overwrite";
const OVERWRITE_RECOVERY_MANIFEST_NAME: &str = ".transaction.json";
const OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME: &str = ".transaction.next.json";
const OVERWRITE_COMMITTED_ANCHOR_PREFIX: &str = ".committed-output-";
const OVERWRITE_RECOVERY_MANIFEST_VERSION: u32 = 4;
const OVERWRITE_RECOVERY_LEGACY_MANIFEST_VERSION: u32 = 3;
const OVERWRITE_RECOVERY_OLDEST_MANIFEST_VERSION: u32 = 2;
const OVERWRITE_RECOVERY_MANIFEST_LIMIT: usize = 16 * 1024;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandProcessGroup {
    #[cfg(unix)]
    Owned(libc::pid_t),
    #[cfg(unix)]
    Inherited(libc::pid_t),
    None,
}

impl CommandProcessGroup {
    #[cfg(unix)]
    fn id(self) -> Option<libc::pid_t> {
        match self {
            Self::Owned(id) | Self::Inherited(id) => Some(id),
            Self::None => None,
        }
    }

    fn is_inherited(self) -> bool {
        #[cfg(unix)]
        {
            matches!(self, Self::Inherited(_))
        }

        #[cfg(not(unix))]
        {
            let _ = self;
            false
        }
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct AdditionalInheritedCommandFd {
    source: OwnedFd,
    target: i32,
}

#[cfg(not(unix))]
#[derive(Debug)]
struct AdditionalInheritedCommandFd;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandTotalDeadline {
    Configured,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandProcessGroupMode {
    Owned,
    Inherited,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandExecutionPolicy {
    total_deadline: CommandTotalDeadline,
    process_group: CommandProcessGroupMode,
    descendant_fence: bool,
}

impl CommandExecutionPolicy {
    const EXTERNAL: Self = Self {
        total_deadline: CommandTotalDeadline::Configured,
        process_group: CommandProcessGroupMode::Owned,
        descendant_fence: false,
    };

    const BILIBILI_WORKER: Self = Self {
        total_deadline: CommandTotalDeadline::Disabled,
        process_group: CommandProcessGroupMode::Owned,
        descendant_fence: false,
    };

    const BILIBILI_MUX: Self = Self {
        total_deadline: CommandTotalDeadline::Configured,
        process_group: CommandProcessGroupMode::Inherited,
        descendant_fence: true,
    };
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobReport {
    pub saved_location: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgress {
    pub message: String,
}

pub type JobProgressSender = watch::Sender<Option<JobProgress>>;
pub type JobProgressReceiver = watch::Receiver<Option<JobProgress>>;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BilibiliWorkerRequest {
    version: u32,
    config: AppConfig,
    url: String,
    selection: Option<BilibiliSelection>,
    expected_overwrite_identity: Option<VideoIdentity>,
    logical_output_dir: PathBuf,
    output_root_device: u64,
    output_root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
}

pub fn job_progress_channel() -> (JobProgressSender, JobProgressReceiver) {
    watch::channel(None)
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
    pub(crate) overwrite_confirmation: Option<VideoOverwriteConfirmation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VideoOverwriteConfirmation {
    root_identity: EntryIdentity,
    target_file: BoundFile,
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
        if self.overwrite_target().is_none() || self.overwrite_confirmation.is_none() {
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

    fn with_overwrite_confirmation(mut self, confirmation: VideoOverwriteConfirmation) -> Self {
        self.overwrite_confirmation = Some(confirmation);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct VideoIdentity {
    pub provider: VideoProvider,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
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
    #[serde(skip)]
    bound_output: Option<BoundFile>,
    #[serde(skip)]
    bound_inputs: Vec<BoundBilibiliMuxInput>,
    #[serde(skip)]
    recovery: Option<BilibiliMuxRecovery>,
}

#[derive(Debug, Clone)]
struct BoundBilibiliMuxInput {
    path: PathBuf,
    entry: BoundEntry,
    file: BoundFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BilibiliMuxRecoveryManifest {
    version: u32,
    phase: BilibiliMuxRecoveryPhase,
    parent_device: u64,
    parent_inode: u64,
    transaction_device: u64,
    transaction_inode: u64,
    staged_file_name: PathBuf,
    output_file_name: PathBuf,
    output_device: u64,
    output_inode: u64,
    inputs: Vec<BilibiliMuxRecoveryInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BilibiliMuxRecoveryPhase {
    Muxing,
    CleaningInputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BilibiliMuxRecoveryInput {
    file_name: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone)]
struct BilibiliMuxRecovery {
    directory: PathBuf,
    directory_entry: BoundEntry,
    directory_identity: EntryIdentity,
    manifest_path: PathBuf,
    manifest_entry: BoundEntry,
    manifest_identity: EntryIdentity,
    manifest: BilibiliMuxRecoveryManifest,
    anchor: BoundFile,
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
                        bound_output: None,
                        bound_inputs: Vec::new(),
                        recovery: None,
                    }),
                })
                .collect(),
        }
    }
}

pub async fn run_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<JobProgressSender>,
) -> Result<JobReport> {
    match job {
        JobRequest::Bilibili { .. } | JobRequest::Youtube { .. } => {
            run_video_job_staged_keep_both(config, job, progress).await
        }
        JobRequest::Pdf { .. } => run_simple_job(config, job, progress).await,
    }
}

pub async fn run_job_with_duplicate_action(
    config: &AppConfig,
    job: &JobRequest,
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    progress: Option<JobProgressSender>,
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
    progress: Option<JobProgressSender>,
) -> Result<JobReport> {
    let Some(identity) = fallback_video_identity(job) else {
        return run_simple_job(config, job, progress).await;
    };
    let duplicate = VideoDuplicate {
        overwrite_confirmation: None,
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
            Err(err) if should_propagate_bilibili_probe_error(*selection, &err) => {
                return Err(err).with_context(|| {
                    format!("failed to probe Bilibili plan for duplicate check: {url}")
                });
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
    root: Option<RootedFs>,
    file_identities: BTreeMap<PathBuf, EntryIdentity>,
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

    fn overwrite_confirmation(&self, video: &Path) -> Option<VideoOverwriteConfirmation> {
        let root = self.root.as_ref()?;
        let expected = *self.file_identities.get(video)?;
        let target_file = root.open_bound_file(video).ok().flatten()?;
        if target_file.identity() != expected {
            return None;
        }
        Some(VideoOverwriteConfirmation {
            root_identity: root.root_identity(),
            target_file,
        })
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
    let root = RootedFs::new(root)?;
    let inventory = build_identity_media_inventory(root.logical_root_path(), primary_media_kind)?;
    let mut index = VideoIdentityIndex {
        root: Some(root.clone()),
        ..VideoIdentityIndex::default()
    };
    for video in inventory.media_files {
        let Some(file_identity) = root.entry_identity(&video)? else {
            continue;
        };
        if !file_identity.is_file() {
            continue;
        }
        index.file_identities.insert(video.clone(), file_identity);
        let sidecars = inventory
            .metadata_sidecars
            .get(&video)
            .map(Vec::as_slice)
            .unwrap_or_default();
        index_video_identities(&mut index, &video, sidecars, read_policy)?;
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
        let existing_videos = exact_videos.into_iter().collect::<Vec<_>>();
        let overwrite_confirmation = existing_videos
            .first()
            .and_then(|video| index.overwrite_confirmation(video));
        return Some(VideoDuplicate {
            overwrite_confirmation,
            identity,
            existing_videos,
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

    matched_identity.map(|identity| {
        let overwrite_confirmation = (existing_videos.len() == 1)
            .then(|| index.overwrite_confirmation(&existing_videos[0]))
            .flatten();
        VideoDuplicate {
            overwrite_confirmation,
            identity,
            existing_videos,
        }
    })
}

fn duplicate_scan_video_dir(config: &AppConfig, _job: &JobRequest) -> PathBuf {
    config.downloads.video_dir.clone()
}

async fn run_simple_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<JobProgressSender>,
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

async fn run_staged_bilibili_worker(
    config: &AppConfig,
    staging: &BoundStagingDir,
    url: &str,
    selection: Option<BilibiliSelection>,
    expected_overwrite_identity: Option<&VideoIdentity>,
    progress: Option<JobProgressSender>,
) -> Result<JobReport> {
    let request =
        build_bilibili_worker_request(config, url, selection, expected_overwrite_identity, staging);
    let contents =
        serde_json::to_vec(&request).context("failed to encode Bilibili worker request")?;
    if contents.len() > BILIBILI_WORKER_REQUEST_LIMIT {
        bail!("Bilibili worker request exceeds its size limit");
    }
    let request_path = staging.path().join(BILIBILI_WORKER_REQUEST_FILE_NAME);
    let (_, request_identity) = staging
        .root
        .create_new_bound_file(&request_path, &contents, 0o600)
        .context("failed to persist Bilibili worker request")?;
    let request_file = staging
        .root
        .open_bound_file(&request_path)?
        .context("Bilibili worker request disappeared")?;
    if request_file.identity() != request_identity {
        bail!("Bilibili worker request identity changed after creation");
    }
    request_file.validate_private_single_link(0o600)?;
    let executable = std::env::current_exe().context("failed to resolve downloader executable")?;
    let spec = CommandSpec {
        program: executable,
        args: vec!["--bilibili-worker".to_string()],
        cwd: staging.path().to_path_buf(),
        activity_dir: Some(staging.path().to_path_buf()),
        cleanup_paths: Vec::new(),
    };
    #[cfg(unix)]
    let (additional_inherited_fds, _parent_liveness) = {
        let (worker_liveness, parent_liveness) =
            command_liveness_pair().context("failed to create Bilibili worker liveness channel")?;
        let inherited =
            prepare_additional_inherited_command_fd(&worker_liveness, BILIBILI_WORKER_LIVENESS_FD)?;
        (vec![inherited], parent_liveness)
    };
    #[cfg(not(unix))]
    let additional_inherited_fds = Vec::new();
    let output = run_command_with_bound_cwd_and_inherited_files_with_policy(
        config,
        &spec,
        &staging.directory,
        std::slice::from_ref(&request_file),
        additional_inherited_fds,
        progress,
        CommandExecutionPolicy::BILIBILI_WORKER,
    )
    .await?;
    staging.validate_for_path_access()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        bail!(
            "Bilibili worker exited with status {}\n{}",
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }
    let report = last_nonempty_line(&stdout).context("Bilibili worker returned no report")?;
    serde_json::from_str(report).context("failed to parse Bilibili worker report")
}

fn build_bilibili_worker_request(
    config: &AppConfig,
    url: &str,
    selection: Option<BilibiliSelection>,
    expected_overwrite_identity: Option<&VideoIdentity>,
    staging: &BoundStagingDir,
) -> BilibiliWorkerRequest {
    let mut worker_config = config.clone();
    worker_config.telegram.token = "redacted-worker-token".to_string();
    worker_config.telegram.allowed_chat_ids.clear();
    worker_config.telegram.allow_all_chats = true;
    worker_config.downloads.video_dir = PathBuf::from(".");
    worker_config.downloads.pdf_dir = PathBuf::from(".");
    BilibiliWorkerRequest {
        version: BILIBILI_WORKER_REQUEST_VERSION,
        config: worker_config,
        url: url.to_string(),
        selection,
        expected_overwrite_identity: expected_overwrite_identity.cloned(),
        logical_output_dir: staging.path().to_path_buf(),
        output_root_device: staging.root.root_identity().device(),
        output_root_inode: staging.root.root_identity().inode(),
        staging_device: staging.identity.device(),
        staging_inode: staging.identity.inode(),
    }
}

#[cfg(unix)]
fn validate_bilibili_worker_staging(
    root: &RootedFs,
    request: &BilibiliWorkerRequest,
) -> Result<()> {
    if root.root_identity().device() != request.staging_device
        || root.root_identity().inode() != request.staging_inode
        || !root.root_identity().is_dir()
    {
        bail!("Bilibili worker staging directory does not match its request");
    }
    let owner_path = root.logical_root_path().join(VIDEO_STAGING_OWNER_FILE_NAME);
    let owner = root
        .open_bound_file(&owner_path)?
        .context("Bilibili worker staging ownership record is missing")?;
    owner.validate_private_single_link(0o600)?;
    let actual: VideoStagingOwner =
        serde_json::from_slice(&owner.read_limited(VIDEO_STAGING_OWNER_LIMIT)?)
            .context("failed to parse Bilibili worker staging ownership record")?;
    if actual.version != VIDEO_STAGING_OWNER_VERSION
        || actual.root_device != request.output_root_device
        || actual.root_inode != request.output_root_inode
        || actual.staging_device != request.staging_device
        || actual.staging_inode != request.staging_inode
    {
        bail!("Bilibili worker staging ownership record does not match its request");
    }
    if root.entry_identity(&owner_path)? != Some(owner.identity()) {
        bail!("Bilibili worker staging ownership record changed during validation");
    }
    Ok(())
}

#[cfg(unix)]
fn persist_bilibili_worker_completion(
    root: &RootedFs,
    request: &BilibiliWorkerRequest,
) -> Result<()> {
    validate_bilibili_worker_staging(root, request)?;
    persist_video_staging_retention_marker(
        root,
        &root
            .logical_root_path()
            .join(VIDEO_STAGING_RETENTION_FILE_NAME),
        request.output_root_device,
        request.output_root_inode,
        request.staging_device,
        request.staging_inode,
        VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON,
    )
    .context("failed to persist Bilibili worker completion marker")
}

fn persist_bilibili_core_download_completion(
    root: &RootedFs,
    request: Option<&BilibiliWorkerRequest>,
) -> Result<()> {
    let Some(request) = request else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        persist_bilibili_worker_completion(root, request)
    }
    #[cfg(not(unix))]
    {
        let _ = (root, request);
        bail!("Bilibili worker completion markers require a Unix platform")
    }
}

#[cfg(unix)]
pub async fn run_bilibili_worker() -> Result<()> {
    ensure_current_process_owns_its_group()
        .context("Bilibili worker requires a dedicated process group")?;
    if BILIBILI_WORKER_PROCESS.swap(true, Ordering::AcqRel) {
        bail!("Bilibili worker mode was initialized more than once");
    }
    let liveness = inherited_worker_liveness_stream()?;
    let request_fd = unsafe { OwnedFd::from_raw_fd(BILIBILI_MUX_FD_BASE) };
    let mut reader = std::fs::File::from(request_fd)
        .take((BILIBILI_WORKER_REQUEST_LIMIT as u64).saturating_add(1));
    let mut contents = Vec::new();
    reader
        .read_to_end(&mut contents)
        .context("failed to read inherited Bilibili worker request")?;
    if contents.len() > BILIBILI_WORKER_REQUEST_LIMIT {
        bail!("inherited Bilibili worker request exceeds its size limit");
    }
    let request: BilibiliWorkerRequest =
        serde_json::from_slice(&contents).context("failed to parse Bilibili worker request")?;
    if request.version != BILIBILI_WORKER_REQUEST_VERSION
        || request.config.downloads.video_dir != Path::new(".")
    {
        bail!("invalid Bilibili worker request");
    }
    let root = RootedFs::new(Path::new("."))?;
    validate_bilibili_worker_staging(&root, &request)?;
    let (progress, mut progress_receiver) = job_progress_channel();
    let progress_writer = tokio::spawn(async move {
        while progress_receiver.changed().await.is_ok() {
            let current = progress_receiver.borrow_and_update().clone();
            if let Some(current) = current {
                println!("{}", current.message);
                let _ = std::io::stdout().flush();
            }
        }
    });
    let result = tokio::select! {
        result = run_bilibili_job_locked(
            &request.config,
            &root,
            &request.url,
            request.selection,
            request.expected_overwrite_identity.as_ref(),
            Some(&request),
            Some(progress),
        ) => result,
        liveness_result = wait_for_liveness_peer_close(liveness) => {
            let reason = match liveness_result {
                Ok(()) => "Bilibili worker parent exited",
                Err(_) => "Bilibili worker parent liveness check failed",
            };
            terminate_current_process_group(reason);
        }
    };
    let report = match result {
        Ok(report) => {
            persist_bilibili_worker_completion(&root, &request)?;
            report
        }
        Err(err) => {
            progress_writer
                .await
                .context("failed to join Bilibili worker progress writer")?;
            return Err(err);
        }
    };
    progress_writer
        .await
        .context("failed to join Bilibili worker progress writer")?;
    println!(
        "{}",
        serde_json::to_string(&report).context("failed to encode Bilibili worker report")?
    );
    std::io::stdout()
        .flush()
        .context("failed to flush Bilibili worker report")
}

#[cfg(not(unix))]
pub async fn run_bilibili_worker() -> Result<()> {
    bail!("Bilibili worker requires a Unix platform")
}

async fn run_bilibili_job_locked(
    config: &AppConfig,
    root: &RootedFs,
    url: &str,
    selection: Option<BilibiliSelection>,
    expected_overwrite_identity: Option<&VideoIdentity>,
    worker_request: Option<&BilibiliWorkerRequest>,
    progress: Option<JobProgressSender>,
) -> Result<JobReport> {
    sync_bilibili_rust_credentials(config).await?;
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
    if let Some(expected_identity) = expected_overwrite_identity {
        ensure_bilibili_overwrite_plan_matches(&plan, expected_identity)?;
    }
    let progress_reporter = BilibiliCoreProgress::new(progress.clone());
    let command_started_at = SystemTime::now();
    let core_report = client
        .download_plan_with_progress(&core_plan, options, &progress_reporter)
        .await?;
    let mut report = BilibiliDownloadReport::from(&core_report);
    persist_bilibili_core_download_completion(root, worker_request)?;
    let output_dir = bilibili_core::output_dir(config);
    if mux_locally {
        mux_bilibili_report_media(
            config,
            root,
            &output_dir,
            &mut report,
            command_started_at,
            progress.clone(),
        )
        .await?;
    }
    cleanup_bilibili_mux_input_files(root, &output_dir, &report)?;
    let primary_videos = bilibili_report_primary_media(&output_dir, &report);
    let reported_output_dir = worker_request
        .map(|request| request.logical_output_dir.as_path())
        .unwrap_or(output_dir.as_path());
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
                let reported_nfos = created_nfos
                    .iter()
                    .map(|path| rebase_download_path(path, &output_dir, reported_output_dir))
                    .collect::<Vec<_>>();
                details.push(format!("NFO: {}", join_paths(&reported_nfos)));
            }
            Ok(_) => {}
            Err(err) => details.push(format!("NFO skipped: {err}")),
        }
    }
    let reported_primary_videos = primary_videos
        .iter()
        .map(|path| rebase_download_path(path, &output_dir, reported_output_dir))
        .collect::<Vec<_>>();
    let fallback_output = rebase_download_path(
        &resolve_command_output_path(&output_dir, &report.output_dir),
        &output_dir,
        reported_output_dir,
    );

    Ok(JobReport {
        saved_location: if reported_primary_videos.is_empty() {
            fallback_output.display().to_string()
        } else if reported_primary_videos.len() == 1 {
            reported_primary_videos[0].display().to_string()
        } else {
            join_paths(&reported_primary_videos)
        },
        details: nonempty_join(details),
    })
}

#[derive(Clone)]
struct BilibiliCoreProgress {
    progress: Option<JobProgressSender>,
}

impl BilibiliCoreProgress {
    fn new(progress: Option<JobProgressSender>) -> Self {
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

fn rebase_download_path(path: &Path, access_root: &Path, logical_root: &Path) -> PathBuf {
    if let Ok(relative) = path.strip_prefix(access_root) {
        logical_root.join(relative)
    } else if path.is_relative() {
        logical_root.join(path)
    } else {
        path.to_path_buf()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BilibiliMediaInput {
    kind: String,
    path: PathBuf,
}

#[derive(Debug)]
struct OwnedTemporaryFile {
    root: RootedFs,
    entry: BoundEntry,
    identity: EntryIdentity,
    file: BoundFile,
}

impl OwnedTemporaryFile {
    fn path(&self) -> &Path {
        self.entry.path()
    }

    fn bound_file(&self) -> &BoundFile {
        &self.file
    }
}

#[derive(Debug)]
struct BilibiliMuxCommand {
    spec: CommandSpec,
    concat_file: Option<OwnedTemporaryFile>,
    inherited_files: Vec<BoundFile>,
}

impl Drop for OwnedTemporaryFile {
    fn drop(&mut self) {
        let _ = self
            .root
            .remove_bound_file_if_identity(&self.entry, self.identity);
    }
}

#[derive(Debug)]
struct ReservedMuxOutput {
    root: RootedFs,
    final_entry: BoundEntry,
    staging_dir_entry: BoundEntry,
    staging_dir_identity: EntryIdentity,
    staged_entry: BoundEntry,
    file: BoundFile,
    manifest_path: PathBuf,
    manifest_entry: BoundEntry,
    manifest_identity: EntryIdentity,
    manifest: BilibiliMuxRecoveryManifest,
    active: bool,
    published: bool,
}

impl ReservedMuxOutput {
    fn create(
        root: &RootedFs,
        final_path: &Path,
        inputs: &[BoundBilibiliMuxInput],
    ) -> Result<Self> {
        let entry_dir = final_path
            .parent()
            .context("Bilibili mux output has no parent")?;
        let final_entry = root.bind_entry(final_path, false)?;
        if root.bound_entry_identity(&final_entry)?.is_some() {
            bail!(
                "Bilibili mux output already exists: {}",
                final_path.display()
            );
        }
        let (staging_dir, staging_dir_entry, staging_dir_identity) =
            create_bilibili_mux_staging_dir(root, entry_dir)?;
        let mut staged_name = std::ffi::OsString::from("output");
        if let Some(extension) = final_path.extension() {
            staged_name.push(".");
            staged_name.push(extension);
        }
        let staged_path = staging_dir.join(staged_name);
        let (staged_entry, identity) = match root.create_new_bound_file(&staged_path, b"", 0o600) {
            Ok(created) => created,
            Err(err) => {
                let cleanup =
                    root.remove_bound_dir_if_identity(&staging_dir_entry, staging_dir_identity);
                let error = err.context(format!(
                    "failed to reserve private Bilibili mux output {}",
                    staged_path.display()
                ));
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(anyhow!(
                        "{error:#}; Bilibili mux staging cleanup failed: {cleanup:#}"
                    )),
                };
            }
        };
        let file = match root.open_bound_file(&staged_path) {
            Ok(Some(file)) if file.identity() == identity => file,
            Ok(Some(_)) => {
                return Err(with_reserved_mux_cleanup_error(
                    root,
                    &staging_dir_entry,
                    staging_dir_identity,
                    &staged_entry,
                    identity,
                    anyhow!(
                        "reserved Bilibili mux output identity changed: {}",
                        staged_path.display()
                    ),
                ));
            }
            Ok(None) => {
                return Err(with_reserved_mux_cleanup_error(
                    root,
                    &staging_dir_entry,
                    staging_dir_identity,
                    &staged_entry,
                    identity,
                    anyhow!(
                        "reserved Bilibili mux output disappeared: {}",
                        staged_path.display()
                    ),
                ));
            }
            Err(err) => {
                return Err(with_reserved_mux_cleanup_error(
                    root,
                    &staging_dir_entry,
                    staging_dir_identity,
                    &staged_entry,
                    identity,
                    err.context(format!(
                        "failed to bind reserved Bilibili mux output {}",
                        staged_path.display()
                    )),
                ));
            }
        };
        let (manifest_path, manifest_entry, manifest_identity, manifest) =
            match create_bilibili_mux_recovery_manifest(
                root,
                entry_dir,
                &staging_dir,
                staging_dir_identity,
                &staged_path,
                identity,
                final_path,
                inputs,
            ) {
                Ok(manifest) => manifest,
                Err(err) => {
                    return Err(with_reserved_mux_tree_cleanup_error(
                        root,
                        &staging_dir_entry,
                        staging_dir_identity,
                        err,
                    ));
                }
            };
        Ok(Self {
            root: root.clone(),
            final_entry,
            staging_dir_entry,
            staging_dir_identity,
            staged_entry,
            file,
            manifest_path,
            manifest_entry,
            manifest_identity,
            manifest,
            active: true,
            published: false,
        })
    }

    fn command_path(&self) -> &Path {
        self.staged_entry.path()
    }

    fn commit(mut self) -> Result<(BoundFile, BilibiliMuxRecovery)> {
        self.file
            .sync_all()
            .context("failed to persist private Bilibili mux output")?;
        if self.root.bound_entry_identity(&self.staged_entry)? != Some(self.file.identity()) {
            bail!(
                "reserved Bilibili mux output identity changed: {}",
                self.staged_entry.path().display()
            );
        }
        if self.file.byte_len()? == 0 {
            bail!(
                "Bilibili mux command did not write the reserved output: {}",
                self.staged_entry.path().display()
            );
        }
        self.root
            .rename_via_bound_parents_noreplace_if_identity(
                &self.staged_entry,
                &self.final_entry,
                self.file.identity(),
            )
            .with_context(|| {
                format!(
                    "failed to publish Bilibili mux output {}",
                    self.final_entry.path().display()
                )
            })?;
        self.published = true;
        let anchor_path = self
            .staging_dir_entry
            .path()
            .join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME);
        let anchor_entry = self.root.bind_entry(&anchor_path, false)?;
        self.root
            .hard_link_via_bound_parents_noreplace_if_identity(
                &self.final_entry,
                &anchor_entry,
                self.file.identity(),
            )
            .with_context(|| {
                format!(
                    "failed to anchor published Bilibili mux output {}",
                    self.final_entry.path().display()
                )
            })?;
        let anchor = self
            .root
            .open_bound_file(&anchor_path)?
            .context("published Bilibili mux output anchor is missing")?;
        if anchor.identity() != self.file.identity() {
            bail!("published Bilibili mux output anchor identity changed");
        }
        self.manifest.phase = BilibiliMuxRecoveryPhase::CleaningInputs;
        let (manifest_entry, manifest_identity) = replace_bilibili_mux_recovery_manifest(
            &self.root,
            &self.staging_dir_entry,
            self.staging_dir_identity,
            &self.manifest_path,
            &self.manifest_entry,
            self.manifest_identity,
            &self.manifest,
        )?;
        self.manifest_entry = manifest_entry;
        self.manifest_identity = manifest_identity;
        self.active = false;
        Ok((
            self.file.clone(),
            BilibiliMuxRecovery {
                directory: self.staging_dir_entry.path().to_path_buf(),
                directory_entry: self.staging_dir_entry.clone(),
                directory_identity: self.staging_dir_identity,
                manifest_path: self.manifest_path.clone(),
                manifest_entry: self.manifest_entry.clone(),
                manifest_identity: self.manifest_identity,
                manifest: self.manifest.clone(),
                anchor,
            },
        ))
    }
}

impl Drop for ReservedMuxOutput {
    fn drop(&mut self) {
        if self.active
            && !self.published
            && let Err(err) = self.root.remove_bound_tree_durably_if_identity(
                &self.staging_dir_entry,
                self.staging_dir_identity,
            )
        {
            info!(
                path = %self.staging_dir_entry.path().display(),
                error = %err,
                "failed to clean private Bilibili mux staging directory"
            );
        } else if self.active && self.published {
            info!(
                path = %self.staging_dir_entry.path().display(),
                "retained published Bilibili mux transaction for startup recovery"
            );
        }
    }
}

fn with_reserved_mux_cleanup_error(
    root: &RootedFs,
    staging_dir_entry: &BoundEntry,
    staging_dir_identity: EntryIdentity,
    staged_entry: &BoundEntry,
    identity: EntryIdentity,
    error: anyhow::Error,
) -> anyhow::Error {
    let staged_cleanup = root.remove_bound_file_if_identity(staged_entry, identity);
    let directory_cleanup = if staged_cleanup.is_ok() {
        root.remove_bound_dir_if_identity(staging_dir_entry, staging_dir_identity)
    } else {
        Ok(())
    };
    match (staged_cleanup, directory_cleanup) {
        (Ok(()), Ok(())) => error,
        (Err(cleanup), _) => anyhow!(
            "{error:#}; failed to clean reserved Bilibili mux output {}: {cleanup:#}",
            staged_entry.path().display()
        ),
        (Ok(()), Err(cleanup)) => anyhow!(
            "{error:#}; failed to clean Bilibili mux staging directory {}: {cleanup:#}",
            staging_dir_entry.path().display()
        ),
    }
}

fn with_reserved_mux_tree_cleanup_error(
    root: &RootedFs,
    staging_dir_entry: &BoundEntry,
    staging_dir_identity: EntryIdentity,
    error: anyhow::Error,
) -> anyhow::Error {
    match root.remove_bound_tree_durably_if_identity(staging_dir_entry, staging_dir_identity) {
        Ok(()) => error,
        Err(cleanup) => anyhow!(
            "{error:#}; failed to durably clean Bilibili mux transaction {}: {cleanup:#}",
            staging_dir_entry.path().display()
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn create_bilibili_mux_recovery_manifest(
    root: &RootedFs,
    parent: &Path,
    transaction: &Path,
    transaction_identity: EntryIdentity,
    staged_path: &Path,
    output_identity: EntryIdentity,
    output_path: &Path,
    inputs: &[BoundBilibiliMuxInput],
) -> Result<(
    PathBuf,
    BoundEntry,
    EntryIdentity,
    BilibiliMuxRecoveryManifest,
)> {
    let parent_identity = rooted_directory_identity(root, parent)?;
    if !parent_identity.is_dir() || transaction.parent() != Some(parent) {
        bail!("Bilibili mux transaction is outside its bound parent");
    }
    let staged_file_name = single_bilibili_mux_control_name(staged_path, transaction)?;
    let output_file_name = single_bilibili_mux_parent_name(output_path, parent)?;
    let mut recovery_inputs = Vec::with_capacity(inputs.len());
    let mut seen = BTreeSet::new();
    for input in inputs {
        input.file.validate_identity()?;
        if root.bound_entry_identity(&input.entry)? != Some(input.file.identity()) {
            bail!(
                "Bilibili mux input identity changed: {}",
                input.path.display()
            );
        }
        let file_name = single_bilibili_mux_parent_name(&input.path, parent)?;
        if !seen.insert(file_name.clone()) {
            bail!(
                "Bilibili mux recovery manifest repeats input {}",
                file_name.display()
            );
        }
        recovery_inputs.push(BilibiliMuxRecoveryInput {
            file_name,
            device: input.file.identity().device(),
            inode: input.file.identity().inode(),
        });
    }
    let manifest = BilibiliMuxRecoveryManifest {
        version: BILIBILI_MUX_RECOVERY_MANIFEST_VERSION,
        phase: BilibiliMuxRecoveryPhase::Muxing,
        parent_device: parent_identity.device(),
        parent_inode: parent_identity.inode(),
        transaction_device: transaction_identity.device(),
        transaction_inode: transaction_identity.inode(),
        staged_file_name,
        output_file_name,
        output_device: output_identity.device(),
        output_inode: output_identity.inode(),
        inputs: recovery_inputs,
    };
    let contents = serde_json::to_vec_pretty(&manifest)
        .context("failed to encode Bilibili mux recovery manifest")?;
    if contents.len() > BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT {
        bail!("Bilibili mux recovery manifest exceeds its size limit");
    }
    let manifest_path = transaction.join(BILIBILI_MUX_RECOVERY_MANIFEST_NAME);
    let (manifest_entry, manifest_identity) = root
        .create_new_bound_file(&manifest_path, &contents, 0o600)
        .context("failed to create Bilibili mux recovery manifest")?;
    let manifest_file = root
        .open_bound_file(&manifest_path)?
        .context("Bilibili mux recovery manifest disappeared after creation")?;
    if manifest_file.identity() != manifest_identity {
        bail!("Bilibili mux recovery manifest identity changed after creation");
    }
    manifest_file.validate_private_single_link(0o600)?;
    Ok((manifest_path, manifest_entry, manifest_identity, manifest))
}

fn replace_bilibili_mux_recovery_manifest(
    root: &RootedFs,
    transaction_entry: &BoundEntry,
    transaction_identity: EntryIdentity,
    manifest_path: &Path,
    manifest_entry: &BoundEntry,
    manifest_identity: EntryIdentity,
    manifest: &BilibiliMuxRecoveryManifest,
) -> Result<(BoundEntry, EntryIdentity)> {
    root.validate_private_bound_directory(transaction_entry, transaction_identity, 0o700)?;
    let transaction = transaction_entry.path();
    if manifest_path.parent() != Some(transaction) {
        bail!("Bilibili mux manifest is outside its transaction directory");
    }
    remove_valid_bilibili_mux_manifest_temp(root, transaction, transaction_identity)?;
    let contents = serde_json::to_vec_pretty(manifest)
        .context("failed to encode Bilibili mux recovery manifest")?;
    if contents.len() > BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT {
        bail!("Bilibili mux recovery manifest exceeds its size limit");
    }
    let temp_path = transaction.join(BILIBILI_MUX_RECOVERY_MANIFEST_TEMP_NAME);
    let (entry, identity) = root.replace_bound_file_atomically_if_identity(
        manifest_entry,
        manifest_identity,
        &temp_path,
        &contents,
        0o600,
    )?;
    let file = root
        .open_bound_file(manifest_path)?
        .context("Bilibili mux recovery manifest disappeared after replacement")?;
    if file.identity() != identity {
        bail!("Bilibili mux recovery manifest identity changed after replacement");
    }
    file.validate_private_single_link(0o600)?;
    root.validate_private_bound_directory(transaction_entry, transaction_identity, 0o700)?;
    Ok((entry, identity))
}

fn remove_valid_bilibili_mux_manifest_temp(
    root: &RootedFs,
    transaction: &Path,
    transaction_identity: EntryIdentity,
) -> Result<()> {
    let path = transaction.join(BILIBILI_MUX_RECOVERY_MANIFEST_TEMP_NAME);
    let Some(file) = root.open_bound_file(&path)? else {
        return Ok(());
    };
    file.validate_private_single_link(0o600)?;
    let manifest: BilibiliMuxRecoveryManifest =
        serde_json::from_slice(&file.read_limited(BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT)?)
            .context("failed to parse interrupted Bilibili mux manifest transition")?;
    if manifest.version != BILIBILI_MUX_RECOVERY_MANIFEST_VERSION
        || manifest.transaction_device != transaction_identity.device()
        || manifest.transaction_inode != transaction_identity.inode()
    {
        bail!(
            "refused to remove unrecognized Bilibili mux manifest transition {}",
            path.display()
        );
    }
    let entry = root.bind_entry(&path, false)?;
    root.remove_bound_file_if_identity(&entry, file.identity())
        .with_context(|| {
            format!(
                "failed to remove interrupted Bilibili mux manifest transition {}",
                path.display()
            )
        })
}

fn single_bilibili_mux_control_name(path: &Path, parent: &Path) -> Result<PathBuf> {
    if path.parent() != Some(parent) {
        bail!("Bilibili mux control path is outside its transaction directory");
    }
    let name = path
        .file_name()
        .context("Bilibili mux control path has no file name")?;
    Ok(PathBuf::from(name))
}

fn single_bilibili_mux_parent_name(path: &Path, parent: &Path) -> Result<PathBuf> {
    if path.parent() != Some(parent) {
        bail!(
            "Bilibili mux path is outside its transaction parent: {}",
            path.display()
        );
    }
    let name = PathBuf::from(
        path.file_name()
            .context("Bilibili mux path has no file name")?,
    );
    if is_bilibili_mux_control_name(&name) {
        bail!("Bilibili mux path conflicts with a recovery control name");
    }
    Ok(name)
}

fn is_bilibili_mux_control_name(name: &Path) -> bool {
    name == Path::new(BILIBILI_MUX_RECOVERY_MANIFEST_NAME)
        || name == Path::new(BILIBILI_MUX_RECOVERY_MANIFEST_TEMP_NAME)
        || name == Path::new(BILIBILI_MUX_RECOVERY_ANCHOR_NAME)
}

fn create_bilibili_mux_staging_dir(
    root: &RootedFs,
    entry_dir: &Path,
) -> Result<(PathBuf, BoundEntry, EntryIdentity)> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..128 {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = entry_dir.join(format!(
            "{BILIBILI_MUX_STAGING_DIR_PREFIX}-{}-{stamp:x}-{counter:x}",
            std::process::id()
        ));
        let Some(identity) = root.create_dir(&path, 0o700)? else {
            continue;
        };
        let entry = root.bind_entry(&path, false)?;
        if root.bound_entry_identity(&entry)? != Some(identity) {
            bail!(
                "Bilibili mux staging directory identity changed: {}",
                path.display()
            );
        }
        return Ok((path, entry, identity));
    }
    bail!("failed to allocate a private Bilibili mux staging directory")
}

async fn mux_bilibili_report_media(
    config: &AppConfig,
    root: &RootedFs,
    cwd: &Path,
    report: &mut BilibiliDownloadReport,
    since: SystemTime,
    progress: Option<JobProgressSender>,
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
        let bound_inputs = bind_bilibili_mux_inputs(root, &media_inputs)?;
        let output_reservation = ReservedMuxOutput::create(root, &output_path, &bound_inputs)?;
        let BilibiliMuxCommand {
            spec,
            concat_file,
            inherited_files,
        } = bilibili_local_mux_command_spec(
            config,
            root,
            &media_inputs,
            &bound_inputs,
            &entry_dir,
            output_reservation.command_path(),
        )?;
        let output_result = run_command_with_execution_context(
            config,
            &spec,
            None,
            &inherited_files,
            progress.clone(),
            CommandExecutionPolicy::BILIBILI_MUX,
        )
        .await;
        drop(concat_file);
        let output_result = match output_result {
            Ok(output_result) => output_result,
            Err(err) => return Err(err),
        };
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
        let (bound_output, recovery) = output_reservation.commit()?;
        entry.mux = Some(BilibiliMuxReport {
            output_path,
            bound_output: Some(bound_output),
            bound_inputs,
            recovery: Some(recovery),
        });
    }
    Ok(())
}

fn bind_bilibili_mux_inputs(
    root: &RootedFs,
    media_inputs: &[BilibiliMediaInput],
) -> Result<Vec<BoundBilibiliMuxInput>> {
    media_inputs
        .iter()
        .map(|input| {
            let entry = root.bind_entry(&input.path, false)?;
            let file = root.open_bound_file(&input.path)?.with_context(|| {
                format!("Bilibili mux input is missing: {}", input.path.display())
            })?;
            Ok(BoundBilibiliMuxInput {
                path: input.path.clone(),
                entry,
                file,
            })
        })
        .collect()
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
    root: &RootedFs,
    media_inputs: &[BilibiliMediaInput],
    bound_inputs: &[BoundBilibiliMuxInput],
    entry_dir: &Path,
    output: &Path,
) -> Result<BilibiliMuxCommand> {
    if media_inputs.len() != bound_inputs.len()
        || media_inputs
            .iter()
            .zip(bound_inputs)
            .any(|(media, bound)| media.path != bound.path)
    {
        bail!("Bilibili mux inputs do not match their bound file descriptors");
    }
    let mut args = vec![
        "-hide_banner".to_string(),
        "-y".to_string(),
        "-nostdin".to_string(),
    ];
    let (concat_file, inherited_files) = if only_bilibili_flv_segments(media_inputs) {
        let concat_file = create_bilibili_concat_file(
            root,
            entry_dir,
            ffmpeg_concat_file_list(media_inputs.len(), 1)?.as_bytes(),
        )?;
        args.extend([
            "-f".to_string(),
            "concat".to_string(),
            "-safe".to_string(),
            "0".to_string(),
            "-i".to_string(),
            inherited_command_path(0)?,
        ]);
        let mut inherited_files = Vec::with_capacity(bound_inputs.len() + 1);
        inherited_files.push(concat_file.bound_file().clone());
        inherited_files.extend(bound_inputs.iter().map(|input| input.file.clone()));
        (Some(concat_file), inherited_files)
    } else {
        for index in 0..media_inputs.len() {
            args.push("-i".to_string());
            args.push(inherited_command_path(index)?);
        }
        for index in 0..media_inputs.len() {
            args.push("-map".to_string());
            args.push(format!("{index}:0"));
        }
        (
            None,
            bound_inputs
                .iter()
                .map(|input| input.file.clone())
                .collect(),
        )
    };
    args.extend([
        "-c".to_string(),
        "copy".to_string(),
        "-movflags".to_string(),
        "+faststart".to_string(),
        command_path_arg(output),
    ]);
    Ok(BilibiliMuxCommand {
        spec: CommandSpec {
            program: config.tools.ffmpeg.clone(),
            args,
            cwd: entry_dir.to_path_buf(),
            activity_dir: Some(entry_dir.to_path_buf()),
            cleanup_paths: Vec::new(),
        },
        concat_file,
        inherited_files,
    })
}

fn create_bilibili_concat_file(
    root: &RootedFs,
    entry_dir: &Path,
    contents: &[u8],
) -> Result<OwnedTemporaryFile> {
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
        root.validate_configured_root()?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        }
        let mut file = match options.open(&path) {
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
        let identity = identity_for_open_file(&file)?;
        let entry = root.bind_entry(&path, false)?;
        if root.bound_entry_identity(&entry)? != Some(identity) {
            bail!(
                "Bilibili ffmpeg concat list identity changed while binding: {}",
                path.display()
            );
        }
        if let Err(err) = file.write_all(contents).and_then(|()| file.sync_all()) {
            drop(file);
            let error = anyhow!(err).context(format!(
                "failed to write Bilibili ffmpeg concat list {}",
                path.display()
            ));
            return match root.remove_bound_file_if_identity(&entry, identity) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(anyhow!(
                    "{error:#}; failed to clean Bilibili ffmpeg concat list: {cleanup:#}"
                )),
            };
        }
        let bound_file = match root.open_bound_file(&path).and_then(|file| {
            file.with_context(|| {
                format!(
                    "Bilibili ffmpeg concat list disappeared after creation: {}",
                    path.display()
                )
            })
        }) {
            Ok(bound_file) => bound_file,
            Err(error) => {
                drop(file);
                return match root.remove_bound_file_if_identity(&entry, identity) {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(anyhow!(
                        "{error:#}; failed to clean Bilibili ffmpeg concat list: {cleanup:#}"
                    )),
                };
            }
        };
        if bound_file.identity() != identity || root.bound_entry_identity(&entry)? != Some(identity)
        {
            drop(file);
            let error = anyhow!(
                "Bilibili ffmpeg concat list identity changed after creation: {}",
                path.display()
            );
            return match root.remove_bound_file_if_identity(&entry, identity) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(anyhow!(
                    "{error:#}; failed to clean Bilibili ffmpeg concat list: {cleanup:#}"
                )),
            };
        }
        drop(file);
        return Ok(OwnedTemporaryFile {
            root: root.clone(),
            entry,
            identity,
            file: bound_file,
        });
    }
    bail!("failed to allocate a unique Bilibili ffmpeg concat list")
}

fn only_bilibili_flv_segments(media_inputs: &[BilibiliMediaInput]) -> bool {
    !media_inputs.is_empty() && media_inputs.iter().all(|input| input.kind == "flv_segment")
}

fn ffmpeg_concat_file_list(input_count: usize, inherited_offset: usize) -> Result<String> {
    (0..input_count)
        .map(|index| {
            inherited_command_path(inherited_offset + index).map(|path| format!("file '{path}'\n"))
        })
        .collect()
}

#[cfg(unix)]
fn inherited_command_path(index: usize) -> Result<String> {
    let index = i32::try_from(index).context("too many inherited Bilibili mux inputs")?;
    let descriptor = BILIBILI_MUX_FD_BASE
        .checked_add(index)
        .context("too many inherited Bilibili mux inputs")?;
    Ok(format!("/dev/fd/{descriptor}"))
}

#[cfg(not(unix))]
fn inherited_command_path(_index: usize) -> Result<String> {
    bail!("descriptor-bound Bilibili muxing requires a Unix platform")
}

fn cleanup_bilibili_mux_input_files(
    root: &RootedFs,
    _cwd: &Path,
    report: &BilibiliDownloadReport,
) -> Result<()> {
    let muxes = report
        .entries
        .iter()
        .filter_map(|entry| entry.mux.as_ref())
        .filter(|mux| mux.bound_output.is_some() && mux.recovery.is_some())
        .collect::<Vec<_>>();
    for mux in &muxes {
        validate_bilibili_mux_recovery(root, mux)?;
        for input in &mux.bound_inputs {
            input.file.validate_identity()?;
            if root.bound_entry_identity(&input.entry)? != Some(input.file.identity()) {
                bail!(
                    "Bilibili mux input identity changed: {}",
                    input.path.display()
                );
            }
        }
    }

    for mux in &muxes {
        for input in &mux.bound_inputs {
            for current in &muxes {
                validate_bilibili_mux_recovery(root, current)?;
            }
            input.file.validate_identity()?;
            root.remove_bound_file_if_identity(&input.entry, input.file.identity())
                .with_context(|| {
                    format!(
                        "failed to remove raw Bilibili input {}",
                        input.path.display()
                    )
                })?;
        }
    }
    for mux in &muxes {
        validate_bilibili_mux_recovery(root, mux)?;
    }
    for mux in &muxes {
        finalize_bilibili_mux_recovery(root, mux)?;
    }
    Ok(())
}

fn validate_bilibili_mux_output(root: &RootedFs, mux: &BilibiliMuxReport) -> Result<()> {
    let output = mux
        .bound_output
        .as_ref()
        .context("Bilibili mux output was not bound before raw-input cleanup")?;
    output.validate_identity()?;
    if root.entry_identity(&mux.output_path)? != Some(output.identity()) {
        bail!(
            "Bilibili mux output identity changed before raw-input cleanup: {}",
            mux.output_path.display()
        );
    }
    Ok(())
}

fn validate_bilibili_mux_recovery(root: &RootedFs, mux: &BilibiliMuxReport) -> Result<()> {
    validate_bilibili_mux_output(root, mux)?;
    let output = mux
        .bound_output
        .as_ref()
        .context("Bilibili mux output is missing during recovery validation")?;
    let recovery = mux
        .recovery
        .as_ref()
        .context("Bilibili mux recovery transaction is missing")?;
    root.validate_private_bound_directory(
        &recovery.directory_entry,
        recovery.directory_identity,
        0o700,
    )?;
    if recovery.directory != *recovery.directory_entry.path()
        || recovery.manifest_path.parent() != Some(recovery.directory.as_path())
        || root.bound_entry_identity(&recovery.manifest_entry)? != Some(recovery.manifest_identity)
    {
        bail!("Bilibili mux recovery paths or identities changed");
    }
    let manifest_file = root
        .open_bound_file(&recovery.manifest_path)?
        .context("Bilibili mux recovery manifest is missing")?;
    if manifest_file.identity() != recovery.manifest_identity {
        bail!("Bilibili mux recovery manifest identity changed");
    }
    manifest_file.validate_private_single_link(0o600)?;
    let persisted: BilibiliMuxRecoveryManifest =
        serde_json::from_slice(&manifest_file.read_limited(BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT)?)
            .context("failed to parse Bilibili mux recovery manifest")?;
    if persisted != recovery.manifest
        || persisted.version != BILIBILI_MUX_RECOVERY_MANIFEST_VERSION
        || persisted.phase != BilibiliMuxRecoveryPhase::CleaningInputs
        || persisted.transaction_device != recovery.directory_identity.device()
        || persisted.transaction_inode != recovery.directory_identity.inode()
    {
        bail!("Bilibili mux recovery manifest changed");
    }
    let parent = recovery
        .directory
        .parent()
        .context("Bilibili mux recovery directory has no parent")?;
    let parent_identity = rooted_directory_identity(root, parent)?;
    if persisted.parent_device != parent_identity.device()
        || persisted.parent_inode != parent_identity.inode()
        || parent.join(&persisted.output_file_name) != mux.output_path
        || persisted.output_device != output.identity().device()
        || persisted.output_inode != output.identity().inode()
    {
        bail!("Bilibili mux recovery manifest does not bind the published output");
    }
    recovery.anchor.validate_identity()?;
    if recovery.anchor.identity() != output.identity()
        || root.entry_identity(&recovery.directory.join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME))?
            != Some(output.identity())
    {
        bail!("Bilibili mux output anchor changed");
    }
    let actual_inputs = mux
        .bound_inputs
        .iter()
        .map(|input| {
            Ok((
                single_bilibili_mux_parent_name(&input.path, parent)?,
                input.file.identity(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    if actual_inputs.len() != persisted.inputs.len()
        || persisted.inputs.iter().any(|input| {
            actual_inputs.get(&input.file_name).is_none_or(|identity| {
                identity.device() != input.device || identity.inode() != input.inode
            })
        })
    {
        bail!("Bilibili mux recovery input set changed");
    }
    Ok(())
}

fn finalize_bilibili_mux_recovery(root: &RootedFs, mux: &BilibiliMuxReport) -> Result<()> {
    validate_bilibili_mux_recovery(root, mux)?;
    let recovery = mux
        .recovery
        .as_ref()
        .context("Bilibili mux recovery transaction is missing")?;
    let expected = BTreeSet::from([
        std::ffi::OsString::from(BILIBILI_MUX_RECOVERY_MANIFEST_NAME),
        std::ffi::OsString::from(BILIBILI_MUX_RECOVERY_ANCHOR_NAME),
    ]);
    let actual = root
        .list_bound_directory(&recovery.directory_entry, recovery.directory_identity)?
        .into_iter()
        .map(|(name, _)| name)
        .collect::<BTreeSet<_>>();
    if actual != expected {
        bail!(
            "refused to finalize Bilibili mux transaction with unexpected entries in {}",
            recovery.directory.display()
        );
    }
    root.remove_bound_tree_durably_if_identity(
        &recovery.directory_entry,
        recovery.directory_identity,
    )
    .with_context(|| {
        format!(
            "failed to durably finalize Bilibili mux transaction {}",
            recovery.directory.display()
        )
    })
}

#[derive(Debug, Default)]
struct BilibiliMuxRecoveryReport {
    messages: Vec<String>,
    unresolved: bool,
}

#[derive(Debug)]
struct PendingBilibiliMuxRecovery {
    directory: PathBuf,
    directory_entry: BoundEntry,
    directory_identity: EntryIdentity,
    manifest_path: PathBuf,
    manifest_entry: BoundEntry,
    manifest_identity: EntryIdentity,
    manifest: BilibiliMuxRecoveryManifest,
}

fn recover_pending_bilibili_mux_transactions_locked(
    root: &RootedFs,
    video_dir: &Path,
) -> Result<BilibiliMuxRecoveryReport> {
    let mut directories = Vec::new();
    let mut report = BilibiliMuxRecoveryReport::default();
    collect_bilibili_mux_recovery_directories(
        root,
        video_dir,
        video_dir,
        &mut directories,
        &mut report.messages,
    )?;
    report.unresolved = !report.messages.is_empty();
    directories.sort();
    for directory in directories {
        match recover_bilibili_mux_transaction(root, &directory) {
            Ok(message) => report.messages.push(message),
            Err(err) => {
                report.unresolved = true;
                report.messages.push(format!(
                    "Retained unresolved Bilibili mux transaction {}: {err:#}",
                    directory.display()
                ));
            }
        }
    }
    Ok(report)
}

fn collect_bilibili_mux_recovery_directories(
    root: &RootedFs,
    scan_root: &Path,
    directory: &Path,
    recovered: &mut Vec<PathBuf>,
    issues: &mut Vec<String>,
) -> Result<()> {
    root.validate_configured_root()?;
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(err) if directory != scan_root => {
            issues.push(format!(
                "Skipped unreadable directory during Bilibili mux recovery scan {}: {err}",
                directory.display()
            ));
            return Ok(());
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to scan Bilibili mux recovery root {}",
                    directory.display()
                )
            });
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                issues.push(format!(
                    "Skipped unreadable entry during Bilibili mux recovery scan {}: {err}",
                    directory.display()
                ));
                continue;
            }
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) => {
                issues.push(format!(
                    "Skipped uninspectable entry during Bilibili mux recovery scan {}: {err}",
                    entry.path().display()
                ));
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(BILIBILI_MUX_STAGING_DIR_PREFIX) {
            recovered.push(path);
            continue;
        }
        if name == VIDEO_STAGING_DIR_NAME
            || name == VIDEO_CONTROL_DIR_NAME
            || name.starts_with(OVERWRITE_BACKUP_DIR_PREFIX)
        {
            continue;
        }
        collect_bilibili_mux_recovery_directories(root, scan_root, &path, recovered, issues)?;
    }
    root.validate_configured_root()
}

fn recover_bilibili_mux_transaction(root: &RootedFs, directory: &Path) -> Result<String> {
    let mut pending = load_bilibili_mux_recovery(root, directory)?;
    remove_valid_bilibili_mux_manifest_temp(root, directory, pending.directory_identity)?;
    let parent = directory
        .parent()
        .context("Bilibili mux recovery directory has no parent")?;
    let staged_name =
        bilibili_mux_manifest_name(&pending.manifest.staged_file_name, "staged output")?;
    let staged_path = directory.join(&staged_name);
    let staged_identity = root.entry_identity(&staged_path)?;
    let staged_matches = staged_identity.is_some_and(|identity| {
        identity.is_file()
            && identity.device() == pending.manifest.output_device
            && identity.inode() == pending.manifest.output_inode
    });

    if pending.manifest.phase == BilibiliMuxRecoveryPhase::Muxing && staged_matches {
        let output_name =
            bilibili_mux_manifest_name(&pending.manifest.output_file_name, "published output")?;
        if root.entry_identity(&parent.join(output_name))?.is_some() {
            bail!("both staged and published Bilibili mux outputs exist");
        }
        let expected = BTreeSet::from([
            std::ffi::OsString::from(BILIBILI_MUX_RECOVERY_MANIFEST_NAME),
            staged_name,
        ]);
        require_bilibili_mux_transaction_entries(root, &pending, &expected)?;
        root.remove_bound_tree_durably_if_identity(
            &pending.directory_entry,
            pending.directory_identity,
        )?;
        return Ok(format!(
            "Discarded interrupted Bilibili mux staging: {}",
            directory.display()
        ));
    }
    if staged_identity.is_some() {
        bail!("Bilibili mux staged output identity changed");
    }

    let (output_path, output, anchor) = ensure_bilibili_mux_output_anchor(root, &pending)?;
    if pending.manifest.phase == BilibiliMuxRecoveryPhase::Muxing {
        pending.manifest.phase = BilibiliMuxRecoveryPhase::CleaningInputs;
        let (entry, identity) = replace_bilibili_mux_recovery_manifest(
            root,
            &pending.directory_entry,
            pending.directory_identity,
            &pending.manifest_path,
            &pending.manifest_entry,
            pending.manifest_identity,
            &pending.manifest,
        )?;
        pending.manifest_entry = entry;
        pending.manifest_identity = identity;
    }

    for input in &pending.manifest.inputs {
        validate_recovered_bilibili_mux_output(root, &output_path, &output, &pending, &anchor)?;
        let input_name = bilibili_mux_manifest_name(&input.file_name, "raw input")?;
        if input_name == pending.manifest.output_file_name {
            bail!("Bilibili mux recovery input conflicts with its output");
        }
        let input_path = parent.join(input_name);
        let Some(current) = root.entry_identity(&input_path)? else {
            continue;
        };
        if !current.is_file() || current.device() != input.device || current.inode() != input.inode
        {
            bail!(
                "Bilibili mux raw input identity changed: {}",
                input_path.display()
            );
        }
        let input_entry = root.bind_entry(&input_path, false)?;
        root.remove_bound_file_if_identity(&input_entry, current)
            .with_context(|| {
                format!(
                    "failed to finalize recovered Bilibili mux input {}",
                    input_path.display()
                )
            })?;
    }
    validate_recovered_bilibili_mux_output(root, &output_path, &output, &pending, &anchor)?;
    let expected = BTreeSet::from([
        std::ffi::OsString::from(BILIBILI_MUX_RECOVERY_MANIFEST_NAME),
        std::ffi::OsString::from(BILIBILI_MUX_RECOVERY_ANCHOR_NAME),
    ]);
    require_bilibili_mux_transaction_entries(root, &pending, &expected)?;
    root.remove_bound_tree_durably_if_identity(
        &pending.directory_entry,
        pending.directory_identity,
    )?;
    Ok(format!(
        "Recovered Bilibili mux transaction: {}",
        output_path.display()
    ))
}

fn load_bilibili_mux_recovery(
    root: &RootedFs,
    directory: &Path,
) -> Result<PendingBilibiliMuxRecovery> {
    let directory_entry = root.bind_entry(directory, false)?;
    let directory_identity = root
        .bound_entry_identity(&directory_entry)?
        .context("Bilibili mux recovery directory is missing")?;
    root.validate_private_bound_directory(&directory_entry, directory_identity, 0o700)?;
    let manifest_path = directory.join(BILIBILI_MUX_RECOVERY_MANIFEST_NAME);
    let manifest_file = root
        .open_bound_file(&manifest_path)?
        .context("Bilibili mux recovery manifest is missing")?;
    manifest_file.validate_private_single_link(0o600)?;
    let manifest_identity = manifest_file.identity();
    let manifest_entry = root.bind_entry(&manifest_path, false)?;
    if root.bound_entry_identity(&manifest_entry)? != Some(manifest_identity) {
        bail!("Bilibili mux recovery manifest identity changed");
    }
    let manifest: BilibiliMuxRecoveryManifest =
        serde_json::from_slice(&manifest_file.read_limited(BILIBILI_MUX_RECOVERY_MANIFEST_LIMIT)?)
            .context("failed to parse Bilibili mux recovery manifest")?;
    let parent = directory
        .parent()
        .context("Bilibili mux recovery directory has no parent")?;
    let parent_identity = rooted_directory_identity(root, parent)?;
    if manifest.version != BILIBILI_MUX_RECOVERY_MANIFEST_VERSION
        || manifest.transaction_device != directory_identity.device()
        || manifest.transaction_inode != directory_identity.inode()
        || manifest.parent_device != parent_identity.device()
        || manifest.parent_inode != parent_identity.inode()
    {
        bail!("Bilibili mux recovery manifest does not describe this transaction");
    }
    let output_name = bilibili_mux_manifest_name(&manifest.output_file_name, "published output")?;
    let staged_name = bilibili_mux_manifest_name(&manifest.staged_file_name, "staged output")?;
    if is_bilibili_mux_control_name(Path::new(&output_name))
        || is_bilibili_mux_control_name(Path::new(&staged_name))
    {
        bail!("Bilibili mux recovery manifest conflicts with a control name");
    }
    let mut inputs = BTreeSet::new();
    for input in &manifest.inputs {
        let name = bilibili_mux_manifest_name(&input.file_name, "raw input")?;
        if is_bilibili_mux_control_name(Path::new(&name)) || !inputs.insert(name) {
            bail!("Bilibili mux recovery manifest has an invalid input set");
        }
    }
    Ok(PendingBilibiliMuxRecovery {
        directory: directory.to_path_buf(),
        directory_entry,
        directory_identity,
        manifest_path,
        manifest_entry,
        manifest_identity,
        manifest,
    })
}

fn ensure_bilibili_mux_output_anchor(
    root: &RootedFs,
    pending: &PendingBilibiliMuxRecovery,
) -> Result<(PathBuf, BoundFile, BoundFile)> {
    let parent = pending
        .directory
        .parent()
        .context("Bilibili mux recovery directory has no parent")?;
    let output_name =
        bilibili_mux_manifest_name(&pending.manifest.output_file_name, "published output")?;
    let output_path = parent.join(output_name);
    let anchor_path = pending.directory.join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME);
    let output_entry = root.bind_entry(&output_path, false)?;
    let anchor_entry = root.bind_entry(&anchor_path, false)?;
    let output_identity = root.bound_entry_identity(&output_entry)?;
    let anchor_identity = root.bound_entry_identity(&anchor_entry)?;
    for (label, identity) in [("output", output_identity), ("anchor", anchor_identity)] {
        if let Some(identity) = identity
            && (!identity.is_file()
                || identity.device() != pending.manifest.output_device
                || identity.inode() != pending.manifest.output_inode)
        {
            bail!("Bilibili mux recovered {label} identity changed");
        }
    }
    match (output_identity, anchor_identity) {
        (Some(_), None) => root.hard_link_via_bound_parents_noreplace_if_identity(
            &output_entry,
            &anchor_entry,
            output_identity.expect("output identity is present"),
        )?,
        (None, Some(_)) => root.hard_link_via_bound_parents_noreplace_if_identity(
            &anchor_entry,
            &output_entry,
            anchor_identity.expect("anchor identity is present"),
        )?,
        (Some(_), Some(_)) => {}
        (None, None) => bail!("Bilibili mux output and durable anchor are both missing"),
    }
    let output = root
        .open_bound_file(&output_path)?
        .context("recovered Bilibili mux output is missing")?;
    let anchor = root
        .open_bound_file(&anchor_path)?
        .context("recovered Bilibili mux anchor is missing")?;
    validate_recovered_bilibili_mux_output(root, &output_path, &output, pending, &anchor)?;
    Ok((output_path, output, anchor))
}

fn validate_recovered_bilibili_mux_output(
    root: &RootedFs,
    output_path: &Path,
    output: &BoundFile,
    pending: &PendingBilibiliMuxRecovery,
    anchor: &BoundFile,
) -> Result<()> {
    output.validate_identity()?;
    anchor.validate_identity()?;
    if output.identity() != anchor.identity()
        || output.identity().device() != pending.manifest.output_device
        || output.identity().inode() != pending.manifest.output_inode
        || root.entry_identity(output_path)? != Some(output.identity())
        || root.entry_identity(&pending.directory.join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME))?
            != Some(anchor.identity())
    {
        bail!("recovered Bilibili mux output or durable anchor changed");
    }
    Ok(())
}

fn require_bilibili_mux_transaction_entries(
    root: &RootedFs,
    pending: &PendingBilibiliMuxRecovery,
    expected: &BTreeSet<std::ffi::OsString>,
) -> Result<()> {
    let actual = root
        .list_bound_directory(&pending.directory_entry, pending.directory_identity)?
        .into_iter()
        .map(|(name, _)| name)
        .collect::<BTreeSet<_>>();
    if &actual != expected {
        bail!(
            "Bilibili mux recovery directory contains unexpected entries: {}",
            pending.directory.display()
        );
    }
    Ok(())
}

fn bilibili_mux_manifest_name(path: &Path, label: &str) -> Result<std::ffi::OsString> {
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(name)), None) => Ok(name.to_os_string()),
        _ => bail!("Bilibili mux recovery manifest has an invalid {label} name"),
    }
}

fn rooted_directory_identity(root: &RootedFs, path: &Path) -> Result<EntryIdentity> {
    root.validate_configured_root()?;
    if path == root.logical_root_path() || path == root.root_path() {
        return Ok(root.root_identity());
    }
    let identity = root
        .entry_identity(path)?
        .with_context(|| format!("bound directory is missing: {}", path.display()))?;
    if !identity.is_dir() {
        bail!("bound path is not a directory: {}", path.display());
    }
    Ok(identity)
}

#[cfg(test)]
fn bind_existing_bilibili_mux_state(
    root: &RootedFs,
    cwd: &Path,
    report: &mut BilibiliDownloadReport,
) -> Result<()> {
    for entry in &mut report.entries {
        if entry.mux.is_none() {
            continue;
        }
        let media_inputs = bilibili_entry_media_inputs(cwd, entry);
        let mux = entry.mux.as_mut().expect("mux presence was checked");
        mux.bound_inputs = bind_bilibili_mux_inputs(root, &media_inputs)?;
        let output_path = resolve_command_output_path(cwd, &mux.output_path);
        mux.output_path = output_path.clone();
        let output = root
            .open_bound_file(&output_path)?
            .with_context(|| format!("test mux output is missing: {}", output_path.display()))?;
        let parent = output_path
            .parent()
            .context("test mux output has no parent")?;
        let (directory, directory_entry, directory_identity) =
            create_bilibili_mux_staging_dir(root, parent)?;
        let staged_path = directory.join("output.test");
        let (manifest_path, manifest_entry, manifest_identity, mut manifest) =
            create_bilibili_mux_recovery_manifest(
                root,
                parent,
                &directory,
                directory_identity,
                &staged_path,
                output.identity(),
                &output_path,
                &mux.bound_inputs,
            )?;
        let output_entry = root.bind_entry(&output_path, false)?;
        let anchor_path = directory.join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME);
        let anchor_entry = root.bind_entry(&anchor_path, false)?;
        root.hard_link_via_bound_parents_noreplace_if_identity(
            &output_entry,
            &anchor_entry,
            output.identity(),
        )?;
        let anchor = root
            .open_bound_file(&anchor_path)?
            .context("test mux output anchor is missing")?;
        manifest.phase = BilibiliMuxRecoveryPhase::CleaningInputs;
        let (manifest_entry, manifest_identity) = replace_bilibili_mux_recovery_manifest(
            root,
            &directory_entry,
            directory_identity,
            &manifest_path,
            &manifest_entry,
            manifest_identity,
            &manifest,
        )?;
        mux.bound_output = Some(output);
        mux.recovery = Some(BilibiliMuxRecovery {
            directory,
            directory_entry,
            directory_identity,
            manifest_path,
            manifest_entry,
            manifest_identity,
            manifest,
            anchor,
        });
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
    progress: Option<JobProgressSender>,
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

async fn run_youtube_job_locked(
    config: &AppConfig,
    url: &str,
    subtitle_plan: SubtitlePlan,
    progress: Option<JobProgressSender>,
    bound_cwd: &BoundDirectory,
) -> Result<JobReport> {
    let spec = youtube_download_command_spec(config, url, &subtitle_plan);
    let output = run_command_with_bound_cwd(config, &spec, bound_cwd, progress).await?;
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

    Ok(JobReport {
        saved_location,
        details: nonempty_join(vec![subtitle_plan.describe(), tail_lines(&stderr, 6)]),
    })
}

async fn run_staged_video_job(
    config: &AppConfig,
    job: &JobRequest,
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    progress: Option<JobProgressSender>,
) -> Result<JobReport> {
    let guard = video_output_lock(
        &config.downloads.video_dir,
        "Staged video download",
        progress.as_ref(),
    )
    .await?;
    guard.begin_operation();
    let final_dir = config.downloads.video_dir.clone();
    let primary_media_kind = staged_primary_media_kind(config, job)?;
    let root = guard.root().clone();
    let staging = create_video_staging_dir(&root)?;
    let staging_dir = staging.path().to_path_buf();
    staging.validate_for_path_access()?;
    copy_bbdown_config_for_staging(&root, &final_dir, &staging_dir)?;
    staging.validate_for_path_access()?;
    send_progress(
        progress.as_ref(),
        format!("staging: downloading into {}", staging_dir.display()),
    );

    let mut staging_config = config.clone();
    staging_config.downloads.video_dir = staging_dir.clone();
    preserve_bilibili_config_paths_for_staging(&mut staging_config, &final_dir);
    let mut youtube_metadata = None;
    let result = match job {
        JobRequest::Bilibili { url, selection } => {
            let expected_identity =
                matches!(action, VideoDuplicateAction::Overwrite).then_some(&duplicate.identity);
            run_staged_bilibili_worker(
                &staging_config,
                &staging,
                url,
                *selection,
                expected_identity,
                progress.clone(),
            )
            .await
        }
        JobRequest::Youtube { url } => {
            let metadata =
                fetch_youtube_metadata(&staging_config, url, progress.clone(), &staging.directory)
                    .await;
            match metadata {
                Ok(metadata) => {
                    let subtitle_plan =
                        select_subtitles(&metadata, &staging_config.video.subtitle_languages);
                    let result = run_youtube_job_locked(
                        &staging_config,
                        url,
                        subtitle_plan,
                        progress.clone(),
                        &staging.directory,
                    )
                    .await;
                    if result.is_ok() {
                        youtube_metadata = Some((url.clone(), metadata));
                    }
                    result
                }
                Err(err) => Err(err),
            }
        }
        JobRequest::Pdf { .. } => run_simple_job(config, job, progress.clone()).await,
    };

    let mut report = match result {
        Ok(report) => {
            staging
                .retain_for_manual_recovery(VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON)
                .context("failed to persist completed staged download before publication")?;
            report
        }
        Err(err) => {
            match retained_video_staging_reason(&staging.root, staging.path(), staging.identity) {
                Ok(Some(reason)) => {
                    staging.preserve_for_recovery();
                    return Err(err.context(format!(
                        "Bilibili worker retained completed outputs for manual recovery: {reason}"
                    )));
                }
                Ok(None) => return Err(err),
                Err(retention_error) => {
                    staging.preserve_for_recovery();
                    return Err(err.context(format!(
                    "failed to verify the staged download completion marker; staging was retained: {retention_error:#}"
                )));
                }
            }
        }
    };

    staging
        .validate_for_path_access()
        .context("output root or staging directory changed during download")?;
    let mut staged_files = collect_regular_files(&staging_dir)?
        .into_iter()
        .filter(|path| !is_staging_support_file(&staging_dir, path))
        .collect::<Vec<_>>();
    staging
        .validate_for_path_access()
        .context("output root or staging directory changed while collecting download outputs")?;
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

    if config.video.write_nfo
        && let Some((url, metadata)) = youtube_metadata.as_ref()
    {
        let detail = if staged_media.len() == 1 {
            match write_youtube_nfo_bound(&root, &staged_media[0], url, metadata) {
                Ok(nfo_path) => {
                    staged_files.push(nfo_path.clone());
                    format!("NFO: {}", nfo_path.display())
                }
                Err(err) => format!("NFO skipped: {err}"),
            }
        } else {
            format!(
                "NFO skipped: expected one YouTube media file but found {}",
                staged_media.len()
            )
        };
        report.details = nonempty_join(vec![report.details, detail]);
    }

    let preserved_existing_for_multiple_media =
        matches!(action, VideoDuplicateAction::Overwrite) && staged_media.len() > 1;
    if preserved_existing_for_multiple_media {
        send_progress(
            progress.as_ref(),
            format!(
                "staging: download produced {} media files; preserving the existing file and keeping all new outputs",
                staged_media.len()
            ),
        );
    }

    let move_context = MoveExecutionContext {
        root: &root,
        staging: Some(&staging),
    };
    let moved_files = if staged_media.is_empty() && artifact_only {
        move_staged_artifact_files_with_root(
            move_context,
            &staging_dir,
            &final_dir,
            &staged_files,
            action,
            duplicate,
            primary_media_kind,
        )
    } else {
        move_staged_video_files_with_root(
            move_context,
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
    let overwrite_fallback_detail = if preserved_existing_for_multiple_media {
        "Overwrite fallback: the download produced multiple media files, so the existing file was preserved and all new outputs were kept alongside it."
            .to_string()
    } else {
        String::new()
    };
    let details = nonempty_join(vec![
        remove_staging_detail_lines(&report.details, &staging_dir),
        overwrite_fallback_detail,
        format!("Moved: {}", join_paths(&moved_files)),
    ]);
    let report = JobReport {
        saved_location,
        details,
    };
    staging
        .finish()
        .context("failed to durably finalize the staged video job")?;
    guard.mark_operation_clean();
    Ok(report)
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

async fn sync_bilibili_rust_credentials(config: &AppConfig) -> Result<()> {
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    let credential_profile = config.bilibili.auth.credential_profile.clone();
    tokio::task::spawn_blocking(move || {
        bilibili_auth::sync_bbdown_rust_credentials_from_state(
            &state_path,
            &credential_file,
            credential_profile.as_deref(),
        )
    })
    .await
    .context("BBDown credential migration task failed")??;
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
    if spec.args.iter().any(|arg| arg == "--bilibili-worker") {
        return "BBDown-rust".to_string();
    }
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
    progress: Option<JobProgressSender>,
    bound_cwd: &BoundDirectory,
) -> Result<YoutubeMetadata> {
    let spec = youtube_metadata_command_spec(config, url);
    let output = run_command_with_bound_cwd(config, &spec, bound_cwd, progress).await?;
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
    sync_bilibili_rust_credentials(config).await?;
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

struct VideoOutputGuard {
    root: RootedFs,
    recovery_state: VideoRecoveryState,
    _process_guard: MutexGuard<'static, ()>,
    _file_guard: BoundFile,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VideoControlOwner {
    version: u32,
    root_device: u64,
    root_inode: u64,
    control_device: u64,
    control_inode: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VideoStagingOwner {
    version: u32,
    root_device: u64,
    root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VideoStagingRetention {
    version: u32,
    root_device: u64,
    root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
    reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedPublicationManifest {
    version: u32,
    root_device: u64,
    root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
    overwrite: Option<StagedPublicationOverwrite>,
    steps: Vec<StagedPublicationStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedPublicationOverwrite {
    transaction_path: PathBuf,
    transaction_device: u64,
    transaction_inode: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedPublicationStep {
    source_path: PathBuf,
    destination_path: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Debug)]
struct VideoControlDirectory {
    path: PathBuf,
    entry: BoundEntry,
    identity: EntryIdentity,
}

#[derive(Debug)]
struct VideoRecoveryMarker {
    root: RootedFs,
    path: PathBuf,
    temp_path: PathBuf,
    entry: BoundEntry,
    file: BoundFile,
}

impl VideoRecoveryMarker {
    fn write_state(&mut self, state: u8) -> Result<()> {
        if !matches!(
            state,
            VIDEO_RECOVERY_STATE_CLEAN | VIDEO_RECOVERY_STATE_DIRTY
        ) {
            bail!("invalid video recovery state");
        }
        self.file.validate_private_single_link(0o600)?;
        if self.root.bound_entry_identity(&self.entry)? != Some(self.file.identity()) {
            bail!(
                "video recovery state identity changed before replacement: {}",
                self.path.display()
            );
        }
        remove_valid_recovery_state_temp(&self.root, &self.temp_path)?;
        let (entry, identity) = self
            .root
            .replace_bound_file_atomically_if_identity(
                &self.entry,
                self.file.identity(),
                &self.temp_path,
                &[state],
                0o600,
            )
            .context("failed to atomically persist video recovery state")?;
        let file = self
            .root
            .open_bound_file(&self.path)?
            .context("video recovery state disappeared after replacement")?;
        if file.identity() != identity {
            bail!("video recovery state identity changed after replacement");
        }
        file.validate_private_single_link(0o600)?;
        if file.read_limited(1)? != [state] {
            bail!("video recovery state replacement has invalid contents");
        }
        self.entry = entry;
        self.file = file;
        Ok(())
    }
}

impl VideoOutputGuard {
    fn root(&self) -> &RootedFs {
        &self.root
    }

    fn begin_operation(&self) {
        self.recovery_state.operation_clean.set(false);
    }

    fn mark_operation_clean(&self) {
        self.recovery_state.operation_clean.set(true);
    }
}

struct VideoRecoveryState {
    marker: VideoRecoveryMarker,
    operation_clean: Cell<bool>,
    unresolved_before_operation: bool,
}

impl Drop for VideoRecoveryState {
    fn drop(&mut self) {
        let state = if self.operation_clean.get() && !self.unresolved_before_operation {
            VIDEO_RECOVERY_STATE_CLEAN
        } else {
            VIDEO_RECOVERY_STATE_DIRTY
        };
        let _ = self.marker.write_state(state);
    }
}

fn video_output_lock_file(root: &RootedFs) -> Result<BoundFile> {
    root.open_or_create_bound_file(
        &root.logical_root_path().join(VIDEO_OUTPUT_LOCK_FILE_NAME),
        0o600,
    )
    .context("failed to open the cross-process video output lock")
}

fn video_control_directory(root: &RootedFs) -> Result<VideoControlDirectory> {
    let path = root.logical_root_path().join(VIDEO_CONTROL_DIR_NAME);
    if root.entry_identity(&path)?.is_none() {
        install_video_control_directory(root, &path)?;
    }
    validate_video_control_directory(root, &path)
}

fn install_video_control_directory(root: &RootedFs, path: &Path) -> Result<()> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..1000 {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = root.logical_root_path().join(format!(
            "{VIDEO_CONTROL_INITIALIZING_DIR_PREFIX}-{}-{nanos}-{counter}",
            std::process::id()
        ));
        let Some(identity) = root.create_dir(&candidate, 0o700)? else {
            continue;
        };
        let entry = root.bind_entry(&candidate, false)?;
        if root.bound_entry_identity(&entry)? != Some(identity) {
            let _ = root.remove_bound_tree_if_identity(&entry, identity);
            bail!(
                "video control initialization directory identity changed: {}",
                candidate.display()
            );
        }
        root.validate_private_bound_directory(&entry, identity, 0o700)?;
        let owner = video_control_owner(root, identity);
        let contents = serde_json::to_vec(&owner)
            .context("failed to encode video control ownership record")?;
        if let Err(err) = root
            .create_new_bound_file(
                &candidate.join(VIDEO_CONTROL_OWNER_FILE_NAME),
                &contents,
                0o600,
            )
            .context("failed to create video control ownership record")
        {
            let cleanup = root.remove_bound_tree_if_identity(&entry, identity);
            return match cleanup {
                Ok(()) => Err(err),
                Err(cleanup) => Err(err.context(format!(
                    "failed to clean video control initialization directory: {cleanup:#}"
                ))),
            };
        }

        let destination = root.bind_entry(path, false)?;
        if root.bound_entry_identity(&destination)?.is_some() {
            root.remove_bound_tree_if_identity(&entry, identity)?;
            return Ok(());
        }
        if let Err(err) =
            root.rename_via_bound_parents_noreplace_if_identity(&entry, &destination, identity)
        {
            let cleanup = root.remove_bound_tree_if_identity(&entry, identity);
            return match cleanup {
                Ok(()) => Err(err.context("failed to install video control directory")),
                Err(cleanup) => Err(err.context(format!(
                    "failed to install video control directory; cleanup also failed: {cleanup:#}"
                ))),
            };
        }
        return Ok(());
    }
    bail!("failed to allocate a video control initialization directory")
}

fn video_control_owner(root: &RootedFs, identity: EntryIdentity) -> VideoControlOwner {
    VideoControlOwner {
        version: VIDEO_CONTROL_OWNER_VERSION,
        root_device: root.root_identity().device(),
        root_inode: root.root_identity().inode(),
        control_device: identity.device(),
        control_inode: identity.inode(),
    }
}

fn validate_video_control_directory(root: &RootedFs, path: &Path) -> Result<VideoControlDirectory> {
    let entry = root.bind_entry(path, false)?;
    let identity = match root.bound_entry_identity(&entry)? {
        Some(current) if current.is_dir() => current,
        Some(_) => bail!("video control path is not a directory: {}", path.display()),
        None => bail!("video control directory disappeared: {}", path.display()),
    };
    root.validate_private_bound_directory(&entry, identity, 0o700)?;

    let owner_path = path.join(VIDEO_CONTROL_OWNER_FILE_NAME);
    let expected = video_control_owner(root, identity);
    let owner = root.open_bound_file(&owner_path)?.with_context(|| {
        format!(
            "video control directory is not app-owned; missing {}",
            owner_path.display()
        )
    })?;
    owner.validate_private_single_link(0o600)?;
    let actual: VideoControlOwner =
        serde_json::from_slice(&owner.read_limited(VIDEO_CONTROL_OWNER_LIMIT)?)
            .context("failed to parse video control ownership record")?;
    if actual.version != expected.version
        || actual.root_device != expected.root_device
        || actual.root_inode != expected.root_inode
        || actual.control_device != expected.control_device
        || actual.control_inode != expected.control_inode
    {
        bail!("video control ownership record does not match the bound output root");
    }
    if root.entry_identity(&owner_path)? != Some(owner.identity()) {
        bail!("video control ownership record identity changed after validation");
    }
    root.validate_private_bound_directory(&entry, identity, 0o700)?;
    Ok(VideoControlDirectory {
        path: path.to_path_buf(),
        entry,
        identity,
    })
}

fn video_recovery_state_file(root: &RootedFs) -> Result<(VideoRecoveryMarker, bool)> {
    let control = video_control_directory(root)?;
    root.validate_private_bound_directory(&control.entry, control.identity, 0o700)?;
    let path = control.path.join(VIDEO_RECOVERY_STATE_FILE_NAME);
    let temp_path = control.path.join(VIDEO_RECOVERY_STATE_TEMP_FILE_NAME);
    remove_valid_recovery_state_temp(root, &temp_path)?;
    let entry = root.bind_entry(&path, false)?;
    let previous = root.bound_entry_identity(&entry)?;
    if previous.is_some_and(|identity| !identity.is_file()) {
        bail!(
            "video recovery state path is not a regular file: {}",
            path.display()
        );
    }
    let legacy_requires_scan = previous.is_none()
        && root
            .entry_identity(
                &root
                    .logical_root_path()
                    .join(LEGACY_VIDEO_RECOVERY_STATE_FILE_NAME),
            )?
            .is_some();
    if previous.is_none() {
        root.create_new_bound_file(
            &path,
            &[if legacy_requires_scan {
                VIDEO_RECOVERY_STATE_DIRTY
            } else {
                VIDEO_RECOVERY_STATE_CLEAN
            }],
            0o600,
        )
        .context("failed to create the video recovery state marker")?;
    }
    let file = root
        .open_bound_file(&path)?
        .context("video recovery state marker is missing")?;
    if previous.is_some_and(|identity| identity != file.identity()) {
        bail!(
            "video recovery state identity changed while opening: {}",
            path.display()
        );
    }
    file.validate_private_single_link(0o600)?;
    let contents = file.read_limited(1)?;
    if !matches!(
        contents.as_slice(),
        [VIDEO_RECOVERY_STATE_CLEAN | VIDEO_RECOVERY_STATE_DIRTY]
    ) {
        bail!("video recovery state marker has invalid contents");
    }
    Ok((
        VideoRecoveryMarker {
            root: root.clone(),
            path,
            temp_path,
            entry,
            file,
        },
        previous.is_some() || legacy_requires_scan,
    ))
}

fn remove_valid_recovery_state_temp(root: &RootedFs, path: &Path) -> Result<()> {
    let Some(file) = root.open_bound_file(path)? else {
        return Ok(());
    };
    file.validate_private_single_link(0o600)?;
    if !matches!(
        file.read_limited(1)?.as_slice(),
        [VIDEO_RECOVERY_STATE_CLEAN | VIDEO_RECOVERY_STATE_DIRTY]
    ) {
        bail!(
            "refused to remove invalid video recovery state transition: {}",
            path.display()
        );
    }
    let entry = root.bind_entry(path, false)?;
    root.remove_bound_file_if_identity(&entry, file.identity())
        .with_context(|| {
            format!(
                "failed to remove interrupted video recovery state transition {}",
                path.display()
            )
        })
}

fn video_recovery_state_is_clean(marker: &VideoRecoveryMarker) -> Result<bool> {
    marker.file.validate_private_single_link(0o600)?;
    Ok(marker.file.read_limited(1)? == [VIDEO_RECOVERY_STATE_CLEAN])
}

async fn video_output_lock(
    video_dir: &Path,
    job_label: &str,
    progress: Option<&JobProgressSender>,
) -> Result<VideoOutputGuard> {
    let lock = VIDEO_OUTPUT_LOCK.get_or_init(|| Mutex::new(()));
    let process_guard = match lock.try_lock() {
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
    };

    let root = RootedFs::new(video_dir)?;
    let file_guard = video_output_lock_file(&root)?;
    let mut reported_wait = false;
    while !file_guard.try_lock_exclusive()? {
        if !reported_wait {
            send_progress(
                progress,
                format!("{job_label}: waiting for another downloader process"),
            );
            reported_wait = true;
        }
        sleep(Duration::from_millis(250)).await;
    }
    if reported_wait {
        send_progress(
            progress,
            format!("{job_label}: cross-process output slot acquired"),
        );
    }
    let (mut recovery_state_file, recovery_state_existed) = video_recovery_state_file(&root)?;
    let mut recoveries = Vec::new();
    let mut unresolved_recovery = false;
    if recovery_state_existed && !video_recovery_state_is_clean(&recovery_state_file)? {
        let quarantine_recovery = root.reconcile_remove_quarantines_with_status()?;
        let staging_recovery = recover_pending_video_staging_directories_locked(&root)?;
        let mux_recovery = recover_pending_bilibili_mux_transactions_locked(&root, video_dir)?;
        let overwrite_recovery = recover_pending_overwrite_transactions_locked(
            &root,
            video_dir,
            &staging_recovery.blocked_overwrites,
        )?;
        unresolved_recovery = quarantine_recovery.unresolved
            || staging_recovery.unresolved
            || mux_recovery.unresolved
            || overwrite_recovery.unresolved;
        recoveries.extend(quarantine_recovery.messages);
        recoveries.extend(staging_recovery.messages);
        recoveries.extend(mux_recovery.messages);
        recoveries.extend(overwrite_recovery.messages);
    }
    for recovery in recoveries {
        warn_recovered_overwrite(&recovery);
        send_progress(progress, format!("{job_label}: {recovery}"));
    }
    recovery_state_file.write_state(VIDEO_RECOVERY_STATE_DIRTY)?;
    Ok(VideoOutputGuard {
        root,
        recovery_state: VideoRecoveryState {
            marker: recovery_state_file,
            operation_clean: Cell::new(true),
            unresolved_before_operation: unresolved_recovery,
        },
        _process_guard: process_guard,
        _file_guard: file_guard,
    })
}

fn warn_recovered_overwrite(recovery: &str) {
    tracing::warn!(
        message = recovery,
        "recovered overwrite transaction before download"
    );
}

fn send_progress(progress: Option<&JobProgressSender>, message: String) {
    if let Some(progress) = progress {
        progress.send_replace(Some(JobProgress { message }));
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

struct CommandProcessGroupGuard {
    process_group: CommandProcessGroup,
    armed: bool,
}

impl CommandProcessGroupGuard {
    fn new(process_group: CommandProcessGroup) -> Self {
        Self {
            process_group,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CommandProcessGroupGuard {
    fn drop(&mut self) {
        if self.armed {
            force_terminate_process_group(self.process_group);
        }
    }
}

async fn run_command(
    config: &AppConfig,
    spec: &CommandSpec,
    progress: Option<JobProgressSender>,
) -> Result<CommandOutput> {
    run_command_with_inherited_files(config, spec, &[], progress).await
}

async fn run_command_with_inherited_files(
    config: &AppConfig,
    spec: &CommandSpec,
    inherited_files: &[BoundFile],
    progress: Option<JobProgressSender>,
) -> Result<CommandOutput> {
    run_command_with_execution_context(
        config,
        spec,
        None,
        inherited_files,
        progress,
        CommandExecutionPolicy::EXTERNAL,
    )
    .await
}

async fn run_command_with_bound_cwd(
    config: &AppConfig,
    spec: &CommandSpec,
    bound_cwd: &BoundDirectory,
    progress: Option<JobProgressSender>,
) -> Result<CommandOutput> {
    run_command_with_bound_cwd_and_inherited_files(config, spec, bound_cwd, &[], progress).await
}

async fn run_command_with_bound_cwd_and_inherited_files(
    config: &AppConfig,
    spec: &CommandSpec,
    bound_cwd: &BoundDirectory,
    inherited_files: &[BoundFile],
    progress: Option<JobProgressSender>,
) -> Result<CommandOutput> {
    run_command_with_bound_cwd_and_inherited_files_with_policy(
        config,
        spec,
        bound_cwd,
        inherited_files,
        Vec::new(),
        progress,
        CommandExecutionPolicy::EXTERNAL,
    )
    .await
}

async fn run_command_with_bound_cwd_and_inherited_files_with_policy(
    config: &AppConfig,
    spec: &CommandSpec,
    bound_cwd: &BoundDirectory,
    inherited_files: &[BoundFile],
    additional_inherited_fds: Vec<AdditionalInheritedCommandFd>,
    progress: Option<JobProgressSender>,
    policy: CommandExecutionPolicy,
) -> Result<CommandOutput> {
    run_command_with_execution_context_and_additional_fds(
        config,
        spec,
        Some(bound_cwd),
        inherited_files,
        additional_inherited_fds,
        progress,
        policy,
    )
    .await
}

async fn run_command_with_execution_context(
    config: &AppConfig,
    spec: &CommandSpec,
    bound_cwd: Option<&BoundDirectory>,
    inherited_files: &[BoundFile],
    progress: Option<JobProgressSender>,
    policy: CommandExecutionPolicy,
) -> Result<CommandOutput> {
    run_command_with_execution_context_and_additional_fds(
        config,
        spec,
        bound_cwd,
        inherited_files,
        Vec::new(),
        progress,
        policy,
    )
    .await
}

async fn run_command_with_execution_context_and_additional_fds(
    config: &AppConfig,
    spec: &CommandSpec,
    bound_cwd: Option<&BoundDirectory>,
    inherited_files: &[BoundFile],
    mut additional_inherited_fds: Vec<AdditionalInheritedCommandFd>,
    progress: Option<JobProgressSender>,
    policy: CommandExecutionPolicy,
) -> Result<CommandOutput> {
    #[cfg(unix)]
    if matches!(policy.process_group, CommandProcessGroupMode::Inherited)
        && BILIBILI_WORKER_PROCESS.load(Ordering::Acquire)
    {
        ensure_current_process_owns_its_group()
            .context("refused to run Bilibili mux outside its owned worker process group")?;
    }
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

    #[cfg(unix)]
    let descendant_fence = if policy.descendant_fence {
        let (child_fence, parent_fence) =
            command_liveness_pair().context("failed to create command descendant fence")?;
        additional_inherited_fds.push(prepare_additional_inherited_command_fd(
            &child_fence,
            COMMAND_DESCENDANT_FENCE_FD,
        )?);
        Some(parent_fence)
    } else {
        None
    };
    #[cfg(unix)]
    let mut inherited_fds = prepare_inherited_command_fds(inherited_files)?;
    #[cfg(unix)]
    append_additional_inherited_command_fds(&mut inherited_fds, additional_inherited_fds)?;
    #[cfg(unix)]
    let bound_cwd_fd = bound_cwd
        .map(|directory| prepare_bound_command_cwd_fd(directory, inherited_files.len()))
        .transpose()?;
    #[cfg(not(unix))]
    if !inherited_files.is_empty()
        || !additional_inherited_fds.is_empty()
        || bound_cwd.is_some()
        || policy.descendant_fence
    {
        bail!("inherited command files and bound working directories require a Unix platform");
    }

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(if bound_cwd.is_some() {
            Path::new("/")
        } else {
            &spec.cwd
        })
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        if matches!(policy.process_group, CommandProcessGroupMode::Owned) {
            command.process_group(0);
        }
        if bound_cwd_fd.is_some() || !inherited_fds.is_empty() {
            // The source descriptors are collision-free CLOEXEC duplicates. dup2 clears CLOEXEC
            // on each fixed child descriptor without allowing an argv pathname lookup.
            unsafe {
                command.pre_exec(move || {
                    if let Some(directory) = &bound_cwd_fd
                        && libc::fchdir(directory.as_raw_fd()) == -1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    for (source, target) in &inherited_fds {
                        if libc::dup2(source.as_raw_fd(), *target) == -1 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
        }
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to run {}", spec.program.display()))?;
    drop(command);
    let process_group = command_process_group(&child, policy.process_group);
    let mut process_group_guard = CommandProcessGroupGuard::new(process_group);
    #[cfg(unix)]
    let descendant_fence_handle =
        descendant_fence.map(|fence| tokio::spawn(wait_for_liveness_peer_close(fence)));
    #[cfg(not(unix))]
    let descendant_fence_handle: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;

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
    let total_deadline = matches!(policy.total_deadline, CommandTotalDeadline::Configured)
        .then_some(started_at + total_timeout);
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
            _ = async {
                match total_deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                terminate_command_tree(&mut child, process_group).await;
                let (stdout, stderr) =
                    collect_stream_outputs(
                        stdout_handle,
                        stderr_handle,
                        descendant_fence_handle,
                        process_group,
                    ).await;
                process_group_guard.disarm();
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
                    collect_stream_outputs(
                        stdout_handle,
                        stderr_handle,
                        descendant_fence_handle,
                        process_group,
                    ).await;
                process_group_guard.disarm();
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

    let (stdout, stderr) = collect_stream_outputs(
        stdout_handle,
        stderr_handle,
        descendant_fence_handle,
        process_group,
    )
    .await;
    if !status.success() && process_group.is_inherited() {
        let summary = summarize_output(
            &String::from_utf8_lossy(&stdout),
            &String::from_utf8_lossy(&stderr),
        );
        abort_command_process_group(
            process_group,
            &format!(
                "{} exited with status {}\n{summary}",
                spec.program.display(),
                status
            ),
        );
    }
    process_group_guard.disarm();
    if let Some(bound_cwd) = bound_cwd {
        bound_cwd
            .validate_identity()
            .context("bound command working directory identity changed")?;
    }
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
}

#[cfg(unix)]
fn prepare_bound_command_cwd_fd(
    directory: &BoundDirectory,
    inherited_file_count: usize,
) -> Result<OwnedFd> {
    let inherited_file_count = i32::try_from(inherited_file_count)
        .context("too many inherited command files for a bound working directory")?;
    let minimum = BILIBILI_MUX_FD_BASE
        .checked_add(inherited_file_count.saturating_mul(2))
        .and_then(|descriptor| descriptor.checked_add(32))
        .context("too many inherited command files for a bound working directory")?;
    directory.duplicate_fd_cloexec_at_least(minimum)
}

#[cfg(unix)]
fn prepare_inherited_command_fds(files: &[BoundFile]) -> Result<Vec<(OwnedFd, i32)>> {
    let count = i32::try_from(files.len()).context("too many inherited command files")?;
    let minimum_source_fd = BILIBILI_MUX_FD_BASE
        .checked_add(count)
        .and_then(|descriptor| descriptor.checked_add(16))
        .context("too many inherited command files")?;
    files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let index = i32::try_from(index).context("too many inherited command files")?;
            let target = BILIBILI_MUX_FD_BASE
                .checked_add(index)
                .context("too many inherited command files")?;
            let source = file.duplicate_fd_cloexec_at_least(minimum_source_fd)?;
            Ok((source, target))
        })
        .collect()
}

#[cfg(unix)]
fn command_liveness_pair() -> Result<(UnixStream, UnixStream)> {
    let (first, second) = UnixStream::pair().context("failed to create Unix socket pair")?;
    for endpoint in [&first, &second] {
        let flags = rustix::io::fcntl_getfd(endpoint)
            .context("failed to inspect command liveness descriptor flags")?;
        rustix::io::fcntl_setfd(endpoint, flags | rustix::io::FdFlags::CLOEXEC)
            .context("failed to mark command liveness descriptor close-on-exec")?;
        endpoint
            .set_nonblocking(true)
            .context("failed to make command liveness descriptor nonblocking")?;
    }
    Ok((first, second))
}

#[cfg(unix)]
fn inherited_worker_liveness_stream() -> Result<UnixStream> {
    let liveness_fd = unsafe { OwnedFd::from_raw_fd(BILIBILI_WORKER_LIVENESS_FD) };
    let descriptor_flags = rustix::io::fcntl_getfd(&liveness_fd)
        .context("failed to inspect inherited Bilibili worker liveness descriptor")?;
    rustix::io::fcntl_setfd(
        &liveness_fd,
        descriptor_flags | rustix::io::FdFlags::CLOEXEC,
    )
    .context("failed to protect Bilibili worker liveness descriptor from nested commands")?;
    Ok(UnixStream::from(liveness_fd))
}

#[cfg(unix)]
fn prepare_additional_inherited_command_fd(
    source: &impl AsFd,
    target: i32,
) -> Result<AdditionalInheritedCommandFd> {
    if target <= libc::STDERR_FILENO {
        bail!("refused to replace a standard command descriptor");
    }
    let targets = BTreeSet::from([target]);
    let source = duplicate_inherited_command_fd_avoiding_targets(source, &targets)?;
    Ok(AdditionalInheritedCommandFd { source, target })
}

#[cfg(unix)]
fn append_additional_inherited_command_fds(
    inherited_fds: &mut Vec<(OwnedFd, i32)>,
    additional: Vec<AdditionalInheritedCommandFd>,
) -> Result<()> {
    let mut targets = inherited_fds
        .iter()
        .map(|(_, target)| *target)
        .collect::<BTreeSet<_>>();
    for AdditionalInheritedCommandFd { target, .. } in &additional {
        if *target <= libc::STDERR_FILENO || !targets.insert(*target) {
            bail!("inherited command descriptor target is invalid or duplicated");
        }
    }
    inherited_fds.extend(
        additional
            .into_iter()
            .map(|inherited| (inherited.source, inherited.target)),
    );
    for (source, _) in inherited_fds {
        if targets.contains(&source.as_raw_fd()) {
            *source = duplicate_inherited_command_fd_avoiding_targets(source, &targets)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn duplicate_inherited_command_fd_avoiding_targets(
    source: &impl AsFd,
    targets: &BTreeSet<i32>,
) -> Result<OwnedFd> {
    let mut minimum = libc::STDERR_FILENO + 1;
    loop {
        let duplicate = rustix::io::fcntl_dupfd_cloexec(source, minimum)
            .context("failed to allocate a collision-free inherited command descriptor")?;
        let descriptor = duplicate.as_raw_fd();
        if !targets.contains(&descriptor) {
            return Ok(duplicate);
        }
        minimum = descriptor
            .checked_add(1)
            .context("inherited command descriptor range is exhausted")?;
    }
}

fn file_activity_poll_interval(progress_interval: Duration, idle_timeout: Duration) -> Duration {
    let half_idle_timeout = idle_timeout / 2;
    progress_interval.min(if half_idle_timeout.is_zero() {
        idle_timeout
    } else {
        half_idle_timeout
    })
}

fn command_process_group(
    child: &tokio::process::Child,
    mode: CommandProcessGroupMode,
) -> CommandProcessGroup {
    #[cfg(unix)]
    {
        match mode {
            CommandProcessGroupMode::Owned => child
                .id()
                .map(|id| CommandProcessGroup::Owned(id as libc::pid_t))
                .unwrap_or(CommandProcessGroup::None),
            CommandProcessGroupMode::Inherited => {
                if !BILIBILI_WORKER_PROCESS.load(Ordering::Acquire) {
                    return CommandProcessGroup::None;
                }
                let process_group = unsafe { libc::getpgrp() };
                if process_group > 0 && process_group == unsafe { libc::getpid() } {
                    CommandProcessGroup::Inherited(process_group)
                } else {
                    CommandProcessGroup::None
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (child, mode);
        CommandProcessGroup::None
    }
}

#[cfg(unix)]
fn ensure_current_process_owns_its_group() -> Result<()> {
    let process = unsafe { libc::getpid() };
    let process_group = unsafe { libc::getpgrp() };
    if process <= 0 || process_group != process {
        bail!("current process is not the leader of its process group");
    }
    Ok(())
}

async fn terminate_command_tree(
    child: &mut tokio::process::Child,
    process_group: CommandProcessGroup,
) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group.id() {
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
    if let Some(process_group_id) = process_group.id() {
        signal_process_group(process_group_id, libc::SIGKILL);
    }

    #[cfg(not(unix))]
    {
        let _ = process_group;
    }
}

fn abort_command_process_group(process_group: CommandProcessGroup, reason: &str) -> ! {
    eprintln!("{reason}");
    let _ = std::io::stderr().flush();
    force_terminate_process_group(process_group);
    #[cfg(unix)]
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
        libc::_exit(128 + libc::SIGKILL);
    }

    #[cfg(not(unix))]
    std::process::abort();
}

#[cfg(unix)]
fn terminate_current_process_group(reason: &str) -> ! {
    let process_group = unsafe { libc::getpgrp() };
    let process_group = if process_group > 0 {
        CommandProcessGroup::Inherited(process_group)
    } else {
        CommandProcessGroup::None
    };
    abort_command_process_group(process_group, reason)
}

#[cfg(unix)]
async fn wait_for_liveness_peer_close(stream: UnixStream) -> std::io::Result<()> {
    stream.set_nonblocking(true)?;
    let stream = tokio::io::unix::AsyncFd::new(stream)?;
    let mut buffer = [0_u8; 64];
    loop {
        let mut ready = stream.readable().await?;
        match ready.try_io(|inner| {
            let mut inner = inner.get_ref();
            inner.read(&mut buffer)
        }) {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => continue,
        }
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
    mut descendant_fence_handle: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    process_group: CommandProcessGroup,
) -> (Vec<u8>, Vec<u8>) {
    let close_deadline = Instant::now() + OUTPUT_CLOSE_GRACE;
    let mut abort_deadline = close_deadline + OUTPUT_ABORT_GRACE;
    let mut did_terminate_group = false;
    let mut stdout = None;
    let mut stderr = None;
    let mut descendants_closed = descendant_fence_handle.is_none();

    loop {
        if stdout.is_some() && stderr.is_some() && descendants_closed {
            break;
        }

        tokio::select! {
            result = &mut stdout_handle, if stdout.is_none() => {
                stdout = Some(join_stream_output(result));
            }
            result = &mut stderr_handle, if stderr.is_none() => {
                stderr = Some(join_stream_output(result));
            }
            result = async {
                match descendant_fence_handle.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => std::future::pending().await,
                }
            }, if !descendants_closed => {
                match result {
                    Some(Ok(Ok(()))) => descendants_closed = true,
                    Some(Ok(Err(err))) => abort_command_process_group(
                        process_group,
                        &format!("command descendant fence failed: {err}"),
                    ),
                    Some(Err(err)) => abort_command_process_group(
                        process_group,
                        &format!("command descendant fence task failed: {err}"),
                    ),
                    None => unreachable!("a pending descendant fence cannot complete"),
                }
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
                if !descendants_closed {
                    if let Some(handle) = descendant_fence_handle.as_mut() {
                        handle.abort();
                    }
                    abort_command_process_group(
                        process_group,
                        "command descendants did not exit after process termination",
                    );
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
    progress: Option<JobProgressSender>,
    last_message: Option<String>,
    last_output: Option<String>,
    last_file_activity: Option<FileActivityReport>,
    stage: ProgressStage,
}

impl ProgressTracker {
    fn new(
        command_name: String,
        min_interval: Duration,
        progress: Option<JobProgressSender>,
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

    fn send(&mut self, progress: JobProgressSender, message: String, now: Instant) {
        let message = redact_sensitive_output(&message);
        self.last_message = Some(message.clone());
        self.next_send_at = now + self.min_interval;
        info!(command = %self.command_name, message = %message, "command progress");
        progress.send_replace(Some(JobProgress { message }));
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

fn ensure_bilibili_overwrite_plan_matches(
    plan: &BilibiliDownloadPlan,
    expected: &VideoIdentity,
) -> Result<()> {
    if expected.provider != VideoProvider::Bilibili || !is_bilibili_entry_identity(&expected.id) {
        bail!("Bilibili overwrite is not bound to an exact entry identity");
    }
    let current_identities = bilibili_plan_overwrite_identities(plan);
    if current_identities
        .iter()
        .any(|identity| identity == expected)
    {
        return Ok(());
    }
    let current = current_identities
        .iter()
        .map(|identity| identity.id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    bail!(
        "Bilibili overwrite target changed after confirmation: expected {}, resolved {}",
        expected.id,
        if current.is_empty() {
            "no exact entry".to_string()
        } else {
            current
        }
    )
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

fn is_bilibili_selection_required_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<bbdown_core::Error>()
            .is_some_and(|error| matches!(error, bbdown_core::Error::SelectionRequired { .. }))
    })
}

fn should_propagate_bilibili_probe_error(
    selection: Option<BilibiliSelection>,
    error: &anyhow::Error,
) -> bool {
    selection.is_none() && is_bilibili_selection_required_error(error)
}

fn bilibili_id_from_url(raw_url: &str) -> Option<String> {
    let url = url::Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if !domain_or_subdomain(&host, "bilibili.com") && !domain_or_subdomain(&host, "bilibili.tv") {
        return None;
    }
    url.path_segments()?
        .find(|segment| is_direct_bilibili_id(segment))
        .map(str::to_string)
}

fn is_direct_bilibili_id(value: &str) -> bool {
    value
        .strip_prefix("BV")
        .or_else(|| value.strip_prefix("bv"))
        .is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
        || ["av", "ep"].into_iter().any(|prefix| {
            value.strip_prefix(prefix).is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
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
    metadata_sidecars: &[PathBuf],
    read_policy: IdentityIndexReadPolicy,
) -> Result<()> {
    index_video_filename_identities(index, video);

    for path in metadata_sidecars {
        index_metadata_sidecar(index, video, path, path, read_policy)?;
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
    existing_metadata_sidecar_paths(video)
        .ok()
        .into_iter()
        .flatten()
        .any(|path| {
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
    match identity_metadata_kind(path) {
        Some(IdentityMetadataKind::InfoJson) => {
            let metadata = serde_json::from_str::<serde_json::Value>(content)
                .context("failed to parse info JSON")?;
            Ok(info_json_identities(&metadata))
        }
        Some(IdentityMetadataKind::Nfo) => Ok(nfo_identities(content)),
        None => Ok(Vec::new()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentityMetadataKind {
    Nfo,
    InfoJson,
}

fn identity_metadata_kind(path: &Path) -> Option<IdentityMetadataKind> {
    let name = path.file_name()?.to_str()?;
    if name.to_ascii_lowercase().ends_with(".info.json") {
        return Some(IdentityMetadataKind::InfoJson);
    }
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("nfo"))
        .then_some(IdentityMetadataKind::Nfo)
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

fn expected_metadata_sidecar_paths(video: &Path) -> Vec<PathBuf> {
    ["nfo", "info.json"]
        .into_iter()
        .map(|extension| video.with_extension(extension))
        .collect()
}

fn existing_metadata_sidecar_paths(video: &Path) -> Result<Vec<PathBuf>> {
    let Some(parent) = video.parent() else {
        return Ok(Vec::new());
    };
    let Some(stem) = video.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    let mut primary_stems = BTreeSet::from([stem.to_string()]);
    for entry in
        fs::read_dir(parent).with_context(|| format!("failed to read {}", parent.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file()
            && (is_video_file(&path) || is_audio_file(&path))
            && let Some(primary_stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            primary_stems.insert(primary_stem.to_string());
        }
        entries.push(path);
    }

    let prefix = format!("{stem}.");
    Ok(entries
        .into_iter()
        .filter(|path| {
            identity_metadata_kind(path).is_some()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(&prefix))
                && best_primary_stem_for_sidecar(path, &primary_stems).as_deref() == Some(stem)
        })
        .collect())
}

#[derive(Debug, Default)]
struct IdentityMediaInventory {
    media_files: BTreeSet<PathBuf>,
    metadata_sidecars: BTreeMap<PathBuf, Vec<PathBuf>>,
    #[cfg(test)]
    directory_scan_count: usize,
}

fn build_identity_media_inventory(
    root: &Path,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<IdentityMediaInventory> {
    let mut inventory = IdentityMediaInventory::default();
    collect_identity_media_inventory(root, primary_media_kind, &mut inventory)?;
    Ok(inventory)
}

fn collect_identity_media_inventory(
    directory: &Path,
    primary_media_kind: StagedPrimaryMediaKind,
    inventory: &mut IdentityMediaInventory,
) -> Result<()> {
    #[cfg(test)]
    {
        inventory.directory_scan_count += 1;
    }

    let mut regular_files = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name == VIDEO_STAGING_DIR_NAME || name.starts_with(OVERWRITE_BACKUP_DIR_PREFIX)
                })
            {
                continue;
            }
            collect_identity_media_inventory(&path, primary_media_kind, inventory)?;
        } else if file_type.is_file() {
            regular_files.push(path);
        }
    }

    let mut all_primary_stems = BTreeSet::new();
    let mut indexed_media_by_stem = BTreeMap::<String, Vec<PathBuf>>::new();
    for path in &regular_files {
        if (is_video_file(path) || is_audio_file(path))
            && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            all_primary_stems.insert(stem.to_string());
            if is_primary_media_file(path, primary_media_kind) {
                inventory.media_files.insert(path.clone());
                indexed_media_by_stem
                    .entry(stem.to_string())
                    .or_default()
                    .push(path.clone());
            }
        }
    }

    for sidecar in regular_files
        .iter()
        .filter(|path| identity_metadata_kind(path).is_some())
    {
        let Some(stem) = best_primary_stem_for_sidecar(sidecar, &all_primary_stems) else {
            continue;
        };
        let Some(media_files) = indexed_media_by_stem.get(&stem) else {
            continue;
        };
        for media_file in media_files {
            inventory
                .metadata_sidecars
                .entry(media_file.clone())
                .or_default()
                .push(sidecar.clone());
        }
    }

    Ok(())
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
                .is_some_and(|name| {
                    name == VIDEO_STAGING_DIR_NAME || name.starts_with(OVERWRITE_BACKUP_DIR_PREFIX)
                })
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

fn create_video_staging_dir(root: &RootedFs) -> Result<BoundStagingDir> {
    let parent = root.logical_root_path().join(VIDEO_STAGING_DIR_NAME);
    let _ = root.create_dir(&parent, 0o755)?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for index in 0..1000 {
        let token = format!("{}-{nanos}-{index}", std::process::id());
        let initializing = parent.join(format!("{VIDEO_STAGING_INITIALIZING_DIR_PREFIX}{token}"));
        let candidate = parent.join(format!("job-{token}"));
        match root.create_dir(&initializing, 0o700)? {
            Some(identity) => {
                let entry = root.bind_entry(&initializing, false)?;
                if root.bound_entry_identity(&entry)? != Some(identity) {
                    let _ = root.remove_bound_dir_if_identity(&entry, identity);
                    bail!(
                        "staging directory identity changed while binding: {}",
                        initializing.display()
                    );
                }
                root.validate_private_bound_directory(&entry, identity, 0o700)?;
                let owner = VideoStagingOwner {
                    version: VIDEO_STAGING_OWNER_VERSION,
                    root_device: root.root_identity().device(),
                    root_inode: root.root_identity().inode(),
                    staging_device: identity.device(),
                    staging_inode: identity.inode(),
                };
                let owner_contents = serde_json::to_vec(&owner)
                    .context("failed to encode video staging ownership record")?;
                if let Err(err) = root
                    .create_new_bound_file(
                        &initializing.join(VIDEO_STAGING_OWNER_FILE_NAME),
                        &owner_contents,
                        0o600,
                    )
                    .context("failed to create video staging ownership record")
                {
                    let cleanup = root.remove_bound_tree_durably_if_identity(&entry, identity);
                    return match cleanup {
                        Ok(()) => Err(err),
                        Err(cleanup) => Err(err.context(format!(
                            "failed to clean video staging directory: {cleanup:#}"
                        ))),
                    };
                }
                validate_video_staging_directory(root, &initializing, identity)?;
                let destination = root.bind_entry(&candidate, false)?;
                if root.bound_entry_identity(&destination)?.is_some() {
                    root.remove_bound_tree_if_identity(&entry, identity)?;
                    continue;
                }
                if let Err(err) = root.rename_via_bound_parents_noreplace_if_identity(
                    &entry,
                    &destination,
                    identity,
                ) {
                    let cleanup = root.remove_bound_tree_if_identity(&entry, identity);
                    return match cleanup {
                        Ok(()) => Err(err.context("failed to install video staging directory")),
                        Err(cleanup) => Err(err.context(format!(
                            "failed to install video staging directory; cleanup also failed: {cleanup:#}"
                        ))),
                    };
                }
                let entry = validate_video_staging_directory(root, &candidate, identity)?;
                let directory = root.open_bound_directory(&entry, identity)?;
                return Ok(BoundStagingDir {
                    root: root.clone(),
                    entry,
                    directory,
                    identity,
                    removed: false,
                    preserve_on_drop: AtomicBool::new(false),
                });
            }
            None => continue,
        }
    }
    bail!(
        "failed to allocate a unique staging directory under {}",
        parent.display()
    )
}

fn copy_bbdown_config_for_staging(
    root: &RootedFs,
    final_dir: &Path,
    staging_dir: &Path,
) -> Result<()> {
    let source = final_dir.join("BBDown.config");
    let Some(source_file) = root.open_bound_file(&source)? else {
        return Ok(());
    };
    let contents = source_file
        .read_limited(BILIBILI_STAGING_CONFIG_LIMIT)
        .with_context(|| format!("failed to read {}", source.display()))?;
    let destination = staging_dir.join("BBDown.config");
    root.create_new_bound_file(&destination, &contents, 0o600)
        .with_context(|| {
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
        || path == staging_dir.join(VIDEO_STAGING_OWNER_FILE_NAME)
        || path == staging_dir.join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME)
        || path == staging_dir.join(VIDEO_STAGING_RETENTION_FILE_NAME)
        || path == staging_dir.join(BILIBILI_WORKER_REQUEST_FILE_NAME)
}

fn validate_video_staging_directory(
    root: &RootedFs,
    path: &Path,
    expected_identity: EntryIdentity,
) -> Result<BoundEntry> {
    let entry = root.bind_entry(path, false)?;
    if root.bound_entry_identity(&entry)? != Some(expected_identity) || !expected_identity.is_dir()
    {
        bail!(
            "video staging directory identity changed: {}",
            path.display()
        );
    }
    root.validate_private_bound_directory(&entry, expected_identity, 0o700)?;
    let owner_path = path.join(VIDEO_STAGING_OWNER_FILE_NAME);
    let owner = root.open_bound_file(&owner_path)?.with_context(|| {
        format!(
            "video staging directory is not app-owned; missing {}",
            owner_path.display()
        )
    })?;
    owner.validate_private_single_link(0o600)?;
    let actual: VideoStagingOwner =
        serde_json::from_slice(&owner.read_limited(VIDEO_STAGING_OWNER_LIMIT)?)
            .context("failed to parse video staging ownership record")?;
    if actual.version != VIDEO_STAGING_OWNER_VERSION
        || actual.root_device != root.root_identity().device()
        || actual.root_inode != root.root_identity().inode()
        || actual.staging_device != expected_identity.device()
        || actual.staging_inode != expected_identity.inode()
    {
        bail!("video staging ownership record does not match the bound output root");
    }
    if root.entry_identity(&owner_path)? != Some(owner.identity()) {
        bail!("video staging ownership record identity changed after validation");
    }
    root.validate_private_bound_directory(&entry, expected_identity, 0o700)?;
    Ok(entry)
}

fn retained_video_staging_reason(
    root: &RootedFs,
    path: &Path,
    expected_identity: EntryIdentity,
) -> Result<Option<String>> {
    let marker_path = path.join(VIDEO_STAGING_RETENTION_FILE_NAME);
    retained_video_staging_reason_for_identity(
        root,
        &marker_path,
        root.root_identity().device(),
        root.root_identity().inode(),
        expected_identity.device(),
        expected_identity.inode(),
    )
}

#[allow(clippy::too_many_arguments)]
fn retained_video_staging_reason_for_identity(
    root: &RootedFs,
    marker_path: &Path,
    output_root_device: u64,
    output_root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
) -> Result<Option<String>> {
    let Some(marker) = root.open_bound_file(marker_path)? else {
        return Ok(None);
    };
    marker.validate_private_single_link(0o600)?;
    let retention: VideoStagingRetention =
        serde_json::from_slice(&marker.read_limited(VIDEO_STAGING_RETENTION_LIMIT)?)
            .context("failed to parse staged download retention marker")?;
    if retention.version != VIDEO_STAGING_RETENTION_VERSION
        || retention.root_device != output_root_device
        || retention.root_inode != output_root_inode
        || retention.staging_device != staging_device
        || retention.staging_inode != staging_inode
        || retention.reason.trim().is_empty()
    {
        bail!("staged download retention marker does not match its owned directory");
    }
    if root.entry_identity(marker_path)? != Some(marker.identity()) {
        bail!("staged download retention marker identity changed after validation");
    }
    Ok(Some(retention.reason))
}

#[allow(clippy::too_many_arguments)]
fn persist_video_staging_retention_marker(
    root: &RootedFs,
    marker_path: &Path,
    output_root_device: u64,
    output_root_inode: u64,
    staging_device: u64,
    staging_inode: u64,
    reason: &str,
) -> Result<()> {
    if reason.trim().is_empty() {
        bail!("staged download retention reason must not be empty");
    }
    if retained_video_staging_reason_for_identity(
        root,
        marker_path,
        output_root_device,
        output_root_inode,
        staging_device,
        staging_inode,
    )?
    .is_some()
    {
        return Ok(());
    }
    let marker = VideoStagingRetention {
        version: VIDEO_STAGING_RETENTION_VERSION,
        root_device: output_root_device,
        root_inode: output_root_inode,
        staging_device,
        staging_inode,
        reason: reason.to_string(),
    };
    let contents = serde_json::to_vec_pretty(&marker)
        .context("failed to encode staged download retention marker")?;
    if contents.len() > VIDEO_STAGING_RETENTION_LIMIT {
        bail!("staged download retention marker exceeds its size limit");
    }
    let (_, identity) = root
        .create_new_bound_file(marker_path, &contents, 0o600)
        .context("failed to persist staged download retention marker")?;
    let file = root
        .open_bound_file(marker_path)?
        .context("staged download retention marker disappeared")?;
    if file.identity() != identity {
        bail!("staged download retention marker identity changed after creation");
    }
    file.validate_private_single_link(0o600)?;
    Ok(())
}

#[derive(Debug, Default)]
struct VideoStagingRecoveryReport {
    messages: Vec<String>,
    unresolved: bool,
    blocked_overwrites: BTreeSet<StagedPublicationOverwrite>,
}

#[derive(Debug)]
struct VideoStagingRecoveryFailure {
    error: anyhow::Error,
    overwrite: Option<StagedPublicationOverwrite>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedPublicationDirection {
    RollForward,
    RollBack,
}

#[derive(Debug)]
struct IncompleteMoveRollback {
    error: anyhow::Error,
    rollback_error: anyhow::Error,
}

impl std::fmt::Display for IncompleteMoveRollback {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{:#}; staged-file rollback failed: {:#}",
            self.error, self.rollback_error
        )
    }
}

impl std::error::Error for IncompleteMoveRollback {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

fn recover_pending_video_staging_directories_locked(
    root: &RootedFs,
) -> Result<VideoStagingRecoveryReport> {
    recover_pending_video_staging_directories_locked_with_hook(root, &mut |_, _| Ok(()))
}

fn recover_pending_video_staging_directories_locked_with_hook<F>(
    root: &RootedFs,
    hook: &mut F,
) -> Result<VideoStagingRecoveryReport>
where
    F: FnMut(StagedPublicationDirection, &StagedPublicationStep) -> Result<()>,
{
    let parent_path = root.logical_root_path().join(VIDEO_STAGING_DIR_NAME);
    let Some(parent_identity) = root.entry_identity(&parent_path)? else {
        return Ok(VideoStagingRecoveryReport::default());
    };
    if !parent_identity.is_dir() {
        bail!(
            "video staging root is not a directory: {}",
            parent_path.display()
        );
    }
    let parent_entry = root.bind_entry(&parent_path, false)?;
    let mut report = VideoStagingRecoveryReport::default();
    for (name, identity) in root.list_bound_directory(&parent_entry, parent_identity)? {
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("job-") && !name.starts_with(VIDEO_STAGING_INITIALIZING_DIR_PREFIX) {
            continue;
        }
        let path = parent_path.join(name);
        if !identity.is_dir() {
            report.unresolved = true;
            report.messages.push(format!(
                "Retained unresolved staged video job {}: path is not a directory",
                path.display()
            ));
            continue;
        }
        match validate_video_staging_directory(root, &path, identity) {
            Ok(entry) => {
                match recover_owned_video_staging_directory(root, &path, &entry, identity, hook) {
                    Ok(message) => report.messages.push(message),
                    Err(failure) => {
                        report.unresolved = true;
                        if let Some(overwrite) = failure.overwrite {
                            report.blocked_overwrites.insert(overwrite);
                        }
                        report.messages.push(format!(
                            "Retained unresolved staged video job {}: {:#}",
                            path.display(),
                            failure.error
                        ));
                    }
                }
            }
            Err(err) => {
                report.unresolved = true;
                report.messages.push(format!(
                    "Retained unowned staged video job {}: {err:#}",
                    path.display()
                ));
            }
        }
    }
    Ok(report)
}

fn recover_owned_video_staging_directory<F>(
    root: &RootedFs,
    path: &Path,
    entry: &BoundEntry,
    identity: EntryIdentity,
    hook: &mut F,
) -> std::result::Result<String, VideoStagingRecoveryFailure>
where
    F: FnMut(StagedPublicationDirection, &StagedPublicationStep) -> Result<()>,
{
    let mut overwrite = None;
    let result = (|| -> Result<String> {
        let manifest_path = path.join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME);
        let Some(manifest_file) = root.open_bound_file(&manifest_path)? else {
            if let Some(reason) = retained_video_staging_reason(root, path, identity)? {
                return Ok(format!(
                    "Retained completed staged video job for manual recovery: {} ({reason})",
                    path.display()
                ));
            }
            root.remove_bound_tree_durably_if_identity(entry, identity)?;
            return Ok(format!(
                "Discarded interrupted staged video job: {}",
                path.display()
            ));
        };
        manifest_file.validate_private_single_link(0o600)?;
        let manifest: StagedPublicationManifest = serde_json::from_slice(
            &manifest_file.read_limited(VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT)?,
        )
        .context("failed to parse staged publication manifest")?;
        validate_staged_publication_manifest(root, path, identity, &manifest)?;
        overwrite = manifest.overwrite.clone();
        let direction = staged_publication_direction(root, manifest.overwrite.as_ref())?;
        match direction {
            StagedPublicationDirection::RollForward => {
                for step in &manifest.steps {
                    recover_staged_publication_step(root, step, direction, hook)?;
                }
            }
            StagedPublicationDirection::RollBack => {
                for step in manifest.steps.iter().rev() {
                    recover_staged_publication_step(root, step, direction, hook)?;
                }
            }
        }
        validate_video_staging_directory(root, path, identity)?;
        root.remove_bound_tree_durably_if_identity(entry, identity)?;
        let action = match direction {
            StagedPublicationDirection::RollForward => "Rolled forward",
            StagedPublicationDirection::RollBack => "Rolled back",
        };
        Ok(format!(
            "{action} interrupted staged video publication: {}",
            path.display()
        ))
    })();
    result.map_err(|error| VideoStagingRecoveryFailure { error, overwrite })
}

fn validate_staged_publication_manifest(
    root: &RootedFs,
    staging_path: &Path,
    staging_identity: EntryIdentity,
    manifest: &StagedPublicationManifest,
) -> Result<()> {
    if manifest.version != VIDEO_STAGING_PUBLICATION_MANIFEST_VERSION
        || manifest.root_device != root.root_identity().device()
        || manifest.root_inode != root.root_identity().inode()
        || manifest.staging_device != staging_identity.device()
        || manifest.staging_inode != staging_identity.inode()
        || manifest.steps.is_empty()
        || manifest.steps.len() > VIDEO_STAGING_PUBLICATION_MAX_STEPS
    {
        bail!("staged publication manifest does not match the owned staging directory");
    }
    let mut sources = BTreeSet::new();
    let mut destinations = BTreeSet::new();
    for step in &manifest.steps {
        validate_relative_publication_path(&step.source_path)?;
        validate_relative_publication_path(&step.destination_path)?;
        let source = root.logical_root_path().join(&step.source_path);
        let destination = root.logical_root_path().join(&step.destination_path);
        let staged_relative = source.strip_prefix(staging_path).with_context(|| {
            format!(
                "staged publication source is outside the owned job directory: {}",
                source.display()
            )
        })?;
        validate_relative_publication_path(staged_relative)?;
        if destination.starts_with(staging_path)
            || !sources.insert(step.source_path.clone())
            || !destinations.insert(step.destination_path.clone())
        {
            bail!("staged publication manifest contains conflicting paths");
        }
    }
    if let Some(overwrite) = &manifest.overwrite {
        validate_relative_publication_path(&overwrite.transaction_path)?;
    }
    Ok(())
}

fn staged_publication_direction(
    root: &RootedFs,
    overwrite: Option<&StagedPublicationOverwrite>,
) -> Result<StagedPublicationDirection> {
    let Some(overwrite) = overwrite else {
        return Ok(StagedPublicationDirection::RollForward);
    };
    let transaction = root.logical_root_path().join(&overwrite.transaction_path);
    let Some(transaction_identity) = root.entry_identity(&transaction)? else {
        return Ok(StagedPublicationDirection::RollForward);
    };
    if !transaction_identity.is_dir()
        || transaction_identity.device() != overwrite.transaction_device
        || transaction_identity.inode() != overwrite.transaction_inode
    {
        bail!("staged publication overwrite transaction identity changed");
    }
    let transaction_entry = root.bind_entry(&transaction, false)?;
    root.validate_private_bound_directory(&transaction_entry, transaction_identity, 0o700)?;
    let manifest_path = transaction.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
    let manifest_file = root
        .open_bound_file(&manifest_path)?
        .context("staged publication overwrite transaction has no recovery manifest")?;
    manifest_file.validate_private_single_link(0o600)?;
    let manifest: OverwriteRecoveryManifest =
        serde_json::from_slice(&manifest_file.read_limited(OVERWRITE_RECOVERY_MANIFEST_LIMIT)?)
            .context("failed to parse staged publication overwrite transaction")?;
    if manifest.version != OVERWRITE_RECOVERY_MANIFEST_VERSION {
        bail!("staged publication overwrite transaction has no ownership binding");
    }
    validate_overwrite_recovery_ownership(root, &transaction, transaction_identity, &manifest)?;
    Ok(match manifest.phase {
        OverwriteRecoveryPhase::Acquired => StagedPublicationDirection::RollBack,
        OverwriteRecoveryPhase::Committed => StagedPublicationDirection::RollForward,
    })
}

fn recover_staged_publication_step<F>(
    root: &RootedFs,
    step: &StagedPublicationStep,
    direction: StagedPublicationDirection,
    hook: &mut F,
) -> Result<()>
where
    F: FnMut(StagedPublicationDirection, &StagedPublicationStep) -> Result<()>,
{
    let source = root.logical_root_path().join(&step.source_path);
    let destination = root.logical_root_path().join(&step.destination_path);
    let (from, to) = match direction {
        StagedPublicationDirection::RollForward => (&source, &destination),
        StagedPublicationDirection::RollBack => (&destination, &source),
    };
    let from_identity = publication_step_identity(root, from, step)?;
    let to_identity = publication_step_identity(root, to, step)?;
    let (identity, moved) = match (from_identity, to_identity) {
        (Some(identity), None) => {
            let from_entry = root.bind_entry(from, false)?;
            let to_entry = root.bind_entry(to, true)?;
            root.rename_via_bound_parents_noreplace_if_identity(&from_entry, &to_entry, identity)?;
            (identity, true)
        }
        (None, Some(identity)) => (identity, false),
        (Some(_), Some(_)) => bail!(
            "staged publication object exists at both recovery paths: {} and {}",
            from.display(),
            to.display()
        ),
        (None, None) => bail!(
            "staged publication object is missing from both recovery paths: {} and {}",
            from.display(),
            to.display()
        ),
    };
    if moved {
        hook(direction, step)?;
    }
    let file = root
        .open_bound_file(to)?
        .with_context(|| format!("recovered publication file is missing: {}", to.display()))?;
    if file.identity() != identity {
        bail!(
            "recovered publication file identity changed: {}",
            to.display()
        );
    }
    file.sync_all().with_context(|| {
        format!(
            "failed to persist recovered publication file {}",
            to.display()
        )
    })
}

fn publication_step_identity(
    root: &RootedFs,
    path: &Path,
    step: &StagedPublicationStep,
) -> Result<Option<EntryIdentity>> {
    let Some(identity) = root.entry_identity(path)? else {
        return Ok(None);
    };
    if !identity.is_file() || identity.device() != step.device || identity.inode() != step.inode {
        bail!(
            "staged publication path contains an unexpected object: {}",
            path.display()
        );
    }
    Ok(Some(identity))
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
    expected_identity: Option<EntryIdentity>,
}

#[derive(Clone, Copy)]
struct MoveExecutionContext<'a> {
    root: &'a RootedFs,
    staging: Option<&'a BoundStagingDir>,
}

#[derive(Debug)]
struct MovedFile {
    source: PathBuf,
    destination: PathBuf,
    source_entry: BoundEntry,
    destination_entry: BoundEntry,
    identity: EntryIdentity,
}

#[derive(Debug)]
struct MovePlanResult {
    moved: Vec<MovedFile>,
    moved_videos: Vec<PathBuf>,
}

#[derive(Debug)]
struct FileBackup {
    original: PathBuf,
    backup: PathBuf,
    original_entry: BoundEntry,
    backup_entry: BoundEntry,
    identity: EntryIdentity,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverwriteRecoveryManifest {
    version: u32,
    #[serde(default)]
    root_device: Option<u64>,
    #[serde(default)]
    root_inode: Option<u64>,
    #[serde(default)]
    parent_device: Option<u64>,
    #[serde(default)]
    parent_inode: Option<u64>,
    #[serde(default)]
    transaction_device: Option<u64>,
    #[serde(default)]
    transaction_inode: Option<u64>,
    target_file_name: PathBuf,
    phase: OverwriteRecoveryPhase,
    committed_files: Vec<OverwriteCommittedFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OverwriteRecoveryPhase {
    Acquired,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverwriteCommittedFile {
    file_name: PathBuf,
    device: u64,
    inode: u64,
    #[serde(default)]
    anchor_name: Option<PathBuf>,
}

#[derive(Debug)]
struct OverwriteRecoveryFiles {
    backup_dir: PathBuf,
    backup_dir_entry: BoundEntry,
    backup_dir_identity: EntryIdentity,
    manifest_path: PathBuf,
    manifest_entry: BoundEntry,
    manifest_identity: EntryIdentity,
}

#[derive(Debug)]
struct CommittedOverwriteState {
    manifest: OverwriteRecoveryManifest,
    files: BTreeMap<PathBuf, BoundFile>,
    anchors: BTreeMap<PathBuf, BoundFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverwriteCommitCheckpoint {
    BeforeOutputSync,
    BeforeAnchorCreation,
    BeforeManifestReplace,
    AfterManifestReplace,
}

#[derive(Debug)]
struct AcquiredOverwrite {
    root: RootedFs,
    target: PathBuf,
    backups: Vec<FileBackup>,
    recovery: OverwriteRecoveryFiles,
    target_restored: Option<(BoundEntry, EntryIdentity)>,
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
        if let Err(err) = restore_file_backup(&self.root, &backup) {
            self.backups.insert(index, backup);
            return Err(err);
        }
        if original == self.target {
            self.target_restored = Some((backup.original_entry.clone(), backup.identity));
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
        if self.backups.is_empty() {
            return remove_overwrite_transaction_dir(&self.root, &self.recovery);
        }
        if let Some((target_entry, target_identity)) = &self.target_restored {
            restore_remaining_backups(
                &self.root,
                &self.backups,
                &self.recovery,
                target_entry,
                *target_identity,
            )
        } else {
            restore_backups(&self.root, &self.backups, &self.recovery, &self.target)
        }
    }

    fn commit_with_hook<F>(
        mut self,
        moved: &[MovedFile],
        committed_target: &Path,
        hook: &mut F,
    ) -> Result<()>
    where
        F: FnMut(OverwriteCommitCheckpoint) -> Result<()>,
    {
        let committed = persist_committed_overwrite_manifest_with_hook(
            &mut self,
            moved,
            committed_target,
            hook,
        )?;
        remove_backups(&self.root, &self.backups, &self.recovery, &committed)
    }
}

#[derive(Debug)]
struct BoundStagingDir {
    root: RootedFs,
    entry: BoundEntry,
    directory: BoundDirectory,
    identity: EntryIdentity,
    removed: bool,
    preserve_on_drop: AtomicBool,
}

impl BoundStagingDir {
    fn path(&self) -> &Path {
        self.entry.path()
    }

    fn validate_for_path_access(&self) -> Result<()> {
        self.root.validate_configured_root()?;
        self.directory.validate_identity()?;
        validate_video_staging_directory(&self.root, self.path(), self.identity).map(|_| ())
    }

    fn finish(mut self) -> Result<()> {
        self.root
            .remove_bound_tree_durably_if_identity(&self.entry, self.identity)?;
        self.removed = true;
        Ok(())
    }

    fn preserve_for_recovery(&self) {
        self.preserve_on_drop.store(true, Ordering::Release);
    }

    fn retain_for_manual_recovery(&self, reason: &str) -> Result<()> {
        self.preserve_for_recovery();
        self.validate_for_path_access()?;
        let marker_path = self.path().join(VIDEO_STAGING_RETENTION_FILE_NAME);
        persist_video_staging_retention_marker(
            &self.root,
            &marker_path,
            self.root.root_identity().device(),
            self.root.root_identity().inode(),
            self.identity.device(),
            self.identity.inode(),
            reason,
        )?;
        self.validate_for_path_access()
    }
}

impl Drop for BoundStagingDir {
    fn drop(&mut self) {
        if self.removed || self.preserve_on_drop.load(Ordering::Acquire) {
            return;
        }
        if let Err(err) = self
            .root
            .remove_bound_tree_durably_if_identity(&self.entry, self.identity)
        {
            info!(
                path = %self.entry.path().display(),
                error = %err,
                "failed to clean bound staging directory"
            );
        }
    }
}

fn prepare_staged_publication(
    root: &RootedFs,
    staging: &BoundStagingDir,
    plan: &mut [MoveStep],
    overwrite: Option<&AcquiredOverwrite>,
) -> Result<()> {
    prepare_staged_publication_with_limits_and_sync(
        root,
        staging,
        plan,
        overwrite,
        VIDEO_STAGING_PUBLICATION_MAX_STEPS,
        VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT,
        &mut |path, directory| {
            directory.sync_all().with_context(|| {
                format!(
                    "failed to persist staged publication source directory {}",
                    path.display()
                )
            })
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_staged_publication_with_limits_and_sync<F>(
    root: &RootedFs,
    staging: &BoundStagingDir,
    plan: &mut [MoveStep],
    overwrite: Option<&AcquiredOverwrite>,
    max_steps: usize,
    manifest_limit: usize,
    sync_source_directory: &mut F,
) -> Result<()>
where
    F: FnMut(&Path, &BoundDirectory) -> Result<()>,
{
    if plan.is_empty() {
        bail!("staged publication has an invalid number of move steps");
    }
    staging.validate_for_path_access()?;
    let plan_len = plan.len();
    let mut sources = BTreeSet::new();
    let mut destinations = BTreeSet::new();
    let mut source_directories = BTreeSet::new();
    let mut records = Vec::with_capacity(plan.len().min(max_steps.saturating_add(1)));
    for step in plan {
        let source_relative = publication_relative_path(root, &step.source)?;
        let destination_relative = publication_relative_path(root, &step.destination)?;
        let staged_relative = step.source.strip_prefix(staging.path()).with_context(|| {
            format!(
                "staged publication source is outside its owned job directory: {}",
                step.source.display()
            )
        })?;
        validate_relative_publication_path(staged_relative)?;
        collect_staged_source_directories(staging.path(), &step.source, &mut source_directories)?;
        if step.destination.starts_with(staging.path()) {
            bail!(
                "staged publication destination remains inside staging: {}",
                step.destination.display()
            );
        }
        if !sources.insert(source_relative.clone())
            || !destinations.insert(destination_relative.clone())
        {
            bail!("staged publication repeats a source or destination path");
        }
        let destination = root.bind_entry(&step.destination, true)?;
        if root.bound_entry_identity(&destination)?.is_some() {
            bail!(
                "staged publication destination is already occupied: {}",
                step.destination.display()
            );
        }
        let source = root.open_bound_file(&step.source)?.with_context(|| {
            format!(
                "staged publication source is missing: {}",
                step.source.display()
            )
        })?;
        source.sync_all().with_context(|| {
            format!(
                "failed to persist staged publication source {}",
                step.source.display()
            )
        })?;
        let identity = source.identity();
        step.expected_identity = Some(identity);
        if plan_len <= max_steps {
            records.push(StagedPublicationStep {
                source_path: source_relative,
                destination_path: destination_relative,
                device: identity.device(),
                inode: identity.inode(),
            });
        }
    }

    sync_staged_source_directories(root, staging, source_directories, sync_source_directory)?;
    if plan_len > max_steps {
        return Err(retain_oversized_staged_publication(
            staging,
            format!(
                "publication requires {} files, exceeding the {}-file recovery limit",
                plan_len, max_steps
            ),
        ));
    }

    let overwrite = overwrite
        .map(|acquired| {
            if acquired.root.root_identity() != root.root_identity() {
                bail!("overwrite transaction belongs to a different output root");
            }
            Ok(StagedPublicationOverwrite {
                transaction_path: publication_relative_path(root, &acquired.recovery.backup_dir)?,
                transaction_device: acquired.recovery.backup_dir_identity.device(),
                transaction_inode: acquired.recovery.backup_dir_identity.inode(),
            })
        })
        .transpose()?;
    let manifest = StagedPublicationManifest {
        version: VIDEO_STAGING_PUBLICATION_MANIFEST_VERSION,
        root_device: root.root_identity().device(),
        root_inode: root.root_identity().inode(),
        staging_device: staging.identity.device(),
        staging_inode: staging.identity.inode(),
        overwrite,
        steps: records,
    };
    let contents = serde_json::to_vec_pretty(&manifest)
        .context("failed to encode staged publication manifest")?;
    if contents.len() > manifest_limit {
        return Err(retain_oversized_staged_publication(
            staging,
            format!(
                "publication recovery manifest requires {} bytes, exceeding the {}-byte limit",
                contents.len(),
                manifest_limit
            ),
        ));
    }
    let manifest_path = staging.path().join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME);
    let (_, identity) = root
        .create_new_bound_file(&manifest_path, &contents, 0o600)
        .context("failed to persist staged publication manifest")?;
    let file = root
        .open_bound_file(&manifest_path)?
        .context("staged publication manifest disappeared")?;
    if file.identity() != identity {
        bail!("staged publication manifest identity changed after creation");
    }
    file.validate_private_single_link(0o600)?;
    staging.validate_for_path_access()
}

fn collect_staged_source_directories(
    staging_path: &Path,
    source_path: &Path,
    directories: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let mut current = source_path
        .parent()
        .context("staged publication source has no parent directory")?;
    loop {
        if !current.starts_with(staging_path) {
            bail!(
                "staged publication source directory is outside its owned job directory: {}",
                current.display()
            );
        }
        directories.insert(current.to_path_buf());
        if current == staging_path {
            return Ok(());
        }
        current = current
            .parent()
            .context("staged publication source directory has no staging ancestor")?;
    }
}

fn sync_staged_source_directories<F>(
    root: &RootedFs,
    staging: &BoundStagingDir,
    directories: BTreeSet<PathBuf>,
    sync_source_directory: &mut F,
) -> Result<()>
where
    F: FnMut(&Path, &BoundDirectory) -> Result<()>,
{
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by(|left, right| {
        right
            .components()
            .count()
            .cmp(&left.components().count())
            .then_with(|| left.cmp(right))
    });
    for path in directories {
        if path == staging.path() {
            sync_source_directory(&path, &staging.directory)?;
            continue;
        }
        let entry = root.bind_entry(&path, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .with_context(|| format!("staged source directory is missing: {}", path.display()))?;
        if !identity.is_dir() {
            bail!(
                "staged source directory is not a directory: {}",
                path.display()
            );
        }
        let directory = root.open_bound_directory(&entry, identity)?;
        sync_source_directory(&path, &directory)?;
    }
    staging.validate_for_path_access()
}

fn retain_oversized_staged_publication(staging: &BoundStagingDir, reason: String) -> anyhow::Error {
    match staging.retain_for_manual_recovery(&reason) {
        Ok(()) => anyhow!(
            "{reason}; completed outputs were retained at {} for manual recovery",
            staging.path().display()
        ),
        Err(retention_error) => anyhow!(
            "{reason}; failed to persist the manual-recovery marker at {}: {retention_error:#}",
            staging.path().display()
        ),
    }
}

fn publication_relative_path(root: &RootedFs, path: &Path) -> Result<PathBuf> {
    let relative = path
        .strip_prefix(root.logical_root_path())
        .with_context(|| {
            format!(
                "publication path is outside the output root: {}",
                path.display()
            )
        })?;
    validate_relative_publication_path(relative)?;
    Ok(relative.to_path_buf())
}

fn validate_relative_publication_path(path: &Path) -> Result<()> {
    let mut count = 0;
    for component in path.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            bail!("publication manifest contains an invalid relative path");
        }
        count += 1;
    }
    if count == 0 {
        bail!("publication manifest contains an empty relative path");
    }
    Ok(())
}

fn acquire_and_validate_overwrite_target(
    root: &RootedFs,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<AcquiredOverwrite> {
    // Object ownership is tracked by device/inode/type; timestamps are not identity evidence.
    let target = duplicate
        .overwrite_target()
        .context("overwrite target is not an exact unique match")?
        .clone();
    let confirmation = duplicate
        .overwrite_confirmation
        .as_ref()
        .context("overwrite target was not bound when replacement was confirmed")?;
    if root.root_identity() != confirmation.root_identity {
        bail!("output root changed after overwrite confirmation");
    }
    confirmation.target_file.validate_identity()?;
    let confirmed_target_identity = confirmation.target_file.identity();
    match root.entry_identity(&target)? {
        Some(current) if current == confirmed_target_identity => {}
        Some(_) => bail!(
            "overwrite target changed after confirmation: {}",
            target.display()
        ),
        None => bail!("overwrite target is missing: {}", target.display()),
    }

    let mut artifacts = existing_video_artifacts(&target)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    artifacts.remove(&target);
    artifacts.extend(existing_metadata_sidecar_paths(&target)?);
    artifacts.extend(expected_metadata_sidecar_paths(&target));
    let backup_parent = target.parent().context("overwrite target has no parent")?;
    let (backup_dir, backup_dir_entry, backup_dir_identity) =
        create_overwrite_backup_dir(root, backup_parent)?;
    let (manifest_path, manifest_entry, manifest_identity) =
        match create_overwrite_recovery_manifest(root, &backup_dir, backup_dir_identity, &target) {
            Ok(manifest) => manifest,
            Err(err) => {
                let cleanup =
                    root.remove_bound_dir_if_identity(&backup_dir_entry, backup_dir_identity);
                return Err(match cleanup {
                    Ok(()) => err,
                    Err(cleanup) => anyhow!(
                        "{err:#}; failed to clean empty overwrite transaction {}: {cleanup:#}",
                        backup_dir.display()
                    ),
                });
            }
        };
    let mut acquired = AcquiredOverwrite {
        root: root.clone(),
        target,
        backups: Vec::new(),
        recovery: OverwriteRecoveryFiles {
            backup_dir,
            backup_dir_entry,
            backup_dir_identity,
            manifest_path,
            manifest_entry,
            manifest_identity,
        },
        target_restored: None,
    };

    let acquired_target = acquired.target.clone();
    if let Err(err) = acquire_overwrite_path(
        root,
        &acquired_target,
        true,
        Some(confirmed_target_identity),
        &mut acquired,
    ) {
        return Err(rollback_acquired_overwrite(err, acquired, None));
    }
    for artifact in artifacts {
        if let Err(err) = acquire_overwrite_path(root, &artifact, false, None, &mut acquired) {
            return Err(rollback_acquired_overwrite(err, acquired, None));
        }
    }

    if let Err(err) = validate_acquired_overwrite(root, duplicate, primary_media_kind, &acquired) {
        return Err(rollback_acquired_overwrite(err, acquired, None));
    }
    Ok(acquired)
}

fn create_overwrite_backup_dir(
    root: &RootedFs,
    parent: &Path,
) -> Result<(PathBuf, BoundEntry, EntryIdentity)> {
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
        match root.create_dir(&path, 0o700)? {
            Some(identity) => {
                let entry = root.bind_entry(&path, false)?;
                if root.bound_entry_identity(&entry)? != Some(identity) {
                    let _ = root.remove_bound_dir_if_identity(&entry, identity);
                    bail!(
                        "overwrite backup directory identity changed while binding: {}",
                        path.display()
                    );
                }
                return Ok((path, entry, identity));
            }
            None => continue,
        }
    }
    bail!("failed to allocate a unique overwrite backup directory")
}

fn create_overwrite_recovery_manifest(
    root: &RootedFs,
    backup_dir: &Path,
    backup_dir_identity: EntryIdentity,
    target: &Path,
) -> Result<(PathBuf, BoundEntry, EntryIdentity)> {
    if target.parent() != backup_dir.parent() {
        bail!(
            "overwrite target and recovery directory do not share a parent: {}",
            target.display()
        );
    }
    let target_file_name = PathBuf::from(
        target
            .file_name()
            .context("overwrite target has no file name")?,
    );
    let manifest = overwrite_recovery_manifest(
        root,
        backup_dir,
        backup_dir_identity,
        target_file_name,
        OverwriteRecoveryPhase::Acquired,
        Vec::new(),
    )?;
    let contents = encode_overwrite_recovery_manifest(&manifest)?;
    let manifest_path = backup_dir.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
    let (manifest_entry, manifest_identity) = root
        .create_new_bound_file(&manifest_path, &contents, 0o600)
        .with_context(|| {
            format!(
                "failed to persist overwrite recovery manifest {}",
                manifest_path.display()
            )
        })?;
    Ok((manifest_path, manifest_entry, manifest_identity))
}

fn overwrite_recovery_manifest(
    root: &RootedFs,
    backup_dir: &Path,
    backup_dir_identity: EntryIdentity,
    target_file_name: PathBuf,
    phase: OverwriteRecoveryPhase,
    committed_files: Vec<OverwriteCommittedFile>,
) -> Result<OverwriteRecoveryManifest> {
    let backup_entry = root.bind_entry(backup_dir, false)?;
    root.validate_private_bound_directory(&backup_entry, backup_dir_identity, 0o700)?;
    let parent = backup_dir
        .parent()
        .context("overwrite transaction has no parent")?;
    let parent_identity = output_directory_identity(root, parent)?;
    if !parent_identity.is_dir() {
        bail!("overwrite transaction parent is not a directory");
    }
    Ok(OverwriteRecoveryManifest {
        version: OVERWRITE_RECOVERY_MANIFEST_VERSION,
        root_device: Some(root.root_identity().device()),
        root_inode: Some(root.root_identity().inode()),
        parent_device: Some(parent_identity.device()),
        parent_inode: Some(parent_identity.inode()),
        transaction_device: Some(backup_dir_identity.device()),
        transaction_inode: Some(backup_dir_identity.inode()),
        target_file_name,
        phase,
        committed_files,
    })
}

fn encode_overwrite_recovery_manifest(manifest: &OverwriteRecoveryManifest) -> Result<Vec<u8>> {
    let contents = serde_json::to_vec_pretty(manifest)
        .context("failed to encode overwrite recovery manifest")?;
    if contents.len() > OVERWRITE_RECOVERY_MANIFEST_LIMIT {
        bail!("overwrite recovery manifest exceeds its size limit");
    }
    Ok(contents)
}

fn validate_overwrite_recovery_ownership(
    root: &RootedFs,
    backup_dir: &Path,
    backup_dir_identity: EntryIdentity,
    manifest: &OverwriteRecoveryManifest,
) -> Result<()> {
    let parent = backup_dir
        .parent()
        .context("overwrite transaction has no parent")?;
    let parent_identity = output_directory_identity(root, parent)?;
    if !parent_identity.is_dir()
        || manifest.root_device != Some(root.root_identity().device())
        || manifest.root_inode != Some(root.root_identity().inode())
        || manifest.parent_device != Some(parent_identity.device())
        || manifest.parent_inode != Some(parent_identity.inode())
        || manifest.transaction_device != Some(backup_dir_identity.device())
        || manifest.transaction_inode != Some(backup_dir_identity.inode())
    {
        bail!("overwrite recovery ownership record does not match the bound output tree");
    }
    let entry = root.bind_entry(backup_dir, false)?;
    root.validate_private_bound_directory(&entry, backup_dir_identity, 0o700)
}

fn output_directory_identity(root: &RootedFs, path: &Path) -> Result<EntryIdentity> {
    if path == root.logical_root_path() {
        return Ok(root.root_identity());
    }
    root.entry_identity(path)?
        .context("output transaction parent is missing")
}

fn persist_committed_overwrite_manifest(
    acquired: &mut AcquiredOverwrite,
    moved: &[MovedFile],
    committed_target: &Path,
) -> Result<CommittedOverwriteState> {
    persist_committed_overwrite_manifest_with_hook(acquired, moved, committed_target, &mut |_| {
        Ok(())
    })
}

fn persist_committed_overwrite_manifest_with_hook<F>(
    acquired: &mut AcquiredOverwrite,
    moved: &[MovedFile],
    committed_target: &Path,
    hook: &mut F,
) -> Result<CommittedOverwriteState>
where
    F: FnMut(OverwriteCommitCheckpoint) -> Result<()>,
{
    let transaction_parent = acquired
        .recovery
        .backup_dir
        .parent()
        .context("overwrite transaction has no parent")?;
    let mut identities = BTreeMap::new();
    for moved in moved {
        if moved.destination.parent() != Some(transaction_parent) {
            continue;
        }
        require_bound_entry_identity(
            &acquired.root,
            &moved.destination_entry,
            moved.identity,
            "committed overwrite file",
        )?;
        insert_committed_file_identity(&mut identities, &moved.destination, moved.identity)?;
    }
    if let Some((entry, identity)) = &acquired.target_restored {
        require_bound_entry_identity(
            &acquired.root,
            entry,
            *identity,
            "restored artifact-overwrite target",
        )?;
        insert_committed_file_identity(&mut identities, entry.path(), *identity)?;
    }

    if committed_target.parent() != Some(transaction_parent) {
        bail!(
            "committed overwrite target is outside the transaction parent: {}",
            committed_target.display()
        );
    }
    let target_file_name = PathBuf::from(
        committed_target
            .file_name()
            .context("committed overwrite target has no file name")?,
    );
    if !identities.contains_key(&target_file_name) {
        bail!(
            "overwrite commit did not retain a bound target object: {}",
            committed_target.display()
        );
    }

    let committed_files = committed_overwrite_file_records(&identities);
    let manifest = overwrite_recovery_manifest(
        &acquired.root,
        &acquired.recovery.backup_dir,
        acquired.recovery.backup_dir_identity,
        target_file_name,
        OverwriteRecoveryPhase::Committed,
        committed_files.clone(),
    )?;
    let contents = encode_overwrite_recovery_manifest(&manifest)?;

    let mut files = BTreeMap::new();
    for (file_name, identity) in &identities {
        let path = transaction_parent.join(file_name);
        let file = acquired
            .root
            .open_bound_file(&path)?
            .with_context(|| format!("committed overwrite file is missing: {}", path.display()))?;
        if file.identity() != *identity {
            bail!(
                "committed overwrite file identity changed: {}",
                path.display()
            );
        }
        hook(OverwriteCommitCheckpoint::BeforeOutputSync)?;
        file.sync_all().with_context(|| {
            format!(
                "failed to persist committed overwrite file {} before publishing recovery state",
                path.display()
            )
        })?;
        files.insert(file_name.clone(), file);
    }
    let anchors = create_committed_output_anchors(
        acquired,
        transaction_parent,
        &identities,
        &committed_files,
        hook,
    )?;
    let temp_path = acquired
        .recovery
        .backup_dir
        .join(OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME);
    hook(OverwriteCommitCheckpoint::BeforeManifestReplace)?;
    let (manifest_entry, manifest_identity) = acquired
        .root
        .replace_bound_file_atomically_if_identity(
            &acquired.recovery.manifest_entry,
            acquired.recovery.manifest_identity,
            &temp_path,
            &contents,
            0o600,
        )
        .with_context(|| {
            format!(
                "failed to persist committed overwrite state {}",
                acquired.recovery.manifest_path.display()
            )
        })?;
    acquired.recovery.manifest_entry = manifest_entry;
    acquired.recovery.manifest_identity = manifest_identity;
    hook(OverwriteCommitCheckpoint::AfterManifestReplace)?;
    let committed = CommittedOverwriteState {
        manifest,
        files,
        anchors,
    };
    validate_committed_overwrite_state(
        &acquired.root,
        &acquired.backups,
        &acquired.recovery,
        &committed,
    )?;
    Ok(committed)
}

fn committed_overwrite_file_records(
    identities: &BTreeMap<PathBuf, EntryIdentity>,
) -> Vec<OverwriteCommittedFile> {
    identities
        .iter()
        .enumerate()
        .map(|(index, (file_name, identity))| OverwriteCommittedFile {
            file_name: file_name.clone(),
            device: identity.device(),
            inode: identity.inode(),
            anchor_name: Some(PathBuf::from(format!(
                "{OVERWRITE_COMMITTED_ANCHOR_PREFIX}{index:04x}"
            ))),
        })
        .collect()
}

fn create_committed_output_anchors<F>(
    acquired: &AcquiredOverwrite,
    transaction_parent: &Path,
    identities: &BTreeMap<PathBuf, EntryIdentity>,
    committed_files: &[OverwriteCommittedFile],
    hook: &mut F,
) -> Result<BTreeMap<PathBuf, BoundFile>>
where
    F: FnMut(OverwriteCommitCheckpoint) -> Result<()>,
{
    if committed_files.len() != identities.len() {
        bail!("committed overwrite records do not match the bound outputs");
    }
    let mut anchors = BTreeMap::new();
    for committed in committed_files {
        let identity = identities
            .get(&committed.file_name)
            .context("committed overwrite record has no bound output")?;
        if committed.device != identity.device() || committed.inode != identity.inode() {
            bail!("committed overwrite record identity changed before anchoring");
        }
        let anchor_name = committed
            .anchor_name
            .as_ref()
            .context("committed overwrite record has no anchor name")?;
        let source_path = transaction_parent.join(&committed.file_name);
        let anchor_path = acquired.recovery.backup_dir.join(anchor_name);
        let source_entry = acquired.root.bind_entry(&source_path, false)?;
        let anchor_entry = acquired.root.bind_entry(&anchor_path, false)?;
        hook(OverwriteCommitCheckpoint::BeforeAnchorCreation)?;
        acquired
            .root
            .hard_link_via_bound_parents_noreplace_if_identity(
                &source_entry,
                &anchor_entry,
                *identity,
            )
            .with_context(|| {
                format!(
                    "failed to anchor committed overwrite output {}",
                    source_path.display()
                )
            })?;
        let anchor_file = acquired
            .root
            .open_bound_file(&anchor_path)?
            .with_context(|| {
                format!(
                    "committed output anchor is missing: {}",
                    anchor_path.display()
                )
            })?;
        if anchor_file.identity() != *identity {
            bail!(
                "committed output anchor identity changed: {}",
                anchor_path.display()
            );
        }
        anchors.insert(anchor_name.clone(), anchor_file);
    }
    Ok(anchors)
}

fn insert_committed_file_identity(
    identities: &mut BTreeMap<PathBuf, EntryIdentity>,
    path: &Path,
    identity: EntryIdentity,
) -> Result<()> {
    let file_name = PathBuf::from(
        path.file_name()
            .context("committed overwrite path has no file name")?,
    );
    if let Some(previous) = identities.insert(file_name.clone(), identity)
        && previous != identity
    {
        bail!(
            "multiple committed objects used overwrite file name {}",
            file_name.display()
        );
    }
    Ok(())
}

fn acquire_overwrite_path(
    root: &RootedFs,
    original: &Path,
    required: bool,
    expected_identity: Option<EntryIdentity>,
    acquired: &mut AcquiredOverwrite,
) -> Result<()> {
    let identity = match root.entry_identity(original)? {
        Some(identity) => identity,
        None if !required => return Ok(()),
        None => {
            bail!("overwrite target is missing: {}", original.display());
        }
    };
    if !identity.is_file() {
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
    if expected_identity.is_some_and(|expected| expected != identity) {
        bail!(
            "overwrite target changed after confirmation: {}",
            original.display()
        );
    }

    if original.parent() != acquired.recovery.backup_dir.parent() {
        bail!(
            "refused to acquire overwrite path outside the transaction parent: {}",
            original.display()
        );
    }
    let file_name = original
        .file_name()
        .context("overwrite path has no file name")?;
    if is_overwrite_recovery_control_name(file_name) {
        bail!(
            "overwrite path conflicts with a reserved recovery name: {}",
            original.display()
        );
    }
    let backup = acquired.recovery.backup_dir.join(file_name);
    let original_entry = root.bind_entry(original, false)?;
    let backup_entry = root.bind_entry(&backup, false)?;
    root.rename_via_bound_parents_noreplace_if_identity(&original_entry, &backup_entry, identity)
        .with_context(|| {
            format!(
                "failed to acquire overwrite path {} as {}",
                original.display(),
                backup.display()
            )
        })?;
    acquired.backups.push(FileBackup {
        original: original.to_path_buf(),
        backup: backup.clone(),
        original_entry,
        backup_entry,
        identity,
    });
    require_bound_entry_identity(
        root,
        &acquired
            .backups
            .last()
            .expect("backup was just pushed")
            .backup_entry,
        identity,
        "acquired overwrite path",
    )?;
    Ok(())
}

fn validate_acquired_overwrite(
    root: &RootedFs,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
    acquired: &AcquiredOverwrite,
) -> Result<()> {
    require_entry_identity(
        root,
        &acquired.recovery.backup_dir,
        acquired.recovery.backup_dir_identity,
        "overwrite backup directory",
    )?;
    let target_backup = acquired
        .backup_for(acquired.target())
        .context("acquired overwrite target backup is missing")?;
    let target_identity = acquired
        .backups
        .iter()
        .find(|backup| backup.backup == target_backup)
        .map(|backup| backup.identity)
        .context("acquired overwrite target identity is missing")?;
    require_entry_identity(
        root,
        target_backup,
        target_identity,
        "acquired overwrite target",
    )?;
    if !is_primary_media_file(acquired.target(), primary_media_kind) {
        bail!(
            "acquired overwrite target is not a regular primary media file: {}",
            acquired.target().display()
        );
    }

    let mut acquired_index = VideoIdentityIndex::default();
    index_video_filename_identities(&mut acquired_index, acquired.target());
    let mut identity_metadata_paths = expected_metadata_sidecar_paths(acquired.target())
        .into_iter()
        .collect::<BTreeSet<_>>();
    identity_metadata_paths.extend(existing_metadata_sidecar_paths(acquired.target())?);
    identity_metadata_paths.extend(
        acquired
            .backups
            .iter()
            .map(|backup| backup.original.clone())
            .filter(|path| is_identity_metadata_path(path)),
    );
    for logical_path in identity_metadata_paths {
        if let Some(content_path) = acquired.backup_for(&logical_path) {
            index_metadata_sidecar(
                &mut acquired_index,
                acquired.target(),
                &logical_path,
                content_path,
                IdentityIndexReadPolicy::Strict,
            )?;
        }
        if root.entry_exists(&logical_path)? {
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

    if root.entry_exists(acquired.target())? {
        bail!(
            "overwrite target path was recreated during acquisition: {}",
            acquired.target().display()
        );
    }
    let live_index = build_video_identity_index_in_dir(
        root.root_path(),
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
    require_entry_identity(
        root,
        &acquired.recovery.backup_dir,
        acquired.recovery.backup_dir_identity,
        "overwrite backup directory after validation",
    )?;
    Ok(())
}

fn is_identity_metadata_path(path: &Path) -> bool {
    identity_metadata_kind(path).is_some()
}

fn rollback_acquired_overwrite(
    error: anyhow::Error,
    acquired: AcquiredOverwrite,
    staging: Option<&BoundStagingDir>,
) -> anyhow::Error {
    if incomplete_move_rollback(&error) {
        if let Some(staging) = staging {
            staging.preserve_for_recovery();
        }
        return error;
    }
    match acquired.restore() {
        Ok(()) => error,
        Err(restore_error) => {
            if let Some(staging) = staging {
                staging.preserve_for_recovery();
            }
            anyhow!("{error:#}\nfailed to restore acquired overwrite files: {restore_error:#}")
        }
    }
}

fn finish_failed_staged_move(
    error: anyhow::Error,
    acquired: Option<AcquiredOverwrite>,
    staging: Option<&BoundStagingDir>,
) -> anyhow::Error {
    match acquired {
        Some(acquired) => rollback_acquired_overwrite(error, acquired, staging),
        None => {
            if incomplete_move_rollback(&error)
                && let Some(staging) = staging
            {
                staging.preserve_for_recovery();
            }
            error
        }
    }
}

fn commit_acquired_overwrite(
    acquired: AcquiredOverwrite,
    moved: &[MovedFile],
    committed_target: &Path,
    staging: Option<&BoundStagingDir>,
) -> Result<()> {
    commit_acquired_overwrite_with_hook(acquired, moved, committed_target, staging, &mut |_| Ok(()))
}

fn commit_acquired_overwrite_with_hook<F>(
    acquired: AcquiredOverwrite,
    moved: &[MovedFile],
    committed_target: &Path,
    staging: Option<&BoundStagingDir>,
    hook: &mut F,
) -> Result<()>
where
    F: FnMut(OverwriteCommitCheckpoint) -> Result<()>,
{
    let result = acquired.commit_with_hook(moved, committed_target, hook);
    if result.is_err()
        && let Some(staging) = staging
    {
        staging.preserve_for_recovery();
    }
    result
}

#[cfg(test)]
fn bind_test_overwrite_confirmation(
    root: &RootedFs,
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
) -> Result<VideoDuplicate> {
    let mut duplicate = duplicate.clone();
    if !matches!(action, VideoDuplicateAction::Overwrite)
        || duplicate.overwrite_confirmation.is_some()
    {
        return Ok(duplicate);
    }
    let Some(target) = duplicate.overwrite_target() else {
        return Ok(duplicate);
    };
    let target_file = root
        .open_bound_file(target)?
        .with_context(|| format!("overwrite target is missing: {}", target.display()))?;
    duplicate = duplicate.with_overwrite_confirmation(VideoOverwriteConfirmation {
        root_identity: root.root_identity(),
        target_file,
    });
    Ok(duplicate)
}

#[cfg(test)]
fn move_staged_video_files(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let root = RootedFs::new(final_dir)?;
    let duplicate = bind_test_overwrite_confirmation(&root, action, duplicate)?;
    move_staged_video_files_with_root(
        MoveExecutionContext {
            root: &root,
            staging: None,
        },
        staging_dir,
        final_dir,
        staged_files,
        action,
        &duplicate,
        primary_media_kind,
    )
}

fn move_staged_video_files_with_root(
    context: MoveExecutionContext<'_>,
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let root = context.root;
    let staged_media_count = staged_files
        .iter()
        .filter(|path| is_primary_media_file(path, primary_media_kind))
        .count();
    if matches!(action, VideoDuplicateAction::Overwrite) && staged_media_count == 0 {
        bail!("overwrite requires a staged primary media file");
    }
    let effective_action =
        if matches!(action, VideoDuplicateAction::Overwrite) && staged_media_count > 1 {
            VideoDuplicateAction::KeepBoth
        } else {
            action
        };
    let acquisition = if matches!(effective_action, VideoDuplicateAction::Overwrite) {
        Some(acquire_and_validate_overwrite_target(
            root,
            duplicate,
            primary_media_kind,
        )?)
    } else {
        None
    };
    let overwrite_target = acquisition.as_ref().map(AcquiredOverwrite::target);

    let move_result = move_staged_video_files_inner(
        context,
        staging_dir,
        final_dir,
        staged_files,
        overwrite_target,
        primary_media_kind,
        acquisition.as_ref(),
    );
    match move_result {
        Ok(moved) => {
            if let Some(acquisition) = acquisition {
                let committed_target = moved
                    .moved_videos
                    .first()
                    .context("overwrite commit did not produce a primary media destination")?;
                commit_acquired_overwrite(
                    acquisition,
                    &moved.moved,
                    committed_target,
                    context.staging,
                )
                .context("video overwrite succeeded but old-file cleanup failed")?;
            }
            Ok(moved.moved_videos)
        }
        Err(err) => Err(finish_failed_staged_move(err, acquisition, context.staging)),
    }
}

fn move_staged_video_files_inner(
    context: MoveExecutionContext<'_>,
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    overwrite_target: Option<&Path>,
    primary_media_kind: StagedPrimaryMediaKind,
    acquisition: Option<&AcquiredOverwrite>,
) -> Result<MovePlanResult> {
    let root = context.root;
    let mut plan = staged_move_plan(
        staging_dir,
        final_dir,
        staged_files,
        overwrite_target,
        primary_media_kind,
    )?;
    if let Some(staging) = context.staging {
        prepare_staged_publication(root, staging, &mut plan, acquisition)?;
    }
    execute_move_plan(root, plan, primary_media_kind)
}

#[cfg(test)]
fn move_staged_artifact_files(
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let root = RootedFs::new(final_dir)?;
    let duplicate = bind_test_overwrite_confirmation(&root, action, duplicate)?;
    move_staged_artifact_files_with_root(
        MoveExecutionContext {
            root: &root,
            staging: None,
        },
        staging_dir,
        final_dir,
        staged_files,
        action,
        &duplicate,
        primary_media_kind,
    )
}

fn move_staged_artifact_files_with_root(
    context: MoveExecutionContext<'_>,
    staging_dir: &Path,
    final_dir: &Path,
    staged_files: &[PathBuf],
    action: VideoDuplicateAction,
    duplicate: &VideoDuplicate,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<Vec<PathBuf>> {
    let root = context.root;
    let mut acquisition = if matches!(action, VideoDuplicateAction::Overwrite) {
        Some(acquire_and_validate_overwrite_target(
            root,
            duplicate,
            primary_media_kind,
        )?)
    } else {
        None
    };
    let overwrite_target = acquisition.as_ref().map(AcquiredOverwrite::target);
    let plan_result =
        staged_artifact_move_plan(staging_dir, final_dir, staged_files, overwrite_target);
    let mut plan = match plan_result {
        Ok(plan) => plan,
        Err(err) => {
            return Err(match acquisition {
                Some(acquired) => rollback_acquired_overwrite(err, acquired, None),
                None => err,
            });
        }
    };
    if let Some(staging) = context.staging
        && let Err(err) = prepare_staged_publication(root, staging, &mut plan, acquisition.as_ref())
    {
        return Err(match acquisition {
            Some(acquired) => rollback_acquired_overwrite(err, acquired, context.staging),
            None => err,
        });
    }
    let replaced_destinations = plan
        .iter()
        .map(|step| step.destination.clone())
        .collect::<BTreeSet<_>>();
    let move_result = execute_artifact_move_plan(root, plan);
    match move_result {
        Ok(moved) => {
            if let Some(mut acquired) = acquisition.take() {
                if let Err(err) = acquired.restore_unreplaced(&replaced_destinations) {
                    let err = with_move_rollback_error(root, err, &moved);
                    return Err(rollback_acquired_overwrite(err, acquired, context.staging));
                }
                let committed_target = acquired.target.clone();
                commit_acquired_overwrite(acquired, &moved, &committed_target, context.staging)
                    .context("artifact overwrite succeeded but old-sidecar cleanup failed")?;
            }
            Ok(moved.into_iter().map(|moved| moved.destination).collect())
        }
        Err(err) => Err(finish_failed_staged_move(err, acquisition, context.staging)),
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
            expected_identity: None,
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
    let target_stem = target_video.file_stem()?.to_str()?;
    if let Some(suffix) = source
        .file_name()?
        .to_str()?
        .strip_prefix(target_stem)
        .filter(|suffix| suffix.starts_with('.'))
    {
        return sidecar_destination_for_target_video(target_video, suffix);
    }
    let suffix = artifact_sidecar_suffix(source)?;
    Some(
        target_video
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{target_stem}{suffix}")),
    )
}

fn artifact_sidecar_suffix(source: &Path) -> Option<String> {
    let name = source.file_name()?.to_str()?;
    let lower_name = name.to_ascii_lowercase();
    for suffix in [
        "info.json",
        "cover.jpg",
        "cover.jpeg",
        "cover.png",
        "cover.webp",
    ] {
        if lower_name.ends_with(&format!(".{suffix}")) {
            let start = name.len().checked_sub(suffix.len())?;
            return Some(format!(".{}", &name[start..]));
        }
    }
    Some(format!(".{}", source.extension()?.to_str()?))
}

fn execute_artifact_move_plan(root: &RootedFs, plan: Vec<MoveStep>) -> Result<Vec<MovedFile>> {
    let mut moved = Vec::new();
    for step in plan {
        match move_step_with_bound_parents(root, step.clone()) {
            Ok(moved_file) => moved.push(moved_file),
            Err(err) => {
                let err = err.context(format!(
                    "failed to move {} to {}",
                    step.source.display(),
                    step.destination.display()
                ));
                return Err(with_move_rollback_error(root, err, &moved));
            }
        }
    }
    Ok(moved)
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
            expected_identity: None,
        });
    }

    if overwrite_target.is_some() {
        // The primary media path is the durable commit marker used by startup recovery. Move it
        // before sidecars so a missing target cannot coexist with partially committed metadata.
        steps.sort_by_key(|step| !is_primary_media_file(&step.source, primary_media_kind));
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
    root: &RootedFs,
    plan: Vec<MoveStep>,
    primary_media_kind: StagedPrimaryMediaKind,
) -> Result<MovePlanResult> {
    execute_move_plan_with_hook(root, plan, primary_media_kind, &mut |_| Ok(()))
}

fn execute_move_plan_with_hook<F>(
    root: &RootedFs,
    plan: Vec<MoveStep>,
    primary_media_kind: StagedPrimaryMediaKind,
    hook: &mut F,
) -> Result<MovePlanResult>
where
    F: FnMut(&MovedFile) -> Result<()>,
{
    let mut moved = Vec::new();
    let mut moved_videos = Vec::new();
    for step in plan {
        match move_step_with_bound_parents(root, step.clone()) {
            Ok(moved_file) => {
                let is_primary = is_primary_media_file(&moved_file.destination, primary_media_kind);
                moved.push(moved_file);
                if let Err(err) = hook(moved.last().expect("moved file was just pushed")) {
                    return Err(with_move_rollback_error(
                        root,
                        err.context("staged move hook failed after publication"),
                        &moved,
                    ));
                }
                if is_primary {
                    moved_videos.push(
                        moved
                            .last()
                            .expect("moved file was just pushed")
                            .destination
                            .clone(),
                    );
                }
            }
            Err(err) => {
                let err = err.context(format!(
                    "failed to move {} to {}",
                    step.source.display(),
                    step.destination.display()
                ));
                return Err(with_move_rollback_error(root, err, &moved));
            }
        }
    }
    Ok(MovePlanResult {
        moved,
        moved_videos,
    })
}

fn move_step_with_bound_parents(root: &RootedFs, step: MoveStep) -> Result<MovedFile> {
    let source_entry = root.bind_entry(&step.source, false)?;
    let destination_entry = root.bind_entry(&step.destination, true)?;
    let identity = root
        .bound_entry_identity(&source_entry)?
        .with_context(|| format!("move source is missing: {}", step.source.display()))?;
    if !identity.is_file() {
        bail!(
            "move source is not a regular file: {}",
            step.source.display()
        );
    }
    if step
        .expected_identity
        .is_some_and(|expected| expected != identity)
    {
        bail!(
            "move source identity changed after publication planning: {}",
            step.source.display()
        );
    }
    root.validate_configured_root()?;
    root.rename_via_bound_parents_noreplace_if_identity(
        &source_entry,
        &destination_entry,
        identity,
    )?;
    let moved = MovedFile {
        source: step.source,
        destination: step.destination,
        source_entry,
        destination_entry,
        identity,
    };
    if let Err(err) =
        require_entry_identity(root, &moved.destination, identity, "moved destination")
    {
        return Err(with_move_rollback_error(
            root,
            err.context("failed to validate moved destination through the configured output root"),
            std::slice::from_ref(&moved),
        ));
    }
    let destination = root
        .open_bound_file(&moved.destination)?
        .context("moved destination disappeared before durability validation")?;
    if destination.identity() != identity {
        return Err(with_move_rollback_error(
            root,
            anyhow!(
                "moved destination identity changed before durability validation: {}",
                moved.destination.display()
            ),
            std::slice::from_ref(&moved),
        ));
    }
    if let Err(err) = destination.sync_all() {
        return Err(with_move_rollback_error(
            root,
            err.context(format!(
                "failed to persist moved destination {}",
                moved.destination.display()
            )),
            std::slice::from_ref(&moved),
        ));
    }
    Ok(moved)
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
    for path in entries {
        if path == video {
            continue;
        }
        if is_known_video_sidecar(&path)
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
            && best_primary_stem_for_sidecar(&path, &primary_stems).as_deref() == Some(stem)
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

fn rollback_moves(root: &RootedFs, moved: &[MovedFile]) -> Result<()> {
    let mut failures = Vec::new();
    for moved in moved.iter().rev() {
        if let Err(err) = root.rename_via_bound_parents_noreplace_if_identity(
            &moved.destination_entry,
            &moved.source_entry,
            moved.identity,
        ) {
            failures.push(format!(
                "failed to roll back {} to {}: {err:#}",
                moved.destination.display(),
                moved.source.display()
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn with_move_rollback_error(
    root: &RootedFs,
    error: anyhow::Error,
    moved: &[MovedFile],
) -> anyhow::Error {
    match rollback_moves(root, moved) {
        Ok(()) => error,
        Err(rollback_error) => anyhow::Error::new(IncompleteMoveRollback {
            error,
            rollback_error,
        }),
    }
}

fn incomplete_move_rollback(error: &anyhow::Error) -> bool {
    error.downcast_ref::<IncompleteMoveRollback>().is_some()
}

fn require_entry_identity(
    root: &RootedFs,
    path: &Path,
    expected: EntryIdentity,
    label: &str,
) -> Result<()> {
    match root.entry_identity(path)? {
        Some(current) if current == expected => Ok(()),
        Some(_) => bail!("{label} identity changed: {}", path.display()),
        None => bail!("{label} is missing: {}", path.display()),
    }
}

fn require_bound_entry_identity(
    root: &RootedFs,
    entry: &BoundEntry,
    expected: EntryIdentity,
    label: &str,
) -> Result<()> {
    match root.bound_entry_identity(entry)? {
        Some(current) if current == expected => Ok(()),
        Some(_) => bail!("{label} identity changed: {}", entry.path().display()),
        None => bail!("{label} is missing: {}", entry.path().display()),
    }
}

fn restore_file_backup(root: &RootedFs, backup: &FileBackup) -> Result<()> {
    require_bound_entry_identity(
        root,
        &backup.backup_entry,
        backup.identity,
        "overwrite backup",
    )?;
    if root.bound_entry_identity(&backup.original_entry)?.is_some() {
        bail!(
            "restore destination is occupied; retained backup {} for {}",
            backup.backup.display(),
            backup.original.display()
        );
    }
    root.rename_via_bound_parents_noreplace_if_identity(
        &backup.backup_entry,
        &backup.original_entry,
        backup.identity,
    )
    .with_context(|| {
        format!(
            "failed to restore overwrite backup {} to {}",
            backup.backup.display(),
            backup.original.display()
        )
    })
}

fn restore_backups(
    root: &RootedFs,
    backups: &[FileBackup],
    recovery: &OverwriteRecoveryFiles,
    target: &Path,
) -> Result<()> {
    let mut failures = Vec::new();
    let target_backup = backups
        .iter()
        .find(|backup| backup.original == target)
        .context("overwrite target backup is missing during restore")?;
    if let Err(err) = restore_file_backup(root, target_backup) {
        bail!("{err:#}");
    }
    for backup in backups
        .iter()
        .rev()
        .filter(|backup| backup.original != target)
    {
        if let Err(err) = restore_file_backup(root, backup) {
            failures.push(format!("{err:#}"));
        }
    }
    if failures.is_empty()
        && let Err(err) = remove_overwrite_transaction_dir(root, recovery)
    {
        failures.push(format!("{err:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn restore_remaining_backups(
    root: &RootedFs,
    backups: &[FileBackup],
    recovery: &OverwriteRecoveryFiles,
    restored_target: &BoundEntry,
    restored_target_identity: EntryIdentity,
) -> Result<()> {
    require_bound_entry_identity(
        root,
        restored_target,
        restored_target_identity,
        "restored overwrite target",
    )?;

    let mut failures = Vec::new();
    for backup in backups.iter().rev() {
        if let Err(err) = restore_file_backup(root, backup) {
            failures.push(format!("{err:#}"));
        }
    }
    if failures.is_empty()
        && let Err(err) = remove_overwrite_transaction_dir(root, recovery)
    {
        failures.push(format!("{err:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn validate_committed_overwrite_state(
    root: &RootedFs,
    backups: &[FileBackup],
    recovery: &OverwriteRecoveryFiles,
    committed: &CommittedOverwriteState,
) -> Result<()> {
    if committed.manifest.phase != OverwriteRecoveryPhase::Committed {
        bail!("overwrite cleanup requires a committed recovery manifest");
    }
    if committed.manifest.version != OVERWRITE_RECOVERY_MANIFEST_VERSION {
        bail!("overwrite cleanup requires the current recovery manifest version");
    }
    let parent = recovery
        .backup_dir
        .parent()
        .context("overwrite recovery directory has no parent")?;
    let target_file_name = single_recovery_file_name(&committed.manifest.target_file_name)?;
    if !committed.files.contains_key(Path::new(&target_file_name)) {
        bail!("committed overwrite manifest does not bind the target file");
    }
    if committed.files.len() != committed.manifest.committed_files.len() {
        bail!("committed overwrite manifest contains duplicate file identities");
    }
    if committed.anchors.len() != committed.manifest.committed_files.len() {
        bail!("committed overwrite manifest contains duplicate output anchors");
    }

    for record in &committed.manifest.committed_files {
        let file_name = PathBuf::from(single_recovery_file_name(&record.file_name)?);
        let anchor_name = PathBuf::from(committed_recovery_anchor_name(record)?);
        let file = committed.files.get(&file_name).with_context(|| {
            format!(
                "committed overwrite descriptor is missing for {}",
                file_name.display()
            )
        })?;
        let anchor = committed.anchors.get(&anchor_name).with_context(|| {
            format!(
                "committed overwrite anchor descriptor is missing for {}",
                anchor_name.display()
            )
        })?;
        file.validate_identity()?;
        anchor.validate_identity()?;
        let identity = file.identity();
        if identity.device() != record.device || identity.inode() != record.inode {
            bail!(
                "committed overwrite descriptor identity changed for {}",
                file_name.display()
            );
        }
        if anchor.identity() != identity {
            bail!(
                "committed overwrite anchor no longer binds {}",
                file_name.display()
            );
        }
        let path = parent.join(&file_name);
        if root.entry_identity(&path)? != Some(identity) {
            bail!(
                "committed overwrite path identity changed: {}",
                path.display()
            );
        }
        let anchor_path = recovery.backup_dir.join(&anchor_name);
        if root.entry_identity(&anchor_path)? != Some(identity) {
            bail!(
                "committed overwrite anchor path changed: {}",
                anchor_path.display()
            );
        }
    }

    for backup in backups {
        if let Some(current) = root.bound_entry_identity(&backup.original_entry)? {
            let is_committed_object = committed
                .files
                .values()
                .any(|file| file.identity() == current);
            if !is_committed_object {
                bail!(
                    "uncommitted object occupies an overwrite removal path: {}",
                    backup.original.display()
                );
            }
        }
    }
    Ok(())
}

fn remove_backups(
    root: &RootedFs,
    backups: &[FileBackup],
    recovery: &OverwriteRecoveryFiles,
    committed: &CommittedOverwriteState,
) -> Result<()> {
    validate_committed_overwrite_state(root, backups, recovery, committed)?;
    let mut failures = Vec::new();
    for backup in backups {
        if let Err(err) = validate_committed_overwrite_state(root, backups, recovery, committed) {
            failures.push(format!(
                "committed overwrite state changed before removing {}: {err:#}",
                backup.backup.display()
            ));
            break;
        }
        if backup.backup.parent() != Some(recovery.backup_dir.as_path()) {
            failures.push(format!(
                "refused to remove overwrite backup outside owned directory: {}",
                backup.backup.display()
            ));
            continue;
        }
        if let Err(err) = root.remove_bound_file_if_identity(&backup.backup_entry, backup.identity)
        {
            failures.push(format!(
                "failed to remove overwrite backup {}: {err}",
                backup.backup.display()
            ));
        }
    }
    if failures.is_empty()
        && let Err(err) = validate_committed_overwrite_state(root, backups, recovery, committed)
    {
        failures.push(format!(
            "committed overwrite state changed after backup cleanup: {err:#}"
        ));
    }
    if failures.is_empty()
        && let Err(err) = remove_committed_overwrite_transaction(root, backups, recovery, committed)
    {
        failures.push(format!("{err:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

fn remove_committed_overwrite_transaction(
    root: &RootedFs,
    backups: &[FileBackup],
    recovery: &OverwriteRecoveryFiles,
    committed: &CommittedOverwriteState,
) -> Result<()> {
    validate_committed_overwrite_state(root, backups, recovery, committed)?;
    let expected_entries =
        std::iter::once(std::ffi::OsString::from(OVERWRITE_RECOVERY_MANIFEST_NAME))
            .chain(
                committed
                    .manifest
                    .committed_files
                    .iter()
                    .map(committed_recovery_anchor_name)
                    .collect::<Result<Vec<_>>>()?,
            )
            .collect::<BTreeSet<_>>();
    let actual_entries = root
        .list_bound_directory(&recovery.backup_dir_entry, recovery.backup_dir_identity)?
        .into_iter()
        .map(|(name, _)| name)
        .collect::<BTreeSet<_>>();
    if actual_entries != expected_entries {
        bail!(
            "refused to finalize overwrite transaction with unexpected entries in {}",
            recovery.backup_dir.display()
        );
    }
    root.remove_bound_tree_durably_if_identity(
        &recovery.backup_dir_entry,
        recovery.backup_dir_identity,
    )
    .with_context(|| {
        format!(
            "failed to durably remove committed overwrite transaction {}",
            recovery.backup_dir.display()
        )
    })
}

fn remove_overwrite_transaction_dir(
    root: &RootedFs,
    recovery: &OverwriteRecoveryFiles,
) -> Result<()> {
    let unresolved = root
        .list_bound_directory(&recovery.backup_dir_entry, recovery.backup_dir_identity)?
        .into_iter()
        .filter(|(name, identity)| {
            name != std::ffi::OsStr::new(OVERWRITE_RECOVERY_MANIFEST_NAME)
                || *identity != recovery.manifest_identity
        })
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    if !unresolved.is_empty() {
        bail!(
            "refused to remove overwrite recovery manifest while unresolved entries remain in {}: {}",
            recovery.backup_dir.display(),
            unresolved
                .iter()
                .map(|name| name.to_string_lossy())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    root.remove_bound_tree_durably_if_identity(
        &recovery.backup_dir_entry,
        recovery.backup_dir_identity,
    )
    .with_context(|| {
        format!(
            "failed to durably remove overwrite recovery directory {}",
            recovery.backup_dir.display()
        )
    })
}

pub fn recover_pending_overwrite_transactions(video_dir: &Path) -> Result<Vec<String>> {
    let root = RootedFs::new(video_dir)?;
    let recovery_lock = video_output_lock_file(&root)?;
    recovery_lock
        .lock_exclusive()
        .context("failed to serialize overwrite recovery with active downloads")?;
    let (mut recovery_state, _) = video_recovery_state_file(&root)?;
    let quarantine_recovery = root.reconcile_remove_quarantines_with_status()?;
    let staging_recovery = recover_pending_video_staging_directories_locked(&root)?;
    let mux_recovery = recover_pending_bilibili_mux_transactions_locked(&root, video_dir)?;
    let overwrite_recovery = recover_pending_overwrite_transactions_locked(
        &root,
        video_dir,
        &staging_recovery.blocked_overwrites,
    )?;
    let unresolved = quarantine_recovery.unresolved
        || staging_recovery.unresolved
        || mux_recovery.unresolved
        || overwrite_recovery.unresolved;
    recovery_state.write_state(if unresolved {
        VIDEO_RECOVERY_STATE_DIRTY
    } else {
        VIDEO_RECOVERY_STATE_CLEAN
    })?;
    let mut messages = quarantine_recovery.messages;
    messages.extend(staging_recovery.messages);
    messages.extend(mux_recovery.messages);
    messages.extend(overwrite_recovery.messages);
    Ok(messages)
}

#[derive(Debug, Default)]
struct OverwriteRecoveryReport {
    messages: Vec<String>,
    unresolved: bool,
}

fn overwrite_recovery_is_blocked(
    root: &RootedFs,
    directory: &Path,
    blocked: &BTreeSet<StagedPublicationOverwrite>,
) -> Result<bool> {
    if blocked.is_empty() {
        return Ok(false);
    }
    let Some(identity) = root.entry_identity(directory)? else {
        return Ok(false);
    };
    let candidate = StagedPublicationOverwrite {
        transaction_path: publication_relative_path(root, directory)?,
        transaction_device: identity.device(),
        transaction_inode: identity.inode(),
    };
    Ok(blocked.contains(&candidate))
}

fn recover_pending_overwrite_transactions_locked(
    root: &RootedFs,
    video_dir: &Path,
    blocked: &BTreeSet<StagedPublicationOverwrite>,
) -> Result<OverwriteRecoveryReport> {
    let mut directories = Vec::new();
    let mut report = OverwriteRecoveryReport::default();
    collect_overwrite_recovery_directories(
        root,
        video_dir,
        video_dir,
        &mut directories,
        &mut report.messages,
    )?;
    report.unresolved = !report.messages.is_empty();
    directories.sort();

    for directory in directories {
        if overwrite_recovery_is_blocked(root, &directory, blocked)? {
            report.unresolved = true;
            report.messages.push(format!(
                "Deferred overwrite transaction referenced by an unresolved staged publication: {}",
                directory.display()
            ));
            continue;
        }
        let manifest_path = directory.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
        match root.open_bound_file(&manifest_path) {
            Ok(None) => {
                report.unresolved = true;
                report.messages.push(format!(
                    "Retained unrecognized legacy overwrite backup directory: {}",
                    directory.display()
                ));
            }
            Ok(Some(_)) => match recover_overwrite_transaction(root, &directory) {
                Ok(messages) => report.messages.extend(messages),
                Err(err) => {
                    report.unresolved = true;
                    report.messages.push(format!(
                        "Retained unresolved overwrite transaction {}: {err:#}",
                        directory.display()
                    ));
                }
            },
            Err(err) => {
                report.unresolved = true;
                report.messages.push(format!(
                    "Retained unreadable overwrite transaction {}: {err:#}",
                    directory.display()
                ));
            }
        }
    }
    Ok(report)
}

fn collect_overwrite_recovery_directories(
    root: &RootedFs,
    scan_root: &Path,
    directory: &Path,
    recovered: &mut Vec<PathBuf>,
    issues: &mut Vec<String>,
) -> Result<()> {
    root.validate_configured_root()?;
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(err) if directory != scan_root => {
            issues.push(format!(
                "Skipped unreadable directory during overwrite recovery scan {}: {err}",
                directory.display()
            ));
            return Ok(());
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to scan overwrite recovery root {}",
                    directory.display()
                )
            });
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                issues.push(format!(
                    "Skipped unreadable entry during overwrite recovery scan {}: {err}",
                    directory.display()
                ));
                continue;
            }
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) => {
                issues.push(format!(
                    "Skipped uninspectable entry during overwrite recovery scan {}: {err}",
                    entry.path().display()
                ));
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(OVERWRITE_BACKUP_DIR_PREFIX) {
            recovered.push(path);
            continue;
        }
        if name == VIDEO_STAGING_DIR_NAME {
            continue;
        }
        collect_overwrite_recovery_directories(root, scan_root, &path, recovered, issues)?;
    }
    root.validate_configured_root()
}

fn recover_overwrite_transaction(root: &RootedFs, directory: &Path) -> Result<Vec<String>> {
    let directory_entry = root.bind_entry(directory, false)?;
    let directory_identity = root
        .bound_entry_identity(&directory_entry)?
        .with_context(|| {
            format!(
                "overwrite recovery directory is missing: {}",
                directory.display()
            )
        })?;
    if !directory_identity.is_dir() {
        bail!(
            "overwrite recovery path is not a directory: {}",
            directory.display()
        );
    }
    root.validate_private_bound_directory(&directory_entry, directory_identity, 0o700)?;

    let manifest_path = directory.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
    let manifest_file = root.open_bound_file(&manifest_path)?.with_context(|| {
        format!(
            "overwrite recovery manifest is missing: {}",
            manifest_path.display()
        )
    })?;
    manifest_file.validate_private_single_link(0o600)?;
    let manifest_identity = manifest_file.identity();
    let manifest_entry = root.bind_entry(&manifest_path, false)?;
    if root.bound_entry_identity(&manifest_entry)? != Some(manifest_identity) {
        bail!(
            "overwrite recovery manifest identity changed: {}",
            manifest_path.display()
        );
    }
    let manifest: OverwriteRecoveryManifest =
        serde_json::from_slice(&manifest_file.read_limited(OVERWRITE_RECOVERY_MANIFEST_LIMIT)?)
            .with_context(|| {
                format!(
                    "failed to parse overwrite recovery manifest {}",
                    manifest_path.display()
                )
            })?;
    if !matches!(
        manifest.version,
        OVERWRITE_RECOVERY_OLDEST_MANIFEST_VERSION
            | OVERWRITE_RECOVERY_LEGACY_MANIFEST_VERSION
            | OVERWRITE_RECOVERY_MANIFEST_VERSION
    ) {
        bail!(
            "unsupported overwrite recovery manifest version {} in {}",
            manifest.version,
            manifest_path.display()
        );
    }
    if manifest.version != OVERWRITE_RECOVERY_MANIFEST_VERSION {
        bail!(
            "legacy overwrite recovery manifest version {} has no authenticated ownership binding",
            manifest.version
        );
    }
    validate_overwrite_recovery_ownership(root, directory, directory_identity, &manifest)?;
    let target_file_name = single_recovery_file_name(&manifest.target_file_name)?;
    let parent = directory
        .parent()
        .context("overwrite recovery directory has no parent")?;
    let target = parent.join(&target_file_name);

    remove_recovery_transition_temp(root, directory)?;
    let entries = root
        .list_bound_directory(&directory_entry, directory_identity)?
        .into_iter()
        .filter(|(name, _)| name != std::ffi::OsStr::new(OVERWRITE_RECOVERY_MANIFEST_NAME))
        .collect::<Vec<_>>();
    let (mut anchors, mut backups) = entries.into_iter().partition::<Vec<_>, _>(|(name, _)| {
        name.to_string_lossy()
            .starts_with(OVERWRITE_COMMITTED_ANCHOR_PREFIX)
    });
    for (name, identity) in anchors.iter().chain(&backups) {
        if !identity.is_file() {
            bail!(
                "overwrite recovery directory contains a non-file entry: {}",
                directory.join(name).display()
            );
        }
    }
    anchors.sort_by(|left, right| left.0.cmp(&right.0));
    backups.sort_by_key(|(name, _)| name == &target_file_name);

    let target_backup_exists = backups.iter().any(|(name, _)| name == &target_file_name);
    if !backups.is_empty() && root.entry_identity(&target)?.is_none() && !target_backup_exists {
        bail!(
            "overwrite recovery target and its backup are both missing: {}",
            target.display()
        );
    }

    let recovery = OverwriteRecoveryFiles {
        backup_dir: directory.to_path_buf(),
        backup_dir_entry: directory_entry,
        backup_dir_identity: directory_identity,
        manifest_path,
        manifest_entry,
        manifest_identity,
    };
    let mut report = Vec::new();
    let transaction_removed = match manifest.phase {
        OverwriteRecoveryPhase::Acquired => {
            if !manifest.committed_files.is_empty() {
                bail!("acquired overwrite manifest unexpectedly contains committed files");
            }
            remove_uncommitted_recovery_anchors(root, &recovery, &anchors)?;
            for (name, _) in &backups {
                let original = parent.join(name);
                if root.entry_identity(&original)?.is_some() {
                    bail!(
                        "uncommitted overwrite destination is occupied; retained backup for manual recovery: {}",
                        original.display()
                    );
                }
            }
            for (name, backup_identity) in &backups {
                let backup = directory.join(name);
                let original = parent.join(name);
                let backup_entry = root.bind_entry(&backup, false)?;
                let original_entry = root.bind_entry(&original, false)?;
                root.rename_via_bound_parents_noreplace_if_identity(
                    &backup_entry,
                    &original_entry,
                    *backup_identity,
                )
                .with_context(|| {
                    format!(
                        "failed to restore overwrite backup {} to {}",
                        backup.display(),
                        original.display()
                    )
                })?;
                report.push(format!(
                    "Restored interrupted overwrite: {}",
                    original.display()
                ));
            }
            false
        }
        OverwriteRecoveryPhase::Committed => {
            if manifest.version != OVERWRITE_RECOVERY_MANIFEST_VERSION {
                bail!("legacy committed overwrite transaction has no durable output anchors");
            }
            validate_recovery_anchor_set(&manifest, &anchors)?;
            let committed = bind_recovery_committed_files(root, parent, &recovery, manifest)?;
            validate_recovery_committed_files(root, parent, &recovery, &committed)?;
            for (name, _) in &backups {
                let original = parent.join(name);
                if let Some(current) = root.entry_identity(&original)? {
                    let is_committed_object = committed
                        .files
                        .values()
                        .any(|file| file.identity() == current);
                    if !is_committed_object {
                        bail!(
                            "uncommitted object occupies an overwrite removal path; retained backup: {}",
                            original.display()
                        );
                    }
                }
            }
            for (name, backup_identity) in &backups {
                validate_recovery_committed_files(root, parent, &recovery, &committed)?;
                let backup = directory.join(name);
                let original = parent.join(name);
                let backup_entry = root.bind_entry(&backup, false)?;
                root.remove_bound_file_if_identity(&backup_entry, *backup_identity)
                    .with_context(|| {
                        format!(
                            "failed to finalize recovered overwrite backup {}",
                            backup.display()
                        )
                    })?;
                report.push(format!(
                    "Finalized committed overwrite file: {}",
                    original.display()
                ));
            }
            validate_recovery_committed_files(root, parent, &recovery, &committed)?;
            remove_committed_overwrite_transaction(root, &[], &recovery, &committed)?;
            true
        }
    };

    if !transaction_removed {
        remove_overwrite_transaction_dir(root, &recovery)?;
    }
    report.push(format!(
        "Recovered overwrite transaction: {}",
        directory.display()
    ));
    Ok(report)
}

fn bind_recovery_committed_files(
    root: &RootedFs,
    parent: &Path,
    recovery: &OverwriteRecoveryFiles,
    manifest: OverwriteRecoveryManifest,
) -> Result<CommittedOverwriteState> {
    let mut files = BTreeMap::new();
    let mut anchors = BTreeMap::new();
    for record in &manifest.committed_files {
        let file_name = PathBuf::from(single_recovery_file_name(&record.file_name)?);
        let anchor_name = PathBuf::from(committed_recovery_anchor_name(record)?);
        let anchor_path = recovery.backup_dir.join(&anchor_name);
        let anchor = root.open_bound_file(&anchor_path)?.with_context(|| {
            format!(
                "committed overwrite output anchor is missing: {}",
                anchor_path.display()
            )
        })?;
        let identity = anchor.identity();
        if identity.device() != record.device || identity.inode() != record.inode {
            bail!(
                "committed overwrite anchor identity does not match its manifest: {}",
                anchor_path.display()
            );
        }
        let path = parent.join(&file_name);
        let file = root
            .open_bound_file(&path)?
            .with_context(|| format!("committed overwrite file is missing: {}", path.display()))?;
        if file.identity() != identity {
            bail!(
                "committed overwrite output does not match its durable anchor: {}",
                path.display()
            );
        }
        if files.insert(file_name.clone(), file).is_some() {
            bail!(
                "committed overwrite manifest repeats file name {}",
                file_name.display()
            );
        }
        if anchors.insert(anchor_name.clone(), anchor).is_some() {
            bail!(
                "committed overwrite manifest repeats anchor name {}",
                anchor_name.display()
            );
        }
    }
    let target_file_name = PathBuf::from(single_recovery_file_name(&manifest.target_file_name)?);
    if !files.contains_key(&target_file_name) {
        bail!("committed overwrite manifest does not bind the target file");
    }
    Ok(CommittedOverwriteState {
        manifest,
        files,
        anchors,
    })
}

fn validate_recovery_committed_files(
    root: &RootedFs,
    _parent: &Path,
    recovery: &OverwriteRecoveryFiles,
    committed: &CommittedOverwriteState,
) -> Result<()> {
    validate_committed_overwrite_state(root, &[], recovery, committed)
}

fn validate_recovery_anchor_set(
    manifest: &OverwriteRecoveryManifest,
    anchors: &[(std::ffi::OsString, EntryIdentity)],
) -> Result<()> {
    let expected = manifest
        .committed_files
        .iter()
        .map(committed_recovery_anchor_name)
        .collect::<Result<BTreeSet<_>>>()?;
    let actual = anchors
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    if expected != actual || expected.len() != anchors.len() {
        bail!("committed overwrite recovery anchor set does not match its manifest");
    }
    Ok(())
}

fn remove_uncommitted_recovery_anchors(
    root: &RootedFs,
    recovery: &OverwriteRecoveryFiles,
    anchors: &[(std::ffi::OsString, EntryIdentity)],
) -> Result<()> {
    for (name, identity) in anchors {
        let path = recovery.backup_dir.join(name);
        let entry = root.bind_entry(&path, false)?;
        root.remove_bound_file_if_identity(&entry, *identity)
            .with_context(|| {
                format!(
                    "failed to remove uncommitted output anchor {}",
                    path.display()
                )
            })?;
    }
    Ok(())
}

fn remove_recovery_transition_temp(root: &RootedFs, directory: &Path) -> Result<()> {
    let path = directory.join(OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME);
    let Some(file) = root.open_bound_file(&path)? else {
        return Ok(());
    };
    let entry = root.bind_entry(&path, false)?;
    root.remove_bound_file_if_identity(&entry, file.identity())
        .with_context(|| {
            format!(
                "failed to remove interrupted manifest transition {}",
                path.display()
            )
        })
}

fn committed_recovery_anchor_name(record: &OverwriteCommittedFile) -> Result<std::ffi::OsString> {
    let anchor = record
        .anchor_name
        .as_deref()
        .context("committed overwrite manifest does not contain a durable output anchor")?;
    let name = single_recovery_control_file_name(anchor)?;
    if !name
        .to_string_lossy()
        .starts_with(OVERWRITE_COMMITTED_ANCHOR_PREFIX)
    {
        bail!(
            "committed overwrite manifest contains an invalid anchor name: {}",
            anchor.display()
        );
    }
    Ok(name)
}

fn single_recovery_file_name(path: &Path) -> Result<std::ffi::OsString> {
    let name = single_recovery_control_file_name(path)?;
    if is_overwrite_recovery_control_name(&name) {
        bail!("overwrite recovery target conflicts with a reserved recovery name");
    }
    Ok(name)
}

fn single_recovery_control_file_name(path: &Path) -> Result<std::ffi::OsString> {
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(name)), None) => Ok(name.to_os_string()),
        _ => bail!(
            "overwrite recovery manifest contains an invalid file name: {}",
            path.display()
        ),
    }
}

fn is_overwrite_recovery_control_name(name: &std::ffi::OsStr) -> bool {
    name == std::ffi::OsStr::new(OVERWRITE_RECOVERY_MANIFEST_NAME)
        || name == std::ffi::OsStr::new(OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME)
        || name
            .to_string_lossy()
            .starts_with(OVERWRITE_COMMITTED_ANCHOR_PREFIX)
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

fn write_youtube_nfo_bound(
    root: &RootedFs,
    video_path: &Path,
    requested_url: &str,
    metadata: &YoutubeMetadata,
) -> Result<PathBuf> {
    let video = root
        .open_bound_file(video_path)?
        .with_context(|| format!("downloaded media disappeared: {}", video_path.display()))?;
    let title = metadata
        .title
        .as_deref()
        .or_else(|| video_path.file_stem().and_then(|stem| stem.to_str()));
    let source_url = metadata.webpage_url.as_deref().unwrap_or(requested_url);
    let studio = metadata.uploader.as_deref().or(metadata.channel.as_deref());
    let premiered = metadata.upload_date.as_deref().and_then(format_yt_date);
    let nfo = MediaNfo {
        title,
        plot: metadata.description.as_deref(),
        unique_id_type: "youtube",
        unique_id: metadata.id.as_deref().unwrap_or(requested_url),
        alternate_unique_ids: Vec::new(),
        source_url,
        studio,
        premiered: premiered.as_deref(),
    };
    let title = nfo
        .title
        .or_else(|| video_path.file_stem().and_then(|stem| stem.to_str()))
        .unwrap_or("Untitled");
    let nfo_path = video_path.with_extension("nfo");
    let (nfo_entry, nfo_identity) =
        root.create_new_bound_file(&nfo_path, render_nfo(title, &nfo).as_bytes(), 0o644)?;

    let media_still_matches = root
        .open_bound_file(video_path)
        .map(|current| current.as_ref() == Some(&video));
    match media_still_matches {
        Ok(true) => Ok(nfo_path),
        Ok(false) => {
            root.remove_bound_file_if_identity(&nfo_entry, nfo_identity)?;
            bail!(
                "downloaded media identity changed while writing {}",
                nfo_path.display()
            );
        }
        Err(err) => {
            let cleanup = root.remove_bound_file_if_identity(&nfo_entry, nfo_identity);
            match cleanup {
                Ok(()) => Err(err.context(format!(
                    "failed to revalidate downloaded media while writing {}",
                    nfo_path.display()
                ))),
                Err(cleanup) => Err(err.context(format!(
                    "failed to revalidate downloaded media while writing {}; NFO cleanup also failed: {cleanup:#}",
                    nfo_path.display()
                ))),
            }
        }
    }
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

    fn assert_overwrite_commit_fault_recovers(
        checkpoint: OverwriteCommitCheckpoint,
        expect_roll_forward: bool,
    ) {
        let label = match checkpoint {
            OverwriteCommitCheckpoint::BeforeOutputSync => "overwrite-fault-output-sync",
            OverwriteCommitCheckpoint::BeforeAnchorCreation => "overwrite-fault-anchor",
            OverwriteCommitCheckpoint::BeforeManifestReplace => "overwrite-fault-manifest-replace",
            OverwriteCommitCheckpoint::AfterManifestReplace => "overwrite-fault-post-manifest",
        };
        let final_dir = temp_test_dir(label);
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        fs::write(&staged_video, "replacement-video").expect("replacement should stage");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            std::slice::from_ref(&staged_video),
            Some(&existing),
            StagedPrimaryMediaKind::Video,
        )
        .expect("overwrite move plan should build");
        prepare_staged_publication(&root, &staging, &mut plan, Some(&acquired))
            .expect("publication manifest should persist");
        let moved = execute_move_plan(&root, plan, StagedPrimaryMediaKind::Video)
            .expect("replacement should publish");
        let committed_target = moved
            .moved_videos
            .first()
            .expect("published replacement should include primary media")
            .clone();
        let mut injected = false;
        let error = commit_acquired_overwrite_with_hook(
            acquired,
            &moved.moved,
            &committed_target,
            Some(&staging),
            &mut |actual| {
                if !injected && actual == checkpoint {
                    injected = true;
                    bail!("injected overwrite commit failure at {actual:?}");
                }
                Ok(())
            },
        )
        .expect_err("injected overwrite commit failure should propagate");
        assert!(injected, "requested overwrite checkpoint should be reached");
        assert!(format!("{error:#}").contains("injected overwrite commit failure"));
        drop(staging);
        assert!(
            staging_dir.is_dir(),
            "uncertain publication must remain available for startup recovery"
        );
        drop(root);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("startup recovery should resolve the interrupted overwrite");

        if expect_roll_forward {
            assert_eq!(fs::read_to_string(&existing).unwrap(), "replacement-video");
            assert!(!existing.with_extension("nfo").exists());
            assert!(report.iter().any(|line| {
                line.contains("Rolled forward interrupted staged video publication")
            }));
        } else {
            assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
            assert!(existing.with_extension("nfo").is_file());
            assert!(
                report.iter().any(|line| {
                    line.contains("Rolled back interrupted staged video publication")
                })
            );
        }
        assert!(!staging_dir.exists());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    fn command_config_path(spec: &CommandSpec) -> Option<PathBuf> {
        spec.args
            .iter()
            .position(|arg| arg == "--config-file")
            .and_then(|index| spec.args.get(index + 1))
            .map(PathBuf::from)
    }

    fn take_latest_progress(receiver: &mut JobProgressReceiver) -> JobProgress {
        assert!(
            receiver
                .has_changed()
                .expect("progress sender should exist"),
            "progress should be sent"
        );
        receiver
            .borrow_and_update()
            .clone()
            .expect("changed progress should have a value")
    }

    fn assert_no_progress(receiver: &JobProgressReceiver) {
        assert!(
            !receiver
                .has_changed()
                .expect("progress sender should exist")
        );
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
        for token in ["BVopaque", "bvopaque", "av123", "ep456"] {
            assert_eq!(
                video_identity(&JobRequest::Bilibili {
                    url: format!("https://b23.tv/{token}"),
                    selection: None,
                }),
                None,
                "short-link tokens must not be treated as direct Bilibili identities"
            );
        }
        assert_eq!(
            youtube_id_from_url("https://notyoutube.com/watch?v=PHH1wTDF-1M"),
            None
        );
    }

    #[test]
    fn selection_required_probe_errors_override_duplicate_identity_fallbacks() {
        let error = anyhow::Error::from(bbdown_core::Error::SelectionRequired {
            input_kind: "season",
        })
        .context("Bilibili plan probe failed");

        assert!(should_propagate_bilibili_probe_error(None, &error));
        assert!(!should_propagate_bilibili_probe_error(
            Some(BilibiliSelection::Latest),
            &error
        ));
        assert!(!should_propagate_bilibili_probe_error(
            None,
            &anyhow!("ordinary probe failure")
        ));
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
        let nfo = video.with_extension("NFO");
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
        let overwrite_confirmation = index
            .overwrite_confirmation(&video)
            .expect("indexed video should retain an overwrite handle");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: Some(overwrite_confirmation),
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid456".to_string(),
            },
            existing_videos: vec![video.clone()],
        };
        assert!(duplicate.allows_overwrite_for(&job));
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
    fn duplicate_identity_inventory_scans_each_directory_once() {
        let video_dir = temp_test_dir("duplicate-identity-linear-scan");
        for index in 0..64 {
            let video = video_dir.join(format!("Video {index}.mp4"));
            fs::write(&video, "video").expect("video should write");
            fs::write(
                video.with_extension("nfo"),
                format!("<movie><uniqueid type=\"youtube\">id-{index}</uniqueid></movie>"),
            )
            .expect("sidecar should write");
        }

        let inventory = build_identity_media_inventory(&video_dir, StagedPrimaryMediaKind::Video)
            .expect("identity inventory should build");

        assert_eq!(inventory.directory_scan_count, 1);
        assert_eq!(inventory.media_files.len(), 64);
        assert_eq!(inventory.metadata_sidecars.len(), 64);
        assert!(
            inventory
                .metadata_sidecars
                .values()
                .all(|sidecars| sidecars.len() == 1)
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn duplicate_without_a_retained_target_handle_never_allows_overwrite() {
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Youtube,
                id: "PHH1wTDF-1M".to_string(),
            },
            existing_videos: vec![PathBuf::from("Example [PHH1wTDF-1M].mkv")],
        };

        assert!(!duplicate.allows_overwrite_for(&JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
        }));
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
    fn bilibili_overwrite_plan_accepts_the_confirmed_entry_identity() {
        let plan = BilibiliDownloadPlan {
            title: "Season".to_string(),
            entries: vec![BilibiliDownloadEntry {
                index: 1,
                aid: 10,
                bvid: Some("BV123".to_string()),
                cid: 20,
                epid: Some(30),
                title: "Episode".to_string(),
            }],
        };

        ensure_bilibili_overwrite_plan_matches(
            &plan,
            &VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid20".to_string(),
            },
        )
        .expect("same resolved entry should remain overwrite-safe");
    }

    #[test]
    fn bilibili_overwrite_plan_rejects_latest_episode_drift() {
        let plan = BilibiliDownloadPlan {
            title: "Season".to_string(),
            entries: vec![BilibiliDownloadEntry {
                index: 1,
                aid: 10,
                bvid: Some("BV123".to_string()),
                cid: 21,
                epid: Some(31),
                title: "New episode".to_string(),
            }],
        };

        let error = ensure_bilibili_overwrite_plan_matches(
            &plan,
            &VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid20".to_string(),
            },
        )
        .expect_err("changed latest episode must not overwrite the confirmed target");

        assert!(error.to_string().contains("changed after confirmation"));
        assert!(error.to_string().contains("cid21"));
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
            video_path.with_extension("INFO.JSON"),
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

    #[cfg(unix)]
    #[tokio::test]
    async fn bound_staging_rejects_retargeted_root_and_cleans_only_the_original() {
        use std::os::unix::fs::symlink;

        let parent = temp_test_dir("bound-staging-root-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        fs::create_dir_all(&original).expect("original root should create");
        fs::create_dir_all(&replacement).expect("replacement root should create");
        symlink(&original, &configured).expect("configured root symlink should create");
        let root = RootedFs::new(&configured).expect("configured root should bind");
        let staging = create_video_staging_dir(&root).expect("staging directory should create");
        let relative_staging = staging
            .path()
            .strip_prefix(&configured)
            .expect("staging should be output-relative")
            .to_path_buf();

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");
        let replacement_staging = replacement.join(&relative_staging);
        fs::create_dir_all(&replacement_staging)
            .expect("replacement staging directory should create");
        fs::write(replacement_staging.join("sentinel"), "replacement")
            .expect("replacement sentinel should write");
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "printf original > download.mkv".to_string(),
            ],
            cwd: staging.path().to_path_buf(),
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };
        let output = run_command_with_bound_cwd(&test_config(), &spec, &staging.directory, None)
            .await
            .expect("bound child writer should run after root retarget");
        assert!(output.status.success());

        assert_eq!(
            fs::read_to_string(original.join(&relative_staging).join("download.mkv")).unwrap(),
            "original"
        );
        assert!(!replacement_staging.join("download.mkv").exists());

        let error = staging
            .validate_for_path_access()
            .expect_err("retargeted output root must fail before commit");
        assert!(format!("{error:#}").contains("different directory"));
        drop(staging);

        assert!(!original.join(relative_staging).exists());
        assert_eq!(
            fs::read_to_string(replacement_staging.join("sentinel")).unwrap(),
            "replacement"
        );
        let _ = fs::remove_dir_all(parent);
    }

    #[test]
    fn startup_recovery_rolls_forward_partial_keep_both_publication() {
        let final_dir = temp_test_dir("staged-publication-roll-forward");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let mut staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        let staged_nfo = staging_dir.join("Episode.nfo");
        fs::write(&staged_video, "new-video").expect("staged video should write");
        fs::write(&staged_nfo, "new-nfo").expect("staged NFO should write");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            &[staged_video, staged_nfo],
            None,
            StagedPrimaryMediaKind::Video,
        )
        .expect("keep-both move plan should build");
        prepare_staged_publication(&root, &staging, &mut plan, None)
            .expect("publication manifest should persist");
        assert_eq!(plan.len(), 2);
        move_step_with_bound_parents(&root, plan[0].clone())
            .expect("first publication step should move");
        staging.removed = true;
        drop(staging);
        drop(root);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("public startup recovery should roll publication forward");

        assert_eq!(
            fs::read_to_string(final_dir.join("Episode.mkv")).unwrap(),
            "new-video"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("Episode.nfo")).unwrap(),
            "new-nfo"
        );
        assert!(!staging_dir.exists());
        assert!(
            report.iter().any(|line| {
                line.contains("Rolled forward interrupted staged video publication")
            })
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn staged_publication_syncs_nested_source_directories_before_the_manifest() {
        let final_dir = temp_test_dir("staged-publication-directory-sync");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let first_dir = staging_dir.join("series");
        let second_dir = first_dir.join("episode");
        fs::create_dir_all(&second_dir).expect("nested staging directories should create");
        let staged_video = second_dir.join("Episode.mkv");
        let staged_nfo = first_dir.join("Episode.nfo");
        fs::write(&staged_video, "new-video").expect("staged video should write");
        fs::write(&staged_nfo, "new-nfo").expect("staged NFO should write");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            &[staged_video, staged_nfo],
            None,
            StagedPrimaryMediaKind::Video,
        )
        .expect("nested move plan should build");
        let manifest = staging_dir.join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME);
        let mut synced = Vec::new();

        prepare_staged_publication_with_limits_and_sync(
            &root,
            &staging,
            &mut plan,
            None,
            VIDEO_STAGING_PUBLICATION_MAX_STEPS,
            VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT,
            &mut |path, directory| {
                assert!(!manifest.exists(), "manifest must follow directory fsync");
                directory.sync_all()?;
                synced.push(path.to_path_buf());
                Ok(())
            },
        )
        .expect("publication preparation should persist directories and manifest");

        assert_eq!(synced, vec![second_dir, first_dir, staging_dir.clone()]);
        assert!(manifest.is_file());
        drop(staging);
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn completed_staged_download_is_retained_without_dirtying_global_recovery() {
        let final_dir = temp_test_dir("completed-staged-download-retention");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        fs::write(&staged_video, "completed-video").expect("completed video should write");

        staging
            .retain_for_manual_recovery(VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON)
            .expect("completion marker should persist");
        staging
            .retain_for_manual_recovery("a more specific later retention reason")
            .expect("completion marker persistence should be idempotent");
        drop(staging);
        drop(root);

        let messages = recover_pending_overwrite_transactions(&final_dir)
            .expect("startup recovery should accept the retained completed download");
        let root = RootedFs::new(&final_dir).expect("output root should reopen");
        let (state, _) =
            video_recovery_state_file(&root).expect("recovery state should reopen cleanly");

        assert!(messages.iter().any(|message| {
            message.contains("Retained completed staged video job for manual recovery")
                && message.contains(VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON)
        }));
        assert!(
            video_recovery_state_is_clean(&state)
                .expect("manual retention should be a terminal recovery state")
        );
        assert_eq!(
            fs::read_to_string(&staged_video).unwrap(),
            "completed-video"
        );
        assert!(staging_dir.is_dir());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn oversized_staged_publications_survive_startup_recovery() {
        for (case, max_steps, manifest_limit, expected_reason) in [
            (
                "steps",
                1,
                VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT,
                "file recovery limit",
            ),
            (
                "bytes",
                VIDEO_STAGING_PUBLICATION_MAX_STEPS,
                1,
                "byte limit",
            ),
        ] {
            let final_dir = temp_test_dir(&format!("staged-publication-retained-{case}"));
            let root = RootedFs::new(&final_dir).expect("output root should bind");
            let staging = create_video_staging_dir(&root)
                .expect("production staging directory should create");
            let staging_dir = staging.path().to_path_buf();
            let nested = staging_dir.join("series").join("episode");
            fs::create_dir_all(&nested).expect("nested staging directory should create");
            let staged_video = nested.join("Episode.mkv");
            let staged_nfo = nested.join("Episode.nfo");
            fs::write(&staged_video, "new-video").expect("staged video should write");
            fs::write(&staged_nfo, "new-nfo").expect("staged NFO should write");
            let mut plan = staged_move_plan(
                &staging_dir,
                &final_dir,
                &[staged_video.clone(), staged_nfo.clone()],
                None,
                StagedPrimaryMediaKind::Video,
            )
            .expect("move plan should build");

            let error = prepare_staged_publication_with_limits_and_sync(
                &root,
                &staging,
                &mut plan,
                None,
                max_steps,
                manifest_limit,
                &mut |_, directory| directory.sync_all(),
            )
            .expect_err("oversized publication should stop before moving outputs");

            assert!(format!("{error:#}").contains(expected_reason));
            assert!(
                staging_dir
                    .join(VIDEO_STAGING_RETENTION_FILE_NAME)
                    .is_file()
            );
            assert!(
                !staging_dir
                    .join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME)
                    .exists()
            );
            drop(staging);
            let report = recover_pending_video_staging_directories_locked(&root)
                .expect("startup recovery should retain completed oversized outputs");

            assert!(!report.unresolved);
            assert!(report.messages.iter().any(|message| {
                message.contains("Retained completed staged video job for manual recovery")
            }));
            assert_eq!(fs::read_to_string(&staged_video).unwrap(), "new-video");
            assert_eq!(fs::read_to_string(&staged_nfo).unwrap(), "new-nfo");
            assert!(staging_dir.is_dir());
            let _ = fs::remove_dir_all(final_dir);
        }
    }

    #[test]
    fn overwrite_output_sync_failure_preserves_staging_for_rollback() {
        assert_overwrite_commit_fault_recovers(OverwriteCommitCheckpoint::BeforeOutputSync, false);
    }

    #[test]
    fn overwrite_anchor_failure_preserves_staging_for_rollback() {
        assert_overwrite_commit_fault_recovers(
            OverwriteCommitCheckpoint::BeforeAnchorCreation,
            false,
        );
    }

    #[test]
    fn overwrite_manifest_replace_failure_preserves_staging_for_rollback() {
        assert_overwrite_commit_fault_recovers(
            OverwriteCommitCheckpoint::BeforeManifestReplace,
            false,
        );
    }

    #[test]
    fn overwrite_post_manifest_failure_preserves_staging_for_roll_forward() {
        assert_overwrite_commit_fault_recovers(
            OverwriteCommitCheckpoint::AfterManifestReplace,
            true,
        );
    }

    #[test]
    fn incomplete_move_rollback_preserves_staging_for_startup_recovery() {
        let final_dir = temp_test_dir("staged-publication-incomplete-move-rollback");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        fs::write(&staged_video, "new-video").expect("staged video should write");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            std::slice::from_ref(&staged_video),
            None,
            StagedPrimaryMediaKind::Video,
        )
        .expect("keep-both move plan should build");
        prepare_staged_publication(&root, &staging, &mut plan, None)
            .expect("publication manifest should persist");
        let error =
            execute_move_plan_with_hook(&root, plan, StagedPrimaryMediaKind::Video, &mut |moved| {
                fs::write(&moved.source, "rollback-blocker")
                    .context("failed to install rollback blocker")?;
                bail!("injected post-publication move failure");
            })
            .expect_err("occupied source should make the injected rollback incomplete");
        assert!(incomplete_move_rollback(&error));
        let error = finish_failed_staged_move(error, None, Some(&staging));
        assert!(incomplete_move_rollback(&error));
        drop(staging);
        assert!(staging_dir.is_dir());
        assert!(
            staging_dir
                .join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME)
                .is_file()
        );

        fs::remove_file(&staged_video).expect("rollback blocker should remove");
        let report = recover_pending_video_staging_directories_locked(&root)
            .expect("startup should finish the retained publication");

        assert!(!report.unresolved);
        assert_eq!(
            fs::read_to_string(final_dir.join("Episode.mkv")).unwrap(),
            "new-video"
        );
        assert!(!staging_dir.exists());
        assert!(
            report.messages.iter().any(|line| {
                line.contains("Rolled forward interrupted staged video publication")
            })
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn unresolved_staging_rollback_defers_referenced_overwrite_recovery() {
        let final_dir = temp_test_dir("staged-publication-deferred-overwrite");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        fs::write(&staged_video, "replacement-video").expect("replacement should stage");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            std::slice::from_ref(&staged_video),
            Some(&existing),
            StagedPrimaryMediaKind::Video,
        )
        .expect("overwrite move plan should build");
        prepare_staged_publication(&root, &staging, &mut plan, Some(&acquired))
            .expect("publication manifest should persist");
        execute_move_plan(&root, plan, StagedPrimaryMediaKind::Video)
            .expect("replacement should publish");
        staging.preserve_for_recovery();
        drop(staging);
        drop(acquired);

        let mut injected = false;
        let first_staging = recover_pending_video_staging_directories_locked_with_hook(
            &root,
            &mut |direction, _| {
                if !injected && direction == StagedPublicationDirection::RollBack {
                    injected = true;
                    bail!("injected failure after rollback rename");
                }
                Ok(())
            },
        )
        .expect("failed staging recovery should be reported");
        assert!(injected);
        assert!(first_staging.unresolved);
        assert_eq!(first_staging.blocked_overwrites.len(), 1);
        assert!(!existing.exists());
        assert_eq!(
            fs::read_to_string(&staged_video).unwrap(),
            "replacement-video"
        );

        let first_overwrite = recover_pending_overwrite_transactions_locked(
            &root,
            &final_dir,
            &first_staging.blocked_overwrites,
        )
        .expect("referenced overwrite scan should defer without failing");
        assert!(first_overwrite.unresolved);
        assert!(first_overwrite.messages.iter().any(|line| {
            line.contains(
                "Deferred overwrite transaction referenced by an unresolved staged publication",
            )
        }));
        assert_eq!(overwrite_backup_dirs(&final_dir).len(), 1);
        assert!(!existing.exists());

        let second_staging = recover_pending_video_staging_directories_locked(&root)
            .expect("staging rollback retry should finish");
        assert!(!second_staging.unresolved);
        assert!(second_staging.blocked_overwrites.is_empty());
        let second_overwrite = recover_pending_overwrite_transactions_locked(
            &root,
            &final_dir,
            &second_staging.blocked_overwrites,
        )
        .expect("overwrite recovery should resume after staging resolves");

        assert!(!second_overwrite.unresolved);
        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(!staging_dir.exists());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_rolls_back_partial_overwrite_publication() {
        let final_dir = temp_test_dir("staged-publication-roll-back");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let mut staging =
            create_video_staging_dir(&root).expect("production staging directory should create");
        let staging_dir = staging.path().to_path_buf();
        let staged_video = staging_dir.join("Episode.mkv");
        fs::write(&staged_video, "replacement-video").expect("replacement should stage");
        let mut plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            &[staged_video],
            Some(&existing),
            StagedPrimaryMediaKind::Video,
        )
        .expect("overwrite move plan should build");
        prepare_staged_publication(&root, &staging, &mut plan, Some(&acquired))
            .expect("overwrite publication manifest should persist");
        move_step_with_bound_parents(&root, plan[0].clone())
            .expect("replacement publication step should move");
        staging.removed = true;
        drop(staging);
        drop(acquired);
        drop(root);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("public startup recovery should roll overwrite back");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(!staging_dir.exists());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        assert!(
            report
                .iter()
                .any(|line| { line.contains("Rolled back interrupted staged video publication") })
        );
        assert!(
            report
                .iter()
                .any(|line| line.contains("Restored interrupted"))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn staged_move_rollback_uses_captured_parent_directories() {
        let root_dir = temp_test_dir("bound-staged-move-rollback");
        let staging_dir = root_dir.join("staging");
        let destination_dir = root_dir.join("library");
        let renamed_destination_dir = root_dir.join("library-renamed");
        fs::create_dir_all(&staging_dir).expect("staging directory should create");
        fs::create_dir_all(&destination_dir).expect("destination directory should create");
        let source = staging_dir.join("video.mkv");
        let destination = destination_dir.join("video.mkv");
        fs::write(&source, "video").expect("staged video should write");
        let root = RootedFs::new(&root_dir).expect("output root should bind");
        let moved = move_step_with_bound_parents(
            &root,
            MoveStep {
                source: source.clone(),
                destination: destination.clone(),
                expected_identity: None,
            },
        )
        .expect("staged file should move");

        fs::rename(&destination_dir, &renamed_destination_dir)
            .expect("destination parent should rename");
        rollback_moves(&root, &[moved])
            .expect("rollback should use the captured destination parent");

        assert_eq!(fs::read_to_string(&source).unwrap(), "video");
        assert!(!renamed_destination_dir.join("video.mkv").exists());
        let _ = fs::remove_dir_all(root_dir);
    }

    #[test]
    fn overwrite_rejects_a_same_identity_replacement_after_confirmation() {
        let mut config = test_config();
        let final_dir = temp_test_dir("overwrite-confirmed-object-replaced");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging directory should create");
        config.downloads.video_dir = final_dir.clone();
        let existing = final_dir.join("Example [PHH1wTDF-1M].mkv");
        fs::write(&existing, "confirmed-video").expect("confirmed video should write");
        let job = JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string(),
        };
        let duplicate = find_video_duplicate_without_probe(&config, &job)
            .expect("duplicate scan should succeed")
            .expect("confirmed duplicate should exist");
        let confirmed_file = duplicate
            .overwrite_confirmation
            .as_ref()
            .expect("overwrite confirmation should retain a file handle")
            .target_file
            .clone();

        fs::remove_file(&existing).expect("confirmed video should remove");
        confirmed_file
            .validate_identity()
            .expect("the unlinked confirmed object should remain held open");
        fs::write(&existing, "replacement-video").expect("replacement video should write");
        let staged = staging_dir.join("Example [PHH1wTDF-1M].mkv");
        fs::write(&staged, "new-video").expect("staged video should write");
        let staged_files = collect_regular_files(&staging_dir).expect("staging should scan");
        let root = RootedFs::new(&final_dir).expect("output root should bind");

        let error = move_staged_video_files_with_root(
            MoveExecutionContext {
                root: &root,
                staging: None,
            },
            &staging_dir,
            &final_dir,
            &staged_files,
            VideoDuplicateAction::Overwrite,
            &duplicate,
            StagedPrimaryMediaKind::Video,
        )
        .expect_err("replacement object must not inherit overwrite confirmation");

        assert!(error.to_string().contains("changed after confirmation"));
        assert_eq!(fs::read_to_string(existing).unwrap(), "replacement-video");
        assert_eq!(fs::read_to_string(staged).unwrap(), "new-video");
        let _ = fs::remove_dir_all(final_dir);
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
    fn artifact_only_overwrite_preserves_a_dotted_video_stem() {
        let final_dir = temp_test_dir("artifact-only-overwrite-dotted-stem");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Show.S01E01.mkv");
        let xml = final_dir.join("Show.S01E01.xml");
        fs::write(&existing, "video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        fs::write(&xml, "old-xml").expect("old xml should write");
        fs::write(staging_dir.join("New.Title.xml"), "new-xml").expect("new xml should write");
        let staged_files = collect_regular_files(&staging_dir).expect("staged files should scan");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
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
        .expect("dotted sidecar should overwrite in place");

        assert_eq!(moved, vec![xml.clone()]);
        assert_eq!(fs::read_to_string(&xml).unwrap(), "new-xml");
        assert!(!final_dir.join("Show.S01E01.S01E01.xml").exists());
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
    fn overwrite_commits_when_the_replacement_extension_changes() {
        let final_dir = temp_test_dir("overwrite-extension-change");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Episode [PHH1wTDF-1M].mkv");
        fs::write(&existing, "old-video").expect("existing video should write");
        fs::write(existing.with_extension("nfo"), "old-nfo").expect("old nfo should write");
        let staged = staging_dir.join("Episode [PHH1wTDF-1M].mp4");
        fs::write(&staged, "new-video").expect("staged video should write");
        fs::write(staged.with_extension("nfo"), "new-nfo").expect("new nfo should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
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
        .expect("extension-changing overwrite should commit");

        let replacement = existing.with_extension("mp4");
        assert_eq!(moved, vec![replacement.clone()]);
        assert!(!existing.exists());
        assert_eq!(fs::read_to_string(replacement).unwrap(), "new-video");
        assert_eq!(
            fs::read_to_string(existing.with_extension("nfo")).unwrap(),
            "new-nfo"
        );
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_preserves_ambiguous_bare_bilibili_danmaku_sidecar() {
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
            overwrite_confirmation: None,
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
        assert_eq!(
            fs::read_to_string(final_dir.join("danmaku.xml"))
                .expect("ambiguous bare danmaku should remain"),
            "old-danmaku"
        );
        assert!(
            !final_dir.join("danmaku (2).xml").exists(),
            "new bare danmaku should not be moved as an unrelated relative artifact"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_preserves_ambiguous_unbound_bilibili_subtitle_sidecar() {
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
            overwrite_confirmation: None,
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
        assert_eq!(
            fs::read_to_string(final_dir.join("subtitle-zh-01-old.ass"))
                .expect("ambiguous unbound subtitle should remain"),
            "old-subtitle"
        );
        assert!(
            !final_dir.join("subtitle-zh-01-new.ass").exists(),
            "new unbound subtitle should not be moved as an unrelated relative artifact"
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_preserves_ambiguous_unbound_bilibili_cover_sidecar() {
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
            overwrite_confirmation: None,
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
        assert_eq!(
            fs::read_to_string(final_dir.join("cover-image-old.jpg"))
                .expect("ambiguous unbound cover should remain"),
            "old-cover"
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
            overwrite_confirmation: None,
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
    fn overwrite_falls_back_to_keep_both_for_multiple_primary_files() {
        let final_dir = temp_test_dir("overwrite-multiple-primary-files");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Episode [cid123].flv");
        fs::write(&existing, "old-video").expect("existing video should write");
        let first_staged = staging_dir.join("Episode [cid123].flv");
        let second_staged = staging_dir.join("Episode [cid123].part2.flv");
        fs::write(&first_staged, "new-first").expect("first staged video should write");
        fs::write(&second_staged, "new-second").expect("second staged video should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
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
        .expect("multiple completed outputs should be kept alongside the existing video");

        let kept_first = final_dir.join("Episode [cid123] (2).flv");
        let kept_second = final_dir.join("Episode [cid123].part2.flv");
        assert_eq!(
            moved.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([kept_first.clone(), kept_second.clone()])
        );
        assert_eq!(fs::read_to_string(existing).unwrap(), "old-video");
        assert_eq!(fs::read_to_string(kept_first).unwrap(), "new-first");
        assert_eq!(fs::read_to_string(kept_second).unwrap(), "new-second");
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidates_mixed_case_identity_sidecars() {
        let final_dir = temp_test_dir("overwrite-mixed-case-identity-sidecars");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "old-video").expect("existing video should write");
        let old_nfo = existing.with_extension("NFO");
        fs::write(
            &old_nfo,
            "<movie><uniqueid type=\"bilibili-cid\">cid123</uniqueid></movie>",
        )
        .expect("mixed-case identity NFO should write");
        let staged = staging_dir.join("Episode.mkv");
        fs::write(&staged, "new-video").expect("staged video should write");
        fs::write(staged.with_extension("nfo"), "new-nfo").expect("staged NFO should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
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
        .expect("mixed-case identity metadata should survive overwrite revalidation");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(fs::read_to_string(&existing).unwrap(), "new-video");
        assert_eq!(
            fs::read_to_string(existing.with_extension("nfo")).unwrap(),
            "new-nfo"
        );
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_revalidates_mixed_case_info_json_sidecars() {
        let final_dir = temp_test_dir("overwrite-mixed-case-info-json");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        fs::create_dir_all(&staging_dir).expect("staging dir should create");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "old-video").expect("existing video should write");
        let old_info_json = existing.with_extension("INFO.JSON");
        fs::write(&old_info_json, r#"{"id":"cid123","extractor":"bilibili"}"#)
            .expect("mixed-case identity info JSON should write");
        let staged = staging_dir.join("Episode.mkv");
        fs::write(&staged, "new-video").expect("staged video should write");
        fs::write(staged.with_extension("info.json"), r#"{"id":"cid123"}"#)
            .expect("staged info JSON should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
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
        .expect("mixed-case info JSON should survive overwrite revalidation");

        assert_eq!(moved, vec![existing.clone()]);
        assert_eq!(fs::read_to_string(&existing).unwrap(), "new-video");
        assert_eq!(
            fs::read_to_string(existing.with_extension("info.json")).unwrap(),
            r#"{"id":"cid123"}"#
        );
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
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
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };

        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let backup_dir = acquired.recovery.backup_dir.clone();
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
    fn acquired_overwrite_restores_through_renamed_parent_binding() {
        let final_dir = temp_test_dir("overwrite-renamed-parent-rollback");
        let library = final_dir.join("library");
        let renamed_library = final_dir.join("library-renamed");
        fs::create_dir_all(&library).expect("library should create");
        let existing = library.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");

        fs::rename(&library, &renamed_library).expect("library should be renamed");
        acquired
            .restore()
            .expect("bound rollback should restore into the same directory object");

        assert_eq!(
            fs::read_to_string(renamed_library.join("Episode.mkv"))
                .expect("video should be restored under renamed parent"),
            "original-video"
        );
        assert!(renamed_library.join("Episode.nfo").is_file());
        assert!(
            fs::read_dir(&renamed_library)
                .expect("renamed library should scan")
                .all(|entry| !entry
                    .expect("entry should read")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(OVERWRITE_BACKUP_DIR_PREFIX))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_restores_an_interrupted_overwrite() {
        let final_dir = temp_test_dir("overwrite-startup-restore");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        drop(acquired);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("interrupted overwrite should recover");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        assert!(
            report
                .iter()
                .any(|line| line.contains("Restored interrupted"))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_ignores_an_incomplete_atomic_manifest_temp() {
        let final_dir = temp_test_dir("overwrite-startup-manifest-temp");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let transition_temp = acquired
            .recovery
            .backup_dir
            .join(OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME);
        fs::write(&transition_temp, b"{partial-committed-manifest")
            .expect("interrupted transition temp should write");
        drop(acquired);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("intact acquired manifest should remain recoverable");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        assert!(
            report
                .iter()
                .any(|line| line.contains("Restored interrupted"))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn overwrite_move_plan_commits_primary_media_before_sidecars() {
        let final_dir = temp_test_dir("overwrite-primary-first");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-1");
        let staged_video = staging_dir.join("Episode.mkv");
        let staged_nfo = staging_dir.join("Episode.nfo");
        let overwrite_target = final_dir.join("Existing.mkv");

        let plan = staged_move_plan(
            &staging_dir,
            &final_dir,
            &[staged_nfo, staged_video.clone()],
            Some(&overwrite_target),
            StagedPrimaryMediaKind::Video,
        )
        .expect("overwrite move plan should build");

        assert_eq!(plan[0].source, staged_video);
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_retains_a_malformed_transaction_without_blocking_startup() {
        let final_dir = temp_test_dir("overwrite-startup-malformed");
        let transaction = final_dir.join(format!("{OVERWRITE_BACKUP_DIR_PREFIX}-malformed"));
        fs::create_dir(&transaction).expect("transaction directory should create");
        fs::set_permissions(&transaction, fs::Permissions::from_mode(0o700))
            .expect("transaction directory should become private");
        let manifest = transaction.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
        fs::write(&manifest, b"{").expect("malformed manifest should write");
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600))
            .expect("manifest should become private");

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("malformed recovery state should be reported without aborting startup");

        assert!(transaction.is_dir());
        assert!(manifest.is_file());
        assert!(report.iter().any(|line| {
            line.contains("Retained unresolved overwrite transaction")
                && line.contains("failed to parse overwrite recovery manifest")
        }));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_retains_legacy_committed_transactions_without_anchors() {
        let final_dir = temp_test_dir("overwrite-startup-legacy-committed");
        let output = final_dir.join("Episode.mkv");
        fs::write(&output, "new-video").expect("committed output should write");
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let identity = root
            .entry_identity(&output)
            .expect("output identity should read")
            .expect("output should exist");
        let transaction = final_dir.join(format!("{OVERWRITE_BACKUP_DIR_PREFIX}-legacy-committed"));
        fs::create_dir(&transaction).expect("transaction directory should create");
        fs::set_permissions(&transaction, fs::Permissions::from_mode(0o700))
            .expect("transaction directory should become private");
        fs::write(transaction.join("Episode.mkv"), "old-video")
            .expect("legacy backup should write");
        let manifest = serde_json::json!({
            "version": OVERWRITE_RECOVERY_LEGACY_MANIFEST_VERSION,
            "target_file_name": "Episode.mkv",
            "phase": "committed",
            "committed_files": [{
                "file_name": "Episode.mkv",
                "device": identity.device(),
                "inode": identity.inode()
            }]
        });
        let manifest_path = transaction.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("legacy manifest should encode"),
        )
        .expect("legacy manifest should write");
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
            .expect("legacy manifest should become private");

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("legacy committed transaction should be retained without blocking startup");

        assert_eq!(fs::read_to_string(&output).unwrap(), "new-video");
        assert_eq!(
            fs::read_to_string(transaction.join("Episode.mkv")).unwrap(),
            "old-video"
        );
        assert!(report.iter().any(|line| {
            line.contains("Retained unresolved overwrite transaction")
                && line.contains("no authenticated ownership binding")
        }));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[cfg(unix)]
    #[test]
    fn startup_recovery_retains_a_copied_current_overwrite_transaction() {
        let final_dir = temp_test_dir("overwrite-startup-copied-current");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing video should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let forged = final_dir.join(format!("{OVERWRITE_BACKUP_DIR_PREFIX}-copied"));
        fs::create_dir(&forged).expect("copied transaction directory should create");
        fs::set_permissions(&forged, fs::Permissions::from_mode(0o700))
            .expect("copied transaction directory should become private");
        for entry in
            fs::read_dir(&acquired.recovery.backup_dir).expect("authentic transaction should scan")
        {
            let entry = entry.expect("authentic transaction entry should read");
            if entry
                .file_type()
                .expect("transaction entry type should read")
                .is_file()
            {
                fs::copy(entry.path(), forged.join(entry.file_name()))
                    .expect("transaction entry should copy");
            }
        }
        let forged_manifest = forged.join(OVERWRITE_RECOVERY_MANIFEST_NAME);
        fs::set_permissions(&forged_manifest, fs::Permissions::from_mode(0o600))
            .expect("copied manifest should become private");
        acquired
            .restore()
            .expect("authentic transaction should restore before startup recovery");
        drop(root);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("copied transaction should be retained without blocking startup");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert_eq!(
            fs::read_to_string(forged.join("Episode.mkv")).unwrap(),
            "original-video"
        );
        assert!(forged.is_dir());
        assert!(report.iter().any(|line| {
            line.contains("Retained unresolved overwrite transaction")
                && line.contains("ownership record does not match")
        }));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[cfg(unix)]
    #[test]
    fn startup_recovery_skips_an_unreadable_unrelated_directory() {
        let final_dir = temp_test_dir("overwrite-startup-unreadable-child");
        let unreadable = final_dir.join("unrelated");
        fs::create_dir(&unreadable).expect("unrelated directory should create");
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000))
            .expect("unrelated directory should become unreadable");

        let recovery = recover_pending_overwrite_transactions(&final_dir);
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o700))
            .expect("unrelated directory permissions should restore");
        let report = recovery.expect("an unrelated unreadable child must not block startup");

        assert!(report.iter().any(|line| {
            line.contains("Skipped unreadable directory during overwrite recovery scan")
                && line.contains("unrelated")
        }));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_retains_an_uncommitted_replacement_and_its_backups() {
        let final_dir = temp_test_dir("overwrite-startup-uncommitted-replacement");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        fs::write(&existing, "replacement-video").expect("replacement should write");
        drop(acquired);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("uncommitted overwrite should be retained without blocking startup");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "replacement-video");
        assert!(!existing.with_extension("nfo").exists());
        let transactions = overwrite_backup_dirs(&final_dir);
        assert_eq!(transactions.len(), 1);
        assert_eq!(
            fs::read_to_string(transactions[0].join("Episode.mkv")).unwrap(),
            "original-video"
        );
        assert!(transactions[0].join("Episode.nfo").is_file());
        assert!(report.iter().any(|line| {
            line.contains("Retained unresolved overwrite transaction")
                && line.contains("uncommitted overwrite destination is occupied")
        }));
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_finalizes_a_committed_replacement_by_identity() {
        let final_dir = temp_test_dir("overwrite-startup-committed");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let mut acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let staged = final_dir
            .join(VIDEO_STAGING_DIR_NAME)
            .join("job-1")
            .join("Episode.mkv");
        fs::create_dir_all(staged.parent().unwrap()).expect("staging directory should create");
        fs::write(&staged, "replacement-video").expect("replacement should stage");
        let moved = move_step_with_bound_parents(
            &root,
            MoveStep {
                source: staged,
                destination: existing.clone(),
                expected_identity: None,
            },
        )
        .expect("replacement should move into the acquired target");
        let transaction = acquired.recovery.backup_dir.clone();
        let committed = persist_committed_overwrite_manifest(&mut acquired, &[moved], &existing)
            .expect("committed replacement identity should persist");
        assert_eq!(committed.anchors.len(), committed.files.len());
        fs::write(
            transaction.join(OVERWRITE_RECOVERY_MANIFEST_TEMP_NAME),
            b"old-complete-acquired-manifest",
        )
        .expect("displaced manifest temp should write");
        drop(committed);
        drop(acquired);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("committed overwrite should recover");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "replacement-video");
        assert!(!existing.with_extension("nfo").exists());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        assert!(
            report
                .iter()
                .any(|line| line.contains("Finalized committed overwrite file"))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn oversized_committed_overwrite_manifest_stays_in_recoverable_acquired_state() {
        let final_dir = temp_test_dir("overwrite-oversized-committed-manifest");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should bind");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let mut acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let staging_dir = final_dir.join(VIDEO_STAGING_DIR_NAME).join("job-oversized");
        fs::create_dir_all(&staging_dir).expect("staging directory should create");
        let mut moved = Vec::new();
        for index in 0..128 {
            let file_name = if index == 0 {
                "Episode.mkv".to_string()
            } else {
                format!("artifact-{index:03}-{}.json", "x".repeat(160))
            };
            let source = staging_dir.join(&file_name);
            let destination = final_dir.join(&file_name);
            fs::write(&source, format!("replacement-{index}"))
                .expect("replacement output should stage");
            moved.push(
                move_step_with_bound_parents(
                    &root,
                    MoveStep {
                        source,
                        destination,
                        expected_identity: None,
                    },
                )
                .expect("replacement output should publish"),
            );
        }

        let error = persist_committed_overwrite_manifest(&mut acquired, &moved, &existing)
            .expect_err("oversized committed recovery state should be rejected");

        assert!(
            format!("{error:#}").contains("overwrite recovery manifest exceeds its size limit")
        );
        let manifest_file = root
            .open_bound_file(&acquired.recovery.manifest_path)
            .expect("acquired manifest should open")
            .expect("acquired manifest should remain");
        let manifest: OverwriteRecoveryManifest = serde_json::from_slice(
            &manifest_file
                .read_limited(OVERWRITE_RECOVERY_MANIFEST_LIMIT)
                .expect("acquired manifest should remain readable"),
        )
        .expect("acquired manifest should parse");
        assert_eq!(manifest.phase, OverwriteRecoveryPhase::Acquired);
        assert!(
            fs::read_dir(&acquired.recovery.backup_dir)
                .expect("overwrite transaction should read")
                .filter_map(|entry| entry.ok())
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(OVERWRITE_COMMITTED_ANCHOR_PREFIX))
        );

        for output in &moved {
            fs::remove_file(&output.destination).expect("uncommitted output should remove");
        }
        drop(moved);
        drop(acquired);
        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("acquired overwrite should recover");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(overwrite_backup_dirs(&final_dir).is_empty());
        assert!(
            report
                .iter()
                .any(|line| line.contains("Restored interrupted overwrite"))
        );
        let _ = fs::remove_dir_all(final_dir);
    }

    #[test]
    fn startup_recovery_preserves_backups_when_a_committed_path_is_replaced() {
        let final_dir = temp_test_dir("overwrite-startup-committed-replaced");
        let existing = final_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        write_bilibili_identity_nfo(&existing, "cid123");
        let root = RootedFs::new(&final_dir).expect("output root should open");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let mut acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        let staged = final_dir
            .join(VIDEO_STAGING_DIR_NAME)
            .join("job-1")
            .join("Episode.mkv");
        fs::create_dir_all(staged.parent().unwrap()).expect("staging directory should create");
        fs::write(&staged, "committed-video").expect("replacement should stage");
        let moved = move_step_with_bound_parents(
            &root,
            MoveStep {
                source: staged,
                destination: existing.clone(),
                expected_identity: None,
            },
        )
        .expect("replacement should move into the acquired target");
        let committed = persist_committed_overwrite_manifest(&mut acquired, &[moved], &existing)
            .expect("committed replacement identity should persist");
        let anchor_name = committed.manifest.committed_files[0]
            .anchor_name
            .as_ref()
            .expect("committed file should have an anchor");
        let anchor_path = acquired.recovery.backup_dir.join(anchor_name);
        fs::remove_file(&existing).expect("committed path should unlink");
        fs::write(&existing, "third-party-replacement").expect("third-party file should write");
        assert_eq!(
            fs::read_to_string(&anchor_path).expect("anchor should retain committed output"),
            "committed-video"
        );
        assert_ne!(
            root.entry_identity(&existing).unwrap(),
            root.entry_identity(&anchor_path).unwrap()
        );
        drop(committed);
        drop(acquired);

        let report = recover_pending_overwrite_transactions(&final_dir)
            .expect("identity mismatch should be reported without blocking startup");

        assert_eq!(
            fs::read_to_string(&existing).unwrap(),
            "third-party-replacement"
        );
        let transactions = overwrite_backup_dirs(&final_dir);
        assert_eq!(transactions.len(), 1);
        assert_eq!(
            fs::read_to_string(transactions[0].join("Episode.mkv")).unwrap(),
            "original-video"
        );
        assert!(report.iter().any(|line| {
            line.contains("Retained unresolved overwrite transaction")
                && line.contains("does not match its durable anchor")
        }));
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
            overwrite_confirmation: None,
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
        config.bot.command_timeout_seconds = 1;
        config.bot.command_idle_timeout_seconds = 37;
        config.bilibili.danmaku.enabled = false;
        config.bilibili.download_args = vec!["--only".to_string(), "audio".to_string()];

        let options =
            bilibili_core::download_options(&config).expect("Bilibili direct options should build");

        assert_eq!(options.output_dir, video_dir);
        assert_eq!(options.mode, DownloadMode::AudioOnly);
        assert!(!options.include_danmaku);
        assert!(!options.sidecars.danmaku);
        assert!(!options.media_hosts.allow_pcdn);
        assert_eq!(options.download_idle_timeout, Some(Duration::from_secs(37)));
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
        let mut report = parse_bilibili_download_report(
            r#"
            {"title":"Season","output_dir":".","entries":[{"index":1,"title":"Episode 1","directory":"Episode 1","files":[{"kind":"video","path":"Episode 1/video.m4s"},{"kind":"audio","path":"Episode 1/audio.m4s"},{"kind":"danmaku","path":"Episode 1/Episode 1.xml"}],"mux":{"output_path":"Episode 1/Episode 1.mkv"}}]}
            "#,
        )
        .expect("download report JSON should parse");

        let rooted = RootedFs::new(&root).expect("output root should bind");
        bind_existing_bilibili_mux_state(&rooted, &root, &mut report)
            .expect("mux state should bind");
        cleanup_bilibili_mux_input_files(&rooted, &root, &report)
            .expect("raw inputs should clean up");

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

    #[cfg(unix)]
    #[test]
    fn bilibili_raw_cleanup_rejects_a_retargeted_output_root() {
        use std::os::unix::fs::symlink;

        let parent = temp_test_dir("bilibili-cleanup-root-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        fs::create_dir_all(&original).expect("original root should create");
        fs::create_dir_all(&replacement).expect("replacement root should create");
        fs::write(original.join("video.m4s"), "original")
            .expect("original raw stream should write");
        fs::write(original.join("Episode.mkv"), "mux").expect("mux output should write");
        fs::write(replacement.join("video.m4s"), "replacement")
            .expect("replacement raw stream should write");
        symlink(&original, &configured).expect("configured root symlink should create");
        let root = RootedFs::new(&configured).expect("configured root should bind");
        let mut report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","directory":".","files":[{"kind":"video","path":"video.m4s"}],"mux":{"output_path":"Episode.mkv"}}]}"#,
        )
        .expect("download report should parse");
        bind_existing_bilibili_mux_state(&root, &configured, &mut report)
            .expect("mux state should bind");

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");
        let error = cleanup_bilibili_mux_input_files(&root, &configured, &report)
            .expect_err("retargeted root must reject raw-stream cleanup");

        assert!(format!("{error:#}").contains("different directory"));
        assert_eq!(
            fs::read_to_string(replacement.join("video.m4s")).unwrap(),
            "replacement"
        );
        assert_eq!(
            fs::read_to_string(original.join("video.m4s")).unwrap(),
            "original"
        );
        let _ = fs::remove_dir_all(parent);
    }

    #[test]
    fn bilibili_raw_cleanup_preserves_inputs_when_mux_output_is_replaced() {
        let root = temp_test_dir("bilibili-cleanup-output-replaced");
        let video = root.join("video.m4s");
        let output = root.join("Episode.mkv");
        fs::write(&video, "video").expect("raw video should write");
        fs::write(&output, "mux").expect("mux output should write");
        let mut report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","directory":".","files":[{"kind":"video","path":"video.m4s"}],"mux":{"output_path":"Episode.mkv"}}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");
        bind_existing_bilibili_mux_state(&rooted, &root, &mut report)
            .expect("mux state should bind");

        fs::remove_file(&output).expect("bound mux output should unlink");
        fs::write(&output, "replacement").expect("replacement output should write");
        let error = cleanup_bilibili_mux_input_files(&rooted, &root, &report)
            .expect_err("replacement output must stop raw-input cleanup");

        assert!(format!("{error:#}").contains("mux output identity changed"));
        assert_eq!(fs::read_to_string(video).unwrap(), "video");
        assert_eq!(fs::read_to_string(output).unwrap(), "replacement");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_recovery_discards_interrupted_bilibili_mux_staging() {
        let root = temp_test_dir("bilibili-mux-staging-recovery");
        let rooted = RootedFs::new(&root).expect("output root should bind");
        let staging = create_video_staging_dir(&rooted).expect("production staging should create");
        let staging_path = staging.path().to_path_buf();
        let raw = staging_path.join("video.m4s");
        let output = staging_path.join("Episode.mp4");
        fs::write(&raw, "raw-video").expect("raw input should write");
        let inputs = vec![BilibiliMediaInput {
            kind: "video".to_string(),
            path: raw.clone(),
        }];
        let bound_inputs =
            bind_bilibili_mux_inputs(&rooted, &inputs).expect("raw input should bind");
        let reservation = ReservedMuxOutput::create(&rooted, &output, &bound_inputs)
            .expect("mux transaction should reserve");
        let transaction = reservation.staging_dir_entry.path().to_path_buf();
        fs::write(reservation.command_path(), "partial-mux")
            .expect("partial mux output should write");
        std::mem::forget(reservation);
        let mut staging = staging;
        staging.removed = true;
        drop(staging);

        let report = recover_pending_overwrite_transactions(&root)
            .expect("public startup recovery should recover interrupted production staging");

        assert!(!raw.exists());
        assert!(!output.exists());
        assert!(!transaction.exists());
        assert!(!staging_path.exists());
        assert!(
            report
                .iter()
                .any(|line| { line.contains("Discarded interrupted staged video job") })
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_recovery_retains_unowned_staging_job() {
        let root = temp_test_dir("unowned-staging-recovery");
        let staging = root.join(VIDEO_STAGING_DIR_NAME).join("job-user-owned");
        fs::create_dir_all(&staging).expect("unowned staging directory should create");
        let sentinel = staging.join("user.txt");
        fs::write(&sentinel, "user-owned").expect("sentinel should write");
        let rooted = RootedFs::new(&root).expect("output root should bind");

        let report = recover_pending_video_staging_directories_locked(&rooted)
            .expect("unowned staging scan should complete");

        assert!(report.unresolved);
        assert_eq!(fs::read_to_string(&sentinel).unwrap(), "user-owned");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_recovery_restores_anchored_mux_output_and_finishes_raw_cleanup() {
        let root = temp_test_dir("bilibili-mux-published-recovery");
        let raw = root.join("video.m4s");
        let output = root.join("Episode.mkv");
        fs::write(&raw, "raw-video").expect("raw input should write");
        fs::write(&output, "mux-output").expect("mux output should write");
        let mut download_report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","directory":".","files":[{"kind":"video","path":"video.m4s"}],"mux":{"output_path":"Episode.mkv"}}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");
        bind_existing_bilibili_mux_state(&rooted, &root, &mut download_report)
            .expect("mux recovery state should bind");
        let transaction = download_report.entries[0]
            .mux
            .as_ref()
            .and_then(|mux| mux.recovery.as_ref())
            .expect("mux recovery transaction should exist")
            .directory
            .clone();
        fs::remove_file(&output).expect("published output should unlink");

        let report = recover_pending_bilibili_mux_transactions_locked(&rooted, &root)
            .expect("published mux transaction should recover");

        assert!(!report.unresolved);
        assert_eq!(fs::read_to_string(&output).unwrap(), "mux-output");
        assert!(!raw.exists());
        assert!(!transaction.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_recovery_retains_anchor_when_mux_output_is_replaced() {
        let root = temp_test_dir("bilibili-mux-output-replaced-recovery");
        let raw = root.join("video.m4s");
        let output = root.join("Episode.mkv");
        fs::write(&raw, "raw-video").expect("raw input should write");
        fs::write(&output, "mux-output").expect("mux output should write");
        let mut download_report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","directory":".","files":[{"kind":"video","path":"video.m4s"}],"mux":{"output_path":"Episode.mkv"}}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");
        bind_existing_bilibili_mux_state(&rooted, &root, &mut download_report)
            .expect("mux recovery state should bind");
        let transaction = download_report.entries[0]
            .mux
            .as_ref()
            .and_then(|mux| mux.recovery.as_ref())
            .expect("mux recovery transaction should exist")
            .directory
            .clone();
        fs::remove_file(&output).expect("published output should unlink");
        fs::write(&output, "replacement").expect("replacement should write");

        let report = recover_pending_bilibili_mux_transactions_locked(&rooted, &root)
            .expect("replacement should be reported without destructive cleanup");

        assert!(report.unresolved);
        assert_eq!(fs::read_to_string(&output).unwrap(), "replacement");
        assert_eq!(fs::read_to_string(&raw).unwrap(), "raw-video");
        assert_eq!(
            fs::read_to_string(transaction.join(BILIBILI_MUX_RECOVERY_ANCHOR_NAME)).unwrap(),
            "mux-output"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn staging_support_files_include_managed_config_and_owner() {
        let staging_dir = PathBuf::from("/tmp/staging");

        assert!(is_staging_support_file(
            &staging_dir,
            &staging_dir.join("BBDown.config")
        ));
        assert!(is_staging_support_file(
            &staging_dir,
            &staging_dir.join(VIDEO_STAGING_OWNER_FILE_NAME)
        ));
        assert!(is_staging_support_file(
            &staging_dir,
            &staging_dir.join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME)
        ));
        assert!(is_staging_support_file(
            &staging_dir,
            &staging_dir.join(BILIBILI_WORKER_REQUEST_FILE_NAME)
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
    fn bilibili_worker_request_redacts_telegram_configuration() {
        let mut config = test_config();
        config.telegram.token = "telegram-secret-token".to_string();
        config.telegram.allowed_chat_ids = vec![42];
        let expected_identity = VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: "cid123".to_string(),
        };
        let video_dir = temp_test_dir("bilibili-worker-request");
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        let staging =
            create_video_staging_dir(&root).expect("worker staging directory should create");
        let logical_output_dir = staging.path().to_path_buf();

        let request = build_bilibili_worker_request(
            &config,
            "https://www.bilibili.com/video/BV123",
            Some(BilibiliSelection::Latest),
            Some(&expected_identity),
            &staging,
        );
        let encoded = serde_json::to_string(&request).expect("worker request should encode");

        assert!(!encoded.contains("telegram-secret-token"));
        assert_eq!(request.config.telegram.token, "redacted-worker-token");
        assert!(request.config.telegram.allowed_chat_ids.is_empty());
        assert!(request.config.telegram.allow_all_chats);
        assert_eq!(request.config.downloads.video_dir, Path::new("."));
        assert_eq!(request.expected_overwrite_identity, Some(expected_identity));
        assert_eq!(request.logical_output_dir, logical_output_dir);
        assert_eq!(request.output_root_device, root.root_identity().device());
        assert_eq!(request.output_root_inode, root.root_identity().inode());
        assert_eq!(request.staging_device, staging.identity.device());
        assert_eq!(request.staging_inode, staging.identity.inode());
        let worker_root =
            RootedFs::new(staging.path()).expect("worker should bind the staging directory");
        persist_bilibili_worker_completion(&worker_root, &request)
            .expect("worker should persist completion before reporting success");
        assert_eq!(
            retained_video_staging_reason(&root, staging.path(), staging.identity)
                .expect("parent should validate the worker completion marker")
                .as_deref(),
            Some(VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON)
        );
        drop(staging);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn completed_bilibili_segments_survive_an_oversized_mux_manifest() {
        let config = test_config();
        let video_dir = temp_test_dir("bilibili-worker-oversized-mux-manifest");
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        let staging =
            create_video_staging_dir(&root).expect("worker staging directory should create");
        let request = build_bilibili_worker_request(
            &config,
            "https://www.bilibili.com/video/BV123",
            None,
            None,
            &staging,
        );
        let worker_root =
            RootedFs::new(staging.path()).expect("worker should bind the staging directory");
        let mut media_inputs = Vec::new();
        for index in 0..128 {
            let path = staging
                .path()
                .join(format!("segment-{index:03}-{}.flv", "x".repeat(160)));
            fs::write(&path, "segment").expect("downloaded segment should write");
            media_inputs.push(BilibiliMediaInput {
                kind: "flv_segment".to_string(),
                path,
            });
        }
        let bound_inputs = bind_bilibili_mux_inputs(&worker_root, &media_inputs)
            .expect("downloaded segments should bind");

        persist_bilibili_core_download_completion(&worker_root, Some(&request))
            .expect("completed core download should be retained before mux setup");
        let error = ReservedMuxOutput::create(
            &worker_root,
            &staging.path().join("Episode.mp4"),
            &bound_inputs,
        )
        .expect_err("oversized mux recovery manifest should be rejected");

        assert!(format!("{error:#}").contains("mux recovery manifest exceeds its size limit"));
        assert_eq!(
            retained_video_staging_reason(&root, staging.path(), staging.identity)
                .expect("parent should validate the completion marker")
                .as_deref(),
            Some(VIDEO_STAGING_DOWNLOAD_COMPLETED_REASON)
        );
        assert!(media_inputs.iter().all(|input| input.path.is_file()));
        drop(staging);
        let _ = fs::remove_dir_all(video_dir);
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
        for input in &inputs {
            fs::write(&input.path, &input.kind).expect("mux input should write");
        }
        let output = entry_dir.join("Episode.mp4");
        let root = RootedFs::new(&entry_dir).expect("entry root should bind");
        let bound_inputs =
            bind_bilibili_mux_inputs(&root, &inputs).expect("mux inputs should bind");

        let BilibiliMuxCommand {
            spec,
            concat_file,
            inherited_files,
        } = bilibili_local_mux_command_spec(
            &config,
            &root,
            &inputs,
            &bound_inputs,
            &entry_dir,
            &output,
        )
        .expect("dash mux spec should build");

        assert!(concat_file.is_none());
        assert_eq!(inherited_files.len(), 2);
        assert_eq!(spec.program, PathBuf::from("/opt/bin/ffmpeg"));
        assert_eq!(spec.cwd, entry_dir);
        assert!(spec.args.contains(&"-nostdin".to_string()));
        assert!(spec.args.contains(&"/dev/fd/64".to_string()));
        assert!(spec.args.contains(&"/dev/fd/65".to_string()));
        assert!(!spec.args.iter().any(|arg| arg.ends_with("video.m4s")));
        assert!(!spec.args.iter().any(|arg| arg.ends_with("audio.m4s")));
        assert!(spec.args.windows(2).any(|args| args == ["-map", "0:0"]));
        assert!(spec.args.windows(2).any(|args| args == ["-map", "1:0"]));
        assert!(!spec.args.windows(2).any(|args| args == ["-f", "concat"]));
        assert_eq!(
            fs::read_to_string(user_concat).expect("user concat file should remain"),
            "user-owned"
        );
        let _ = fs::remove_dir_all(entry_dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bilibili_mux_rejects_a_replaced_reserved_output() {
        let root = temp_test_dir("bilibili-local-mux-output-replaced");
        let video = root.join("video.m4s");
        let fake_ffmpeg = root.join("fake-ffmpeg.sh");
        fs::write(&video, "video").expect("raw video should write");
        fs::write(
            &fake_ffmpeg,
            r#"#!/bin/sh
output=
for arg do
    output=$arg
done
test -f "$output" || exit 42
rm "$output" || exit 43
printf replacement > "$output"
"#,
        )
        .expect("fake ffmpeg should write");
        fs::set_permissions(&fake_ffmpeg, fs::Permissions::from_mode(0o700))
            .expect("fake ffmpeg should become executable");

        let mut config = test_config();
        config.tools.ffmpeg = fake_ffmpeg;
        config.bot.command_timeout_seconds = 5;
        config.bot.command_idle_timeout_seconds = 5;
        let mut report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","files":[{"kind":"video","path":"video.m4s"}]}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");

        let error =
            mux_bilibili_report_media(&config, &rooted, &root, &mut report, UNIX_EPOCH, None)
                .await
                .expect_err("replaced reserved output must reject the mux result");

        let output = root.join("Episode.mp4");
        assert!(format!("{error:#}").contains("reserved Bilibili mux output identity changed"));
        assert!(!output.exists());
        assert_eq!(fs::read_to_string(video).unwrap(), "video");
        assert!(report.entries[0].mux.is_none());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(BILIBILI_MUX_STAGING_DIR_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bilibili_mux_rejects_an_untouched_reserved_output() {
        let root = temp_test_dir("bilibili-local-mux-output-empty");
        let video = root.join("video.m4s");
        let fake_ffmpeg = root.join("fake-ffmpeg.sh");
        fs::write(&video, "video").expect("raw video should write");
        fs::write(&fake_ffmpeg, "#!/bin/sh\nexit 0\n").expect("fake ffmpeg should write");
        fs::set_permissions(&fake_ffmpeg, fs::Permissions::from_mode(0o700))
            .expect("fake ffmpeg should become executable");

        let mut config = test_config();
        config.tools.ffmpeg = fake_ffmpeg;
        config.bot.command_timeout_seconds = 5;
        config.bot.command_idle_timeout_seconds = 5;
        let mut report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","files":[{"kind":"video","path":"video.m4s"}]}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");

        let error =
            mux_bilibili_report_media(&config, &rooted, &root, &mut report, UNIX_EPOCH, None)
                .await
                .expect_err("untouched reserved output must reject the mux result");

        assert!(format!("{error:#}").contains("did not write the reserved output"));
        assert!(!root.join("Episode.mp4").exists());
        assert_eq!(fs::read_to_string(video).unwrap(), "video");
        assert!(report.entries[0].mux.is_none());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(BILIBILI_MUX_STAGING_DIR_PREFIX)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bilibili_mux_reads_the_bound_input_across_a_path_aba_replacement() {
        let root = temp_test_dir("bilibili-local-mux-bound-input");
        let video = root.join("video.m4s");
        let fake_ffmpeg = root.join("fake-ffmpeg.sh");
        fs::write(&video, "original-video").expect("raw video should write");
        fs::write(
            &fake_ffmpeg,
            r#"#!/bin/sh
input=
output=
next_is_input=no
for arg do
    output=$arg
    if test "$next_is_input" = yes; then
        input=$arg
        next_is_input=no
    elif test "$arg" = -i; then
        next_is_input=yes
    fi
done
test -n "$input" || exit 41
mv video.m4s video.original || exit 42
printf replacement > video.m4s || exit 43
cat "$input" > "$output" || exit 44
rm video.m4s || exit 45
mv video.original video.m4s || exit 46
"#,
        )
        .expect("fake ffmpeg should write");
        fs::set_permissions(&fake_ffmpeg, fs::Permissions::from_mode(0o700))
            .expect("fake ffmpeg should become executable");

        let mut config = test_config();
        config.tools.ffmpeg = fake_ffmpeg;
        config.bot.command_timeout_seconds = 5;
        config.bot.command_idle_timeout_seconds = 5;
        let mut report = parse_bilibili_download_report(
            r#"{"title":"Episode","output_dir":".","entries":[{"index":1,"title":"Episode","files":[{"kind":"video","path":"video.m4s"}]}]}"#,
        )
        .expect("download report should parse");
        let rooted = RootedFs::new(&root).expect("output root should bind");

        mux_bilibili_report_media(&config, &rooted, &root, &mut report, UNIX_EPOCH, None)
            .await
            .expect("descriptor-bound mux should succeed");

        assert_eq!(
            fs::read_to_string(root.join("Episode.mp4")).expect("mux output should exist"),
            "original-video"
        );
        assert_eq!(fs::read_to_string(&video).unwrap(), "original-video");
        let _ = fs::remove_dir_all(root);
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
        for input in &inputs {
            fs::write(&input.path, &input.kind).expect("mux input should write");
        }
        let output = entry_dir.join("Episode.mp4");
        let root = RootedFs::new(&entry_dir).expect("entry root should bind");
        let bound_inputs =
            bind_bilibili_mux_inputs(&root, &inputs).expect("mux inputs should bind");

        let BilibiliMuxCommand {
            spec,
            concat_file,
            inherited_files,
        } = bilibili_local_mux_command_spec(
            &config,
            &root,
            &inputs,
            &bound_inputs,
            &entry_dir,
            &output,
        )
        .expect("flv mux spec should build");

        let concat_file = concat_file.expect("flv mux should create concat list");
        assert_eq!(inherited_files.len(), 3);
        let concat_path = concat_file.path().to_path_buf();
        let concat = fs::read_to_string(&concat_path).expect("concat list should read");
        assert_eq!(concat, "file '/dev/fd/65'\nfile '/dev/fd/66'\n");
        assert_ne!(concat_path, user_concat);
        assert!(
            concat_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(BILIBILI_FFMPEG_CONCAT_FILE_PREFIX))
        );
        assert!(spec.args.windows(2).any(|args| args == ["-f", "concat"]));
        assert!(
            spec.args
                .windows(2)
                .any(|args| args[0] == "-i" && args[1] == "/dev/fd/64")
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
    fn writes_youtube_nfo_through_bound_output_root() {
        let root_dir = temp_test_dir("youtube-bound-nfo");
        let video = root_dir.join("Example.mkv");
        fs::write(&video, "video").expect("video should write");
        let root = RootedFs::new(&root_dir).expect("output root should bind");
        let metadata = YoutubeMetadata {
            id: Some("PHH1wTDF-1M".to_string()),
            title: Some("Example & title".to_string()),
            description: Some("Description".to_string()),
            uploader: Some("Uploader".to_string()),
            upload_date: Some("20260517".to_string()),
            webpage_url: Some("https://www.youtube.com/watch?v=PHH1wTDF-1M".to_string()),
            ..YoutubeMetadata::default()
        };

        let nfo_path =
            write_youtube_nfo_bound(&root, &video, "https://youtu.be/PHH1wTDF-1M", &metadata)
                .expect("bound NFO should write");
        let nfo = fs::read_to_string(&nfo_path).expect("NFO should read");

        assert_eq!(nfo_path, video.with_extension("nfo"));
        assert!(nfo.contains("<title>Example &amp; title</title>"));
        assert!(nfo.contains("<uniqueid type=\"youtube\" default=\"true\">PHH1wTDF-1M</uniqueid>"));
        assert!(nfo.contains("<premiered>2026-05-17</premiered>"));
        let _ = fs::remove_dir_all(root_dir);
    }

    #[cfg(unix)]
    #[test]
    fn bound_youtube_nfo_rejects_a_retargeted_output_root() {
        use std::os::unix::fs::symlink;

        let parent = temp_test_dir("youtube-bound-nfo-root-retarget");
        let original = parent.join("original");
        let replacement = parent.join("replacement");
        let configured = parent.join("configured");
        fs::create_dir_all(&original).expect("original root should create");
        fs::create_dir_all(&replacement).expect("replacement root should create");
        let original_video = original.join("Example.mkv");
        fs::write(&original_video, "video").expect("original video should write");
        fs::write(replacement.join("sentinel"), "replacement")
            .expect("replacement sentinel should write");
        symlink(&original, &configured).expect("configured root symlink should create");
        let root = RootedFs::new(&configured).expect("configured root should bind");
        let configured_video = configured.join("Example.mkv");

        fs::remove_file(&configured).expect("configured symlink should remove");
        symlink(&replacement, &configured).expect("configured symlink should retarget");
        let error = write_youtube_nfo_bound(
            &root,
            &configured_video,
            "https://youtu.be/PHH1wTDF-1M",
            &YoutubeMetadata::default(),
        )
        .expect_err("retargeted output root must reject NFO creation");

        assert!(format!("{error:#}").contains("different directory"));
        assert!(!original_video.with_extension("nfo").exists());
        assert!(!replacement.join("Example.nfo").exists());
        assert_eq!(
            fs::read_to_string(replacement.join("sentinel")).unwrap(),
            "replacement"
        );
        let _ = fs::remove_dir_all(parent);
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
        let (tx, mut rx) = job_progress_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(
            CommandStream::Stdout,
            b"Debug: --cookie SESSDATA=secret; bili_jct=csrf; ac_time_value=token",
        );

        let message = take_latest_progress(&mut rx).message;
        assert!(!message.contains("secret"));
        assert!(!message.contains("csrf"));
        assert!(!message.contains("token"));
        assert!(message.contains("--cookie <redacted Bilibili cookie>"));
    }

    #[test]
    fn progress_channel_coalesces_bursts_to_the_latest_message() {
        let (tx, mut rx) = job_progress_channel();

        for index in 0..1_000 {
            send_progress(Some(&tx), format!("event {index}"));
        }

        assert_eq!(take_latest_progress(&mut rx).message, "event 999");
        assert_no_progress(&rx);
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
        let (tx, mut rx) = job_progress_channel();
        let mut tracker =
            ProgressTracker::new("yt-dlp".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, b"[download] 1.0%");
        let first = take_latest_progress(&mut rx).message;
        assert!(first.contains("yt-dlp: downloading media"));
        assert!(first.contains("Done: resolve"));
        assert!(first.contains("Todo: metadata, embed, move"));
        assert!(first.contains("Last output: yt-dlp: 1%"));

        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        assert_no_progress(&rx);

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        let second = take_latest_progress(&mut rx).message;
        assert!(second.contains("Last output: yt-dlp: 2%"));
    }

    #[test]
    fn tracks_youtube_metadata_as_resolving() {
        let config = test_config();
        let spec = youtube_metadata_command_spec(&config, "https://youtu.be/abc");
        let command_name = command_progress_name(&spec);
        assert_eq!(command_name, "yt-dlp metadata");

        let (tx, mut rx) = job_progress_channel();
        let mut tracker = ProgressTracker::new(command_name, Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, b"[download] 1.0%");
        let first = take_latest_progress(&mut rx).message;
        assert!(first.contains("yt-dlp metadata: resolving metadata"));
        assert!(first.contains("Done: -"));
        assert!(first.contains("Todo: download, embed, move"));
        assert!(!first.contains("downloading media"));
    }

    #[test]
    fn throttles_file_activity_progress_updates() {
        let (tx, mut rx) = job_progress_channel();
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
        let first = take_latest_progress(&mut rx).message;
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
        assert_no_progress(&rx);
    }

    #[test]
    fn tracks_bbdown_stage_from_output() {
        let (tx, mut rx) = job_progress_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, "开始下载P1视频\n".as_bytes());
        let first = take_latest_progress(&mut rx).message;
        assert!(first.contains("BBDown: downloading video"));
        assert!(first.contains("Done: resolve"));
        assert!(first.contains("Todo: audio, mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "下载P1视频完毕\n".as_bytes());
        let video_done = take_latest_progress(&mut rx).message;
        assert!(video_done.contains("BBDown: downloading video"));
        assert!(video_done.contains("Done: resolve"));
        assert!(video_done.contains("Todo: audio, mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "开始下载P1音频\n".as_bytes());
        let second = take_latest_progress(&mut rx).message;
        assert!(second.contains("BBDown: downloading audio"));
        assert!(second.contains("Done: resolve, video"));
        assert!(second.contains("Todo: mux, move"));

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, "任务完成\n".as_bytes());
        let third = take_latest_progress(&mut rx).message;
        assert!(third.contains("BBDown: download complete"));
        assert!(third.contains("Done: resolve, video, audio"));
        assert!(third.contains("Todo: mux, move"));
    }

    #[tokio::test]
    async fn reports_only_contended_video_output_lock_waits() {
        let video_dir = temp_test_dir("video-output-lock-progress");
        let (tx, mut rx) = job_progress_channel();
        let guard = video_output_lock(&video_dir, "Bilibili download", Some(&tx))
            .await
            .expect("output lock should acquire");
        assert_no_progress(&rx);
        drop(guard);

        let held_guard = VIDEO_OUTPUT_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .await;
        let waiter_video_dir = video_dir.clone();
        let waiter = tokio::spawn(async move {
            let _guard = video_output_lock(&waiter_video_dir, "Bilibili download", Some(&tx))
                .await
                .expect("waiting output lock should acquire");
        });

        assert_eq!(
            {
                rx.changed().await.expect("waiting progress should be sent");
                rx.borrow_and_update()
                    .clone()
                    .expect("waiting progress should have a value")
            },
            JobProgress {
                message: "Bilibili download: waiting for video output slot".to_string()
            }
        );

        drop(held_guard);
        assert_eq!(
            {
                rx.changed()
                    .await
                    .expect("acquired progress should be sent");
                rx.borrow_and_update()
                    .clone()
                    .expect("acquired progress should have a value")
            },
            JobProgress {
                message: "Bilibili download: video output slot acquired".to_string()
            }
        );
        waiter.await.expect("waiter should finish");
        let _ = fs::remove_dir_all(video_dir);
    }

    #[tokio::test]
    async fn video_output_lock_recovers_an_interrupted_overwrite_after_process_handoff() {
        let video_dir = temp_test_dir("video-output-lock-overwrite-recovery");
        let existing = video_dir.join("Episode.mkv");
        fs::write(&existing, "original-video").expect("existing file should write");
        write_bilibili_identity_nfo(&existing, "cid123");
        let duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: VideoIdentity {
                provider: VideoProvider::Bilibili,
                id: "cid123".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        let duplicate =
            bind_test_overwrite_confirmation(&root, VideoDuplicateAction::Overwrite, &duplicate)
                .expect("overwrite confirmation should bind");
        let acquired =
            acquire_and_validate_overwrite_target(&root, &duplicate, StagedPrimaryMediaKind::Video)
                .expect("overwrite target should be acquired");
        drop(acquired);
        assert!(!existing.exists());
        video_recovery_state_file(&root)
            .expect("recovery state should open")
            .0
            .write_state(VIDEO_RECOVERY_STATE_DIRTY)
            .expect("crashed owner should leave a dirty recovery state");

        let blocker = video_output_lock_file(&root).expect("blocking lock should open");
        assert!(
            blocker
                .try_lock_exclusive()
                .expect("blocking lock should acquire")
        );
        let (tx, mut rx) = job_progress_channel();
        let waiter_dir = video_dir.clone();
        let waiter = tokio::spawn(async move {
            video_output_lock(&waiter_dir, "Staged video download", Some(&tx)).await
        });
        tokio_timeout(Duration::from_secs(2), rx.changed())
            .await
            .expect("cross-process wait should be reported promptly")
            .expect("progress sender should remain open");
        assert!(
            rx.borrow_and_update()
                .as_ref()
                .is_some_and(|progress| progress.message.contains("another downloader process"))
        );

        drop(blocker);
        let guard = tokio_timeout(Duration::from_secs(3), waiter)
            .await
            .expect("output lock handoff should finish")
            .expect("output lock task should join")
            .expect("output lock should recover the transaction");

        assert_eq!(fs::read_to_string(&existing).unwrap(), "original-video");
        assert!(existing.with_extension("nfo").is_file());
        assert!(overwrite_backup_dirs(&video_dir).is_empty());
        drop(guard);
        assert!(
            video_recovery_state_is_clean(
                &video_recovery_state_file(&root)
                    .expect("recovery state should reopen")
                    .0
            )
            .expect("recovery state should read")
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[tokio::test]
    async fn clean_video_output_lock_skips_recursive_recovery_scans() {
        let video_dir = temp_test_dir("video-output-lock-clean-recovery-state");
        let nested = video_dir.join("library");
        let unresolved = nested.join(format!("{OVERWRITE_BACKUP_DIR_PREFIX}-legacy"));
        fs::create_dir_all(&unresolved).expect("legacy recovery directory should create");
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        video_recovery_state_file(&root)
            .expect("recovery state should open")
            .0
            .write_state(VIDEO_RECOVERY_STATE_CLEAN)
            .expect("recovery state should become clean");
        let (tx, rx) = job_progress_channel();

        let guard = video_output_lock(&video_dir, "Bilibili download", Some(&tx))
            .await
            .expect("clean output lock should acquire");

        assert_no_progress(&rx);
        assert!(unresolved.is_dir());
        drop(guard);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn recovery_state_ignores_legacy_root_marker_contents() {
        let video_dir = temp_test_dir("legacy-root-recovery-marker");
        let legacy = video_dir.join(LEGACY_VIDEO_RECOVERY_STATE_FILE_NAME);
        fs::write(&legacy, "user-owned-legacy-data").expect("legacy file should write");
        let root = RootedFs::new(&video_dir).expect("output root should bind");

        let (mut marker, existed) =
            video_recovery_state_file(&root).expect("private recovery state should create");
        marker
            .write_state(VIDEO_RECOVERY_STATE_DIRTY)
            .expect("private recovery state should update");

        assert!(existed);
        assert_eq!(
            fs::read_to_string(legacy).unwrap(),
            "user-owned-legacy-data"
        );
        assert_eq!(
            fs::read(
                video_dir
                    .join(VIDEO_CONTROL_DIR_NAME)
                    .join(VIDEO_RECOVERY_STATE_FILE_NAME)
            )
            .unwrap(),
            [VIDEO_RECOVERY_STATE_DIRTY]
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn interrupted_control_initialization_does_not_block_startup() {
        let video_dir = temp_test_dir("interrupted-video-control-initialization");
        let stale = video_dir.join(format!("{VIDEO_CONTROL_INITIALIZING_DIR_PREFIX}-stale"));
        fs::create_dir(&stale).expect("stale initialization directory should create");
        fs::write(stale.join("partial"), "interrupted").expect("partial owner should write");
        let root = RootedFs::new(&video_dir).expect("output root should bind");

        let control = video_control_directory(&root)
            .expect("a new complete control directory should install atomically");

        assert_eq!(control.path, video_dir.join(VIDEO_CONTROL_DIR_NAME));
        assert!(control.path.join(VIDEO_CONTROL_OWNER_FILE_NAME).is_file());
        assert_eq!(
            fs::read_to_string(stale.join("partial")).unwrap(),
            "interrupted"
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[cfg(unix)]
    #[test]
    fn recovery_state_rejects_an_unowned_control_directory() {
        use std::os::unix::fs::PermissionsExt;

        let video_dir = temp_test_dir("unowned-video-control");
        let control = video_dir.join(VIDEO_CONTROL_DIR_NAME);
        fs::create_dir(&control).expect("unowned control directory should create");
        fs::set_permissions(&control, fs::Permissions::from_mode(0o700))
            .expect("control permissions should set");
        fs::write(control.join("user.txt"), "user-owned").expect("user file should write");
        let root = RootedFs::new(&video_dir).expect("output root should bind");

        let error = video_recovery_state_file(&root)
            .expect_err("control directory without ownership record must be rejected");

        assert!(format!("{error:#}").contains("not app-owned"));
        assert_eq!(
            fs::read_to_string(control.join("user.txt")).unwrap(),
            "user-owned"
        );
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn recovery_state_rejects_hard_links_without_modifying_them() {
        let video_dir = temp_test_dir("hard-linked-recovery-state");
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        drop(
            video_recovery_state_file(&root)
                .expect("private recovery state should create")
                .0,
        );
        let state = video_dir
            .join(VIDEO_CONTROL_DIR_NAME)
            .join(VIDEO_RECOVERY_STATE_FILE_NAME);
        let linked = video_dir.join("user-linked-state");
        fs::hard_link(&state, &linked).expect("state hard link should create");

        let error = video_recovery_state_file(&root)
            .expect_err("hard-linked recovery state must be rejected");

        assert!(format!("{error:#}").contains("link count"));
        assert_eq!(fs::read(&state).unwrap(), [VIDEO_RECOVERY_STATE_CLEAN]);
        assert_eq!(fs::read(&linked).unwrap(), [VIDEO_RECOVERY_STATE_CLEAN]);
        let _ = fs::remove_dir_all(video_dir);
    }

    #[test]
    fn video_output_file_lock_serializes_independent_open_handles() {
        let video_dir = temp_test_dir("video-output-file-lock");
        let root = RootedFs::new(&video_dir).expect("output root should bind");
        let first = video_output_lock_file(&root).expect("first lock handle should open");
        let second = video_output_lock_file(&root).expect("second lock handle should open");

        assert!(
            first
                .try_lock_exclusive()
                .expect("first lock should acquire")
        );
        assert!(
            !second
                .try_lock_exclusive()
                .expect("contended lock should report busy")
        );
        drop(first);
        assert!(
            second
                .try_lock_exclusive()
                .expect("second lock should acquire after release")
        );

        drop(second);
        let _ = fs::remove_dir_all(video_dir);
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
        let (tx, mut rx) = job_progress_channel();
        let progress_handle = tokio::spawn(async move {
            let mut messages = Vec::new();
            while rx.changed().await.is_ok() {
                if let Some(progress) = rx.borrow_and_update().clone() {
                    messages.push(progress.message);
                }
            }
            messages
        });

        let result = tokio_timeout(
            Duration::from_secs(8),
            run_command(&config, &spec, Some(tx)),
        )
        .await
        .expect("silent file activity command should not hang")
        .expect("silent file activity command should succeed");

        assert!(result.stdout.is_empty());
        assert!(result.stderr.is_empty());
        let messages = progress_handle
            .await
            .expect("progress collector should finish");
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
    async fn bilibili_worker_policy_disables_only_the_total_deadline() {
        let root = temp_test_dir("bilibili-worker-total-deadline");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 1;
        config.bot.command_idle_timeout_seconds = 5;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_string(), "sleep 2; printf finished".to_string()],
            cwd: root.clone(),
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };

        let output = tokio_timeout(
            Duration::from_secs(5),
            run_command_with_execution_context(
                &config,
                &spec,
                None,
                &[],
                None,
                CommandExecutionPolicy::BILIBILI_WORKER,
            ),
        )
        .await
        .expect("deadline-free worker command should finish")
        .expect("deadline-free worker command should succeed");

        assert!(output.status.success());
        assert_eq!(output.stdout, b"finished");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bilibili_mux_policy_inherits_the_worker_process_group() {
        let root = temp_test_dir("bilibili-mux-process-group");
        let process_group_file = root.join("process-group");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 5;
        config.bot.command_idle_timeout_seconds = 5;
        let spec = CommandSpec {
            program: std::env::current_exe().expect("test binary should resolve"),
            args: vec![
                "--ignored".to_string(),
                "--exact".to_string(),
                "downloader::tests::bilibili_mux_process_group_probe_child".to_string(),
                "--nocapture".to_string(),
            ],
            cwd: root.clone(),
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };

        let output = run_command_with_execution_context(
            &config,
            &spec,
            None,
            &[],
            None,
            CommandExecutionPolicy::BILIBILI_MUX,
        )
        .await
        .expect("mux process-group probe should run");
        assert!(output.status.success());
        let child_process_group = fs::read_to_string(&process_group_file)
            .expect("process group probe should write")
            .trim()
            .parse::<libc::pid_t>()
            .expect("process group output should parse");

        assert_eq!(child_process_group, unsafe { libc::getpgrp() });
        assert_eq!(
            CommandExecutionPolicy::BILIBILI_MUX.total_deadline,
            CommandTotalDeadline::Configured
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "spawned by bilibili_mux_policy_inherits_the_worker_process_group"]
    fn bilibili_mux_process_group_probe_child() {
        fs::write("process-group", unsafe { libc::getpgrp() }.to_string())
            .expect("process group probe should write");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_liveness_monitor_waits_for_its_peer_to_close() {
        let (monitor, keepalive) = command_liveness_pair().expect("liveness pair should create");
        let mut wait = Box::pin(wait_for_liveness_peer_close(monitor));

        assert!(
            tokio_timeout(Duration::from_millis(50), &mut wait)
                .await
                .is_err(),
            "an open peer must keep the monitor pending"
        );
        drop(keepalive);
        tokio_timeout(Duration::from_secs(1), wait)
            .await
            .expect("peer closure should wake the monitor")
            .expect("peer closure should be observed without an I/O error");
    }

    #[cfg(unix)]
    #[test]
    fn inherited_worker_descriptors_fit_below_a_low_process_limit() {
        use std::os::unix::process::CommandExt;

        let mut command = std::process::Command::new(
            std::env::current_exe().expect("test binary should resolve"),
        );
        command
            .arg("--ignored")
            .arg("--exact")
            .arg("downloader::tests::inherited_worker_descriptors_low_limit_child")
            .arg("--nocapture")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
                if libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                let mut limits = limits.assume_init();
                limits.rlim_cur = limits.rlim_cur.min(256 as libc::rlim_t);
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limits) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let output = command.output().expect("low-limit helper should run");

        assert!(
            output.status.success(),
            "low-limit helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "spawned by inherited_worker_descriptors_fit_below_a_low_process_limit"]
    async fn inherited_worker_descriptors_low_limit_child() {
        let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) };
        assert_eq!(result, 0, "process file descriptor limit should read");
        let limits = unsafe { limits.assume_init() };
        assert!(limits.rlim_cur <= 256 as libc::rlim_t);

        let root = temp_test_dir("inherited-worker-low-fd-limit");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 5;
        config.bot.command_idle_timeout_seconds = 5;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_string(), "exit 0".to_string()],
            cwd: root.clone(),
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };
        let (worker_liveness, _parent_liveness) =
            command_liveness_pair().expect("worker liveness pair should create");
        let inherited =
            prepare_additional_inherited_command_fd(&worker_liveness, BILIBILI_WORKER_LIVENESS_FD)
                .expect("worker liveness descriptor should fit below the limit");

        let output = run_command_with_execution_context_and_additional_fds(
            &config,
            &spec,
            None,
            &[],
            vec![inherited],
            None,
            CommandExecutionPolicy::BILIBILI_WORKER,
        )
        .await
        .expect("worker command should start below the low descriptor limit");

        assert!(output.status.success());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn closing_the_parent_liveness_channel_terminates_the_worker_group() {
        use std::os::unix::process::ExitStatusExt;

        let root = temp_test_dir("bilibili-worker-parent-liveness");
        let ready = root.join("worker.ready");
        let (worker_liveness, parent_liveness) =
            command_liveness_pair().expect("worker liveness pair should create");
        let inherited =
            prepare_additional_inherited_command_fd(&worker_liveness, BILIBILI_WORKER_LIVENESS_FD)
                .expect("worker liveness descriptor should duplicate");
        drop(worker_liveness);
        let mut config = test_config();
        config.bot.command_idle_timeout_seconds = 10;
        let spec = CommandSpec {
            program: std::env::current_exe().expect("test binary should resolve"),
            args: vec![
                "--ignored".to_string(),
                "--exact".to_string(),
                "downloader::tests::bilibili_parent_liveness_child".to_string(),
                "--nocapture".to_string(),
            ],
            cwd: root.clone(),
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };
        let child_root = root.clone();
        let command = tokio::spawn(async move {
            run_command_with_execution_context_and_additional_fds(
                &config,
                &spec,
                None,
                &[],
                vec![inherited],
                None,
                CommandExecutionPolicy::BILIBILI_WORKER,
            )
            .await
        });

        for _ in 0..100 {
            if ready.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready.is_file(), "worker liveness helper did not start");
        drop(parent_liveness);
        let output = tokio_timeout(Duration::from_secs(5), command)
            .await
            .expect("worker should terminate after its parent channel closes")
            .expect("worker command task should join")
            .expect("worker command runner should return its exit status");

        assert_eq!(output.status.signal(), Some(libc::SIGKILL));
        let _ = fs::remove_dir_all(child_root);
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "spawned by closing_the_parent_liveness_channel_terminates_the_worker_group"]
    async fn bilibili_parent_liveness_child() {
        let liveness =
            inherited_worker_liveness_stream().expect("worker liveness descriptor should open");
        fs::write("worker.ready", b"ready").expect("worker readiness marker should write");
        let result = wait_for_liveness_peer_close(liveness).await;
        let reason = match result {
            Ok(()) => "test worker parent exited",
            Err(_) => "test worker parent liveness check failed",
        };
        terminate_current_process_group(reason);
    }

    #[cfg(unix)]
    #[test]
    fn bilibili_mux_descendant_fence_terminates_the_worker_group() {
        use std::os::unix::process::{CommandExt, ExitStatusExt};

        let root = temp_test_dir("bilibili-mux-descendant-fence");
        let pid_file = root.join("descendant.pid");
        let mut child = std::process::Command::new(
            std::env::current_exe().expect("test binary should resolve"),
        );
        child
            .arg("--ignored")
            .arg("--exact")
            .arg("downloader::tests::bilibili_mux_descendant_fence_child")
            .arg("--nocapture")
            .env("TVD_MUX_FENCE_TEST_ROOT", &root)
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = child.spawn().expect("isolated mux helper should start");
        let child_process_group = child.id() as libc::pid_t;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.try_wait().expect("helper status should read") {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                unsafe {
                    libc::kill(-child_process_group, libc::SIGKILL);
                }
                let _ = child.wait();
                let _ = fs::remove_dir_all(&root);
                panic!("mux helper did not terminate its inherited process group");
            }
            std::thread::sleep(Duration::from_millis(50));
        };

        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let descendant = fs::read_to_string(&pid_file)
            .expect("descendant pid should be written")
            .trim()
            .parse::<libc::pid_t>()
            .expect("descendant pid should parse");
        for _ in 0..40 {
            if !process_exists(descendant) {
                let _ = fs::remove_dir_all(&root);
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        unsafe {
            libc::kill(descendant, libc::SIGKILL);
        }
        let _ = fs::remove_dir_all(&root);
        panic!("mux descendant {descendant} survived worker-group termination");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "spawned by bilibili_mux_descendant_fence_terminates_the_worker_group"]
    async fn bilibili_mux_descendant_fence_child() {
        BILIBILI_WORKER_PROCESS.store(true, Ordering::Release);
        let root = PathBuf::from(
            std::env::var_os("TVD_MUX_FENCE_TEST_ROOT")
                .expect("isolated mux helper root should be provided"),
        );
        let pid_file = root.join("descendant.pid");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 30;
        config.bot.command_idle_timeout_seconds = 30;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "sleep 30 & echo $! > \"$0\"; exit 0".to_string(),
                pid_file.display().to_string(),
            ],
            cwd: root,
            activity_dir: None,
            cleanup_paths: Vec::new(),
        };

        let result = run_command_with_execution_context(
            &config,
            &spec,
            None,
            &[],
            None,
            CommandExecutionPolicy::BILIBILI_MUX,
        )
        .await;
        panic!("mux descendant fence returned instead of terminating its group: {result:?}");
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
    async fn cancelling_a_command_future_terminates_its_process_group() {
        let root = temp_test_dir("process-group-cancellation");
        let pid_file = root.join("child.pid");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 30;
        config.bot.command_idle_timeout_seconds = 30;
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
        let task = tokio::spawn(async move { run_command(&config, &spec, None).await });
        for _ in 0..50 {
            if pid_file.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let pid = fs::read_to_string(&pid_file)
            .expect("child pid should be written before cancellation")
            .trim()
            .parse::<libc::pid_t>()
            .expect("child pid should parse");

        task.abort();
        let _ = task.await;
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
        panic!("descendant process {pid} survived command cancellation");
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
