mod bilibili_auth;
mod bilibili_core;
mod config;
mod downloader;
mod redaction;
mod router;
mod safe_fs;
mod telegram;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bbdown_core::{
    AccessKeyLoginTicket, CredentialHealthReport, CredentialHealthScope, CredentialHealthStatus,
    CredentialKind, CredentialSource, QrLoginKind, QrLoginState,
};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, timeout as tokio_timeout};
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::downloader::{
    JobProgress, JobProgressReceiver, VideoDuplicate, VideoDuplicateAction,
    find_video_duplicate_with_probe, job_progress_channel, recover_pending_overwrite_transactions,
    run_bilibili_worker, run_job, run_job_with_duplicate_action, run_video_job_staged_keep_both,
};
use crate::redaction::redact_sensitive_text;
use crate::router::{
    BilibiliAuthCommand, BilibiliAuthLoginMode, BilibiliSelection, JobRequest, RouteResult,
    route_message,
};
use crate::telegram::{
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, TelegramClient,
};

static BILIBILI_LOGIN_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_LOGIN_CANCEL_NOTIFY: OnceLock<Notify> = OnceLock::new();
static BILIBILI_AUTH_STATE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_AUTH_REPLY_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_AUTH_GENERATION: AtomicU64 = AtomicU64::new(0);
static BILIBILI_CREDENTIAL_REVISION: AtomicU64 = AtomicU64::new(0);
static PENDING_DUPLICATE_JOBS: OnceLock<Mutex<HashMap<u64, PendingDuplicateJob>>> = OnceLock::new();
static PENDING_BILIBILI_SELECTION_JOBS: OnceLock<Mutex<HashMap<u64, PendingBilibiliSelectionJob>>> =
    OnceLock::new();
static PENDING_BILIBILI_ACCESS_KEY_LOGINS: OnceLock<
    Mutex<HashMap<i64, PendingBilibiliAccessKeyLogin>>,
> = OnceLock::new();
static DUPLICATE_CALLBACK_COUNTER: AtomicU64 = AtomicU64::new(1);
static BILIBILI_SELECTION_CALLBACK_COUNTER: AtomicU64 = AtomicU64::new(1);
static BILIBILI_ACCESS_KEY_TICKET_COUNTER: AtomicU64 = AtomicU64::new(1);
const DUPLICATE_DECISION_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_PENDING_DUPLICATE_JOBS: usize = 64;
const PENDING_DUPLICATE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const BILIBILI_SELECTION_DECISION_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_PENDING_BILIBILI_SELECTION_JOBS: usize = 256;
const BILIBILI_ACCESS_KEY_LOGIN_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone)]
struct PendingDuplicateJob {
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    duplicate: VideoDuplicate,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct PendingBilibiliSelectionJob {
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct PendingBilibiliAccessKeyLogin {
    auth_generation: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    ticket_id: u64,
    ticket: AccessKeyLoginTicket,
    created_at: Instant,
    in_progress: bool,
}

#[derive(Debug, Clone)]
enum PendingBilibiliAccessKeyLoginClaim {
    Claimed(PendingBilibiliAccessKeyLogin),
    InProgress,
    Missing,
}

#[derive(Debug, Clone)]
struct DuplicateRun {
    action: VideoDuplicateAction,
    duplicate: VideoDuplicate,
}

#[derive(Debug, Clone)]
enum JobRunMode {
    Direct,
    StagedKeepBoth,
    Duplicate(DuplicateRun),
}

#[derive(Clone)]
struct JobDispatch {
    download_semaphore: Arc<Semaphore>,
    duplicate_scan_semaphore: Arc<Semaphore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_video_downloader=info,info".into()),
        )
        .init();

    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|arg| arg == std::ffi::OsStr::new("--bilibili-worker"))
    {
        return run_bilibili_worker().await;
    }
    if args
        .first()
        .is_some_and(|arg| arg == std::ffi::OsStr::new("--replay-message"))
    {
        let config_path = args
            .get(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("config.toml"));
        let message = args
            .get(2..)
            .unwrap_or(&[])
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        return replay_message(config_path, message).await;
    }

    let config_path = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let config = Arc::new(AppConfig::load(&config_path)?);
    config.ensure_runtime_dirs()?;
    for recovery in bilibili_auth::recover_interrupted_auth_cleanup(
        &config.bilibili.auth.state_path,
        &config.bilibili.auth.credential_file,
    )? {
        warn!(message = %recovery, "recovered interrupted BBDown auth cleanup");
    }
    for recovery in recover_pending_overwrite_transactions(&config.downloads.video_dir)? {
        warn!(message = %recovery, "recovered interrupted overwrite transaction");
    }
    tokio::spawn(expire_pending_duplicate_jobs());

    let telegram = TelegramClient::new(config.telegram.token.clone());
    if let Err(err) = telegram.set_my_commands(default_bot_commands()).await {
        warn!(error = %err, "failed to register Telegram bot commands");
    }
    let job_dispatch = JobDispatch {
        download_semaphore: Arc::new(Semaphore::new(config.bot.concurrency)),
        duplicate_scan_semaphore: Arc::new(Semaphore::new(config.bot.concurrency)),
    };
    let next_job_id = Arc::new(AtomicU64::new(1));
    let mut offset = None;

    info!(
        concurrency = config.bot.concurrency,
        "telegram local downloader started"
    );

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            signal = &mut shutdown => {
                signal?;
                info!("shutdown requested");
                break;
            }
            updates = telegram.get_updates(offset, config.bot.poll_timeout_seconds) => {
                match updates {
                    Ok(updates) => {
                        for update in updates {
                            offset = Some(update.update_id + 1);
                            if let Some(message) = update.message {
                                handle_message(
                                    telegram.clone(),
                                    Arc::clone(&config),
                                    job_dispatch.clone(),
                                    Arc::clone(&next_job_id),
                                    message.chat.id,
                                    message.chat.is_private(),
                                    message.text.as_deref(),
                                )
                                .await;
                            }
                            if let Some(callback_query) = update.callback_query {
                                handle_callback_query(
                                    telegram.clone(),
                                    Arc::clone(&config),
                                    job_dispatch.clone(),
                                    callback_query,
                                )
                                .await;
                            }
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to fetch telegram updates");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to listen for SIGTERM")?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl-c")?;
            }
            signal = terminate.recv() => {
                if signal.is_none() {
                    bail!("SIGTERM listener closed unexpectedly");
                }
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for ctrl-c")
    }
}

async fn replay_message(config_path: PathBuf, text: String) -> Result<()> {
    if text.trim().is_empty() {
        bail!("usage: telegram-video-downloader --replay-message config.toml <message>");
    }

    let config = AppConfig::load(&config_path)?;
    config.ensure_runtime_dirs()?;
    for recovery in bilibili_auth::recover_interrupted_auth_cleanup(
        &config.bilibili.auth.state_path,
        &config.bilibili.auth.credential_file,
    )? {
        warn!(message = %recovery, "recovered interrupted BBDown auth cleanup");
    }
    for recovery in recover_pending_overwrite_transactions(&config.downloads.video_dir)? {
        warn!(message = %recovery, "recovered interrupted overwrite transaction");
    }

    match route_message(&text, &config.pdf.auto_domains) {
        RouteResult::Jobs(jobs) => {
            let mut failed_jobs = Vec::new();
            for (index, job) in jobs.iter().enumerate() {
                let job_id = index + 1;
                println!("Queued replay job #{job_id}: {}", job.label());
                if job.requires_bilibili_selection() {
                    println!(
                        "Failed replay job #{job_id}: {}\nBilibili ss/md links require a Telegram selection prompt.",
                        job.label()
                    );
                    failed_jobs.push(format!("#{job_id} {}", job.label()));
                    continue;
                }
                println!("Started replay job #{job_id}: {}", job.label());
                let (progress_tx, mut progress_rx) = job_progress_channel();
                let progress_handle = tokio::spawn(async move {
                    while progress_rx.changed().await.is_ok() {
                        if let Some(progress) = progress_rx.borrow_and_update().clone() {
                            println!("Progress replay job #{job_id}: {}", progress.message);
                        }
                    }
                });
                let result = match job {
                    JobRequest::Bilibili { .. } | JobRequest::Youtube { .. } => {
                        run_video_job_staged_keep_both(&config, job, Some(progress_tx)).await
                    }
                    JobRequest::Pdf { .. } => run_job(&config, job, Some(progress_tx)).await,
                };
                let _ = progress_handle.await;
                match result {
                    Ok(report) => {
                        println!(
                            "Finished replay job #{job_id}: {}\nSaved: {}",
                            job.label(),
                            report.saved_location
                        );
                        if !report.details.is_empty() {
                            println!("{}", redact_sensitive_text(&report.details));
                        }
                    }
                    Err(err) => {
                        println!(
                            "Failed replay job #{job_id}: {}\n{}",
                            job.label(),
                            redact_sensitive_text(&format!("{err:#}"))
                        );
                        failed_jobs.push(format!("#{job_id} {}", job.label()));
                    }
                }
            }
            if failed_jobs.is_empty() {
                Ok(())
            } else {
                bail!(
                    "{} replay job(s) failed: {}",
                    failed_jobs.len(),
                    failed_jobs.join(", ")
                )
            }
        }
        RouteResult::PdfUsage => bail!("usage: /pdf https://example.com"),
        RouteResult::Help => {
            println!("{}", help_message());
            Ok(())
        }
        RouteResult::BilibiliAuth(_) | RouteResult::BilibiliAuthUsage => {
            bail!("bbdown auth commands require Telegram bot chat")
        }
        RouteResult::UnsupportedLinks => bail!("no supported links found"),
        RouteResult::Empty => bail!("message did not contain text to route"),
    }
}

async fn handle_message(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    next_job_id: Arc<AtomicU64>,
    chat_id: i64,
    is_private_chat: bool,
    text: Option<&str>,
) {
    let Some(text) = text else {
        return;
    };

    if !config.telegram.is_chat_allowed(chat_id) {
        warn!(chat_id, "ignoring message from unauthorized chat");
        return;
    }

    if is_private_chat
        && maybe_complete_pending_bilibili_access_key_login(
            telegram.clone(),
            Arc::clone(&config),
            chat_id,
            text,
        )
        .await
    {
        return;
    }

    match route_message(text, &config.pdf.auto_domains) {
        RouteResult::Jobs(jobs) => {
            for job in jobs {
                let job_id = next_job_id.fetch_add(1, Ordering::Relaxed);
                queue_or_prompt_job(
                    telegram.clone(),
                    Arc::clone(&config),
                    job_dispatch.clone(),
                    chat_id,
                    job_id,
                    job,
                );
            }
        }
        RouteResult::BilibiliAuth(command) => {
            handle_bilibili_auth_command(telegram, config, chat_id, is_private_chat, command).await;
        }
        RouteResult::Help => {
            send_or_log(&telegram, chat_id, help_message()).await;
        }
        RouteResult::BilibiliAuthUsage => {
            let message = if is_private_chat {
                bbdown_auth_usage()
            } else {
                "Please manage BBDown login state in a private chat with this bot.".to_string()
            };
            send_or_log(&telegram, chat_id, message).await;
        }
        RouteResult::PdfUsage => {
            send_or_log(
                &telegram,
                chat_id,
                "Usage: /pdf https://example.com".to_string(),
            )
            .await;
        }
        RouteResult::UnsupportedLinks => {
            send_or_log(
                &telegram,
                chat_id,
                "No supported links found. Send Bilibili/YouTube links directly, use /pdf URL, or configure a PDF auto-domain."
                    .to_string(),
            )
            .await;
        }
        RouteResult::Empty => {}
    }
}

fn default_bot_commands() -> Vec<BotCommand> {
    vec![
        BotCommand {
            command: "help".to_string(),
            description: "Show supported commands and link handling.".to_string(),
        },
        BotCommand {
            command: "pdf".to_string(),
            description: "Save a webpage as PDF.".to_string(),
        },
        BotCommand {
            command: "bbdown".to_string(),
            description: "Manage BBDown Bilibili login state.".to_string(),
        },
    ]
}

fn help_message() -> String {
    [
        "Telegram Local Downloader Bot",
        "",
        "Send Bilibili or YouTube links directly to download videos.",
        "Bilibili opus links and configured PDF domains are saved as PDF automatically.",
        "",
        "Commands:",
        "/help - Show this help.",
        "/pdf URL - Save a webpage as PDF.",
        "/bbdown login [web|tv|access-key] - Log in to Bilibili for BBDown downloads.",
        "/bbdown status - Check saved BBDown credentials.",
        "/bbdown logout - Clear the local BBDown credential state.",
    ]
    .join("\n")
}

async fn handle_bilibili_auth_command(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    is_private_chat: bool,
    command: BilibiliAuthCommand,
) {
    if !is_private_chat {
        send_or_log(
            &telegram,
            chat_id,
            "Please manage BBDown login state in a private chat with this bot.".to_string(),
        )
        .await;
        return;
    }

    match command {
        BilibiliAuthCommand::Login(mode) => {
            if has_pending_bilibili_access_key_login().await {
                send_or_log(
                    &telegram,
                    chat_id,
                    "BBDown login is already waiting for an access-key callback. Send the callback message, or use /bbdown logout to cancel."
                        .to_string(),
                )
                .await;
                return;
            }
            let lock = BILIBILI_LOGIN_LOCK.get_or_init(|| Mutex::new(()));
            let guard = match lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    send_or_log(
                        &telegram,
                        chat_id,
                        "BBDown login is already in progress. Finish or wait for the current QR login to expire.".to_string(),
                    )
                    .await;
                    return;
                }
            };
            let auth_generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
            tokio::spawn(async move {
                let _guard = guard;
                run_bbdown_login(telegram, config, chat_id, auth_generation, mode).await;
            });
        }
        BilibiliAuthCommand::Status => {
            tokio::spawn(async move {
                run_bbdown_status(telegram, config, chat_id).await;
            });
        }
        BilibiliAuthCommand::Logout => {
            run_bbdown_logout(telegram, config, chat_id).await;
        }
    }
}

async fn run_bbdown_login(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    auth_generation: u64,
    mode: BilibiliAuthLoginMode,
) {
    let auth_epoch = match current_bbdown_auth_epoch(&config).await {
        Ok(epoch) => epoch,
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Failed to lock BBDown credential state for login:\n{}",
                    summarize_bbdown_auth_error(&err)
                ),
            )
            .await;
            return;
        }
    };
    let preparing = match mode {
        BilibiliAuthLoginMode::Web => "Preparing BBDown Web QR login...",
        BilibiliAuthLoginMode::Tv => "Preparing BBDown TV QR login...",
        BilibiliAuthLoginMode::AccessKey => "Preparing BBDown access-key authorization...",
    };
    send_or_log(&telegram, chat_id, preparing.to_string()).await;

    let result = match mode {
        BilibiliAuthLoginMode::Web | BilibiliAuthLoginMode::Tv => run_bbdown_qr_login(
            &telegram,
            &config,
            chat_id,
            auth_generation,
            auth_epoch,
            mode,
        )
        .await
        .map(BbdownLoginOutcome::Saved),
        BilibiliAuthLoginMode::AccessKey => {
            start_bbdown_access_key_login(&telegram, &config, chat_id, auth_generation, auth_epoch)
                .await
                .map(|()| BbdownLoginOutcome::PendingAccessKey)
        }
    };

    match result {
        Ok(BbdownLoginOutcome::Saved(saved)) => {
            send_current_bbdown_login_success(
                &telegram,
                &config,
                chat_id,
                &saved,
                format!(
                    "BBDown {} login saved.\n{}",
                    bbdown_login_mode_label(mode),
                    format_bbdown_credential_summary(&saved.summary)
                ),
            )
            .await;
        }
        Ok(BbdownLoginOutcome::PendingAccessKey) => {}
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "BBDown {} login failed:\n{}",
                    bbdown_login_mode_label(mode),
                    summarize_bbdown_auth_error(&err)
                ),
            )
            .await;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BbdownLoginOutcome {
    Saved(SavedBilibiliLogin),
    PendingAccessKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SavedBilibiliLogin {
    summary: CredentialSource,
    auth_generation: u64,
    credential_revision: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
}

async fn run_bbdown_qr_login(
    telegram: &TelegramClient,
    config: &AppConfig,
    chat_id: i64,
    auth_generation: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    mode: BilibiliAuthLoginMode,
) -> Result<SavedBilibiliLogin> {
    let client = bilibili_core::anonymous_client(config)?;
    let ticket = match mode {
        BilibiliAuthLoginMode::Web => {
            await_bbdown_login_active(auth_generation, client.create_web_qr_login()).await??
        }
        BilibiliAuthLoginMode::Tv => {
            await_bbdown_login_active(auth_generation, client.create_tv_qr_login()).await??
        }
        BilibiliAuthLoginMode::AccessKey => bail!("access-key login is not a QR polling command"),
    };
    let output = ticket.output();
    await_bbdown_login_active(
        auth_generation,
        send_bbdown_auth_ticket(
            telegram,
            chat_id,
            mode,
            &output.url,
            &output.qr_payload,
            config.bilibili.auth.login_timeout_seconds,
        ),
    )
    .await??;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(
            config.bilibili.auth.login_timeout_seconds,
        ))
        .ok_or_else(|| anyhow::anyhow!("BBDown login timeout is too large"))?;
    let interval = Duration::from_secs(config.bilibili.auth.poll_interval_seconds);
    let cancel = bbdown_login_cancel_notify().notified();
    tokio::pin!(cancel);
    cancel.as_mut().enable();
    let mut last_waiting_state: Option<&'static str> = None;

    loop {
        ensure_bbdown_login_active(auth_generation)?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                anyhow::anyhow!("BBDown {} login timed out", bbdown_login_mode_label(mode))
            })?;
        tokio::select! {
            () = &mut cancel => {
                bail!("BBDown login was canceled by a later /bbdown logout");
            }
            state = poll_bbdown_qr_login(&client, &ticket, remaining) => {
                match state? {
                    QrLoginState::WaitingForScan => {
                        last_waiting_state = Some("waiting_for_scan");
                    }
                    QrLoginState::WaitingForConfirm => {
                        if last_waiting_state != Some("waiting_for_confirm") {
                            await_bbdown_login_active(
                                auth_generation,
                                send_or_log(
                                    telegram,
                                    chat_id,
                                    "BBDown QR scanned; confirm the login in the Bilibili app."
                                        .to_string(),
                                ),
                            )
                            .await?;
                        }
                        last_waiting_state = Some("waiting_for_confirm");
                    }
                    QrLoginState::Expired => bail!("BBDown QR code expired"),
                    QrLoginState::Succeeded { credentials } => {
                        return save_bbdown_login_credentials(
                            config,
                            auth_generation,
                            auth_epoch,
                            credentials,
                        )
                        .await;
                    }
                }
            }
        }
        let now = Instant::now();
        let sleep_duration = deadline
            .checked_duration_since(now)
            .map_or(Duration::ZERO, |remaining| remaining.min(interval));
        if !sleep_duration.is_zero() {
            await_bbdown_login_active(auth_generation, sleep(sleep_duration)).await?;
        }
    }
}

async fn start_bbdown_access_key_login(
    telegram: &TelegramClient,
    config: &AppConfig,
    chat_id: i64,
    auth_generation: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
) -> Result<()> {
    ensure_bbdown_login_active(auth_generation)?;
    let ticket = bilibili_core::create_access_key_ticket()?;
    let ticket_id = BILIBILI_ACCESS_KEY_TICKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let output = ticket.output();
    {
        let mut logins =
            await_bbdown_login_active(auth_generation, pending_bilibili_access_key_logins().lock())
                .await?;
        prune_expired_pending_bilibili_access_key_logins(&mut logins, Instant::now());
        ensure_bbdown_login_active(auth_generation)?;
        logins.insert(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation,
                auth_epoch,
                ticket_id,
                ticket,
                created_at: Instant::now(),
                in_progress: false,
            },
        );
    }
    let delivery = await_bbdown_login_active(
        auth_generation,
        send_bbdown_auth_ticket(
            telegram,
            chat_id,
            BilibiliAuthLoginMode::AccessKey,
            &output.url,
            &output.qr_payload,
            config.bilibili.auth.login_timeout_seconds,
        ),
    )
    .await
    .and_then(|result| result);
    if let Err(err) = delivery {
        clear_pending_bilibili_access_key_login(chat_id, auth_generation, ticket_id).await;
        return Err(err);
    }
    await_bbdown_login_active(
        auth_generation,
        send_or_log(
            telegram,
            chat_id,
            "After authorizing, send the callback URL or balh-login-credentials message to this private chat. Use /bbdown logout to cancel."
                .to_string(),
        ),
    )
    .await?;
    Ok(())
}

async fn maybe_complete_pending_bilibili_access_key_login(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    text: &str,
) -> bool {
    if text.trim_start().starts_with('/') {
        return false;
    }
    if !bilibili_core::looks_like_access_key_login_input(text) {
        return false;
    }
    let claim = {
        let mut logins = pending_bilibili_access_key_logins().lock().await;
        claim_pending_bilibili_access_key_login(&mut logins, chat_id, Instant::now())
    };
    let pending = match claim {
        PendingBilibiliAccessKeyLoginClaim::Claimed(pending) => pending,
        PendingBilibiliAccessKeyLoginClaim::InProgress => return true,
        PendingBilibiliAccessKeyLoginClaim::Missing => return false,
    };
    let input = text.to_string();
    tokio::spawn(async move {
        complete_bbdown_access_key_login(telegram, config, chat_id, pending, input).await;
    });
    true
}

async fn complete_bbdown_access_key_login(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    pending: PendingBilibiliAccessKeyLogin,
    input: String,
) {
    send_or_log(
        &telegram,
        chat_id,
        "Completing BBDown access-key login...".to_string(),
    )
    .await;
    match complete_bbdown_access_key_login_inner(&config, &pending, &input).await {
        Ok(saved) => {
            clear_pending_bilibili_access_key_login(
                chat_id,
                pending.auth_generation,
                pending.ticket_id,
            )
            .await;
            send_current_bbdown_login_success(
                &telegram,
                &config,
                chat_id,
                &saved,
                format!(
                    "BBDown access-key login saved.\n{}",
                    format_bbdown_credential_summary(&saved.summary)
                ),
            )
            .await;
        }
        Err(err) => {
            let retryable = release_pending_bilibili_access_key_login(
                &config,
                chat_id,
                pending.auth_generation,
                pending.ticket_id,
            )
            .await;
            let retry_hint = if retryable {
                "; send a corrected callback to retry"
            } else {
                "; start a new /bbdown login access-key flow to retry"
            };
            let message = format!(
                "BBDown access-key login failed{retry_hint}:\n{}",
                summarize_bbdown_auth_error(&err)
            );
            send_or_log(&telegram, chat_id, message).await;
        }
    }
}

async fn complete_bbdown_access_key_login_inner(
    config: &AppConfig,
    pending: &PendingBilibiliAccessKeyLogin,
    input: &str,
) -> Result<SavedBilibiliLogin> {
    ensure_bbdown_login_active(pending.auth_generation)?;
    let credentials = bilibili_core::access_key_login_credentials(&pending.ticket, input)?;
    save_bbdown_login_credentials(
        config,
        pending.auth_generation,
        pending.auth_epoch,
        credentials,
    )
    .await
}

async fn save_bbdown_login_credentials(
    config: &AppConfig,
    auth_generation: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    credentials: bbdown_core::Credentials,
) -> Result<SavedBilibiliLogin> {
    let credential_profile = config.bilibili.auth.credential_profile.clone();
    save_bbdown_login_credentials_with_sync(
        config,
        auth_generation,
        auth_epoch,
        credentials,
        move |transaction| {
            transaction
                .sync_legacy_cookie(credential_profile.as_deref())
                .context(
                    "failed to migrate legacy BBDown auth state before saving fresh credentials",
                )?;
            Ok(())
        },
    )
    .await
}

async fn save_bbdown_login_credentials_with_sync<F>(
    config: &AppConfig,
    auth_generation: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    credentials: bbdown_core::Credentials,
    sync_legacy: F,
) -> Result<SavedBilibiliLogin>
where
    F: for<'a> FnOnce(&bilibili_auth::LockedAuthMutation<'a>) -> Result<()> + Send + 'static,
{
    let _state_guard = bbdown_auth_state_lock().lock().await;
    ensure_bbdown_login_active(auth_generation)?;
    let credential_profile = config.bilibili.auth.credential_profile.clone();
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    let (summary, auth_epoch) = tokio::task::spawn_blocking(move || {
        bilibili_auth::with_auth_mutation_transaction_at_epoch(
            &state_path,
            &credential_file,
            auth_epoch,
            |transaction| {
                ensure_bbdown_login_active(auth_generation)?;
                sync_legacy(transaction)?;
                ensure_bbdown_login_active(auth_generation)?;
                let summary = bilibili_core::CredentialRuntime::from_credential_file(
                    transaction.credential_file()?,
                    credential_profile.as_deref(),
                )?
                .save_merged(credentials)?;
                ensure_bbdown_login_active(auth_generation)?;
                if let Err(err) = transaction.delete_legacy_state() {
                    warn!(
                        error = %err,
                        path = %state_path.display(),
                        "fresh BBDown credentials saved but legacy auth state cleanup failed"
                    );
                }
                ensure_bbdown_login_active(auth_generation)?;
                Ok(summary)
            },
        )
    })
    .await
    .context("BBDown credential save task failed")??;
    ensure_bbdown_login_active(auth_generation)?;
    let credential_revision = BILIBILI_CREDENTIAL_REVISION
        .fetch_add(1, Ordering::SeqCst)
        .wrapping_add(1);
    Ok(SavedBilibiliLogin {
        summary,
        auth_generation,
        credential_revision,
        auth_epoch,
    })
}

async fn send_current_bbdown_login_success(
    telegram: &TelegramClient,
    config: &AppConfig,
    chat_id: i64,
    saved: &SavedBilibiliLogin,
    success_message: String,
) {
    let _reply_guard = bbdown_auth_reply_lock().lock().await;
    let auth_generation = saved.auth_generation;
    let credential_revision = saved.credential_revision;
    let auth_epoch = saved.auth_epoch;
    send_bbdown_auth_reply(
        telegram,
        config,
        chat_id,
        move |current_epoch| match current_epoch {
            Ok(current_epoch) => current_bbdown_login_success_message(
                auth_generation,
                credential_revision,
                auth_epoch,
                current_epoch,
                success_message,
            ),
            Err(err) => format!(
                "Failed to confirm BBDown login state:\n{}",
                summarize_bbdown_auth_error(&err)
            ),
        },
    )
    .await;
}

fn current_bbdown_login_success_message(
    auth_generation: u64,
    credential_revision: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    current_auth_epoch: bilibili_auth::AuthEpoch,
    success_message: String,
) -> String {
    if auth_generation.is_multiple_of(2)
        && BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) == auth_generation
        && BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst) == credential_revision
        && current_auth_epoch == auth_epoch
    {
        success_message
    } else {
        "BBDown credential state changed before the login confirmation was sent; run /bbdown status again."
            .to_string()
    }
}

async fn poll_bbdown_qr_login(
    client: &bbdown_core::BiliClient,
    ticket: &bbdown_core::QrLoginTicket,
    timeout: Duration,
) -> Result<QrLoginState> {
    Ok(tokio_timeout(timeout, async {
        match ticket.kind {
            QrLoginKind::Web => client.poll_web_qr_login(&ticket.key).await,
            QrLoginKind::Tv => client.poll_tv_qr_login(ticket).await,
        }
    })
    .await
    .context("BBDown QR login timed out")??)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn await_bbdown_login_active<F, T>(auth_generation: u64, future: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    await_bbdown_login_active_with_hook(auth_generation, future, || {}).await
}

async fn await_bbdown_login_active_with_hook<F, T, H>(
    auth_generation: u64,
    future: F,
    after_generation_check: H,
) -> Result<T>
where
    F: std::future::Future<Output = T>,
    H: FnOnce(),
{
    let cancel = bbdown_login_cancel_notify().notified();
    tokio::pin!(cancel);
    cancel.as_mut().enable();
    ensure_bbdown_login_active(auth_generation)?;
    after_generation_check();
    let result = tokio::select! {
        result = future => result,
        () = &mut cancel => {
            bail!("BBDown login was canceled by a later /bbdown logout");
        }
    };
    ensure_bbdown_login_active(auth_generation)?;
    Ok(result)
}

fn ensure_bbdown_login_active(auth_generation: u64) -> Result<()> {
    if BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) != auth_generation {
        bail!("BBDown login was canceled by a later /bbdown logout");
    }
    Ok(())
}

async fn send_bbdown_auth_ticket(
    telegram: &TelegramClient,
    chat_id: i64,
    mode: BilibiliAuthLoginMode,
    url: &str,
    qr_payload: &str,
    timeout_seconds: u64,
) -> Result<()> {
    let caption = match mode {
        BilibiliAuthLoginMode::Web => format!(
            "Scan this BBDown Web login QR in the Bilibili app. It expires in {} seconds.",
            timeout_seconds
        ),
        BilibiliAuthLoginMode::Tv => format!(
            "Scan this BBDown TV login QR in the Bilibili app. It expires in {} seconds.",
            timeout_seconds
        ),
        BilibiliAuthLoginMode::AccessKey => {
            "Scan this BBDown access-key authorization QR, or use the authorization link sent above."
                .to_string()
        }
    };
    if matches!(mode, BilibiliAuthLoginMode::AccessKey) {
        let link_delivered = match telegram
            .send_auth_secret_message(chat_id, format!("Authorization link:\n{url}"))
            .await
        {
            Ok(_) => true,
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to send BBDown access-key authorization link"
                );
                false
            }
        };
        let qr_delivered = match bilibili_auth::render_qr_png(qr_payload) {
            Ok(png) => match telegram.send_photo(chat_id, caption, png).await {
                Ok(()) => true,
                Err(err) => {
                    warn!(
                        error = %err,
                        "failed to send BBDown access-key QR image after authorization link"
                    );
                    if link_delivered {
                        let message = "BBDown access-key QR image could not be sent. Use the authorization link above."
                            .to_string();
                        send_or_log(telegram, chat_id, message).await;
                    }
                    false
                }
            },
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to render BBDown access-key QR image after authorization link"
                );
                if link_delivered {
                    let message = "BBDown access-key QR image could not be rendered. Use the authorization link above."
                        .to_string();
                    send_or_log(telegram, chat_id, message).await;
                }
                false
            }
        };
        if !link_delivered && !qr_delivered {
            bail!("failed to send BBDown access-key authorization link or QR image");
        }
        return Ok(());
    }
    let png = bilibili_auth::render_qr_png(qr_payload)?;
    telegram
        .send_photo(chat_id, caption, png)
        .await
        .context("failed to send BBDown auth QR image")?;
    Ok(())
}

fn bbdown_login_mode_label(mode: BilibiliAuthLoginMode) -> &'static str {
    match mode {
        BilibiliAuthLoginMode::Web => "web",
        BilibiliAuthLoginMode::Tv => "tv",
        BilibiliAuthLoginMode::AccessKey => "access-key",
    }
}

fn format_bbdown_credential_summary(summary: &CredentialSource) -> String {
    format!(
        "Stored credentials: cookie={}, access_key={}, tv_access_key={}.",
        yes_no(summary.has_cookie),
        yes_no(summary.has_access_key),
        yes_no(summary.has_tv_access_key)
    )
}

fn format_bbdown_credential_health_report(report: &CredentialHealthReport) -> String {
    let mut lines = vec![
        "BBDown credential health:".to_string(),
        format_bbdown_credential_summary(&report.credentials),
    ];
    if report.probes.is_empty() {
        lines.push("No health probes were reported.".to_string());
    } else {
        for probe in &report.probes {
            let mut line = format!(
                "{} ({}): {}",
                credential_kind_label(probe.kind),
                credential_health_scope_label(probe.scope),
                credential_health_status_label(probe.status)
            );
            if let Some(code) = probe.api_code {
                line.push_str(&format!(" code={code}"));
            }
            if let Some(message) = probe
                .message
                .as_deref()
                .filter(|message| !message.is_empty())
            {
                line.push_str(" - ");
                line.push_str(message);
            }
            lines.push(line);
        }
    }
    lines.join("\n")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn credential_kind_label(kind: CredentialKind) -> &'static str {
    match kind {
        CredentialKind::Cookie => "cookie",
        CredentialKind::AccessKey => "access_key",
        CredentialKind::TvAccessKey => "tv_access_key",
    }
}

fn credential_health_scope_label(scope: CredentialHealthScope) -> &'static str {
    match scope {
        CredentialHealthScope::WebCookie => "web",
        CredentialHealthScope::IntlBstar => "intl/bstar",
        CredentialHealthScope::Tv => "tv",
    }
}

fn credential_health_status_label(status: CredentialHealthStatus) -> &'static str {
    match status {
        CredentialHealthStatus::Missing => "missing",
        CredentialHealthStatus::Valid => "valid",
        CredentialHealthStatus::Rejected => "rejected",
        CredentialHealthStatus::RequestFailed => "request_failed",
    }
}

fn pending_bilibili_access_key_logins()
-> &'static Mutex<HashMap<i64, PendingBilibiliAccessKeyLogin>> {
    PENDING_BILIBILI_ACCESS_KEY_LOGINS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn claim_pending_bilibili_access_key_login(
    logins: &mut HashMap<i64, PendingBilibiliAccessKeyLogin>,
    chat_id: i64,
    now: Instant,
) -> PendingBilibiliAccessKeyLoginClaim {
    prune_expired_pending_bilibili_access_key_logins(logins, now);
    let Some(login) = logins.get_mut(&chat_id) else {
        return PendingBilibiliAccessKeyLoginClaim::Missing;
    };
    if login.in_progress {
        return PendingBilibiliAccessKeyLoginClaim::InProgress;
    }
    login.in_progress = true;
    PendingBilibiliAccessKeyLoginClaim::Claimed(login.clone())
}

async fn release_pending_bilibili_access_key_login(
    config: &AppConfig,
    chat_id: i64,
    auth_generation: u64,
    ticket_id: u64,
) -> bool {
    let current_auth_epoch = current_bbdown_auth_epoch(config).await.ok();
    let mut logins = pending_bilibili_access_key_logins().lock().await;
    let current_auth_generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
    release_claimed_bilibili_access_key_login(
        &mut logins,
        chat_id,
        auth_generation,
        ticket_id,
        current_auth_generation,
        current_auth_epoch,
        Instant::now(),
    )
}

fn release_claimed_bilibili_access_key_login(
    logins: &mut HashMap<i64, PendingBilibiliAccessKeyLogin>,
    chat_id: i64,
    auth_generation: u64,
    ticket_id: u64,
    current_auth_generation: u64,
    current_auth_epoch: Option<bilibili_auth::AuthEpoch>,
    now: Instant,
) -> bool {
    prune_expired_pending_bilibili_access_key_logins(logins, now);
    if current_auth_generation != auth_generation {
        return false;
    }
    let Some(login) = logins.get(&chat_id) else {
        return false;
    };
    if login.auth_generation != auth_generation || login.ticket_id != ticket_id {
        return false;
    }
    if current_auth_epoch != Some(login.auth_epoch) {
        logins.remove(&chat_id);
        return false;
    }
    if now.duration_since(login.created_at) > BILIBILI_ACCESS_KEY_LOGIN_TTL {
        logins.remove(&chat_id);
        return false;
    }
    logins
        .get_mut(&chat_id)
        .expect("ticket was checked above")
        .in_progress = false;
    true
}

async fn clear_pending_bilibili_access_key_login(
    chat_id: i64,
    auth_generation: u64,
    ticket_id: u64,
) {
    let mut logins = pending_bilibili_access_key_logins().lock().await;
    clear_claimed_bilibili_access_key_login(&mut logins, chat_id, auth_generation, ticket_id);
}

fn clear_claimed_bilibili_access_key_login(
    logins: &mut HashMap<i64, PendingBilibiliAccessKeyLogin>,
    chat_id: i64,
    auth_generation: u64,
    ticket_id: u64,
) -> bool {
    if logins.get(&chat_id).is_some_and(|login| {
        login.auth_generation == auth_generation && login.ticket_id == ticket_id
    }) {
        logins.remove(&chat_id);
        true
    } else {
        false
    }
}

async fn has_pending_bilibili_access_key_login() -> bool {
    let mut logins = pending_bilibili_access_key_logins().lock().await;
    prune_expired_pending_bilibili_access_key_logins(&mut logins, Instant::now());
    !logins.is_empty()
}

fn prune_expired_pending_bilibili_access_key_logins(
    logins: &mut HashMap<i64, PendingBilibiliAccessKeyLogin>,
    now: Instant,
) {
    logins.retain(|_, login| {
        login.in_progress || now.duration_since(login.created_at) <= BILIBILI_ACCESS_KEY_LOGIN_TTL
    });
}

async fn run_bbdown_status(telegram: TelegramClient, config: Arc<AppConfig>, chat_id: i64) {
    let (auth_generation, credential_revision, sync_observation) = {
        let _state_guard = bbdown_auth_state_lock().lock().await;
        let auth_generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let credential_revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
        let state_path = config.bilibili.auth.state_path.clone();
        let credential_file = config.bilibili.auth.credential_file.clone();
        let credential_profile = config.bilibili.auth.credential_profile.clone();
        let sync_observation = match tokio::task::spawn_blocking(move || {
            bilibili_auth::sync_bbdown_rust_credentials_from_state_with_epoch(
                &state_path,
                &credential_file,
                credential_profile.as_deref(),
            )
        })
        .await
        {
            Ok(result) => result,
            Err(err) => Err(anyhow::anyhow!(
                "BBDown credential migration task failed: {err}"
            )),
        };
        (auth_generation, credential_revision, sync_observation)
    };
    let (message, auth_epoch) = match sync_observation {
        Ok((sync_result, auth_epoch)) => {
            let message = match sync_result {
                Ok(_) => match bilibili_core::credential_health(&config).await {
                    Ok(report) => format_bbdown_credential_health_report(&report),
                    Err(err) => format!(
                        "Failed to check BBDown credential health:\n{}",
                        summarize_bbdown_auth_error(&err)
                    ),
                },
                Err(err) => format!(
                    "Failed to check BBDown credential health:\n{}",
                    summarize_bbdown_auth_error(&err)
                ),
            };
            (message, Some(auth_epoch))
        }
        Err(err) => (
            format!(
                "Failed to check BBDown credential health:\n{}",
                summarize_bbdown_auth_error(&err)
            ),
            None,
        ),
    };
    let _reply_guard = bbdown_auth_reply_lock().lock().await;
    send_bbdown_auth_reply(&telegram, &config, chat_id, move |current_epoch| {
        match (auth_epoch, current_epoch) {
            (Some(auth_epoch), Ok(current_auth_epoch)) => current_bbdown_status_message(
                auth_generation,
                credential_revision,
                auth_epoch,
                current_auth_epoch,
                message,
            ),
            (None, Ok(_)) => message,
            (_, Err(err)) => format!(
                "Failed to confirm BBDown credential state after status check:\n{}",
                summarize_bbdown_auth_error(&err)
            ),
        }
    })
    .await;
}

fn current_bbdown_status_message(
    auth_generation: u64,
    credential_revision: u64,
    auth_epoch: bilibili_auth::AuthEpoch,
    current_auth_epoch: bilibili_auth::AuthEpoch,
    message: String,
) -> String {
    if auth_generation.is_multiple_of(2)
        && BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) == auth_generation
        && BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst) == credential_revision
        && current_auth_epoch == auth_epoch
    {
        message
    } else {
        "BBDown credential state changed while status was checked; run /bbdown status again."
            .to_string()
    }
}

fn bbdown_auth_state_lock() -> &'static Mutex<()> {
    BILIBILI_AUTH_STATE_LOCK.get_or_init(|| Mutex::new(()))
}

fn bbdown_auth_reply_lock() -> &'static Mutex<()> {
    BILIBILI_AUTH_REPLY_LOCK.get_or_init(|| Mutex::new(()))
}

async fn acquire_bbdown_auth_reply_file_lock(
    config: &AppConfig,
) -> Result<bilibili_auth::AuthReplyFileLock> {
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    tokio::task::spawn_blocking(move || {
        bilibili_auth::acquire_auth_reply_file_lock(&state_path, &credential_file)
    })
    .await
    .context("BBDown auth lock task failed")?
}

async fn send_bbdown_auth_reply<F>(
    telegram: &TelegramClient,
    config: &AppConfig,
    chat_id: i64,
    message_for_epoch: F,
) where
    F: FnOnce(Result<bilibili_auth::AuthEpoch>) -> String + Send + 'static,
{
    let blocking_telegram = telegram.clone();
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    let runtime = tokio::runtime::Handle::current();
    let sent = tokio::task::spawn_blocking(move || {
        let file_lock = bilibili_auth::acquire_auth_reply_file_lock(&state_path, &credential_file);
        let current_epoch = match &file_lock {
            Ok(file_lock) => file_lock.current_epoch(),
            Err(err) => Err(anyhow::anyhow!("{err:#}")),
        };
        let message = message_for_epoch(current_epoch);
        runtime.block_on(send_or_log(&blocking_telegram, chat_id, message));
        drop(file_lock);
    })
    .await;
    if let Err(err) = sent {
        send_or_log(
            telegram,
            chat_id,
            format!("Failed to send BBDown auth reply from blocking task: {err}"),
        )
        .await;
    }
}

async fn current_bbdown_auth_epoch(config: &AppConfig) -> Result<bilibili_auth::AuthEpoch> {
    let file_lock = acquire_bbdown_auth_reply_file_lock(config).await?;
    file_lock.current_epoch()
}

fn bbdown_login_cancel_notify() -> &'static Notify {
    BILIBILI_LOGIN_CANCEL_NOTIFY.get_or_init(Notify::new)
}

async fn run_bbdown_logout(telegram: TelegramClient, config: Arc<AppConfig>, chat_id: i64) {
    // Odd generations mark an in-progress credential mutation. This also cancels active logins
    // before local credential cleanup waits on any other async work.
    BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
    bbdown_login_cancel_notify().notify_waiters();
    pending_bilibili_access_key_logins().lock().await.clear();
    let cleanup = {
        let _state_guard = bbdown_auth_state_lock().lock().await;
        let state_path = config.bilibili.auth.state_path.clone();
        let credential_file = config.bilibili.auth.credential_file.clone();
        let credential_profile = config.bilibili.auth.credential_profile.clone();
        let cleanup = tokio::task::spawn_blocking(move || {
            bilibili_auth::clear_auth_state_and_credentials_with_epoch(
                &state_path,
                &credential_file,
                |transaction| {
                    bilibili_core::CredentialRuntime::from_credential_file(
                        transaction.credential_file()?,
                        credential_profile.as_deref(),
                    )
                    .and_then(|runtime| runtime.logout())
                },
            )
        })
        .await
        .context("BBDown logout task failed")
        .and_then(|result| result);
        // Return to a stable even generation and invalidate status checks that started after
        // logout was requested but before the credential files were cleared.
        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        BILIBILI_CREDENTIAL_REVISION.fetch_add(1, Ordering::SeqCst);
        cleanup
    };
    let (message, auth_epoch) = match cleanup {
        Ok(((Ok(_), Ok(())), auth_epoch)) => (
            "BBDown credential state cleared.".to_string(),
            Some(auth_epoch),
        ),
        Ok(((Ok(_), Err(err)), auth_epoch)) => (
            format!(
                "Failed to clear BBDown credential state:\n{}",
                summarize_bbdown_auth_error(&err)
            ),
            Some(auth_epoch),
        ),
        Ok(((Err(err), _), auth_epoch)) => (
            format!(
                "Failed to clear legacy BBDown login state:\n{}",
                truncate(&err.to_string())
            ),
            Some(auth_epoch),
        ),
        Err(err) => (
            format!(
                "Failed to lock BBDown credential state for logout:\n{}",
                truncate(&err.to_string())
            ),
            None,
        ),
    };
    let _reply_guard = bbdown_auth_reply_lock().lock().await;
    send_bbdown_auth_reply(&telegram, &config, chat_id, move |current_epoch| {
        match (auth_epoch, current_epoch) {
            (Some(auth_epoch), Ok(current_auth_epoch)) => {
                current_bbdown_logout_message(auth_epoch, current_auth_epoch, message)
            }
            (None, Ok(_)) => message,
            (_, Err(err)) => format!(
                "Failed to confirm BBDown credential state after logout:\n{}",
                summarize_bbdown_auth_error(&err)
            ),
        }
    })
    .await;
}

fn current_bbdown_logout_message(
    auth_epoch: bilibili_auth::AuthEpoch,
    current_auth_epoch: bilibili_auth::AuthEpoch,
    message: String,
) -> String {
    if current_auth_epoch == auth_epoch {
        message
    } else {
        "BBDown credential state changed before the logout confirmation was sent; run /bbdown status again."
            .to_string()
    }
}

fn bbdown_auth_usage() -> String {
    "Usage: /bbdown login [web|tv|access-key] | /bbdown status | /bbdown logout".to_string()
}

#[cfg_attr(not(test), allow(dead_code))]
fn bbdown_qr_photo_failed_message() -> String {
    "Could not send the QR image. BBDown login canceled; try /bbdown login again after Telegram photo delivery is working.".to_string()
}

fn queue_or_prompt_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
) {
    if job.requires_bilibili_selection() {
        tokio::spawn(prompt_bilibili_selection(telegram, chat_id, job_id, job));
        return;
    }

    tokio::spawn(process_job_after_duplicate_check(
        telegram,
        config,
        job_dispatch,
        chat_id,
        job_id,
        job,
    ));
}

async fn prompt_bilibili_selection(
    telegram: TelegramClient,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
) {
    let token = next_bilibili_selection_callback_token(job_id);
    let now = Instant::now();
    {
        let mut pending_jobs = pending_bilibili_selection_jobs().lock().await;
        prune_expired_pending_bilibili_selection_jobs(&mut pending_jobs, now);
        pending_jobs.insert(
            token,
            PendingBilibiliSelectionJob {
                chat_id,
                job_id,
                job,
                created_at: now,
            },
        );
        cap_pending_bilibili_selection_jobs(&mut pending_jobs, Some(token));
    }

    match telegram
        .send_message_with_inline_keyboard(
            chat_id,
            bilibili_selection_message(job_id),
            bilibili_selection_keyboard(token),
        )
        .await
    {
        Ok(_) => {}
        Err(err) => {
            pending_bilibili_selection_jobs()
                .lock()
                .await
                .remove(&token);
            warn!(chat_id, job_id, error = %err, "failed to send Bilibili selection prompt");
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Bilibili selection prompt failed for job #{job_id}; job canceled. Send the link again to retry.\n{}",
                    truncate(&err.to_string())
                ),
            )
            .await;
        }
    }
}

async fn process_job_after_duplicate_check(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
) {
    if matches!(job, JobRequest::Pdf { .. }) {
        queue_job(
            telegram,
            config,
            Arc::clone(&job_dispatch.download_semaphore),
            chat_id,
            job_id,
            job,
            JobRunMode::Direct,
        )
        .await;
        return;
    }

    let duplicate_scan_permit = match Arc::clone(&job_dispatch.duplicate_scan_semaphore)
        .acquire_owned()
        .await
    {
        Ok(permit) => permit,
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Duplicate check unavailable for job #{job_id}; continuing without duplicate prompt.\n{}",
                    truncate(&err.to_string())
                ),
            )
            .await;
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
            return;
        }
    };
    let duplicate_scan_result = find_video_duplicate_async(Arc::clone(&config), job.clone()).await;
    drop(duplicate_scan_permit);

    match duplicate_scan_result {
        Ok(Some(duplicate)) => {
            prompt_duplicate_choice(&telegram, chat_id, job_id, job, duplicate).await;
        }
        Ok(None) => {
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
        }
        Err(err) if should_prompt_bilibili_selection_after_probe_error(&job, &err) => {
            prompt_bilibili_selection(telegram, chat_id, job_id, job).await;
        }
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Duplicate check failed for job #{job_id}; continuing without duplicate prompt.\n{}",
                    truncate(&err.to_string())
                ),
            )
            .await;
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
        }
    }
}

fn should_prompt_bilibili_selection_after_probe_error(
    job: &JobRequest,
    error: &anyhow::Error,
) -> bool {
    matches!(
        job,
        JobRequest::Bilibili {
            selection: None,
            ..
        }
    ) && is_bilibili_selection_required_error(error)
}

fn is_bilibili_selection_required_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<bbdown_core::Error>()
            .is_some_and(|err| matches!(err, bbdown_core::Error::SelectionRequired { .. }))
    })
}

async fn find_video_duplicate_async(
    config: Arc<AppConfig>,
    job: JobRequest,
) -> Result<Option<VideoDuplicate>> {
    find_video_duplicate_with_probe(&config, &job).await
}

async fn prompt_duplicate_choice(
    telegram: &TelegramClient,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    duplicate: VideoDuplicate,
) {
    let token = next_duplicate_callback_token(job_id);
    let prompt = duplicate_choice_message(job_id, job.label(), &duplicate);
    let allow_overwrite = job_allows_duplicate_overwrite(&job, &duplicate);
    let now = Instant::now();
    {
        let mut pending_jobs = pending_duplicate_jobs().lock().await;
        prune_expired_pending_duplicate_jobs(&mut pending_jobs, now);
        pending_jobs.insert(
            token,
            PendingDuplicateJob {
                chat_id,
                job_id,
                job,
                duplicate,
                created_at: now,
            },
        );
        cap_pending_duplicate_jobs(&mut pending_jobs, Some(token));
    }
    match telegram
        .send_message_with_inline_keyboard(
            chat_id,
            truncate(&prompt),
            duplicate_choice_keyboard(token, allow_overwrite),
        )
        .await
    {
        Ok(_) => {}
        Err(err) => {
            pending_duplicate_jobs().lock().await.remove(&token);
            warn!(chat_id, job_id, error = %err, "failed to send duplicate choice prompt");
            send_or_log(
                telegram,
                chat_id,
                format!("Duplicate found for job #{job_id}, but Telegram choice prompt failed. Job canceled; send the link again to retry."),
            )
            .await;
        }
    }
}

async fn queue_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    run_mode: JobRunMode,
) {
    send_or_log(
        &telegram,
        chat_id,
        format!("Queued job #{job_id}: {}", job.label()),
    )
    .await;

    tokio::spawn(run_queued_job(
        telegram, config, semaphore, chat_id, job_id, job, run_mode,
    ));
}

async fn handle_callback_query(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    callback_query: CallbackQuery,
) {
    let callback_id = callback_query.id.clone();
    let Some(data) = callback_query.data.as_deref() else {
        answer_callback_or_log(&telegram, callback_id, "Unsupported button.".to_string()).await;
        return;
    };
    let Some(message) = callback_query.message else {
        answer_callback_or_log(
            &telegram,
            callback_id,
            "This choice has expired.".to_string(),
        )
        .await;
        return;
    };
    let chat_id = message.chat.id;
    if !config.telegram.is_chat_allowed(chat_id) {
        warn!(chat_id, "ignoring callback from unauthorized chat");
        answer_callback_or_log(&telegram, callback_id, "Unauthorized chat.".to_string()).await;
        return;
    }

    if let Some(callback) = parse_bilibili_selection_callback_data(data) {
        handle_bilibili_selection_callback(
            telegram,
            config,
            job_dispatch,
            callback_id,
            chat_id,
            message.message_id,
            callback,
        )
        .await;
        return;
    }

    let Some(callback) = parse_duplicate_callback_data(data) else {
        answer_callback_or_log(&telegram, callback_id, "Unsupported button.".to_string()).await;
        return;
    };

    let pending = take_pending_duplicate_job(callback.token, chat_id).await;
    let Some(pending) = pending else {
        answer_callback_or_log(
            &telegram,
            callback_id,
            "This choice has expired.".to_string(),
        )
        .await;
        return;
    };

    match callback.action {
        DuplicateCallbackAction::Cancel => {
            answer_callback_or_log(&telegram, callback_id, "Canceled.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message.message_id,
                format!("Canceled job #{}: {}", pending.job_id, pending.job.label()),
            )
            .await;
        }
        DuplicateCallbackAction::Run(action) => {
            if matches!(action, VideoDuplicateAction::Overwrite)
                && !job_allows_duplicate_overwrite(&pending.job, &pending.duplicate)
            {
                answer_callback_or_log(
                    &telegram,
                    callback_id,
                    "Overwrite is not available for this job.".to_string(),
                )
                .await;
                edit_without_keyboard_or_send(
                    &telegram,
                    chat_id,
                    message.message_id,
                    format!("Canceled job #{}: {}", pending.job_id, pending.job.label()),
                )
                .await;
                return;
            }
            let action_label = match action {
                VideoDuplicateAction::Overwrite => "overwrite",
                VideoDuplicateAction::KeepBoth => "keep both",
            };
            answer_callback_or_log(&telegram, callback_id, "Queued.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message.message_id,
                format!(
                    "Selected {action_label} for job #{}: {}",
                    pending.job_id,
                    pending.job.label()
                ),
            )
            .await;
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                pending.job_id,
                pending.job,
                JobRunMode::Duplicate(DuplicateRun {
                    action,
                    duplicate: pending.duplicate,
                }),
            )
            .await;
        }
    }
}

async fn handle_bilibili_selection_callback(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    callback_id: String,
    chat_id: i64,
    message_id: i64,
    callback: BilibiliSelectionCallback,
) {
    let pending = take_pending_bilibili_selection_job(callback.token, chat_id).await;
    let Some(pending) = pending else {
        answer_callback_or_log(
            &telegram,
            callback_id,
            "This choice has expired.".to_string(),
        )
        .await;
        return;
    };

    match callback.action {
        BilibiliSelectionCallbackAction::Cancel => {
            answer_callback_or_log(&telegram, callback_id, "Canceled.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message_id,
                format!("Canceled job #{}: {}", pending.job_id, pending.job.label()),
            )
            .await;
        }
        BilibiliSelectionCallbackAction::Run(selection) => {
            let job = apply_bilibili_selection(pending.job, selection);
            answer_callback_or_log(&telegram, callback_id, "Queued.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message_id,
                format!(
                    "Selected {} for job #{}: {}",
                    selection.label(),
                    pending.job_id,
                    job.label()
                ),
            )
            .await;
            tokio::spawn(process_job_after_duplicate_check(
                telegram,
                config,
                job_dispatch,
                chat_id,
                pending.job_id,
                job,
            ));
        }
    }
}

fn default_run_mode(job: &JobRequest) -> JobRunMode {
    match job {
        JobRequest::Bilibili { .. } | JobRequest::Youtube { .. } => JobRunMode::StagedKeepBoth,
        JobRequest::Pdf { .. } => JobRunMode::Direct,
    }
}

fn job_allows_duplicate_overwrite(job: &JobRequest, duplicate: &VideoDuplicate) -> bool {
    duplicate.allows_overwrite_for(job)
}

impl From<DuplicateRun> for JobRunMode {
    fn from(value: DuplicateRun) -> Self {
        Self::Duplicate(value)
    }
}

fn pending_bilibili_selection_jobs() -> &'static Mutex<HashMap<u64, PendingBilibiliSelectionJob>> {
    PENDING_BILIBILI_SELECTION_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn take_pending_bilibili_selection_job(
    token: u64,
    chat_id: i64,
) -> Option<PendingBilibiliSelectionJob> {
    let mut jobs = pending_bilibili_selection_jobs().lock().await;
    prune_expired_pending_bilibili_selection_jobs(&mut jobs, Instant::now());
    match jobs.get(&token) {
        Some(job) if job.chat_id == chat_id => jobs.remove(&token),
        _ => None,
    }
}

fn prune_expired_pending_bilibili_selection_jobs(
    jobs: &mut HashMap<u64, PendingBilibiliSelectionJob>,
    now: Instant,
) {
    jobs.retain(|_, job| now.duration_since(job.created_at) <= BILIBILI_SELECTION_DECISION_TTL);
}

fn cap_pending_bilibili_selection_jobs(
    jobs: &mut HashMap<u64, PendingBilibiliSelectionJob>,
    protected_token: Option<u64>,
) {
    while jobs.len() > MAX_PENDING_BILIBILI_SELECTION_JOBS {
        let Some(oldest_job_id) = jobs
            .iter()
            .filter(|(token, _)| Some(**token) != protected_token)
            .min_by_key(|(_, job)| job.created_at)
            .map(|(job_id, _)| *job_id)
        else {
            break;
        };
        jobs.remove(&oldest_job_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BilibiliSelectionCallback {
    token: u64,
    action: BilibiliSelectionCallbackAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BilibiliSelectionCallbackAction {
    Run(BilibiliSelection),
    Cancel,
}

fn parse_bilibili_selection_callback_data(data: &str) -> Option<BilibiliSelectionCallback> {
    let mut parts = data.split(':');
    let prefix = parts.next()?;
    let token = u64::from_str_radix(parts.next()?, 16).ok()?;
    let action = parts.next()?;
    if parts.next().is_some() || prefix != "bsel" {
        return None;
    }
    let action = match action {
        "latest" => BilibiliSelectionCallbackAction::Run(BilibiliSelection::Latest),
        "all" => BilibiliSelectionCallbackAction::Run(BilibiliSelection::All),
        "cancel" => BilibiliSelectionCallbackAction::Cancel,
        _ => return None,
    };
    Some(BilibiliSelectionCallback { token, action })
}

fn next_bilibili_selection_callback_token(job_id: u64) -> u64 {
    let counter = BILIBILI_SELECTION_CALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    nanos ^ counter.rotate_left(19) ^ job_id.rotate_left(35) ^ (std::process::id() as u64)
}

fn bilibili_selection_callback_data(token: u64, action: &str) -> String {
    format!("bsel:{token:016x}:{action}")
}

fn bilibili_selection_keyboard(token: u64) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            vec![
                InlineKeyboardButton {
                    text: "Latest episode".to_string(),
                    callback_data: bilibili_selection_callback_data(token, "latest"),
                },
                InlineKeyboardButton {
                    text: "All episodes".to_string(),
                    callback_data: bilibili_selection_callback_data(token, "all"),
                },
            ],
            vec![InlineKeyboardButton {
                text: "Cancel".to_string(),
                callback_data: bilibili_selection_callback_data(token, "cancel"),
            }],
        ],
    }
}

fn bilibili_selection_message(job_id: u64) -> String {
    format!("Bilibili season/media link queued as job #{job_id}. Choose what to download:")
}

fn apply_bilibili_selection(job: JobRequest, selection: BilibiliSelection) -> JobRequest {
    match job {
        JobRequest::Bilibili { url, .. } => JobRequest::Bilibili {
            url,
            selection: Some(selection),
        },
        other => other,
    }
}

fn pending_duplicate_jobs() -> &'static Mutex<HashMap<u64, PendingDuplicateJob>> {
    PENDING_DUPLICATE_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn take_pending_duplicate_job(token: u64, chat_id: i64) -> Option<PendingDuplicateJob> {
    let mut jobs = pending_duplicate_jobs().lock().await;
    prune_expired_pending_duplicate_jobs(&mut jobs, Instant::now());
    match jobs.get(&token) {
        Some(job) if job.chat_id == chat_id => jobs.remove(&token),
        _ => None,
    }
}

fn prune_expired_pending_duplicate_jobs(
    jobs: &mut HashMap<u64, PendingDuplicateJob>,
    now: Instant,
) {
    jobs.retain(|_, job| now.duration_since(job.created_at) <= DUPLICATE_DECISION_TTL);
}

fn cap_pending_duplicate_jobs(
    jobs: &mut HashMap<u64, PendingDuplicateJob>,
    protected_token: Option<u64>,
) {
    let limit = pending_duplicate_job_limit();
    while jobs.len() > limit {
        let Some(oldest_job_id) = jobs
            .iter()
            .filter(|(token, _)| Some(**token) != protected_token)
            .min_by_key(|(_, job)| job.created_at)
            .map(|(job_id, _)| *job_id)
        else {
            break;
        };
        jobs.remove(&oldest_job_id);
    }
}

fn pending_duplicate_job_limit() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let descriptor_budget = usize::try_from(limit.rlim_cur)
                .unwrap_or(usize::MAX)
                .saturating_div(4)
                .max(1);
            return descriptor_budget.min(MAX_PENDING_DUPLICATE_JOBS);
        }
    }
    MAX_PENDING_DUPLICATE_JOBS
}

async fn expire_pending_duplicate_jobs() {
    let mut ticker = interval(PENDING_DUPLICATE_SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let mut jobs = pending_duplicate_jobs().lock().await;
        prune_expired_pending_duplicate_jobs(&mut jobs, Instant::now());
        cap_pending_duplicate_jobs(&mut jobs, None);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DuplicateCallback {
    token: u64,
    action: DuplicateCallbackAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DuplicateCallbackAction {
    Run(VideoDuplicateAction),
    Cancel,
}

fn parse_duplicate_callback_data(data: &str) -> Option<DuplicateCallback> {
    let mut parts = data.split(':');
    let prefix = parts.next()?;
    let token = u64::from_str_radix(parts.next()?, 16).ok()?;
    let action = parts.next()?;
    if parts.next().is_some() || prefix != "dup" {
        return None;
    }
    let action = match action {
        "overwrite" => DuplicateCallbackAction::Run(VideoDuplicateAction::Overwrite),
        "keep" => DuplicateCallbackAction::Run(VideoDuplicateAction::KeepBoth),
        "cancel" => DuplicateCallbackAction::Cancel,
        _ => return None,
    };
    Some(DuplicateCallback { token, action })
}

fn next_duplicate_callback_token(job_id: u64) -> u64 {
    let counter = DUPLICATE_CALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    nanos ^ counter.rotate_left(17) ^ job_id.rotate_left(32) ^ (std::process::id() as u64)
}

fn duplicate_callback_data(token: u64, action: &str) -> String {
    format!("dup:{token:016x}:{action}")
}

fn duplicate_choice_keyboard(token: u64, allow_overwrite: bool) -> InlineKeyboardMarkup {
    let mut first_row = Vec::new();
    if allow_overwrite {
        first_row.push(InlineKeyboardButton {
            text: "Overwrite".to_string(),
            callback_data: duplicate_callback_data(token, "overwrite"),
        });
    }
    first_row.push(InlineKeyboardButton {
        text: "Keep both".to_string(),
        callback_data: duplicate_callback_data(token, "keep"),
    });
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            first_row,
            vec![InlineKeyboardButton {
                text: "Cancel".to_string(),
                callback_data: duplicate_callback_data(token, "cancel"),
            }],
        ],
    }
}

fn duplicate_choice_message(job_id: u64, job_label: &str, duplicate: &VideoDuplicate) -> String {
    format!(
        "Existing video found for job #{job_id}: {job_label}\nIdentity: {} {}\n\nChoose how to handle it:\n{}",
        duplicate.identity.provider.as_str(),
        duplicate.identity.id,
        duplicate.describe_existing_videos(5)
    )
}

async fn answer_callback_or_log(
    telegram: &TelegramClient,
    callback_query_id: String,
    text: String,
) {
    if let Err(err) = telegram
        .answer_callback_query(callback_query_id, text)
        .await
    {
        warn!(error = %err, "failed to answer telegram callback query");
    }
}

async fn run_queued_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    run_mode: JobRunMode,
) {
    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(err) => {
            error!(job_id, error = %err, "job semaphore closed");
            return;
        }
    };

    let status_message_id = send_or_log_message_id(
        &telegram,
        chat_id,
        job_status_message(job_id, job.label(), "Started", None),
    )
    .await;

    let (progress_tx, progress_rx) = job_progress_channel();
    let progress_task = tokio::spawn(forward_progress(
        telegram.clone(),
        chat_id,
        job_id,
        job.label(),
        status_message_id,
        progress_rx,
        Duration::from_secs(config.bot.progress_update_seconds),
    ));
    let result = match run_mode {
        JobRunMode::Duplicate(duplicate_run) => {
            run_job_with_duplicate_action(
                &config,
                &job,
                duplicate_run.action,
                &duplicate_run.duplicate,
                Some(progress_tx),
            )
            .await
        }
        JobRunMode::StagedKeepBoth => {
            run_video_job_staged_keep_both(&config, &job, Some(progress_tx)).await
        }
        JobRunMode::Direct => run_job(&config, &job, Some(progress_tx)).await,
    };
    let _ = progress_task.await;
    drop(permit);

    let message = match result {
        Ok(report) => {
            let details = if report.details.is_empty() {
                String::new()
            } else {
                format!("\n{}", report.details)
            };
            format!(
                "Finished job #{job_id}: {}\nSaved: {}{}",
                job.label(),
                report.saved_location,
                details
            )
        }
        Err(err) => {
            let error_chain = redact_sensitive_text(&format!("{err:#}"));
            error!(job_id, error = %error_chain, "job failed");
            failed_job_message(job_id, job.label(), &error_chain)
        }
    };

    if let Some(message_id) = status_message_id {
        edit_or_send(&telegram, chat_id, message_id, message).await;
    } else {
        send_or_log(&telegram, chat_id, message).await;
    }
}

fn failed_job_message(job_id: u64, job_label: &str, error_chain: &str) -> String {
    format!(
        "Failed job #{job_id}: {job_label}\n{}",
        truncate(&redact_sensitive_text(error_chain))
    )
}

async fn forward_progress(
    telegram: TelegramClient,
    chat_id: i64,
    job_id: u64,
    job_label: &'static str,
    status_message_id: Option<i64>,
    mut progress_rx: JobProgressReceiver,
    update_interval: Duration,
) {
    let mut delivery = ProgressDelivery::from_message_id(status_message_id);
    let mut ticker = interval(update_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut pending = None;
    loop {
        tokio::select! {
            changed = progress_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                pending = progress_rx.borrow_and_update().clone();
            }
            _ = ticker.tick(), if pending.is_some() => {
                let progress = pending.take().expect("guarded by is_some");
                delivery = deliver_progress(
                    &telegram,
                    chat_id,
                    job_id,
                    job_label,
                    delivery,
                    progress,
                ).await;
            }
        }
    }
}

async fn deliver_progress(
    telegram: &TelegramClient,
    chat_id: i64,
    job_id: u64,
    job_label: &'static str,
    mut delivery: ProgressDelivery,
    progress: JobProgress,
) -> ProgressDelivery {
    let message = job_status_message(job_id, job_label, "Running", Some(&progress.message));
    match delivery {
        ProgressDelivery::Edit(message_id) => {
            if edit_or_log(telegram, chat_id, message_id, message).await {
                return delivery;
            }
            delivery = delivery.after_edit_result(false);
            send_or_log(
                telegram,
                chat_id,
                progress_fallback_message(job_id, &progress.message),
            )
            .await;
        }
        ProgressDelivery::Send => {
            send_or_log(
                telegram,
                chat_id,
                progress_fallback_message(job_id, &progress.message),
            )
            .await;
        }
    }
    delivery
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressDelivery {
    Edit(i64),
    Send,
}

impl ProgressDelivery {
    fn from_message_id(message_id: Option<i64>) -> Self {
        message_id.map_or(Self::Send, Self::Edit)
    }

    fn after_edit_result(self, succeeded: bool) -> Self {
        if succeeded { self } else { Self::Send }
    }
}

async fn send_or_log(telegram: &TelegramClient, chat_id: i64, text: String) {
    let _ = send_or_log_message_id(telegram, chat_id, text).await;
}

async fn send_or_log_message_id(
    telegram: &TelegramClient,
    chat_id: i64,
    text: String,
) -> Option<i64> {
    match telegram.send_message(chat_id, truncate(&text)).await {
        Ok(message_id) => Some(message_id),
        Err(err) => {
            warn!(chat_id, error = %err, "failed to send telegram message");
            None
        }
    }
}

async fn edit_or_log(
    telegram: &TelegramClient,
    chat_id: i64,
    message_id: i64,
    text: String,
) -> bool {
    match telegram
        .edit_message_text(chat_id, message_id, truncate(&text))
        .await
    {
        Ok(()) => true,
        Err(err) => {
            warn!(
                chat_id,
                message_id,
                error = %err,
                "failed to edit telegram message"
            );
            false
        }
    }
}

async fn edit_or_send(telegram: &TelegramClient, chat_id: i64, message_id: i64, text: String) {
    if let Err(err) = telegram
        .edit_message_text(chat_id, message_id, truncate(&text))
        .await
    {
        warn!(
            chat_id,
            message_id,
            error = %err,
            "failed to edit telegram message; sending a new message"
        );
        send_or_log(telegram, chat_id, text).await;
    }
}

async fn edit_without_keyboard_or_send(
    telegram: &TelegramClient,
    chat_id: i64,
    message_id: i64,
    text: String,
) {
    if let Err(err) = telegram
        .edit_message_text_without_inline_keyboard(chat_id, message_id, truncate(&text))
        .await
    {
        warn!(
            chat_id,
            message_id,
            error = %err,
            "failed to edit telegram message without inline keyboard; sending a new message"
        );
        send_or_log(telegram, chat_id, text).await;
    }
}

fn job_status_message(job_id: u64, job_label: &str, state: &str, progress: Option<&str>) -> String {
    let mut message = format!("{state} job #{job_id}: {job_label}");
    if let Some(progress) = progress.filter(|progress| !progress.trim().is_empty()) {
        message.push('\n');
        message.push_str(progress);
    }
    message
}

fn progress_fallback_message(job_id: u64, progress: &str) -> String {
    format!("Progress job #{job_id}: {progress}")
}

fn truncate(text: &str) -> String {
    const MAX_CHARS: usize = 3500;
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}\n... <truncated>")
    } else {
        truncated
    }
}

fn summarize_bbdown_auth_error(error: &anyhow::Error) -> String {
    truncate(&redact_bbdown_auth_secrets(&error.to_string()))
}

fn redact_bbdown_auth_secrets(text: &str) -> String {
    redact_sensitive_text(text)
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::anyhow;

    use super::*;

    static TEST_AUTH_GENERATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_auth_epoch(value: u64) -> bilibili_auth::AuthEpoch {
        bilibili_auth::AuthEpoch::for_test(value)
    }

    #[test]
    fn redacts_bilibili_login_qr_urls_from_auth_errors() {
        let summary = summarize_bbdown_auth_error(&anyhow!(
            "failed after https://passport.bilibili.com/h5-app/passport/login/scan?qrcode_key=secret"
        ));

        assert!(!summary.contains("secret"));
        assert!(!summary.contains("qrcode_key="));
        assert!(summary.contains("<redacted Bilibili login QR URL>"));
    }

    #[test]
    fn redacts_bbdown_access_key_auth_secrets() {
        let summary = summarize_bbdown_auth_error(&anyhow!(
            "open https://www.biliplus.com/login?balh_auth=1&balh_auth_origin=https%3A%2F%2Fwww.bilibili.com\nthen https://www.bilibili.com/callback?access_token=secret&refresh_token=refresh\nfragment #access_key=secret\nraw {{\"access_key\":\"jsonsecret\",\"refresh_token\":\"jsonrefresh\"}}\nbalh-login-credentials: {{\"access_key\":\"secret\"}}"
        ));

        assert!(!summary.contains("access_token="));
        assert!(!summary.contains("access_key="));
        assert!(!summary.contains("\"access_key\""));
        assert!(!summary.contains("\"refresh_token\""));
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("jsonrefresh"));
        assert!(summary.contains("<redacted BBDown access-key authorization URL>"));
        assert!(summary.contains("<redacted BBDown access-key callback URL>"));
        assert!(summary.contains("<redacted BBDown access-key callback message>"));
    }

    #[test]
    fn qr_photo_failure_message_does_not_include_login_url() {
        let message = bbdown_qr_photo_failed_message();

        assert!(!message.contains("passport.bilibili.com"));
        assert!(!message.contains("qrcode_key="));
    }

    #[test]
    fn help_message_lists_supported_commands() {
        let message = help_message();

        for expected in [
            "/help",
            "/pdf URL",
            "/bbdown login [web|tv|access-key]",
            "/bbdown status",
        ] {
            assert!(message.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn formats_bbdown_credential_health_report() {
        let report: CredentialHealthReport = serde_json::from_value(serde_json::json!({
            "credentials": {
                "has_cookie": true,
                "has_access_key": false,
                "has_tv_access_key": true
            },
            "probes": [{
                "kind": "tv_access_key",
                "scope": "tv",
                "status": "valid",
                "api_code": 0
            }]
        }))
        .expect("health report sample should deserialize");

        let message = format_bbdown_credential_health_report(&report);

        assert!(message.contains("cookie=yes"));
        assert!(message.contains("access_key=no"));
        assert!(message.contains("tv_access_key (tv): valid code=0"));
    }

    #[test]
    fn failed_access_key_callback_releases_same_ticket_for_retry() {
        let chat_id = 123;
        let auth_generation = 42;
        let ticket_id = 7;
        let created_at = Instant::now();
        let mut logins = HashMap::from([(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation,
                auth_epoch: test_auth_epoch(3),
                ticket_id,
                ticket: bilibili_core::create_access_key_ticket()
                    .expect("access-key ticket should be created"),
                created_at,
                in_progress: false,
            },
        )]);

        let first = match claim_pending_bilibili_access_key_login(&mut logins, chat_id, created_at)
        {
            PendingBilibiliAccessKeyLoginClaim::Claimed(pending) => pending,
            claim => panic!("expected first claim, got {claim:?}"),
        };
        assert!(
            bilibili_core::access_key_login_credentials(&first.ticket, r#"{"access_key":"#)
                .is_err()
        );
        assert!(matches!(
            claim_pending_bilibili_access_key_login(&mut logins, chat_id, created_at),
            PendingBilibiliAccessKeyLoginClaim::InProgress
        ));

        assert!(release_claimed_bilibili_access_key_login(
            &mut logins,
            chat_id,
            auth_generation,
            ticket_id,
            auth_generation,
            Some(test_auth_epoch(3)),
            created_at,
        ));
        let second = match claim_pending_bilibili_access_key_login(&mut logins, chat_id, created_at)
        {
            PendingBilibiliAccessKeyLoginClaim::Claimed(pending) => pending,
            claim => panic!("expected retry claim, got {claim:?}"),
        };

        assert_eq!(second.auth_generation, first.auth_generation);
        assert_eq!(second.created_at, first.created_at);
    }

    #[test]
    fn stale_access_key_callback_does_not_release_newer_generation() {
        let chat_id = 123;
        let auth_generation = 42;
        let ticket_id = 7;
        let now = Instant::now();
        let mut logins = HashMap::from([(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation,
                auth_epoch: test_auth_epoch(3),
                ticket_id,
                ticket: bilibili_core::create_access_key_ticket()
                    .expect("access-key ticket should be created"),
                created_at: now,
                in_progress: true,
            },
        )]);

        assert!(!release_claimed_bilibili_access_key_login(
            &mut logins,
            chat_id,
            auth_generation,
            ticket_id,
            auth_generation + 1,
            Some(test_auth_epoch(3)),
            now,
        ));

        assert!(logins[&chat_id].in_progress);
    }

    #[test]
    fn stale_access_key_completion_cannot_clear_or_release_new_ticket() {
        let chat_id = 123;
        let auth_generation = 42;
        let now = Instant::now();
        let old_ticket_id = 7;
        let new_ticket_id = 8;
        let mut logins = HashMap::from([(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation,
                auth_epoch: test_auth_epoch(3),
                ticket_id: new_ticket_id,
                ticket: bilibili_core::create_access_key_ticket()
                    .expect("access-key ticket should be created"),
                created_at: now,
                in_progress: false,
            },
        )]);

        assert!(!clear_claimed_bilibili_access_key_login(
            &mut logins,
            chat_id,
            auth_generation,
            old_ticket_id,
        ));
        assert!(!release_claimed_bilibili_access_key_login(
            &mut logins,
            chat_id,
            auth_generation,
            old_ticket_id,
            auth_generation,
            Some(test_auth_epoch(3)),
            now,
        ));
        assert_eq!(logins[&chat_id].ticket_id, new_ticket_id);
    }

    #[test]
    fn failed_access_key_callback_discards_a_stale_epoch_ticket() {
        let chat_id = 123;
        let auth_generation = 42;
        let now = Instant::now();
        let mut logins = HashMap::from([(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation,
                auth_epoch: test_auth_epoch(3),
                ticket_id: 7,
                ticket: bilibili_core::create_access_key_ticket()
                    .expect("access-key ticket should be created"),
                created_at: now,
                in_progress: true,
            },
        )]);

        assert!(!release_claimed_bilibili_access_key_login(
            &mut logins,
            chat_id,
            auth_generation,
            7,
            auth_generation,
            Some(test_auth_epoch(4)),
            now,
        ));
        assert!(!logins.contains_key(&chat_id));
    }

    #[test]
    fn pruning_keeps_in_progress_access_key_ticket_until_completion() {
        let chat_id = 123;
        let created_at = Instant::now();
        let mut logins = HashMap::from([(
            chat_id,
            PendingBilibiliAccessKeyLogin {
                auth_generation: 42,
                auth_epoch: test_auth_epoch(3),
                ticket_id: 7,
                ticket: bilibili_core::create_access_key_ticket()
                    .expect("access-key ticket should be created"),
                created_at,
                in_progress: true,
            },
        )]);

        prune_expired_pending_bilibili_access_key_logins(
            &mut logins,
            created_at + BILIBILI_ACCESS_KEY_LOGIN_TTL + Duration::from_secs(1),
        );

        assert!(logins.contains_key(&chat_id));
    }

    #[tokio::test]
    async fn fresh_login_preserves_malformed_legacy_auth_state() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let root = temp_main_test_dir("fresh-login-malformed-legacy");
        fs::create_dir_all(&root).expect("temp dir should create");
        let mut config = AppConfig::for_test();
        config.bilibili.auth.state_path = root.join("bilibili-auth.json");
        config.bilibili.auth.credential_file = root.join("bbdown-credentials.json");
        fs::write(&config.bilibili.auth.state_path, "{not-json")
            .expect("malformed legacy state should write");
        #[cfg(unix)]
        fs::set_permissions(
            &config.bilibili.auth.state_path,
            fs::Permissions::from_mode(0o600),
        )
        .expect("malformed legacy state permissions should update");
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let auth_epoch = current_bbdown_auth_epoch(&config)
            .await
            .expect("auth epoch should load");

        let error = save_bbdown_login_credentials(
            &config,
            generation,
            auth_epoch,
            bbdown_core::Credentials::default().with_access_key("fresh-access-key"),
        )
        .await
        .expect_err("malformed legacy state must stop fresh credential save");

        assert!(format!("{error:#}").contains("failed to parse Bilibili auth state"));
        assert!(config.bilibili.auth.state_path.is_file());
        assert!(!config.bilibili.auth.credential_file.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn fresh_login_preserves_unreadable_legacy_auth_state() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let root = temp_main_test_dir("fresh-login-cleanup-failure");
        fs::create_dir_all(&root).expect("temp dir should create");
        let mut config = AppConfig::for_test();
        config.bilibili.auth.state_path = root.join("legacy-auth-directory");
        config.bilibili.auth.credential_file = root.join("bbdown-credentials.json");
        fs::create_dir(&config.bilibili.auth.state_path)
            .expect("legacy state directory should create");
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let auth_epoch = current_bbdown_auth_epoch(&config)
            .await
            .expect("auth epoch should load");

        let error = save_bbdown_login_credentials(
            &config,
            generation,
            auth_epoch,
            bbdown_core::Credentials::default().with_access_key("fresh-access-key"),
        )
        .await
        .expect_err("unreadable legacy state must stop fresh credential save");

        assert!(format!("{error:#}").contains("failed to read Bilibili auth state"));
        assert!(config.bilibili.auth.state_path.is_dir());
        assert!(!config.bilibili.auth.credential_file.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn fresh_login_preserves_legacy_state_when_migration_fails() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let root = temp_main_test_dir("fresh-login-migration-failure");
        fs::create_dir_all(&root).expect("temp dir should create");
        let mut config = AppConfig::for_test();
        config.bilibili.auth.state_path = root.join("bilibili-auth.json");
        config.bilibili.auth.credential_file = root.join("bbdown-credentials.json");
        bilibili_auth::save_auth_state(
            &config.bilibili.auth.state_path,
            &bilibili_auth::AuthState {
                cookie: "SESSDATA=legacy".to_string(),
                mid: 1,
                uname: "legacy".to_string(),
                stored_at_unix: 1,
            },
        )
        .expect("legacy state should save");
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let auth_epoch = current_bbdown_auth_epoch(&config)
            .await
            .expect("auth epoch should load");

        let error = save_bbdown_login_credentials_with_sync(
            &config,
            generation,
            auth_epoch,
            bbdown_core::Credentials::default().with_access_key("fresh-access-key"),
            |_| Err(anyhow::anyhow!("synthetic migration failure")),
        )
        .await
        .expect_err("failed migration must stop the fresh credential save");

        assert!(error.to_string().contains("synthetic migration failure"));
        assert!(config.bilibili.auth.state_path.is_file());
        assert!(!config.bilibili.auth.credential_file.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fresh_login_runs_the_file_lock_transaction_off_the_runtime_thread() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let root = temp_main_test_dir("fresh-login-blocking-transaction");
        fs::create_dir_all(&root).expect("temp dir should create");
        let mut config = AppConfig::for_test();
        config.bilibili.auth.state_path = root.join("bilibili-auth.json");
        config.bilibili.auth.credential_file = root.join("bbdown-credentials.json");
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
        let auth_epoch = current_bbdown_auth_epoch(&config)
            .await
            .expect("auth epoch should load");
        let runtime_thread = std::thread::current().id();
        let (thread_tx, thread_rx) = std::sync::mpsc::sync_channel(1);

        save_bbdown_login_credentials_with_sync(
            &config,
            generation,
            auth_epoch,
            bbdown_core::Credentials::default().with_access_key("fresh-access-key"),
            move |_| {
                thread_tx
                    .send(std::thread::current().id())
                    .expect("test should receive the transaction thread");
                Ok(())
            },
        )
        .await
        .expect("fresh credentials should save");

        assert_ne!(
            thread_rx
                .recv()
                .expect("transaction thread should be recorded"),
            runtime_thread
        );
        BILIBILI_CREDENTIAL_REVISION.store(revision, Ordering::SeqCst);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn fresh_login_cannot_recreate_credentials_after_an_external_logout_epoch() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let root = temp_main_test_dir("fresh-login-stale-auth-epoch");
        fs::create_dir_all(&root).expect("temp dir should create");
        let mut config = AppConfig::for_test();
        config.bilibili.auth.state_path = root.join("bilibili-auth.json");
        config.bilibili.auth.credential_file = root.join("bbdown-credentials.json");
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let login_epoch = current_bbdown_auth_epoch(&config)
            .await
            .expect("login should capture the auth epoch");
        bilibili_auth::with_auth_mutation_transaction(
            &config.bilibili.auth.state_path,
            &config.bilibili.auth.credential_file,
            |_| Ok(()),
        )
        .expect("external logout epoch should commit");

        let error = save_bbdown_login_credentials(
            &config,
            generation,
            login_epoch,
            bbdown_core::Credentials::default().with_access_key("stale-access-key"),
        )
        .await
        .expect_err("stale login must not recreate credentials");

        assert!(error.to_string().contains("credential state changed"));
        assert!(!config.bilibili.auth.credential_file.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn default_bot_commands_match_supported_commands() {
        let commands = default_bot_commands();

        assert_eq!(
            commands
                .iter()
                .map(|command| command.command.as_str())
                .collect::<Vec<_>>(),
            vec!["help", "pdf", "bbdown"]
        );
    }

    #[test]
    fn job_status_message_includes_progress_when_present() {
        assert_eq!(
            job_status_message(7, "Bilibili download", "Started", None),
            "Started job #7: Bilibili download"
        );
        assert_eq!(
            job_status_message(7, "Bilibili download", "Running", Some("BBDown: 42%")),
            "Running job #7: Bilibili download\nBBDown: 42%"
        );
    }

    #[test]
    fn failed_job_message_preserves_recovery_details_from_the_error_chain() {
        let message = failed_job_message(
            9,
            "Bilibili download",
            "failed to move staged files: retained backup /tmp/library/.transaction/Episode.mkv",
        );

        assert!(message.contains("failed to move staged files"));
        assert!(message.contains("retained backup"));
        assert!(message.contains("/tmp/library/.transaction/Episode.mkv"));
    }

    #[test]
    fn failed_job_message_redacts_worker_credentials() {
        let message = failed_job_message(
            10,
            "Bilibili download",
            "worker preflight failed: access_key=APP_ACCESS credentials={\"access_token\":\"TV_ACCESS\",\"refresh_token\":\"REFRESH\"}",
        );

        for secret in ["APP_ACCESS", "TV_ACCESS", "REFRESH"] {
            assert!(!message.contains(secret));
        }
        assert!(message.contains("<redacted BBDown access-key callback message>"));
    }

    #[test]
    fn progress_delivery_falls_back_to_send_after_edit_failure() {
        assert_eq!(
            ProgressDelivery::from_message_id(Some(42)),
            ProgressDelivery::Edit(42)
        );
        assert_eq!(
            ProgressDelivery::Edit(42).after_edit_result(true),
            ProgressDelivery::Edit(42)
        );
        assert_eq!(
            ProgressDelivery::Edit(42).after_edit_result(false),
            ProgressDelivery::Send
        );
        assert_eq!(
            progress_fallback_message(7, "BBDown: 42%"),
            "Progress job #7: BBDown: 42%"
        );
    }

    #[test]
    fn parses_duplicate_callback_data() {
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:overwrite"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Run(VideoDuplicateAction::Overwrite)
            })
        );
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:keep"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Run(VideoDuplicateAction::KeepBoth)
            })
        );
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:cancel"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Cancel
            })
        );
        assert_eq!(parse_duplicate_callback_data("dup:nothex:keep"), None);
        assert_eq!(parse_duplicate_callback_data("other:42:keep"), None);
        assert_eq!(parse_duplicate_callback_data("dup:42:unknown"), None);
    }

    #[test]
    fn parses_bilibili_selection_callback_data() {
        assert_eq!(
            parse_bilibili_selection_callback_data("bsel:000000000000002a:latest"),
            Some(BilibiliSelectionCallback {
                token: 42,
                action: BilibiliSelectionCallbackAction::Run(BilibiliSelection::Latest)
            })
        );
        assert_eq!(
            parse_bilibili_selection_callback_data("bsel:000000000000002a:all"),
            Some(BilibiliSelectionCallback {
                token: 42,
                action: BilibiliSelectionCallbackAction::Run(BilibiliSelection::All)
            })
        );
        assert_eq!(
            parse_bilibili_selection_callback_data("bsel:000000000000002a:cancel"),
            Some(BilibiliSelectionCallback {
                token: 42,
                action: BilibiliSelectionCallbackAction::Cancel
            })
        );
        assert_eq!(
            parse_bilibili_selection_callback_data("bsel:nothex:latest"),
            None
        );
        assert_eq!(
            parse_bilibili_selection_callback_data("dup:000000000000002a:latest"),
            None
        );
        assert_eq!(
            parse_bilibili_selection_callback_data("bsel:000000000000002a:unknown"),
            None
        );
    }

    #[test]
    fn builds_bilibili_selection_keyboard_and_applies_selection() {
        let keyboard = bilibili_selection_keyboard(42);
        let data = keyboard
            .inline_keyboard
            .iter()
            .flatten()
            .map(|button| button.callback_data.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            data,
            vec![
                "bsel:000000000000002a:latest",
                "bsel:000000000000002a:all",
                "bsel:000000000000002a:cancel"
            ]
        );

        assert_eq!(
            apply_bilibili_selection(
                JobRequest::Bilibili {
                    url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                    selection: None,
                },
                BilibiliSelection::All
            ),
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: Some(BilibiliSelection::All),
            }
        );
    }

    #[test]
    fn builds_duplicate_choice_keyboard() {
        let keyboard = duplicate_choice_keyboard(42, true);
        let data = keyboard
            .inline_keyboard
            .iter()
            .flatten()
            .map(|button| button.callback_data.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            data,
            vec![
                "dup:000000000000002a:overwrite",
                "dup:000000000000002a:keep",
                "dup:000000000000002a:cancel"
            ]
        );
        assert!(data.iter().all(|value| value.len() <= 64));
    }

    #[test]
    fn duplicate_choice_keyboard_can_disable_overwrite() {
        let root = temp_main_test_dir("duplicate-choice-overwrite");
        fs::create_dir_all(&root).expect("test directory should create");
        let existing = root.join("episode.mp4");
        fs::write(&existing, "video").expect("test video should write");
        fs::write(
            existing.with_extension("nfo"),
            "<movie><uniqueid type=\"bilibili-cid\">cid456</uniqueid></movie>",
        )
        .expect("test NFO should write");
        let keyboard = duplicate_choice_keyboard(42, false);
        let data = keyboard
            .inline_keyboard
            .iter()
            .flatten()
            .map(|button| button.callback_data.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            data,
            vec!["dup:000000000000002a:keep", "dup:000000000000002a:cancel"]
        );
        let exact_duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: crate::downloader::VideoIdentity {
                provider: crate::downloader::VideoProvider::Bilibili,
                id: "cid456".to_string(),
            },
            existing_videos: vec![existing.clone()],
        };
        assert!(!job_allows_duplicate_overwrite(
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: Some(BilibiliSelection::All)
            },
            &exact_duplicate,
        ));
        assert!(!job_allows_duplicate_overwrite(
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: Some(BilibiliSelection::Latest)
            },
            &exact_duplicate,
        ));
        let broad_duplicate = VideoDuplicate {
            overwrite_confirmation: None,
            identity: crate::downloader::VideoIdentity {
                provider: crate::downloader::VideoProvider::Bilibili,
                id: "BV123".to_string(),
            },
            existing_videos: vec![existing],
        };
        assert!(!job_allows_duplicate_overwrite(
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string(),
                selection: None,
            },
            &broad_duplicate,
        ));
        let _ = fs::remove_dir_all(root);
    }

    fn pending_duplicate_job(job_id: u64, created_at: Instant) -> PendingDuplicateJob {
        PendingDuplicateJob {
            chat_id: 1,
            job_id,
            job: JobRequest::Youtube {
                url: format!("https://youtu.be/{job_id}"),
            },
            duplicate: VideoDuplicate {
                overwrite_confirmation: None,
                identity: crate::downloader::VideoIdentity {
                    provider: crate::downloader::VideoProvider::Youtube,
                    id: job_id.to_string(),
                },
                existing_videos: Vec::new(),
            },
            created_at,
        }
    }

    #[test]
    fn pending_duplicate_jobs_expire_and_cap() {
        let now = Instant::now();
        let expired = now
            .checked_sub(DUPLICATE_DECISION_TTL + Duration::from_secs(1))
            .expect("test instant should support subtraction");
        let mut jobs = HashMap::from([
            (1, pending_duplicate_job(1, expired)),
            (2, pending_duplicate_job(2, now)),
        ]);

        prune_expired_pending_duplicate_jobs(&mut jobs, now);
        assert!(!jobs.contains_key(&1));
        assert!(jobs.contains_key(&2));

        for index in 3..=(MAX_PENDING_DUPLICATE_JOBS as u64 + 3) {
            jobs.insert(index, pending_duplicate_job(index, now));
        }
        cap_pending_duplicate_jobs(&mut jobs, None);
        assert!(jobs.len() <= pending_duplicate_job_limit());
    }

    #[test]
    fn pending_duplicate_jobs_cap_preserves_protected_token() {
        let now = Instant::now();
        let protected_token = 1;
        let mut jobs = HashMap::new();
        for token in 1..=(MAX_PENDING_DUPLICATE_JOBS as u64 + 1) {
            jobs.insert(token, pending_duplicate_job(token, now));
        }

        cap_pending_duplicate_jobs(&mut jobs, Some(protected_token));

        assert_eq!(jobs.len(), pending_duplicate_job_limit());
        assert!(jobs.contains_key(&protected_token));
    }

    #[test]
    fn detects_deferred_bilibili_selection_required_errors() {
        let error = anyhow::Error::from(bbdown_core::Error::SelectionRequired {
            input_kind: "season",
        })
        .context("failed to probe Bilibili plan");
        let job = JobRequest::Bilibili {
            url: "https://b23.tv/season-short-link".to_string(),
            selection: None,
        };

        assert!(is_bilibili_selection_required_error(&error));
        assert!(should_prompt_bilibili_selection_after_probe_error(
            &job, &error
        ));
        assert!(!should_prompt_bilibili_selection_after_probe_error(
            &JobRequest::Bilibili {
                url: "https://b23.tv/season-short-link".to_string(),
                selection: Some(BilibiliSelection::Latest),
            },
            &error
        ));
        assert!(!should_prompt_bilibili_selection_after_probe_error(
            &JobRequest::Youtube {
                url: "https://youtu.be/PHH1wTDF-1M".to_string(),
            },
            &error
        ));
        assert!(!is_bilibili_selection_required_error(&anyhow!(
            "ordinary probe failure"
        )));
    }

    #[tokio::test]
    async fn login_cancel_check_handles_future_notify_waiters() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        let result = await_bbdown_login_active(generation, async { "completed" }).await;
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        let err = result.expect_err("stale generation should cancel immediately");
        assert!(err.to_string().contains("canceled"));
    }

    #[tokio::test]
    async fn bbdown_status_discards_a_result_after_logout_generation_changes() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
        assert_eq!(
            current_bbdown_status_message(
                generation,
                revision,
                test_auth_epoch(7),
                test_auth_epoch(7),
                "Credential valid".to_string(),
            ),
            "Credential valid"
        );

        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        let message = current_bbdown_status_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "Credential valid".to_string(),
        );
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        assert!(message.contains("state changed"));
        assert!(!message.contains("Credential valid"));
    }

    #[tokio::test]
    async fn bbdown_status_rejects_a_generation_captured_during_logout() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
        assert!(generation.is_multiple_of(2));

        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        let during_logout = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let in_progress = current_bbdown_status_message(
            during_logout,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "Credential valid".to_string(),
        );

        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        let after_logout = current_bbdown_status_message(
            during_logout,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "Credential valid".to_string(),
        );
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        assert!(in_progress.contains("state changed"));
        assert!(after_logout.contains("state changed"));
        assert!(!in_progress.contains("Credential valid"));
        assert!(!after_logout.contains("Credential valid"));
    }

    #[tokio::test]
    async fn bbdown_status_discards_a_result_after_successful_login() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);

        BILIBILI_CREDENTIAL_REVISION.fetch_add(1, Ordering::SeqCst);
        let message = current_bbdown_status_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "Credential invalid".to_string(),
        );
        BILIBILI_CREDENTIAL_REVISION.store(revision, Ordering::SeqCst);

        assert!(message.contains("state changed"));
        assert!(!message.contains("Credential invalid"));
    }

    #[tokio::test]
    async fn bbdown_status_discards_a_result_after_cross_process_epoch_changes() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);

        let message = current_bbdown_status_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(8),
            "Credential valid".to_string(),
        );

        assert!(message.contains("state changed"));
        assert!(!message.contains("Credential valid"));
    }

    #[tokio::test]
    async fn bbdown_login_success_discards_a_result_after_logout() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
        assert!(generation.is_multiple_of(2));
        assert_eq!(
            current_bbdown_login_success_message(
                generation,
                revision,
                test_auth_epoch(7),
                test_auth_epoch(7),
                "BBDown login saved.".to_string(),
            ),
            "BBDown login saved."
        );

        BILIBILI_AUTH_GENERATION.fetch_add(2, Ordering::SeqCst);
        BILIBILI_CREDENTIAL_REVISION.fetch_add(1, Ordering::SeqCst);
        let message = current_bbdown_login_success_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "BBDown login saved.".to_string(),
        );
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);
        BILIBILI_CREDENTIAL_REVISION.store(revision, Ordering::SeqCst);

        assert!(message.contains("state changed"));
        assert!(!message.contains("login saved"));
    }

    #[tokio::test]
    async fn bbdown_login_success_discards_a_result_after_a_newer_login() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);

        BILIBILI_CREDENTIAL_REVISION.fetch_add(1, Ordering::SeqCst);
        let message = current_bbdown_login_success_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(7),
            "BBDown login saved.".to_string(),
        );
        BILIBILI_CREDENTIAL_REVISION.store(revision, Ordering::SeqCst);

        assert!(message.contains("state changed"));
        assert!(!message.contains("login saved"));
    }

    #[tokio::test]
    async fn bbdown_login_success_discards_a_cross_process_epoch() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);

        let message = current_bbdown_login_success_message(
            generation,
            revision,
            test_auth_epoch(7),
            test_auth_epoch(8),
            "BBDown login saved.".to_string(),
        );

        assert!(message.contains("state changed"));
        assert!(!message.contains("login saved"));
    }

    #[test]
    fn bbdown_logout_discards_a_cross_process_epoch() {
        let message = current_bbdown_logout_message(
            test_auth_epoch(7),
            test_auth_epoch(8),
            "BBDown credential state cleared.".to_string(),
        );

        assert!(message.contains("state changed"));
        assert!(!message.contains("state cleared"));
    }

    #[tokio::test]
    async fn login_cancel_cannot_be_lost_after_generation_check() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let result = tokio_timeout(
            Duration::from_secs(1),
            await_bbdown_login_active_with_hook(generation, std::future::pending::<()>(), || {
                BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
                bbdown_login_cancel_notify().notify_waiters();
            }),
        )
        .await
        .expect("registered logout notification should wake the waiter");
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        let error = result.expect_err("logout should cancel after the generation check");
        assert!(error.to_string().contains("canceled"));
    }

    #[tokio::test]
    async fn login_cancel_interrupts_a_pending_request() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            await_bbdown_login_active(generation, async move {
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            })
            .await
        });
        started_rx.await.expect("request should start");

        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        bbdown_login_cancel_notify().notify_waiters();
        let result = tokio_timeout(Duration::from_secs(1), task)
            .await
            .expect("pending request should cancel promptly")
            .expect("request task should join");
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        let error = result.expect_err("pending request should be canceled");
        assert!(error.to_string().contains("canceled"));
    }

    #[tokio::test]
    async fn login_cancel_interrupts_a_poll_sleep() {
        let _guard = TEST_AUTH_GENERATION_LOCK.lock().await;
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            await_bbdown_login_active(generation, async move {
                let _ = started_tx.send(());
                sleep(Duration::from_secs(60)).await;
            })
            .await
        });
        started_rx.await.expect("poll sleep should start");

        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        bbdown_login_cancel_notify().notify_waiters();
        let result = tokio_timeout(Duration::from_secs(1), task)
            .await
            .expect("poll sleep should cancel promptly")
            .expect("sleep task should join");
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        let error = result.expect_err("poll sleep should be canceled");
        assert!(error.to_string().contains("canceled"));
    }

    fn temp_main_test_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("telegram-video-downloader-{label}-{unique}"))
    }
}
