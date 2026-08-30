#![allow(dead_code)]

use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use bbdown_core::{CredentialProfileSelection, CredentialStore};
use image::{DynamicImage, ImageFormat, Luma};
use qrcode::QrCode;
use reqwest::Client;
use reqwest::header::{COOKIE, HeaderMap, SET_COOKIE, USER_AGENT};
use rustix::fs::{CWD, FlockOperation, RenameFlags};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::safe_fs::{BoundEntry, BoundFile, EntryIdentity, RootedFs};

const USER_AGENT_VALUE: &str = "Mozilla/5.0";
const QRCODE_GENERATE_URL: &str =
    "https://passport.bilibili.com/x/passport-login/web/qrcode/generate";
const QRCODE_POLL_URL: &str = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll";
const NAV_URL: &str = "https://api.bilibili.com/x/web-interface/nav";
static AUTH_FILE_LOCK: Mutex<()> = Mutex::new(());
static ACTIVE_BBDOWN_CONFIG_FILES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const AUTH_MUTATION_LOCK_SUFFIX: &str = ".telegram-video-downloader.auth.lock";
const AUTH_MUTATION_LOCK_ANCHOR_SUFFIX: &str = ".anchor";
const AUTH_EPOCH_LOG_LIMIT: u64 = 64 * 1024;
const AUTH_EPOCH_SLOT_SIZE: usize = 64;
const AUTH_EPOCH_SLOT_COUNT: usize = 2;
const AUTH_EPOCH_SLOT_MAGIC: &[u8; 8] = b"TVDAUTH1";
const AUTH_LOCK_HEADER: &[u8] = b"telegram-video-downloader-auth-lock-v1\n";
const LOGIN_URL_COOKIE_NAMES: &[&str] = &[
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthState {
    pub cookie: String,
    pub mid: u64,
    pub uname: String,
    pub stored_at_unix: u64,
}

impl fmt::Debug for AuthState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthState")
            .field("cookie", &"<redacted>")
            .field("mid", &self.mid)
            .field("uname", &self.uname)
            .field("stored_at_unix", &self.stored_at_unix)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginQr {
    pub url: String,
    pub qrcode_key: String,
    pub png: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BbdownAuthTicket {
    pub kind: String,
    pub url: String,
    pub qr_payload: String,
    pub message_origin: Option<String>,
    pub callback_origin: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BbdownCredentialSummary {
    pub has_cookie: bool,
    pub has_access_key: bool,
    #[serde(default)]
    pub has_tv_access_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BbdownAuthEvent {
    Ticket(BbdownAuthTicket),
    Saved {
        kind: String,
        saved: BbdownCredentialSummary,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BbdownCredentialHealthReport {
    pub credentials: BbdownCredentialSummary,
    #[serde(default)]
    pub probes: Vec<BbdownCredentialHealthProbe>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BbdownCredentialHealthProbe {
    pub kind: String,
    pub scope: String,
    pub status: String,
    pub endpoint: Option<String>,
    pub api_code: Option<i64>,
    pub message: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum LoginPoll {
    Waiting,
    Scanned,
    Expired,
    Success { cookie: String },
}

#[derive(Debug)]
struct AuthMutationFileLock {
    file: File,
    path: PathBuf,
    anchor_path: PathBuf,
}

#[derive(Debug)]
pub struct AuthReplyFileLock {
    lock: AuthMutationFileLock,
}

impl AuthReplyFileLock {
    pub fn current_epoch(&self) -> Result<u64> {
        self.lock.current_epoch()
    }
}

pub struct LockedAuthMutation<'a> {
    state_path: &'a Path,
    credential_file: &'a Path,
    _lock: &'a AuthMutationFileLock,
    epoch: u64,
}

pub type AuthCleanupResult = (Result<bool>, Result<()>);

impl LockedAuthMutation<'_> {
    pub fn sync_legacy_cookie(&self, credential_profile: Option<&str>) -> Result<bool> {
        sync_bbdown_rust_credentials_from_state_unlocked(
            self.state_path,
            self.credential_file,
            credential_profile,
        )
    }

    pub fn delete_legacy_state(&self) -> Result<bool> {
        delete_auth_state_unlocked(self.state_path, self.credential_file)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl fmt::Debug for LoginPoll {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Waiting => formatter.write_str("Waiting"),
            Self::Scanned => formatter.write_str("Scanned"),
            Self::Expired => formatter.write_str("Expired"),
            Self::Success { .. } => formatter
                .debug_struct("Success")
                .field("cookie", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BilibiliApiResponse<T> {
    code: i64,
    message: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct QrGenerateData {
    url: String,
    qrcode_key: String,
}

#[derive(Debug, Deserialize)]
struct QrPollData {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NavData {
    #[serde(rename = "isLogin")]
    is_login: bool,
    mid: Option<u64>,
    uname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BbdownRawAuthEvent {
    event: String,
    kind: Option<String>,
    url: Option<String>,
    qr_payload: Option<String>,
    message_origin: Option<String>,
    callback_origin: Option<String>,
    saved: Option<BbdownCredentialSummary>,
}

pub fn parse_bbdown_auth_event_line(line: &str) -> Result<BbdownAuthEvent> {
    let raw: BbdownRawAuthEvent =
        serde_json::from_str(line).context("failed to parse BBDown-rust auth JSON event")?;
    match raw.event.as_str() {
        "ticket" => Ok(BbdownAuthEvent::Ticket(BbdownAuthTicket {
            kind: raw.kind.unwrap_or_else(|| "unknown".to_string()),
            url: raw
                .url
                .filter(|url| !url.trim().is_empty())
                .ok_or_else(|| anyhow!("BBDown-rust auth ticket did not include url"))?,
            qr_payload: raw
                .qr_payload
                .filter(|payload| !payload.trim().is_empty())
                .ok_or_else(|| anyhow!("BBDown-rust auth ticket did not include qr_payload"))?,
            message_origin: raw.message_origin,
            callback_origin: raw.callback_origin,
        })),
        "saved" => Ok(BbdownAuthEvent::Saved {
            kind: raw.kind.unwrap_or_else(|| "unknown".to_string()),
            saved: raw
                .saved
                .ok_or_else(|| anyhow!("BBDown-rust auth saved event did not include summary"))?,
        }),
        event => bail!("unsupported BBDown-rust auth event: {event}"),
    }
}

pub fn parse_bbdown_auth_events(stdout: &str) -> Result<Vec<BbdownAuthEvent>> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_bbdown_auth_event_line)
        .collect()
}

pub fn first_bbdown_auth_ticket(stdout: &str) -> Result<BbdownAuthTicket> {
    parse_bbdown_auth_events(stdout)?
        .into_iter()
        .find_map(|event| match event {
            BbdownAuthEvent::Ticket(ticket) => Some(ticket),
            BbdownAuthEvent::Saved { .. } => None,
        })
        .ok_or_else(|| anyhow!("BBDown-rust auth output did not include a ticket event"))
}

pub fn bbdown_auth_saved_summary(stdout: &str) -> Result<BbdownCredentialSummary> {
    parse_bbdown_auth_events(stdout)?
        .into_iter()
        .find_map(|event| match event {
            BbdownAuthEvent::Saved { saved, .. } => Some(saved),
            BbdownAuthEvent::Ticket(_) => None,
        })
        .ok_or_else(|| anyhow!("BBDown-rust auth output did not include a saved event"))
}

pub fn parse_bbdown_credential_health_report(stdout: &str) -> Result<BbdownCredentialHealthReport> {
    serde_json::from_str(stdout.trim())
        .context("failed to parse BBDown-rust credential health JSON")
}

pub async fn generate_login_qr(client: &Client) -> Result<LoginQr> {
    let response = client
        .get(QRCODE_GENERATE_URL)
        .header(USER_AGENT, USER_AGENT_VALUE)
        .send()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to request Bilibili login QR")?
        .error_for_status()
        .map_err(strip_reqwest_url)
        .context("Bilibili login QR request returned HTTP error")?
        .json::<BilibiliApiResponse<QrGenerateData>>()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to decode Bilibili login QR response")?;

    if response.code != 0 {
        bail!(
            "Bilibili login QR request failed: {} ({})",
            response.message,
            response.code
        );
    }

    let png = render_qr_png(&response.data.url)?;
    Ok(LoginQr {
        url: response.data.url,
        qrcode_key: response.data.qrcode_key,
        png,
    })
}

pub async fn poll_login(client: &Client, qrcode_key: &str) -> Result<LoginPoll> {
    let response = client
        .get(QRCODE_POLL_URL)
        .query(&[("qrcode_key", qrcode_key)])
        .header(USER_AGENT, USER_AGENT_VALUE)
        .send()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to poll Bilibili login QR")?
        .error_for_status()
        .map_err(strip_reqwest_url)
        .context("Bilibili login poll returned HTTP error")?;

    let cookie = extract_cookie_header(response.headers());
    let body = response
        .json::<BilibiliApiResponse<QrPollData>>()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to decode Bilibili login poll response")?;

    login_poll_from_response(body, cookie)
}

pub async fn verify_cookie(client: &Client, cookie: &str) -> Result<AuthState> {
    let response = client
        .get(NAV_URL)
        .header(USER_AGENT, USER_AGENT_VALUE)
        .header(COOKIE, cookie)
        .send()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to verify Bilibili login")?
        .error_for_status()
        .map_err(strip_reqwest_url)
        .context("Bilibili login verification returned HTTP error")?
        .json::<BilibiliApiResponse<NavData>>()
        .await
        .map_err(strip_reqwest_url)
        .context("failed to decode Bilibili login verification response")?;

    auth_state_from_nav_response(response, cookie)
}

fn login_poll_from_response(
    response: BilibiliApiResponse<QrPollData>,
    cookie: Option<String>,
) -> Result<LoginPoll> {
    if response.code != 0 {
        bail!(
            "Bilibili login poll failed: {} ({})",
            response.message,
            response.code
        );
    }

    match response.data.code {
        0 => {
            let cookie = merge_login_cookie_sources(
                cookie.as_deref(),
                response.data.url.as_deref().and_then(cookie_from_login_url),
            );
            let Some(cookie) = cookie else {
                bail!("Bilibili login succeeded without returning cookies");
            };
            Ok(LoginPoll::Success { cookie })
        }
        86_101 => Ok(LoginPoll::Waiting),
        86_090 => Ok(LoginPoll::Scanned),
        86_038 => Ok(LoginPoll::Expired),
        code => bail!(
            "Bilibili login poll returned unexpected status: {} ({})",
            response.data.message,
            code
        ),
    }
}

fn strip_reqwest_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}

fn merge_login_cookie_sources(
    header_cookie: Option<&str>,
    url_cookie: Option<String>,
) -> Option<String> {
    let mut pairs = Vec::new();
    append_cookie_header_pairs(header_cookie, &mut pairs);
    append_cookie_header_pairs(url_cookie.as_deref(), &mut pairs);
    if !cookie_pairs_include_login_keys(&pairs) {
        return None;
    }
    Some(pairs.join("; "))
}

fn append_cookie_header_pairs(cookie: Option<&str>, pairs: &mut Vec<String>) {
    let Some(cookie) = cookie else {
        return;
    };
    for pair in cookie
        .split(';')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
    {
        let Some((name, _)) = pair.split_once('=') else {
            continue;
        };
        if !LOGIN_URL_COOKIE_NAMES.contains(&name) {
            continue;
        }
        if let Some(existing) = pairs.iter().position(|value| {
            value
                .split_once('=')
                .is_some_and(|(existing_name, _)| existing_name == name)
        }) {
            pairs[existing] = pair.to_string();
        } else {
            pairs.push(pair.to_string());
        }
    }
}

fn cookie_pairs_include_login_keys(pairs: &[String]) -> bool {
    pairs.iter().any(|pair| pair.starts_with("SESSDATA="))
        && pairs.iter().any(|pair| pair.starts_with("bili_jct="))
}

fn auth_state_from_nav_response(
    response: BilibiliApiResponse<NavData>,
    cookie: &str,
) -> Result<AuthState> {
    if response.code != 0 || !response.data.is_login {
        bail!(
            "Bilibili account is not logged in: {} ({})",
            response.message,
            response.code
        );
    }

    Ok(AuthState {
        cookie: cookie.to_string(),
        mid: response
            .data
            .mid
            .ok_or_else(|| anyhow!("Bilibili verification response did not include mid"))?,
        uname: response
            .data
            .uname
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| anyhow!("Bilibili verification response did not include uname"))?,
        stored_at_unix: now_unix_seconds(),
    })
}

pub fn render_qr_png(text: &str) -> Result<Vec<u8>> {
    let code = QrCode::new(text.as_bytes()).context("failed to encode QR data")?;
    let image = code
        .render::<Luma<u8>>()
        .quiet_zone(true)
        .module_dimensions(8, 8)
        .build();

    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageLuma8(image)
        .write_to(&mut output, ImageFormat::Png)
        .context("failed to encode QR PNG")?;
    Ok(output.into_inner())
}

pub fn extract_cookie_header(headers: &HeaderMap) -> Option<String> {
    set_cookie_values_to_cookie(
        headers
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok()),
    )
}

pub fn set_cookie_values_to_cookie<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let pairs = values
        .into_iter()
        .filter_map(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| {
            value
                .split_once('=')
                .is_some_and(|(name, _)| LOGIN_URL_COOKIE_NAMES.contains(&name))
        })
        .map(str::to_string)
        .collect::<Vec<_>>();

    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

pub fn cookie_from_login_url(value: &str) -> Option<String> {
    let parsed = Url::parse(value).ok()?;
    let mut pairs = Vec::new();
    append_login_cookie_pairs(parsed.query(), &mut pairs);

    if let Some(fragment) = parsed.fragment() {
        let fragment_query = fragment.split_once('?').map(|(_, query)| query);
        append_login_cookie_pairs(fragment_query, &mut pairs);
    }

    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

fn append_login_cookie_pairs(query: Option<&str>, pairs: &mut Vec<String>) {
    let Some(query) = query else {
        return;
    };

    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if LOGIN_URL_COOKIE_NAMES.contains(&name.as_ref()) && !value.trim().is_empty() {
            pairs.push(format!("{name}={}", encode_cookie_value_for_bbdown(&value)));
        }
    }
}

fn encode_cookie_value_for_bbdown(value: &str) -> String {
    value.replace(',', "%2C")
}

impl AuthMutationFileLock {
    fn current_epoch(&self) -> Result<u64> {
        validate_auth_mutation_lock_identity(&self.file, &self.path, &self.anchor_path)?;
        let epoch = read_auth_epoch(&self.file, &self.path)?;
        validate_auth_mutation_lock_identity(&self.file, &self.path, &self.anchor_path)?;
        Ok(epoch)
    }

    fn bump_epoch(&self) -> Result<u64> {
        validate_auth_mutation_lock_identity(&self.file, &self.path, &self.anchor_path)?;
        let epoch = read_auth_epoch(&self.file, &self.path)?;
        let next = epoch
            .checked_add(1)
            .context("BBDown auth epoch is exhausted")?;
        write_auth_epoch_slot(&self.file, next)?;
        sync_auth_lock_pair_parent(&self.path, &self.anchor_path, &self.file)?;
        validate_auth_mutation_lock_identity(&self.file, &self.path, &self.anchor_path)?;
        let persisted = read_auth_epoch(&self.file, &self.path)?;
        if persisted != next {
            bail!("BBDown auth epoch changed while persisting it")
        }
        Ok(next)
    }
}

fn read_auth_epoch(file: &File, path: &Path) -> Result<u64> {
    let slots = read_auth_epoch_slots(file)?;
    match slots.as_slice() {
        [] => read_auth_epoch_log(file, path).map(|(epoch, _, _)| epoch),
        [epoch] => Ok(*epoch),
        [first, second] => {
            if first.abs_diff(*second) > 1 {
                bail!("BBDown auth epoch slots disagree")
            }
            Ok((*first).max(*second))
        }
        _ => unreachable!("the epoch slot count is fixed"),
    }
}

fn read_auth_epoch_slots(file: &File) -> Result<Vec<u64>> {
    let file_len = file
        .metadata()
        .context("failed to inspect BBDown auth epoch slots")?
        .len();
    let mut epochs = Vec::new();
    for index in 0..AUTH_EPOCH_SLOT_COUNT {
        let offset = auth_epoch_slot_offset(index);
        if file_len <= offset {
            continue;
        }
        let mut reader = file
            .try_clone()
            .context("failed to clone BBDown auth lock for slot read")?;
        reader
            .seek(SeekFrom::Start(offset))
            .context("failed to seek BBDown auth epoch slot")?;
        let mut slot = [0_u8; AUTH_EPOCH_SLOT_SIZE];
        let mut read = 0;
        while read < slot.len() {
            let count = reader
                .read(&mut slot[read..])
                .context("failed to read BBDown auth epoch slot")?;
            if count == 0 {
                break;
            }
            read += count;
        }
        if read == slot.len()
            && let Some(epoch) = decode_auth_epoch_slot(&slot)
        {
            epochs.push(epoch);
        }
    }
    epochs.sort_unstable();
    epochs.dedup();
    Ok(epochs)
}

fn write_auth_epoch_slot(file: &File, epoch: u64) -> Result<()> {
    let index = (epoch as usize) % AUTH_EPOCH_SLOT_COUNT;
    let mut writer = file
        .try_clone()
        .context("failed to clone BBDown auth lock for epoch update")?;
    writer
        .seek(SeekFrom::Start(auth_epoch_slot_offset(index)))
        .context("failed to seek BBDown auth epoch slot")?;
    writer
        .write_all(&encode_auth_epoch_slot(epoch))
        .context("failed to write BBDown auth epoch slot")?;
    writer
        .sync_all()
        .context("failed to persist BBDown auth epoch slot")
}

fn auth_epoch_slot_offset(index: usize) -> u64 {
    AUTH_EPOCH_LOG_LIMIT + (index * AUTH_EPOCH_SLOT_SIZE) as u64
}

fn encode_auth_epoch_slot(epoch: u64) -> [u8; AUTH_EPOCH_SLOT_SIZE] {
    let mut half = [0_u8; AUTH_EPOCH_SLOT_SIZE / 2];
    half[..8].copy_from_slice(AUTH_EPOCH_SLOT_MAGIC);
    half[8..16].copy_from_slice(&epoch.to_le_bytes());
    half[16..24].copy_from_slice(&(!epoch).to_le_bytes());
    let checksum = auth_epoch_slot_checksum(&half[..24]);
    half[24..32].copy_from_slice(&checksum.to_le_bytes());
    let mut slot = [0_u8; AUTH_EPOCH_SLOT_SIZE];
    slot[..half.len()].copy_from_slice(&half);
    slot[half.len()..].copy_from_slice(&half);
    slot
}

fn decode_auth_epoch_slot(slot: &[u8; AUTH_EPOCH_SLOT_SIZE]) -> Option<u64> {
    let (first, second) = slot.split_at(AUTH_EPOCH_SLOT_SIZE / 2);
    if first != second || &first[..8] != AUTH_EPOCH_SLOT_MAGIC {
        return None;
    }
    let epoch = u64::from_le_bytes(first[8..16].try_into().ok()?);
    let inverse = u64::from_le_bytes(first[16..24].try_into().ok()?);
    let checksum = u64::from_le_bytes(first[24..32].try_into().ok()?);
    if inverse != !epoch || checksum != auth_epoch_slot_checksum(&first[..24]) {
        return None;
    }
    Some(epoch)
}

fn auth_epoch_slot_checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn read_auth_epoch_log(file: &File, path: &Path) -> Result<(u64, u64, u64)> {
    let file_len = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth epoch {}", path.display()))?
        .len();
    let legacy_len = file_len.min(AUTH_EPOCH_LOG_LIMIT);
    let mut reader = file
        .try_clone()
        .context("failed to clone BBDown auth lock for epoch read")?;
    reader
        .seek(SeekFrom::Start(0))
        .context("failed to seek BBDown auth epoch log")?;
    let mut contents = Vec::with_capacity(legacy_len as usize);
    reader
        .take(legacy_len)
        .read_to_end(&mut contents)
        .context("failed to read BBDown auth epoch log")?;
    let body_offset = if contents.starts_with(AUTH_LOCK_HEADER) {
        AUTH_LOCK_HEADER.len()
    } else {
        0
    };
    let body = &contents[body_offset..];
    let valid_len = body
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let complete =
        std::str::from_utf8(&body[..valid_len]).context("BBDown auth epoch log is not UTF-8")?;
    let mut epoch = 0_u64;
    for line in complete.lines() {
        let value = line
            .parse::<u64>()
            .context("BBDown auth epoch log contains an invalid record")?;
        let expected = epoch
            .checked_add(1)
            .context("BBDown auth epoch is exhausted")?;
        if value != expected {
            bail!("BBDown auth epoch log is not consecutive")
        }
        epoch = value;
    }
    Ok((epoch, (body_offset + valid_len) as u64, file_len))
}

fn validate_existing_auth_lock_format(file: &File, path: &Path) -> Result<()> {
    let file_len = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", path.display()))?
        .len();
    let maximum_len = AUTH_EPOCH_LOG_LIMIT + (AUTH_EPOCH_SLOT_SIZE * AUTH_EPOCH_SLOT_COUNT) as u64;
    if file_len > maximum_len {
        bail!(
            "BBDown auth lock exceeds its format limit: {}",
            path.display()
        );
    }

    let prefix_len = file_len.min(AUTH_EPOCH_LOG_LIMIT);
    let mut reader = file
        .try_clone()
        .context("failed to clone BBDown auth lock for format validation")?;
    reader
        .seek(SeekFrom::Start(0))
        .context("failed to seek BBDown auth lock for format validation")?;
    let mut prefix = Vec::with_capacity(prefix_len as usize);
    reader
        .take(prefix_len)
        .read_to_end(&mut prefix)
        .context("failed to read BBDown auth lock for format validation")?;

    if let Some(body) = prefix.strip_prefix(AUTH_LOCK_HEADER) {
        if body.iter().any(|byte| *byte != 0) {
            bail!("BBDown auth lock has invalid current-format padding");
        }
        read_auth_epoch(file, path)?;
        return Ok(());
    }

    let slots = read_auth_epoch_slots(file)?;
    let (legacy_epoch, valid_len, _) = read_auth_epoch_log(file, path)?;
    let tail = &prefix[usize::try_from(valid_len).context("invalid auth lock length")?..];
    let digit_len = tail
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(tail.len());
    if tail[..digit_len].iter().any(|byte| !byte.is_ascii_digit())
        || tail[digit_len..].iter().any(|byte| *byte != 0)
    {
        bail!("BBDown auth lock is not a recognized legacy epoch log");
    }
    if digit_len > 0 {
        let partial = std::str::from_utf8(&tail[..digit_len])
            .context("BBDown auth lock legacy tail is not UTF-8")?;
        let expected = legacy_epoch
            .checked_add(1)
            .context("BBDown auth epoch is exhausted")?
            .to_string();
        if !expected.starts_with(partial) {
            bail!("BBDown auth lock has an invalid partial legacy record");
        }
    }
    if legacy_epoch == 0 && slots.is_empty() {
        bail!("BBDown auth lock is not an initialized downloader lock");
    }
    read_auth_epoch(file, path)?;
    Ok(())
}

fn initialize_auth_lock(file: &File, path: &Path) -> Result<()> {
    let mut writer = file
        .try_clone()
        .context("failed to clone new BBDown auth lock")?;
    writer
        .seek(SeekFrom::Start(0))
        .context("failed to seek new BBDown auth lock")?;
    writer
        .write_all(AUTH_LOCK_HEADER)
        .context("failed to initialize BBDown auth lock")?;
    writer
        .sync_all()
        .context("failed to persist new BBDown auth lock")?;
    sync_auth_lock_parent(path, file)
}

#[cfg(unix)]
fn sync_auth_lock_parent(path: &Path, expected_file: &File) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock directory {}",
            parent.display()
        )
    })?;
    let directory = rustix::fs::open(
        &canonical_parent,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
    .with_context(|| {
        format!(
            "failed to open resolved BBDown auth lock directory {}",
            canonical_parent.display()
        )
    })?;
    let leaf = path
        .file_name()
        .context("BBDown auth lock path has no file name")?;
    validate_auth_lock_in_bound_parent(&directory, leaf, expected_file, path, 1)?;
    rustix::fs::fsync(&directory)
        .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
        .with_context(|| {
            format!(
                "failed to sync BBDown auth lock directory {}",
                canonical_parent.display()
            )
        })?;
    validate_auth_lock_in_bound_parent(&directory, leaf, expected_file, path, 1)
}

#[cfg(unix)]
fn sync_auth_lock_pair_parent(path: &Path, anchor_path: &Path, expected_file: &File) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let anchor_parent = anchor_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock directory {}",
            parent.display()
        )
    })?;
    let canonical_anchor_parent = fs::canonicalize(anchor_parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock anchor directory {}",
            anchor_parent.display()
        )
    })?;
    if canonical_parent != canonical_anchor_parent {
        bail!("BBDown auth lock aliases resolve to different directories");
    }
    let directory = rustix::fs::open(
        &canonical_parent,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
    .with_context(|| {
        format!(
            "failed to open resolved BBDown auth lock directory {}",
            canonical_parent.display()
        )
    })?;
    let leaf = path
        .file_name()
        .context("BBDown auth lock path has no file name")?;
    let anchor_leaf = anchor_path
        .file_name()
        .context("BBDown auth lock anchor path has no file name")?;
    validate_auth_lock_in_bound_parent(&directory, leaf, expected_file, path, 2)?;
    validate_auth_lock_in_bound_parent(&directory, anchor_leaf, expected_file, anchor_path, 2)?;
    rustix::fs::fsync(&directory)
        .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
        .with_context(|| {
            format!(
                "failed to sync BBDown auth lock directory {}",
                canonical_parent.display()
            )
        })?;
    validate_auth_lock_in_bound_parent(&directory, leaf, expected_file, path, 2)?;
    validate_auth_lock_in_bound_parent(&directory, anchor_leaf, expected_file, anchor_path, 2)
}

#[cfg(unix)]
fn validate_auth_lock_in_bound_parent(
    directory: &impl std::os::fd::AsFd,
    leaf: &std::ffi::OsStr,
    expected_file: &File,
    path: &Path,
    expected_link_count: u16,
) -> Result<()> {
    // Protected property: the directory sync must cover the exact lock object already held by the
    // caller. Device/inode bind that object; type, owner, mode, and link count bind its access
    // policy. Content and timestamps may change while the epoch is intentionally updated.
    let linked = rustix::fs::openat(
        directory,
        leaf,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::CLOEXEC | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
    .with_context(|| format!("failed to open bound BBDown auth lock {}", path.display()))?;
    let expected = rustix::fs::fstat(expected_file)
        .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
        .context("failed to inspect held BBDown auth lock")?;
    let current = rustix::fs::fstat(&linked)
        .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
        .context("failed to inspect bound BBDown auth lock")?;
    if rustix::fs::FileType::from_raw_mode(expected.st_mode) != rustix::fs::FileType::RegularFile
        || expected.st_dev != current.st_dev
        || expected.st_ino != current.st_ino
        || expected.st_uid != unsafe { libc::geteuid() }
        || current.st_uid != expected.st_uid
        || expected.st_mode & 0o777 != 0o600
        || current.st_mode & 0o777 != 0o600
        || expected.st_nlink != expected_link_count
        || current.st_nlink != expected_link_count
    {
        bail!(
            "BBDown auth lock identity or access policy changed while syncing: {}",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_auth_lock_parent(path: &Path, _expected_file: &File) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock directory {}",
            parent.display()
        )
    })?;
    File::open(&canonical_parent)
        .with_context(|| {
            format!(
                "failed to open BBDown auth lock directory {}",
                canonical_parent.display()
            )
        })?
        .sync_all()
        .with_context(|| {
            format!(
                "failed to sync BBDown auth lock directory {}",
                canonical_parent.display()
            )
        })
}

#[cfg(not(unix))]
fn sync_auth_lock_pair_parent(path: &Path, anchor_path: &Path, expected_file: &File) -> Result<()> {
    sync_auth_lock_parent(path, expected_file)?;
    sync_auth_lock_parent(anchor_path, expected_file)
}

fn with_auth_mutation_lock<T>(
    credential_file: &Path,
    protected_paths: &[&Path],
    operation: impl FnOnce(&AuthMutationFileLock) -> Result<T>,
) -> Result<T> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let file_lock = acquire_auth_mutation_file_lock(credential_file, protected_paths)?;
    operation(&file_lock)
}

pub fn recover_interrupted_auth_cleanup(
    state_path: &Path,
    credential_file: &Path,
) -> Result<Vec<String>> {
    with_auth_mutation_lock(credential_file, &[state_path, credential_file], |lock| {
        reconcile_interrupted_auth_cleanup_and_bump_epoch(lock, state_path, credential_file)
    })
}

fn reconcile_interrupted_auth_cleanup_and_bump_epoch(
    lock: &AuthMutationFileLock,
    state_path: &Path,
    credential_file: &Path,
) -> Result<Vec<String>> {
    let messages = reconcile_interrupted_auth_cleanup_unlocked(state_path, credential_file)?;
    if !messages.is_empty() {
        lock.bump_epoch()?;
    }
    Ok(messages)
}

fn reconcile_interrupted_auth_cleanup_unlocked(
    state_path: &Path,
    credential_file: &Path,
) -> Result<Vec<String>> {
    let state_path = absolute_auth_cleanup_path(state_path)?;
    let parent = state_path
        .parent()
        .context("Bilibili auth state path has no parent")?;
    match fs::metadata(parent) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => bail!(
            "Bilibili auth cleanup root is not a directory: {}",
            parent.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to inspect Bilibili auth cleanup root {}",
                    parent.display()
                )
            });
        }
    }

    let stale_config_dir = bbdown_config_dir(&state_path);
    let mut cleanup_directories = vec![parent.to_path_buf()];
    match fs::metadata(&stale_config_dir) {
        Ok(metadata) if metadata.is_dir() => cleanup_directories.push(stale_config_dir),
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to inspect Bilibili stale auth cleanup directory {}",
                    stale_config_dir.display()
                )
            });
        }
    }

    let mut messages = Vec::new();
    let mut unresolved = false;
    let mut restored = false;
    let mut scanned_directories = HashSet::new();
    for cleanup_directory in cleanup_directories {
        let root = RootedFs::new(&cleanup_directory).with_context(|| {
            format!(
                "failed to bind Bilibili auth cleanup directory {}",
                cleanup_directory.display()
            )
        })?;
        let root_identity = root.root_identity();
        if !scanned_directories.insert((root_identity.device(), root_identity.inode())) {
            continue;
        }
        // Protected property: auth recovery considers managed quarantine entries only in the
        // exact directories where legacy state/config cleanup can create them. Rebinding the
        // active credential for every candidate makes object identity the sole restore signal;
        // unrelated sibling subtrees are outside this recovery authority.
        let report = root
            .reconcile_remove_quarantines_in_current_directory_with_status_and_restore_decider(
                |candidate| {
                    let current_credential = bind_cleanup_target(credential_file)?;
                    Ok(current_credential
                        .as_ref()
                        .map(BoundCleanupTarget::identity)
                        == Some(candidate))
                },
            )?;
        messages.extend(report.messages);
        unresolved |= report.unresolved;
        restored |= report.restored;
    }
    if restored {
        bail!(
            "interrupted Bilibili auth cleanup restored a file that now aliases the active credential; resolve the configured auth paths before retrying"
        );
    }
    if unresolved {
        bail!(
            "interrupted Bilibili auth cleanup could not be verified: {}",
            messages.join("; ")
        );
    }
    Ok(messages)
}

fn acquire_auth_mutation_file_lock(
    credential_file: &Path,
    protected_paths: &[&Path],
) -> Result<AuthMutationFileLock> {
    let lock_path = auth_mutation_lock_path(credential_file);
    let anchor_path = auth_mutation_lock_anchor_path(credential_file);
    if protected_paths
        .iter()
        .any(|path| **path == lock_path || **path == anchor_path)
    {
        bail!(
            "BBDown auth lock path conflicts with an auth data file: {} or {}",
            lock_path.display(),
            anchor_path.display()
        );
    }
    if let Some(parent) = lock_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir_if_missing(parent).with_context(|| {
            format!(
                "failed to create BBDown auth lock directory {}",
                parent.display()
            )
        })?;
    }

    let file = match open_existing_auth_mutation_lock(&anchor_path)? {
        Some(file) => file,
        None => open_or_create_auth_mutation_lock(&lock_path)?,
    };
    rustix::fs::flock(&file, FlockOperation::LockExclusive)
        .map_err(|err| std::io::Error::from_raw_os_error(err.raw_os_error()))
        .with_context(|| format!("failed to lock BBDown auth state {}", lock_path.display()))?;
    validate_auth_mutation_lock_candidate(&file, &lock_path, &anchor_path)?;
    validate_auth_mutation_lock_is_distinct(&file, protected_paths, &lock_path)?;
    validate_existing_auth_lock_format(&file, &lock_path)?;
    set_auth_mutation_lock_private(&file, &lock_path)?;
    ensure_auth_mutation_lock_aliases(&file, &lock_path, &anchor_path)?;
    validate_auth_mutation_lock_identity(&file, &lock_path, &anchor_path)?;
    Ok(AuthMutationFileLock {
        file,
        path: lock_path,
        anchor_path,
    })
}

fn open_or_create_auth_mutation_lock(path: &Path) -> Result<File> {
    for _ in 0..3 {
        if let Some(file) = create_initialized_auth_lock(path)? {
            return Ok(file);
        }

        if let Some(file) = open_existing_auth_mutation_lock(path)? {
            return Ok(file);
        }
    }
    bail!(
        "BBDown auth lock kept changing while opening: {}",
        path.display()
    )
}

fn open_existing_auth_mutation_lock(path: &Path) -> Result<Option<File>> {
    let mut existing = OpenOptions::new();
    existing.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        existing.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    match existing.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("failed to open BBDown auth lock {}", path.display()))
        }
    }
}

fn create_initialized_auth_lock(path: &Path) -> Result<Option<File>> {
    let temp_path = temp_state_path(path);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options.open(&temp_path).with_context(|| {
        format!(
            "failed to create temporary BBDown auth lock {}",
            temp_path.display()
        )
    })?;
    if let Err(err) = initialize_auth_lock(&file, &temp_path) {
        let cleanup = fs::remove_file(&temp_path);
        return match cleanup {
            Ok(()) => Err(err),
            Err(cleanup) => Err(err.context(format!(
                "failed to remove temporary BBDown auth lock: {cleanup}"
            ))),
        };
    }

    match install_auth_lock_noreplace(&temp_path, path) {
        Ok(true) => {
            sync_auth_lock_parent(path, &file)?;
            Ok(Some(file))
        }
        Ok(false) => {
            fs::remove_file(&temp_path).with_context(|| {
                format!(
                    "failed to remove unused temporary BBDown auth lock {}",
                    temp_path.display()
                )
            })?;
            Ok(None)
        }
        Err(err) => {
            let cleanup = fs::remove_file(&temp_path);
            match cleanup {
                Ok(()) => Err(err),
                Err(cleanup) => Err(err.context(format!(
                    "failed to remove temporary BBDown auth lock: {cleanup}"
                ))),
            }
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "linux"))]
fn install_auth_lock_noreplace(source: &Path, destination: &Path) -> Result<bool> {
    match rustix::fs::renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE) {
        Ok(()) => Ok(true),
        Err(err) if err == rustix::io::Errno::EXIST => Ok(false),
        Err(err) => Err(std::io::Error::from_raw_os_error(err.raw_os_error()))
            .context("failed to atomically install BBDown auth lock"),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
fn install_auth_lock_noreplace(source: &Path, destination: &Path) -> Result<bool> {
    match fs::hard_link(source, destination) {
        Ok(()) => {
            fs::remove_file(source).context("failed to unlink temporary BBDown auth lock")?;
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(err).context("failed to install BBDown auth lock without replacement"),
    }
}

pub fn acquire_auth_reply_file_lock(
    state_path: &Path,
    credential_file: &Path,
) -> Result<AuthReplyFileLock> {
    let lock = acquire_auth_mutation_file_lock(credential_file, &[state_path, credential_file])?;
    reconcile_interrupted_auth_cleanup_and_bump_epoch(&lock, state_path, credential_file)?;
    Ok(AuthReplyFileLock { lock })
}

pub fn with_auth_mutation_transaction<T>(
    state_path: &Path,
    credential_file: &Path,
    operation: impl for<'a> FnOnce(&LockedAuthMutation<'a>) -> Result<T>,
) -> Result<(T, u64)> {
    with_auth_mutation_transaction_inner(state_path, credential_file, None, operation)
}

pub fn with_auth_mutation_transaction_at_epoch<T>(
    state_path: &Path,
    credential_file: &Path,
    expected_epoch: u64,
    operation: impl for<'a> FnOnce(&LockedAuthMutation<'a>) -> Result<T>,
) -> Result<(T, u64)> {
    with_auth_mutation_transaction_inner(
        state_path,
        credential_file,
        Some(expected_epoch),
        operation,
    )
}

fn with_auth_mutation_transaction_inner<T>(
    state_path: &Path,
    credential_file: &Path,
    expected_epoch: Option<u64>,
    operation: impl for<'a> FnOnce(&LockedAuthMutation<'a>) -> Result<T>,
) -> Result<(T, u64)> {
    with_auth_mutation_lock(credential_file, &[state_path, credential_file], |lock| {
        reconcile_interrupted_auth_cleanup_and_bump_epoch(lock, state_path, credential_file)?;
        if let Some(expected_epoch) = expected_epoch {
            let current_epoch = lock.current_epoch()?;
            if current_epoch != expected_epoch {
                bail!(
                    "BBDown credential state changed while login was pending (expected epoch {expected_epoch}, current epoch {current_epoch})"
                );
            }
        }
        let epoch = lock.bump_epoch()?;
        let transaction = LockedAuthMutation {
            state_path,
            credential_file,
            _lock: lock,
            epoch,
        };
        operation(&transaction).map(|result| (result, epoch))
    })
}

#[cfg(unix)]
fn validate_auth_mutation_lock_is_distinct(
    file: &File,
    protected_paths: &[&Path],
    lock_path: &Path,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let lock = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", lock_path.display()))?;
    for path in protected_paths {
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect auth data file {}", path.display())
                });
            }
        };
        if lock.dev() == metadata.dev() && lock.ino() == metadata.ino() {
            bail!(
                "BBDown auth lock aliases an auth data file: {}",
                path.display()
            );
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_auth_mutation_lock_is_distinct(
    _file: &File,
    _protected_paths: &[&Path],
    _lock_path: &Path,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn validate_auth_mutation_lock_candidate(
    file: &File,
    path: &Path,
    anchor_path: &Path,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let opened = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", path.display()))?;
    if !opened.is_file() || opened.uid() != unsafe { libc::geteuid() } {
        bail!(
            "BBDown auth lock path or ownership changed while locking: {}",
            path.display()
        );
    }
    let linked_count = [path, anchor_path]
        .into_iter()
        .map(|candidate| auth_mutation_lock_path_matches(&opened, candidate))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|matches| *matches)
        .count() as u64;
    if linked_count == 0 {
        bail!("BBDown auth lock aliases disappeared while locking");
    }
    if opened.nlink() != linked_count {
        bail!("BBDown auth lock has hard-link aliases: {}", path.display());
    }
    Ok(())
}

#[cfg(unix)]
fn auth_mutation_lock_path_matches(opened: &std::fs::Metadata, candidate: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let linked = match fs::symlink_metadata(candidate) {
        Ok(linked) => linked,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to revalidate BBDown auth lock alias {}",
                    candidate.display()
                )
            });
        }
    };
    if !linked.file_type().is_file()
        || opened.dev() != linked.dev()
        || opened.ino() != linked.ino()
        || linked.uid() != unsafe { libc::geteuid() }
    {
        bail!(
            "BBDown auth lock path or ownership changed while locking: {}",
            candidate.display()
        );
    }
    Ok(true)
}

#[cfg(unix)]
fn ensure_auth_mutation_lock_aliases(file: &File, path: &Path, anchor_path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    validate_auth_mutation_lock_candidate(file, path, anchor_path)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let anchor_parent = anchor_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock directory {}",
            parent.display()
        )
    })?;
    if fs::canonicalize(anchor_parent).with_context(|| {
        format!(
            "failed to resolve BBDown auth lock anchor directory {}",
            anchor_parent.display()
        )
    })? != canonical_parent
    {
        bail!("BBDown auth lock aliases resolve to different directories");
    }

    let root = RootedFs::new(&canonical_parent)?;
    let primary_bound_path = canonical_parent.join(
        path.file_name()
            .context("BBDown auth lock path has no file name")?,
    );
    let anchor_bound_path = canonical_parent.join(
        anchor_path
            .file_name()
            .context("BBDown auth lock anchor path has no file name")?,
    );
    let primary_entry = root.bind_entry(&primary_bound_path, false)?;
    let anchor_entry = root.bind_entry(&anchor_bound_path, false)?;
    let primary_identity = root.bound_entry_identity(&primary_entry)?;
    let anchor_identity = root.bound_entry_identity(&anchor_entry)?;
    let metadata = file
        .metadata()
        .context("failed to inspect held BBDown auth lock")?;
    let matches_file = |identity: EntryIdentity| {
        identity.is_file()
            && identity.device() == metadata.dev()
            && identity.inode() == metadata.ino()
    };
    let expected = primary_identity
        .or(anchor_identity)
        .filter(|identity| matches_file(*identity))
        .context("BBDown auth lock aliases disappeared during anchor validation")?;
    if primary_identity.is_some_and(|identity| identity != expected)
        || anchor_identity.is_some_and(|identity| identity != expected)
    {
        bail!("BBDown auth lock alias identity changed during anchor validation");
    }

    match (primary_identity, anchor_identity) {
        (Some(_), Some(_)) => {}
        (Some(_), None) => root.ensure_hard_link_via_bound_parents_if_identity(
            &primary_entry,
            &anchor_entry,
            expected,
        )?,
        (None, Some(_)) => root.ensure_hard_link_via_bound_parents_if_identity(
            &anchor_entry,
            &primary_entry,
            expected,
        )?,
        (None, None) => bail!("BBDown auth lock aliases disappeared during anchor installation"),
    }
    validate_auth_mutation_lock_identity(file, path, anchor_path)?;
    sync_auth_lock_pair_parent(path, anchor_path, file)?;
    validate_auth_mutation_lock_identity(file, path, anchor_path)
}

#[cfg(unix)]
fn validate_auth_mutation_lock_identity(
    file: &File,
    path: &Path,
    anchor_path: &Path,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    validate_auth_mutation_lock_candidate(file, path, anchor_path)?;
    let opened = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", path.display()))?;
    if opened.nlink() != 2 || opened.permissions().mode() & 0o777 != 0o600 {
        bail!("BBDown auth lock anchor or access policy changed");
    }
    if !auth_mutation_lock_path_matches(&opened, path)?
        || !auth_mutation_lock_path_matches(&opened, anchor_path)?
    {
        bail!("BBDown auth lock anchor disappeared");
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_auth_mutation_lock_candidate(
    file: &File,
    path: &Path,
    anchor_path: &Path,
) -> Result<()> {
    if !file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", path.display()))?
        .is_file()
        || (!path.is_file() && !anchor_path.is_file())
    {
        bail!("BBDown auth lock is not a regular file: {}", path.display());
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_auth_mutation_lock_aliases(file: &File, path: &Path, anchor_path: &Path) -> Result<()> {
    validate_auth_mutation_lock_candidate(file, path, anchor_path)?;
    match (path.exists(), anchor_path.exists()) {
        (true, false) => {
            fs::hard_link(path, anchor_path).context("failed to create BBDown auth lock anchor")?
        }
        (false, true) => {
            fs::hard_link(anchor_path, path).context("failed to restore BBDown auth lock path")?
        }
        (true, true) => {}
        (false, false) => bail!("BBDown auth lock aliases disappeared"),
    }
    sync_auth_lock_pair_parent(path, anchor_path, file)
}

#[cfg(not(unix))]
fn validate_auth_mutation_lock_identity(
    file: &File,
    path: &Path,
    anchor_path: &Path,
) -> Result<()> {
    validate_auth_mutation_lock_candidate(file, path, anchor_path)?;
    if !path.is_file() || !anchor_path.is_file() {
        bail!("BBDown auth lock anchor disappeared");
    }
    Ok(())
}

#[cfg(unix)]
fn set_auth_mutation_lock_private(file: &File, path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect BBDown auth lock {}", path.display()))?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)
        .with_context(|| format!("failed to protect BBDown auth lock {}", path.display()))?;
    let mode = file
        .metadata()
        .with_context(|| format!("failed to recheck BBDown auth lock {}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    if mode != 0o600 {
        bail!(
            "BBDown auth lock permissions are not private: {}",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_auth_mutation_lock_private(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

fn auth_mutation_lock_path(credential_file: &Path) -> PathBuf {
    let mut value = credential_file.as_os_str().to_os_string();
    value.push(AUTH_MUTATION_LOCK_SUFFIX);
    PathBuf::from(value)
}

fn auth_mutation_lock_anchor_path(credential_file: &Path) -> PathBuf {
    let mut value = auth_mutation_lock_path(credential_file).into_os_string();
    value.push(AUTH_MUTATION_LOCK_ANCHOR_SUFFIX);
    PathBuf::from(value)
}

pub fn load_auth_state(path: &Path) -> Result<Option<AuthState>> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    load_auth_state_unlocked(path)
}

fn load_auth_state_unlocked(path: &Path) -> Result<Option<AuthState>> {
    match fs::read(path) {
        Ok(content) => serde_json::from_slice(&content)
            .with_context(|| format!("failed to parse Bilibili auth state {}", path.display()))
            .map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read Bilibili auth state {}", path.display())),
    }
}

pub fn save_auth_state(path: &Path, state: &AuthState) -> Result<()> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    save_auth_state_unlocked(path, state)
}

fn save_auth_state_unlocked(path: &Path, state: &AuthState) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir_if_missing(parent).with_context(|| {
            format!("failed to create auth state directory {}", parent.display())
        })?;
    }

    let content =
        serde_json::to_vec_pretty(state).context("failed to encode Bilibili auth state")?;
    let temp_path = temp_state_path(path);
    let _ = fs::remove_file(&temp_path);
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp_path)
            .with_context(|| format!("failed to create temp auth state {}", temp_path.display()))?;
        std::io::Write::write_all(&mut file, &content)
            .with_context(|| format!("failed to write temp auth state {}", temp_path.display()))?;
        std::io::Write::flush(&mut file)
            .with_context(|| format!("failed to flush temp auth state {}", temp_path.display()))?;
    }
    set_file_private(&temp_path);
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace auth state {}", path.display()))?;
    set_file_private(path);
    Ok(())
}

pub fn delete_auth_state(path: &Path, credential_file: &Path) -> Result<bool> {
    with_auth_mutation_transaction(path, credential_file, |transaction| {
        transaction.delete_legacy_state()
    })
    .map(|(removed, _)| removed)
}

fn delete_auth_state_unlocked(path: &Path, credential_file: &Path) -> Result<bool> {
    let config_path = bbdown_config_path(path);
    let legacy_config_path = legacy_bbdown_config_path(path);
    let mut removed = false;
    if remove_legacy_file_if_exists(path, credential_file)
        .with_context(|| format!("failed to delete Bilibili auth state {}", path.display()))?
    {
        removed = true;
    }

    for path in [config_path, legacy_config_path] {
        if remove_legacy_file_if_exists(&path, credential_file)
            .with_context(|| format!("failed to delete BBDown auth config {}", path.display()))?
        {
            removed = true;
        }
    }
    if cleanup_stale_bbdown_config_files_unlocked(path, credential_file)? {
        removed = true;
    }

    Ok(removed)
}

pub fn sync_bbdown_rust_credentials_from_state(
    state_path: &Path,
    credential_file: &Path,
    credential_profile: Option<&str>,
) -> Result<bool> {
    let (result, _) = sync_bbdown_rust_credentials_from_state_with_epoch_and_hook(
        state_path,
        credential_file,
        credential_profile,
        || {},
    )?;
    result
}

pub fn sync_bbdown_rust_credentials_from_state_with_epoch(
    state_path: &Path,
    credential_file: &Path,
    credential_profile: Option<&str>,
) -> Result<(Result<bool>, u64)> {
    sync_bbdown_rust_credentials_from_state_with_epoch_and_hook(
        state_path,
        credential_file,
        credential_profile,
        || {},
    )
}

fn sync_bbdown_rust_credentials_from_state_with_hook(
    state_path: &Path,
    credential_file: &Path,
    credential_profile: Option<&str>,
    after_legacy_read: impl FnOnce(),
) -> Result<bool> {
    let (result, _) = sync_bbdown_rust_credentials_from_state_with_epoch_and_hook(
        state_path,
        credential_file,
        credential_profile,
        after_legacy_read,
    )?;
    result
}

fn sync_bbdown_rust_credentials_from_state_with_epoch_and_hook(
    state_path: &Path,
    credential_file: &Path,
    credential_profile: Option<&str>,
    after_legacy_read: impl FnOnce(),
) -> Result<(Result<bool>, u64)> {
    with_auth_mutation_lock(credential_file, &[state_path, credential_file], |lock| {
        reconcile_interrupted_auth_cleanup_and_bump_epoch(lock, state_path, credential_file)?;
        let current_epoch = lock.current_epoch()?;
        let cookie = match legacy_cookie_from_state_unlocked(state_path) {
            Ok(Some(cookie)) => cookie,
            Ok(None) => return Ok((Ok(false), current_epoch)),
            Err(err) => return Ok((Err(err), current_epoch)),
        };
        let update_needed = match bbdown_rust_cookie_update_needed_unlocked(
            credential_file,
            credential_profile,
            Some(&cookie),
            false,
        ) {
            Ok(update_needed) => update_needed,
            Err(err) => return Ok((Err(err), current_epoch)),
        };
        if !update_needed {
            return Ok((Ok(false), current_epoch));
        }
        after_legacy_read();
        let epoch = lock.bump_epoch()?;
        let result = update_bbdown_rust_cookie_unlocked(
            credential_file,
            credential_profile,
            Some(&cookie),
            false,
        );
        Ok((result, epoch))
    })
}

fn bbdown_rust_cookie_update_needed_unlocked(
    credential_file: &Path,
    credential_profile: Option<&str>,
    cookie: Option<&str>,
    overwrite_existing: bool,
) -> Result<bool> {
    let selection = bbdown_rust_profile_selection(credential_profile)?;
    let credentials =
        CredentialStore::new(credential_file.to_path_buf()).load_selected_profile(&selection)?;
    let current_cookie = credentials.cookie.as_deref().unwrap_or_default().trim();
    if cookie.is_some() && !current_cookie.is_empty() && !overwrite_existing {
        return Ok(false);
    }
    Ok(credentials.cookie.as_deref() != cookie)
}

fn sync_bbdown_rust_credentials_from_state_unlocked(
    state_path: &Path,
    credential_file: &Path,
    credential_profile: Option<&str>,
) -> Result<bool> {
    let Some(cookie) = legacy_cookie_from_state_unlocked(state_path)? else {
        return Ok(false);
    };
    update_bbdown_rust_cookie_unlocked(credential_file, credential_profile, Some(&cookie), false)
}

fn legacy_cookie_from_state_unlocked(state_path: &Path) -> Result<Option<String>> {
    let Some(state) = load_auth_state_unlocked(state_path)? else {
        return Ok(None);
    };
    let cookie = state.cookie.trim();
    if cookie.is_empty() {
        Ok(None)
    } else {
        Ok(Some(cookie.to_string()))
    }
}

pub fn clear_bbdown_rust_cookie(
    credential_file: &Path,
    credential_profile: Option<&str>,
) -> Result<bool> {
    with_auth_mutation_lock(credential_file, &[credential_file], |lock| {
        lock.bump_epoch()?;
        if !credential_file.exists() {
            return Ok(false);
        }
        update_bbdown_rust_cookie_unlocked(credential_file, credential_profile, None, true)
    })
}

pub fn clear_auth_state_and_credentials(
    state_path: &Path,
    credential_file: &Path,
    clear_credentials: impl FnOnce() -> Result<()>,
) -> Result<AuthCleanupResult> {
    clear_auth_state_and_credentials_with_epoch(state_path, credential_file, clear_credentials)
        .map(|(result, _)| result)
}

pub fn clear_auth_state_and_credentials_with_epoch(
    state_path: &Path,
    credential_file: &Path,
    clear_credentials: impl FnOnce() -> Result<()>,
) -> Result<(AuthCleanupResult, u64)> {
    with_auth_mutation_transaction(state_path, credential_file, |transaction| {
        let legacy_state = transaction.delete_legacy_state();
        let credential_state = if legacy_state.is_ok() {
            clear_credentials()
        } else {
            Err(anyhow!(
                "BBDown credential cleanup skipped because legacy login state cleanup failed"
            ))
        };
        Ok((legacy_state, credential_state))
    })
}

fn update_bbdown_rust_cookie_unlocked(
    credential_file: &Path,
    credential_profile: Option<&str>,
    cookie: Option<&str>,
    overwrite_existing: bool,
) -> Result<bool> {
    let selection = bbdown_rust_profile_selection(credential_profile)?;
    let store = CredentialStore::new(credential_file.to_path_buf());
    let changed = std::cell::Cell::new(false);
    store
        .update_selected_profile(&selection, |mut credentials| {
            let current_cookie = credentials.cookie.as_deref().unwrap_or_default().trim();
            if cookie.is_some() && !current_cookie.is_empty() && !overwrite_existing {
                return Ok(credentials);
            }
            let next_cookie = cookie.map(str::to_string);
            if credentials.cookie != next_cookie {
                credentials.cookie = next_cookie;
                changed.set(true);
            }
            Ok(credentials)
        })
        .with_context(|| {
            format!(
                "failed to update BBDown-rust credentials {}",
                credential_file.display()
            )
        })?;
    Ok(changed.get())
}

fn bbdown_rust_profile_selection(
    credential_profile: Option<&str>,
) -> Result<CredentialProfileSelection> {
    match credential_profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
    {
        Some(profile) => CredentialProfileSelection::named(profile)
            .context("invalid BBDown-rust credential profile"),
        None => Ok(CredentialProfileSelection::default_profile()),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn ensure_bbdown_config_file(
    path: &Path,
    credential_file: &Path,
    base_config_path: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let Some(state) = load_auth_state_unlocked(path)? else {
        return Ok(None);
    };
    if state.cookie.trim().is_empty() {
        return Ok(None);
    }

    cleanup_stale_bbdown_config_files_unlocked(path, credential_file)?;
    let config_path = temp_state_path(&bbdown_config_dir(path).join("cookie.config"));
    write_bbdown_config(&config_path, &state.cookie, base_config_path)?;
    active_bbdown_config_files()
        .lock()
        .expect("active BBDown config lock should not poison")
        .insert(config_path.clone());
    Ok(Some(config_path))
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn ensure_isolated_bbdown_config_file_with_lines(
    path: &Path,
    credential_file: &Path,
    base_lines: &[String],
) -> Result<PathBuf> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let state = load_auth_state_unlocked(path)?;

    cleanup_stale_bbdown_config_files_unlocked(path, credential_file)?;
    let config_path = temp_state_path(&bbdown_config_dir(path).join("probe.config"));
    let mut content = Vec::new();
    for line in base_lines {
        content.extend_from_slice(line.as_bytes());
        content.push(b'\n');
    }
    if !content.is_empty() && !content.ends_with(b"\n") {
        content.push(b'\n');
    }
    if let Some(cookie) = state
        .as_ref()
        .map(|state| state.cookie.trim())
        .filter(|cookie| !cookie.is_empty())
    {
        content.extend_from_slice(format!("--cookie\n{cookie}\n").as_bytes());
    }
    write_bbdown_config_content(&config_path, &content)?;
    active_bbdown_config_files()
        .lock()
        .expect("active BBDown config lock should not poison")
        .insert(config_path.clone());
    Ok(config_path)
}

pub fn bbdown_config_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".bbdown.config");
    PathBuf::from(value)
}

fn bbdown_config_dir(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".bbdown.config.d");
    PathBuf::from(value)
}

fn legacy_bbdown_config_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".bbdown.config.json");
    PathBuf::from(value)
}

pub fn release_bbdown_config_file(path: &Path) {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let _ = fs::remove_file(path);
    active_bbdown_config_files()
        .lock()
        .expect("active BBDown config lock should not poison")
        .remove(path);
}

#[cfg_attr(not(test), allow(dead_code))]
fn write_bbdown_config(path: &Path, cookie: &str, base_config_path: Option<&Path>) -> Result<()> {
    let mut content = match base_config_path {
        Some(base_config_path) => fs::read(base_config_path).with_context(|| {
            format!(
                "failed to read BBDown config {}",
                base_config_path.display()
            )
        })?,
        None => Vec::new(),
    };
    if !content.is_empty() && !content.ends_with(b"\n") {
        content.push(b'\n');
    }
    content.extend_from_slice(format!("--cookie\n{cookie}\n").as_bytes());
    write_bbdown_config_content(path, &content)
}

#[cfg_attr(not(test), allow(dead_code))]
fn write_bbdown_config_content(path: &Path, content: &[u8]) -> Result<()> {
    write_private_bytes(path, content, "BBDown auth config")
}

fn write_private_bytes(path: &Path, content: &[u8], label: &str) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir_if_missing(parent)
            .with_context(|| format!("failed to create {label} directory {}", parent.display()))?;
    }

    let temp_path = temp_state_path(path);
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp_path)
            .with_context(|| format!("failed to create temp {label} {}", temp_path.display()))?;
        std::io::Write::write_all(&mut file, content)
            .with_context(|| format!("failed to write temp {label} {}", temp_path.display()))?;
        std::io::Write::flush(&mut file)
            .with_context(|| format!("failed to flush temp {label} {}", temp_path.display()))?;
    }
    set_file_private(&temp_path);
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace {label} {}", path.display()))?;
    set_file_private(path);
    Ok(())
}

struct BoundCleanupTarget {
    root: RootedFs,
    entry: BoundEntry,
    file: BoundFile,
}

impl BoundCleanupTarget {
    fn identity(&self) -> EntryIdentity {
        self.file.identity()
    }
}

fn remove_cleanup_file_if_exists(path: &Path, credential_file: &Path) -> Result<bool> {
    remove_cleanup_file_if_exists_with_hook(path, credential_file, || Ok(()))
}

fn remove_cleanup_file_if_exists_with_hook<F>(
    path: &Path,
    credential_file: &Path,
    after_quarantine_move: F,
) -> Result<bool>
where
    F: FnOnce() -> Result<()>,
{
    let Some(target) = bind_cleanup_target(path)? else {
        return Ok(false);
    };
    // Protected property: never unlink the object selected by the active credential path. Hold
    // the initial object identity, then rebind the credential after the target is quarantined so
    // a namespace replacement either rejects and restores the target or fails closed.
    let initial_credential = bind_cleanup_target(credential_file)?;
    ensure_cleanup_identity_is_not_credential(
        path,
        target.identity(),
        initial_credential
            .as_ref()
            .map(BoundCleanupTarget::identity),
    )?;

    let expected = target.identity();
    target
        .root
        .remove_bound_file_if_identity_with_validation(&target.entry, expected, || {
            after_quarantine_move()?;
            let current_credential = bind_cleanup_target(credential_file)?;
            ensure_cleanup_identity_is_not_credential(
                path,
                expected,
                current_credential
                    .as_ref()
                    .map(BoundCleanupTarget::identity),
            )
        })
        .with_context(|| format!("failed to delete legacy auth file {}", path.display()))?;
    Ok(true)
}

fn bind_cleanup_target(path: &Path) -> Result<Option<BoundCleanupTarget>> {
    let path = absolute_auth_cleanup_path(path)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to inspect Bilibili auth cleanup path {}",
                    path.display()
                )
            });
        }
    }

    let parent = path
        .parent()
        .context("Bilibili auth cleanup path has no parent")?;
    let root = RootedFs::new(parent).with_context(|| {
        format!(
            "failed to bind Bilibili auth cleanup directory {}",
            parent.display()
        )
    })?;
    let Some(file) = root.open_bound_file(&path)? else {
        return Ok(None);
    };
    let entry = root.bind_entry(&path, false)?;
    Ok(Some(BoundCleanupTarget { root, entry, file }))
}

fn absolute_auth_cleanup_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("failed to resolve the current directory for auth cleanup")?
            .join(path))
    }
}

fn ensure_cleanup_identity_is_not_credential(
    path: &Path,
    target: EntryIdentity,
    credential: Option<EntryIdentity>,
) -> Result<()> {
    if credential == Some(target) {
        bail!(
            "legacy Bilibili auth cleanup target aliases the active credential file: {}",
            path.display()
        );
    }
    Ok(())
}

fn cleanup_stale_bbdown_config_files_unlocked(path: &Path, credential_file: &Path) -> Result<bool> {
    let config_dir = bbdown_config_dir(path);
    let entries = match fs::read_dir(&config_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", config_dir.display()));
        }
    };
    let active_files = active_bbdown_config_files()
        .lock()
        .expect("active BBDown config lock should not poison")
        .clone();
    let mut removed = false;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if active_files.contains(&path) {
            continue;
        }
        match entry.file_type() {
            Ok(file_type) if file_type.is_file() => {}
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        }
        if remove_cleanup_file_if_exists(&path, credential_file).with_context(|| {
            format!(
                "failed to delete stale BBDown auth config {}",
                path.display()
            )
        })? {
            removed = true;
        }
    }
    let _ = fs::remove_dir(&config_dir);
    Ok(removed)
}

fn remove_legacy_file_if_exists(path: &Path, credential_file: &Path) -> Result<bool> {
    remove_cleanup_file_if_exists(path, credential_file)
}

fn active_bbdown_config_files() -> &'static Mutex<HashSet<PathBuf>> {
    ACTIVE_BBDOWN_CONFIG_FILES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn temp_state_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    value.push(format!(".{}.{}.{}.tmp", std::process::id(), counter, nanos));
    PathBuf::from(value)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn create_private_dir_if_missing(path: &Path) -> Result<()> {
    let existed = path.exists();
    fs::create_dir_all(path)?;
    if !existed {
        set_dir_private(path);
    }
    Ok(())
}

#[cfg(unix)]
fn set_dir_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn set_dir_private(_path: &Path) {}

#[cfg(unix)]
fn set_file_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn set_file_private(_path: &Path) {}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn temp_state_file(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("telegram-video-downloader-tests-{name}-{unique}"))
            .join("state.json")
    }

    fn test_state() -> AuthState {
        AuthState {
            cookie: "SESSDATA=secret; bili_jct=csrf".to_string(),
            mid: 123,
            uname: "Joey".to_string(),
            stored_at_unix: 1_717_171_717,
        }
    }

    #[test]
    fn extracts_cookie_pairs_from_set_cookie_headers() {
        assert_eq!(
            set_cookie_values_to_cookie([
                "SESSDATA=abc; Path=/; HttpOnly",
                "bili_jct=def; Path=/",
                "ac_time_value=token; Path=/",
                "unknown_cookie=secret; Path=/",
                "",
            ]),
            Some("SESSDATA=abc; bili_jct=def; ac_time_value=token".to_string())
        );
    }

    #[test]
    fn returns_none_for_empty_cookie_headers() {
        assert_eq!(set_cookie_values_to_cookie(["", "   "]), None);
    }

    #[test]
    fn extracts_cookie_from_login_url_query() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security?SESSDATA=secret%2Fvalue&bili_jct=csrf&DedeUserID=123&Expires=999&gourl=https%3A%2F%2Fexample.com",
            ),
            Some("SESSDATA=secret/value; bili_jct=csrf; DedeUserID=123".to_string())
        );
    }

    #[test]
    fn preserves_login_url_cookie_commas_for_bbdown() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security?SESSDATA=secret%2Cvalue&bili_jct=csrf",
            ),
            Some("SESSDATA=secret%2Cvalue; bili_jct=csrf".to_string())
        );
    }

    #[test]
    fn extracts_cookie_from_login_url_fragment_query() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security#/home?SESSDATA=secret&bili_jct=csrf&DedeUserID__ckMd5=hash",
            ),
            Some("SESSDATA=secret; bili_jct=csrf; DedeUserID__ckMd5=hash".to_string())
        );
    }

    #[test]
    fn renders_qr_png() {
        let png = render_qr_png("https://example.com/login").expect("QR should render");
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert!(png.len() > 100);
    }

    #[test]
    fn parses_bbdown_rust_auth_events() {
        let stdout = r#"
{"event":"ticket","kind":"access_key","url":"https://www.biliplus.com/login","qr_payload":"https://www.biliplus.com/login","message_origin":"https://www.biliplus.com","callback_origin":"https://www.bilibili.com"}
{"event":"saved","kind":"access_key","saved":{"has_cookie":false,"has_access_key":true,"has_tv_access_key":false}}
"#;

        assert_eq!(
            first_bbdown_auth_ticket(stdout).expect("ticket should parse"),
            BbdownAuthTicket {
                kind: "access_key".to_string(),
                url: "https://www.biliplus.com/login".to_string(),
                qr_payload: "https://www.biliplus.com/login".to_string(),
                message_origin: Some("https://www.biliplus.com".to_string()),
                callback_origin: Some("https://www.bilibili.com".to_string()),
            }
        );
        assert_eq!(
            bbdown_auth_saved_summary(stdout).expect("saved summary should parse"),
            BbdownCredentialSummary {
                has_cookie: false,
                has_access_key: true,
                has_tv_access_key: false,
            }
        );
    }

    #[test]
    fn parses_bbdown_rust_credential_health_report() {
        let report = parse_bbdown_credential_health_report(
            r#"{"credentials":{"has_cookie":true,"has_access_key":true,"has_tv_access_key":false},"probes":[{"kind":"cookie","scope":"web_cookie","status":"valid","endpoint":"https://api.example/nav","api_code":0,"message":null}]}"#,
        )
        .expect("health report should parse");

        assert!(report.credentials.has_cookie);
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].kind, "cookie");
        assert_eq!(report.probes[0].status, "valid");
    }

    #[test]
    fn parses_login_poll_states() {
        assert_eq!(test_poll(86_101, None), LoginPoll::Waiting);
        assert_eq!(test_poll(86_090, None), LoginPoll::Scanned);
        assert_eq!(test_poll(86_038, None), LoginPoll::Expired);
        assert_eq!(
            test_poll(0, Some("SESSDATA=secret; bili_jct=csrf".to_string())),
            LoginPoll::Success {
                cookie: "SESSDATA=secret; bili_jct=csrf".to_string()
            }
        );
    }

    #[test]
    fn parses_successful_login_poll_cookie_from_url() {
        let poll = login_poll_from_response(
            test_poll_response_with_url(
                0,
                "https://passport.bilibili.com/account/security#/home?SESSDATA=secret&bili_jct=csrf",
            ),
            None,
        )
        .expect("success URL cookies should parse");

        assert_eq!(
            poll,
            LoginPoll::Success {
                cookie: "SESSDATA=secret; bili_jct=csrf".to_string()
            }
        );
    }

    #[test]
    fn combines_non_login_header_cookie_with_login_url_cookie() {
        let poll = login_poll_from_response(
            test_poll_response_with_url(
                0,
                "https://passport.bilibili.com/account/security#/home?SESSDATA=secret&bili_jct=csrf",
            ),
            Some("buvid3=device; b_nut=nut".to_string()),
        )
        .expect("success URL cookies should not be shadowed by device headers");

        assert_eq!(
            poll,
            LoginPoll::Success {
                cookie: "buvid3=device; b_nut=nut; SESSDATA=secret; bili_jct=csrf".to_string()
            }
        );
    }

    #[test]
    fn rejects_successful_login_poll_without_cookie() {
        assert!(
            login_poll_from_response(test_poll_response(0), None)
                .expect_err("success without cookies should fail")
                .to_string()
                .contains("without returning cookies")
        );
    }

    #[test]
    fn parses_nav_account() {
        let state = auth_state_from_nav_response(
            BilibiliApiResponse {
                code: 0,
                message: "OK".to_string(),
                data: NavData {
                    is_login: true,
                    mid: Some(123),
                    uname: Some("Joey".to_string()),
                },
            },
            "SESSDATA=secret",
        )
        .expect("nav should parse");

        assert_eq!(state.cookie, "SESSDATA=secret");
        assert_eq!(state.mid, 123);
        assert_eq!(state.uname, "Joey");
    }

    #[cfg(unix)]
    #[test]
    fn logout_refuses_to_unlink_a_hard_link_to_the_active_credential_file() {
        let state_path = temp_state_file("logout-credential-hard-link");
        let credential_file = state_path.with_file_name("credentials.json");
        fs::create_dir_all(state_path.parent().expect("state should have a parent"))
            .expect("auth directory should create");
        fs::write(&credential_file, b"active-credentials").expect("credential file should write");
        fs::hard_link(&credential_file, &state_path).expect("state hard link should create");

        let error = delete_auth_state(&state_path, &credential_file)
            .expect_err("logout must preserve the active credential object");

        assert!(format!("{error:#}").contains("aliases the active credential file"));
        assert_eq!(fs::read(&credential_file).unwrap(), b"active-credentials");
        assert_eq!(fs::read(&state_path).unwrap(), b"active-credentials");
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn logout_refuses_to_unlink_a_stale_config_hard_link_to_credentials() {
        let state_path = temp_state_file("logout-stale-config-hard-link");
        let credential_file = state_path.with_file_name("credentials.json");
        let stale_config = bbdown_config_dir(&state_path).join("stale.config.tmp");
        save_auth_state(&state_path, &test_state()).expect("state should save");
        fs::write(&credential_file, b"active-credentials").expect("credential file should write");
        fs::create_dir_all(stale_config.parent().expect("config should have a parent"))
            .expect("config directory should create");
        fs::hard_link(&credential_file, &stale_config)
            .expect("stale config hard link should create");

        let error = delete_auth_state(&state_path, &credential_file)
            .expect_err("stale cleanup must preserve the active credential object");

        assert!(format!("{error:#}").contains("aliases the active credential file"));
        assert_eq!(fs::read(&credential_file).unwrap(), b"active-credentials");
        assert_eq!(fs::read(&stale_config).unwrap(), b"active-credentials");
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn isolated_config_cleanup_refuses_a_stale_hard_link_to_credentials() {
        let state_path = temp_state_file("isolated-config-stale-hard-link");
        let credential_file = state_path.with_file_name("credentials.json");
        let stale_config = bbdown_config_dir(&state_path).join("stale.config.tmp");
        fs::create_dir_all(stale_config.parent().expect("config should have a parent"))
            .expect("config directory should create");
        fs::write(&credential_file, b"active-credentials").expect("credential file should write");
        fs::hard_link(&credential_file, &stale_config)
            .expect("stale config hard link should create");

        let error =
            ensure_isolated_bbdown_config_file_with_lines(&state_path, &credential_file, &[])
                .expect_err("config cleanup must preserve the active credential object");

        assert!(format!("{error:#}").contains("aliases the active credential file"));
        assert_eq!(fs::read(&credential_file).unwrap(), b"active-credentials");
        assert_eq!(fs::read(&stale_config).unwrap(), b"active-credentials");
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_restores_a_target_that_becomes_the_active_credential() {
        use std::os::unix::fs::MetadataExt;

        let state_path = temp_state_file("cleanup-credential-namespace-swap");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        fs::create_dir_all(&root).expect("auth directory should create");
        let cleanup_target = root.join("legacy-state.json");
        let credential_file = root.join("credentials.json");
        let credential_candidate = root.join("new-credentials.json");
        fs::write(&cleanup_target, b"newly-saved-credentials")
            .expect("cleanup target should write");
        fs::hard_link(&cleanup_target, &credential_candidate)
            .expect("credential candidate hard link should create");
        fs::write(&credential_file, b"old-credentials").expect("old credential should write");

        let error =
            remove_cleanup_file_if_exists_with_hook(&cleanup_target, &credential_file, || {
                fs::remove_file(&credential_file).context("failed to remove old credential")?;
                fs::rename(&credential_candidate, &credential_file)
                    .context("failed to install raced credential")?;
                Ok(())
            })
            .expect_err("cleanup must restore an object selected as the active credential");

        assert!(format!("{error:#}").contains("aliases the active credential file"));
        assert_eq!(
            fs::read(&credential_file).unwrap(),
            b"newly-saved-credentials"
        );
        assert_eq!(
            fs::read(&cleanup_target).unwrap(),
            b"newly-saved-credentials"
        );
        let credential_metadata = fs::metadata(&credential_file).unwrap();
        let cleanup_metadata = fs::metadata(&cleanup_target).unwrap();
        assert_eq!(credential_metadata.dev(), cleanup_metadata.dev());
        assert_eq!(credential_metadata.ino(), cleanup_metadata.ino());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".telegram-video-downloader-remove")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn logout_refuses_unicode_normalization_alias_of_credentials() {
        let root = temp_state_file("logout-unicode-normalization")
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        fs::create_dir_all(&root).expect("auth directory should create");
        let state_path = root.join("caf\u{e9}.json");
        let credential_file = root.join("cafe\u{301}.json");
        fs::write(&credential_file, b"active-credentials").expect("credential file should write");
        assert!(
            state_path.exists(),
            "test filesystem must normalize Unicode names"
        );

        let error = delete_auth_state(&state_path, &credential_file)
            .expect_err("logout must preserve a normalization-aliased credential object");

        assert!(format!("{error:#}").contains("aliases the active credential file"));
        assert_eq!(fs::read(&credential_file).unwrap(), b"active-credentials");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_logged_out_nav_account() {
        assert!(
            auth_state_from_nav_response(
                BilibiliApiResponse {
                    code: -101,
                    message: "账号未登录".to_string(),
                    data: NavData {
                        is_login: false,
                        mid: None,
                        uname: None,
                    },
                },
                "SESSDATA=secret",
            )
            .expect_err("logged out nav should fail")
            .to_string()
            .contains("not logged in")
        );
    }

    #[test]
    fn saves_loads_and_deletes_auth_state() {
        let path = temp_state_file("state-roundtrip");
        let state = test_state();

        save_auth_state(&path, &state).expect("state should save");
        assert_eq!(
            load_auth_state(&path).expect("state should load"),
            Some(state)
        );
        let credential_file = path.with_file_name("credentials.json");
        assert!(delete_auth_state(&path, &credential_file).expect("state should delete"));
        assert_eq!(
            load_auth_state(&path).expect("state should be missing"),
            None
        );
        assert!(!delete_auth_state(&path, &credential_file).expect("missing delete should be ok"));

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn logout_keeps_credentials_when_symlinked_legacy_state_cannot_be_removed() {
        use std::os::unix::fs::symlink;

        let state_path = temp_state_file("logout-symlinked-legacy-state");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        fs::create_dir_all(&root).expect("auth root should create");
        let legacy_target = root.join("legacy-auth-target.json");
        let credential_file = root.join("credentials.json");
        save_auth_state(&legacy_target, &test_state()).expect("legacy state should save");
        symlink(&legacy_target, &state_path).expect("legacy state symlink should create");
        fs::write(&credential_file, b"active-credentials")
            .expect("active credentials should write");
        let credential_cleanup_called = std::cell::Cell::new(false);

        let (legacy_state, credential_state) =
            clear_auth_state_and_credentials(&state_path, &credential_file, || {
                credential_cleanup_called.set(true);
                fs::remove_file(&credential_file).context("failed to clear active credentials")
            })
            .expect("logout transaction should complete");

        assert!(legacy_state.is_err());
        assert!(credential_state.is_err());
        assert!(!credential_cleanup_called.get());
        assert_eq!(
            fs::read(&credential_file).expect("active credentials should remain"),
            b"active-credentials"
        );
        assert_eq!(
            load_auth_state(&legacy_target).expect("legacy target should load"),
            Some(test_state())
        );
        assert!(
            fs::symlink_metadata(&state_path)
                .expect("legacy symlink should remain")
                .file_type()
                .is_symlink()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_recovery_deletes_an_interrupted_noncredential_auth_cleanup() {
        let state_path = temp_state_file("interrupted-auth-cleanup-delete");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_file = root.join("credentials.json");
        fs::create_dir_all(&root).expect("auth root should create");
        fs::write(&state_path, b"legacy-state").expect("legacy state should write");
        fs::write(&credential_file, b"active-credentials")
            .expect("active credentials should write");
        let target = bind_cleanup_target(&state_path)
            .expect("legacy state should bind")
            .expect("legacy state should exist");
        target
            .root
            .leave_validated_file_removal_quarantined_for_test(&target.entry, target.identity())
            .expect("interrupted cleanup should persist");
        drop(target);

        let messages = recover_interrupted_auth_cleanup(&state_path, &credential_file)
            .expect("noncredential cleanup should recover");

        assert!(!state_path.exists());
        assert_eq!(
            fs::read(&credential_file).expect("active credentials should remain"),
            b"active-credentials"
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("Recovered interrupted bound-path removal"))
        );
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".telegram-video-downloader-remove")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_auth_recovery_ignores_unrelated_unreadable_subdirectories() {
        use std::os::unix::fs::PermissionsExt;

        let state_path = temp_state_file("auth-recovery-unrelated-directory");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_file = root.join("credentials.json");
        let unrelated = root.join("unrelated-private-data");
        fs::create_dir_all(&unrelated).expect("unrelated directory should create");
        fs::write(unrelated.join("sentinel"), b"unrelated")
            .expect("unrelated sentinel should write");
        fs::write(&credential_file, b"active-credentials")
            .expect("active credentials should write");
        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o000))
            .expect("unrelated directory should become unreadable");

        let result = recover_interrupted_auth_cleanup(&state_path, &credential_file);

        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o700))
            .expect("unrelated directory permissions should restore");
        let messages = result.expect("unrelated subtree must not block auth recovery");
        assert!(messages.is_empty());
        assert_eq!(fs::read(unrelated.join("sentinel")).unwrap(), b"unrelated");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_auth_recovery_scans_the_known_stale_config_directory() {
        let state_path = temp_state_file("auth-recovery-stale-config-directory");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_file = root.join("credentials.json");
        let stale_config_dir = bbdown_config_dir(&state_path);
        let stale_config = stale_config_dir.join("stale.config");
        fs::create_dir_all(&stale_config_dir).expect("stale config directory should create");
        fs::write(&stale_config, b"legacy-cookie").expect("stale config should write");
        fs::write(&credential_file, b"active-credentials")
            .expect("active credentials should write");
        let target = bind_cleanup_target(&stale_config)
            .expect("stale config should bind")
            .expect("stale config should exist");
        target
            .root
            .leave_validated_file_removal_quarantined_for_test(&target.entry, target.identity())
            .expect("interrupted stale config cleanup should persist");
        drop(target);

        let messages = recover_interrupted_auth_cleanup(&state_path, &credential_file)
            .expect("known stale config cleanup should recover");

        assert!(!stale_config.exists());
        assert!(messages.iter().any(|message| {
            message.contains("Recovered interrupted bound-path removal")
                && message.contains(".bbdown.config.d")
        }));
        assert!(fs::read_dir(&stale_config_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".telegram-video-downloader-remove")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_recovery_restores_an_interrupted_cleanup_that_now_aliases_credentials() {
        use std::os::unix::fs::MetadataExt;

        let state_path = temp_state_file("interrupted-auth-cleanup-restore");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_file = root.join("credentials.json");
        fs::create_dir_all(&root).expect("auth root should create");
        fs::write(&state_path, b"legacy-state").expect("legacy state should write");
        let target = bind_cleanup_target(&state_path)
            .expect("legacy state should bind")
            .expect("legacy state should exist");
        target
            .root
            .leave_validated_file_removal_quarantined_for_test(&target.entry, target.identity())
            .expect("interrupted cleanup should persist");
        let quarantine = fs::read_dir(&root)
            .expect("auth root should read")
            .map(|entry| entry.expect("auth entry should read").path())
            .find(|path| {
                path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(".telegram-video-downloader-remove")
                }) && path.is_dir()
            })
            .expect("interrupted cleanup quarantine should exist");
        fs::hard_link(quarantine.join("entry"), &credential_file)
            .expect("credential alias should create after quarantine");
        drop(target);

        let error = recover_interrupted_auth_cleanup(&state_path, &credential_file)
            .expect_err("credential alias must block auth recovery");

        assert!(format!("{error:#}").contains("now aliases the active credential"));
        assert_eq!(fs::read(&state_path).unwrap(), b"legacy-state");
        assert_eq!(fs::read(&credential_file).unwrap(), b"legacy-state");
        let state_metadata = fs::metadata(&state_path).unwrap();
        let credential_metadata = fs::metadata(&credential_file).unwrap();
        assert_eq!(state_metadata.dev(), credential_metadata.dev());
        assert_eq!(state_metadata.ino(), credential_metadata.ino());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".telegram-video-downloader-remove")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn syncs_bbdown_rust_flat_credentials_and_preserves_access_keys() {
        let path = temp_state_file("bbdown-rust-flat-sync");
        let credential_file = path.with_file_name("credentials.json");
        save_auth_state(&path, &test_state()).expect("state should save");
        fs::write(
            &credential_file,
            r#"{"access_key":"access","tv_access_key":"tv"}"#,
        )
        .expect("credential file should write");

        assert!(
            sync_bbdown_rust_credentials_from_state(&path, &credential_file, None)
                .expect("credential sync should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert_eq!(value["cookie"], "SESSDATA=secret; bili_jct=csrf");
        assert_eq!(value["access_key"], "access");
        assert_eq!(value["tv_access_key"], "tv");

        assert!(
            clear_bbdown_rust_cookie(&credential_file, None)
                .expect("credential clear should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert!(value["cookie"].is_null());
        assert_eq!(value["access_key"], "access");
        assert_eq!(value["tv_access_key"], "tv");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn sync_does_not_overwrite_existing_bbdown_rust_flat_cookie() {
        let path = temp_state_file("bbdown-rust-flat-existing-cookie");
        let credential_file = path.with_file_name("credentials.json");
        save_auth_state(&path, &test_state()).expect("state should save");
        fs::write(
            &credential_file,
            r#"{"cookie":"fresh-cookie","access_key":"access"}"#,
        )
        .expect("credential file should write");

        assert!(
            !sync_bbdown_rust_credentials_from_state(&path, &credential_file, None)
                .expect("credential sync should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert_eq!(value["cookie"], "fresh-cookie");
        assert_eq!(value["access_key"], "access");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn sync_rejects_malformed_legacy_auth_state() {
        let path = temp_state_file("bbdown-rust-malformed-legacy-sync");
        let credential_file = path.with_file_name("credentials.json");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("state parent should create");
        }
        fs::write(&path, "{not-json").expect("malformed state should write");
        fs::write(&credential_file, r#"{"access_key":"access"}"#)
            .expect("credential file should write");

        let error = sync_bbdown_rust_credentials_from_state(&path, &credential_file, None)
            .expect_err("malformed legacy state must block credential migration");
        assert!(format!("{error:#}").contains("failed to parse Bilibili auth state"));
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert!(value.get("cookie").is_none());
        assert_eq!(value["access_key"], "access");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn syncs_bbdown_rust_selected_profile_cookie_only() {
        let path = temp_state_file("bbdown-rust-profile-sync");
        let credential_file = path.with_file_name("credentials.json");
        save_auth_state(&path, &test_state()).expect("state should save");
        fs::write(
            &credential_file,
            r#"{"version":1,"default_profile":"default","profiles":{"default":{"cookie":"old-cookie"},"intl":{"access_key":"intl-access"}}}"#,
        )
        .expect("credential file should write");

        assert!(
            sync_bbdown_rust_credentials_from_state(&path, &credential_file, Some("intl"))
                .expect("credential sync should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert_eq!(value["profiles"]["default"]["cookie"], "old-cookie");
        assert_eq!(
            value["profiles"]["intl"]["cookie"],
            "SESSDATA=secret; bili_jct=csrf"
        );
        assert_eq!(value["profiles"]["intl"]["access_key"], "intl-access");

        assert!(
            clear_bbdown_rust_cookie(&credential_file, Some("intl"))
                .expect("credential clear should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert_eq!(value["profiles"]["default"]["cookie"], "old-cookie");
        assert!(value["profiles"]["intl"]["cookie"].is_null());
        assert_eq!(value["profiles"]["intl"]["access_key"], "intl-access");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn sync_does_not_overwrite_existing_bbdown_rust_profile_cookie() {
        let path = temp_state_file("bbdown-rust-profile-existing-cookie");
        let credential_file = path.with_file_name("credentials.json");
        save_auth_state(&path, &test_state()).expect("state should save");
        fs::write(
            &credential_file,
            r#"{"version":1,"default_profile":"default","profiles":{"default":{"cookie":"default-cookie"},"intl":{"cookie":"fresh-cookie","access_key":"intl-access"}}}"#,
        )
        .expect("credential file should write");

        assert!(
            !sync_bbdown_rust_credentials_from_state(&path, &credential_file, Some("intl"))
                .expect("credential sync should succeed")
        );
        let value: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&credential_file).expect("credential file should read"),
        )
        .expect("credential file should parse");
        assert_eq!(value["profiles"]["default"]["cookie"], "default-cookie");
        assert_eq!(value["profiles"]["intl"]["cookie"], "fresh-cookie");
        assert_eq!(value["profiles"]["intl"]["access_key"], "intl-access");

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn credential_migration_does_not_overwrite_a_concurrent_core_update() {
        use std::sync::mpsc::sync_channel;
        use std::thread;

        use bbdown_core::Credentials;

        let path = temp_state_file("bbdown-rust-concurrent-migration");
        let credential_file = path.with_file_name("credentials.json");
        save_auth_state(&path, &test_state()).expect("state should save");
        let store = CredentialStore::new(credential_file.clone());
        store
            .save(&Credentials::default().with_access_key("old-access"))
            .expect("initial credentials should save");

        let (locked_tx, locked_rx) = sync_channel(0);
        let (release_tx, release_rx) = sync_channel(0);
        let writer_path = credential_file.clone();
        let writer = thread::spawn(move || {
            CredentialStore::new(writer_path)
                .update_selected_profile(
                    &CredentialProfileSelection::default_profile(),
                    |mut credentials| {
                        locked_tx.send(()).expect("lock signal should send");
                        release_rx.recv().expect("release signal should arrive");
                        credentials.access_key = Some("fresh-access".to_string());
                        Ok(credentials)
                    },
                )
                .expect("concurrent credential update should save");
        });
        locked_rx.recv().expect("writer should acquire the lock");

        let blocked = sync_bbdown_rust_credentials_from_state(&path, &credential_file, None);
        release_tx.send(()).expect("writer should release");
        writer.join().expect("writer thread should finish");

        let error = blocked.expect_err("migration must not race a core credential update");
        assert!(format!("{error:#}").contains("locked by another update"));
        assert!(
            sync_bbdown_rust_credentials_from_state(&path, &credential_file, None)
                .expect("migration retry should succeed")
        );
        let stored = CredentialStore::new(credential_file)
            .load()
            .expect("stored credentials should load");
        assert_eq!(
            stored.cookie.as_deref(),
            Some("SESSDATA=secret; bili_jct=csrf")
        );
        assert_eq!(stored.access_key.as_deref(), Some("fresh-access"));

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_lock_supports_a_symlinked_credential_parent_directory() {
        use std::os::unix::fs::symlink;

        let state_path = temp_state_file("symlinked-credential-parent");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_parent = root.join("credential-store");
        let credential_parent_link = root.join("credential-store-link");
        fs::create_dir_all(&credential_parent).expect("credential parent should create");
        symlink(&credential_parent, &credential_parent_link)
            .expect("credential parent symlink should create");
        let credential_file = credential_parent_link.join("credentials.json");

        let (_, epoch) = with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
            .expect("auth transaction should support a symlinked credential parent");
        let reply_lock = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth reply lock should reopen through the symlinked parent");

        assert_eq!(epoch, 1);
        assert_eq!(reply_lock.current_epoch().unwrap(), epoch);
        let lock_name = auth_mutation_lock_path(&credential_file)
            .file_name()
            .expect("auth lock should have a file name")
            .to_os_string();
        let anchor_name = auth_mutation_lock_anchor_path(&credential_file)
            .file_name()
            .expect("auth lock anchor should have a file name")
            .to_os_string();
        assert!(credential_parent.join(lock_name).is_file());
        assert!(credential_parent.join(anchor_name).is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cross_process_logout_cannot_be_undone_by_legacy_cookie_migration() {
        use std::process::Command;
        use std::sync::mpsc::sync_channel;
        use std::thread;
        use std::time::Duration;

        let state_path = temp_state_file("bbdown-rust-cross-process-logout");
        let root = state_path
            .parent()
            .expect("state should have a parent")
            .to_path_buf();
        let credential_file = root.join("credentials.json");
        let child_started = root.join("logout-child-started");
        save_auth_state(&state_path, &test_state()).expect("state should save");
        let (read_tx, read_rx) = sync_channel(0);
        let (release_tx, release_rx) = sync_channel(0);
        let migration_state = state_path.clone();
        let migration_credentials = credential_file.clone();
        let migration = thread::spawn(move || {
            sync_bbdown_rust_credentials_from_state_with_hook(
                &migration_state,
                &migration_credentials,
                None,
                || {
                    read_tx.send(()).expect("legacy read signal should send");
                    release_rx
                        .recv()
                        .expect("migration release signal should arrive");
                },
            )
        });
        read_rx
            .recv()
            .expect("migration should read legacy state while holding the shared lock");

        let mut child = Command::new(std::env::current_exe().expect("test binary should resolve"))
            .arg("--ignored")
            .arg("--exact")
            .arg("bilibili_auth::tests::cross_process_logout_child")
            .arg("--nocapture")
            .env("TVD_AUTH_RACE_CHILD_ROOT", &root)
            .spawn()
            .expect("logout child should start");
        for _ in 0..100 {
            if child_started.is_file() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            child_started.is_file(),
            "logout child did not reach the lock"
        );
        thread::sleep(Duration::from_millis(100));
        assert!(
            child
                .try_wait()
                .expect("child status should read")
                .is_none(),
            "logout must wait while migration owns the cross-process lock"
        );

        release_tx
            .send(())
            .expect("migration should be released before logout");
        assert!(
            migration
                .join()
                .expect("migration thread should finish")
                .expect("migration should succeed")
        );
        let output = child
            .wait_with_output()
            .expect("logout child output should collect");
        assert!(
            output.status.success(),
            "logout child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!state_path.exists());
        assert!(!credential_file.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[ignore = "spawned by cross_process_logout_cannot_be_undone_by_legacy_cookie_migration"]
    fn cross_process_logout_child() {
        let root = PathBuf::from(
            std::env::var_os("TVD_AUTH_RACE_CHILD_ROOT")
                .expect("child root must be provided by the parent test"),
        );
        let state_path = root.join("state.json");
        let credential_file = root.join("credentials.json");
        fs::write(root.join("logout-child-started"), b"started")
            .expect("child start marker should write");

        let (legacy, credentials) = clear_auth_state_and_credentials(
            &state_path,
            &credential_file,
            || match fs::remove_file(&credential_file) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).context("failed to clear child credentials"),
            },
        )
        .expect("child should acquire the shared auth lock");
        legacy.expect("child should clear legacy state");
        credentials.expect("child should clear credentials");
    }

    fn test_poll(code: i64, cookie: Option<String>) -> LoginPoll {
        login_poll_from_response(test_poll_response(code), cookie).expect("poll should parse")
    }

    fn test_poll_response(code: i64) -> BilibiliApiResponse<QrPollData> {
        BilibiliApiResponse {
            code: 0,
            message: "OK".to_string(),
            data: QrPollData {
                code,
                message: "message".to_string(),
                url: None,
            },
        }
    }

    fn test_poll_response_with_url(code: i64, url: &str) -> BilibiliApiResponse<QrPollData> {
        BilibiliApiResponse {
            code: 0,
            message: "OK".to_string(),
            data: QrPollData {
                code,
                message: "message".to_string(),
                url: Some(url.to_string()),
            },
        }
    }

    #[test]
    fn creates_and_deletes_bbdown_config_file() {
        let path = temp_state_file("bbdown-config");
        save_auth_state(&path, &test_state()).expect("state should save");

        let credential_file = path.with_file_name("credentials.json");
        let config_path = ensure_bbdown_config_file(&path, &credential_file, None)
            .expect("BBDown config should save")
            .expect("BBDown config should be present");
        assert!(
            config_path
                .display()
                .to_string()
                .contains(".bbdown.config.d")
        );
        let legacy_config_path = legacy_bbdown_config_path(&path);
        fs::write(&legacy_config_path, "--cookie legacy\n").expect("legacy config should write");
        let content = fs::read_to_string(&config_path).expect("BBDown config should be readable");
        assert_eq!(content, "--cookie\nSESSDATA=secret; bili_jct=csrf\n");
        assert!(delete_auth_state(&path, &credential_file).expect("auth delete should succeed"));
        assert!(!path.exists());
        assert!(config_path.exists());
        release_bbdown_config_file(&config_path);
        assert!(!config_path.exists());
        let stale_config_path = bbdown_config_dir(&path).join("stale.config.tmp");
        fs::write(&stale_config_path, "--cookie\nstale\n").expect("stale config should write");
        assert!(delete_auth_state(&path, &credential_file).expect("stale config should delete"));
        assert!(!stale_config_path.exists());
        assert!(!legacy_config_path.exists());

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn merges_base_bbdown_config_with_cookie_file() {
        let path = temp_state_file("bbdown-config-merge");
        let base_config_path = temp_state_file("bbdown-base-config");
        save_auth_state(&path, &test_state()).expect("state should save");
        fs::create_dir_all(
            base_config_path
                .parent()
                .expect("base config should have parent"),
        )
        .expect("base config parent should be created");
        fs::write(&base_config_path, "--dfn-priority\n1080P\n").expect("base config should write");

        let credential_file = path.with_file_name("credentials.json");
        let config_path =
            ensure_bbdown_config_file(&path, &credential_file, Some(&base_config_path))
                .expect("BBDown config should save")
                .expect("BBDown config should be present");
        let content = fs::read_to_string(&config_path).expect("BBDown config should be readable");

        assert_eq!(
            content,
            "--dfn-priority\n1080P\n--cookie\nSESSDATA=secret; bili_jct=csrf\n"
        );

        release_bbdown_config_file(&config_path);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&base_config_path);
        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
        if let Some(parent) = base_config_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn temp_state_paths_are_unique_per_call() {
        let path = temp_state_file("temp-state-unique");
        assert_ne!(temp_state_path(&path), temp_state_path(&path));
    }

    #[test]
    fn auth_epoch_persists_across_independent_file_locks() {
        let state_path = temp_state_file("auth-epoch-persistence");
        let credential_file = state_path.with_file_name("credentials.json");

        let initial = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("initial auth lock should open");
        assert_eq!(initial.current_epoch().expect("epoch should read"), 0);
        drop(initial);
        let lock_path = auth_mutation_lock_path(&credential_file);
        assert!(
            fs::read(&lock_path)
                .expect("initialized auth lock should read")
                .starts_with(AUTH_LOCK_HEADER)
        );

        let (_, first_epoch) =
            with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                .expect("first auth mutation should commit its epoch");
        assert_eq!(first_epoch, 1);

        let reopened = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth lock should reopen");
        assert_eq!(reopened.current_epoch().expect("epoch should persist"), 1);
        drop(reopened);

        let (_, second_epoch) = with_auth_mutation_transaction_at_epoch(
            &state_path,
            &credential_file,
            first_epoch,
            |_| Ok(()),
        )
        .expect("second auth mutation should commit its epoch");
        assert_eq!(second_epoch, 2);

        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn auth_epoch_rejects_a_stale_mutation_before_running_it() {
        use std::cell::Cell;

        let state_path = temp_state_file("auth-epoch-stale-mutation");
        let credential_file = state_path.with_file_name("credentials.json");
        let (_, current_epoch) =
            with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                .expect("initial auth mutation should commit");
        let called = Cell::new(false);

        let error = with_auth_mutation_transaction_at_epoch(
            &state_path,
            &credential_file,
            current_epoch - 1,
            |_| {
                called.set(true);
                Ok(())
            },
        )
        .expect_err("stale auth mutation must fail");

        assert!(error.to_string().contains("credential state changed"));
        assert!(!called.get());
        let current = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth lock should reopen");
        assert_eq!(
            current.current_epoch().expect("epoch should remain stable"),
            current_epoch
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_epoch_survives_primary_lock_unlink_before_a_stale_login_reply() {
        use std::cell::Cell;
        use std::os::unix::fs::MetadataExt;

        let state_path = temp_state_file("auth-epoch-primary-unlink");
        let credential_file = state_path.with_file_name("credentials.json");
        let pending_login = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("pending login should capture the initial epoch");
        let pending_epoch = pending_login.current_epoch().expect("epoch should read");
        drop(pending_login);
        let (_, logout_epoch) =
            with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                .expect("logout mutation should advance the epoch");
        assert_eq!(pending_epoch, 0);
        assert_eq!(logout_epoch, 1);

        let lock_path = auth_mutation_lock_path(&credential_file);
        let anchor_path = auth_mutation_lock_anchor_path(&credential_file);
        fs::remove_file(&lock_path).expect("primary auth lock alias should unlink");
        let called = Cell::new(false);
        let error = with_auth_mutation_transaction_at_epoch(
            &state_path,
            &credential_file,
            pending_epoch,
            |_| {
                called.set(true);
                Ok(())
            },
        )
        .expect_err("a stale pending login must remain rejected after primary unlink");

        assert!(error.to_string().contains("credential state changed"));
        assert!(!called.get());
        let primary = fs::symlink_metadata(&lock_path).expect("primary alias should be restored");
        let anchor = fs::symlink_metadata(&anchor_path).expect("anchor should remain linked");
        assert_eq!((primary.dev(), primary.ino()), (anchor.dev(), anchor.ino()));
        assert_eq!(primary.nlink(), 2);
        let current = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth lock should reopen from the anchored epoch");
        assert_eq!(current.current_epoch().unwrap(), logout_epoch);
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_lock_rejects_a_replaced_primary_alias_unchanged() {
        let state_path = temp_state_file("auth-lock-replaced-primary");
        let credential_file = state_path.with_file_name("credentials.json");
        drop(
            acquire_auth_reply_file_lock(&state_path, &credential_file)
                .expect("auth lock should initialize"),
        );
        let lock_path = auth_mutation_lock_path(&credential_file);
        let anchor_path = auth_mutation_lock_anchor_path(&credential_file);
        fs::remove_file(&lock_path).expect("primary auth lock alias should unlink");
        fs::write(&lock_path, b"user-owned replacement").expect("replacement should be written");

        let error = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect_err("a replaced primary auth lock alias must fail closed");

        assert!(format!("{error:#}").contains("path or ownership changed"));
        assert_eq!(fs::read(&lock_path).unwrap(), b"user-owned replacement");
        assert!(anchor_path.is_file());
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn auth_epoch_migrates_an_incomplete_legacy_log_to_fixed_slots() {
        let state_path = temp_state_file("auth-epoch-incomplete-record");
        let credential_file = state_path.with_file_name("credentials.json");
        let lock_path = auth_mutation_lock_path(&credential_file);
        fs::create_dir_all(lock_path.parent().expect("lock path should have a parent"))
            .expect("lock parent should create");
        fs::write(&lock_path, b"1\n2").expect("legacy epoch log should write");

        let (_, repaired_epoch) =
            with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                .expect("next auth mutation should migrate the epoch log");

        assert_eq!(repaired_epoch, 2);
        let reopened = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth lock should reopen");
        assert_eq!(reopened.current_epoch().expect("epoch should persist"), 2);
        assert!(
            fs::metadata(&lock_path)
                .expect("auth epoch file should exist")
                .len()
                <= AUTH_EPOCH_LOG_LIMIT + (AUTH_EPOCH_SLOT_SIZE * AUTH_EPOCH_SLOT_COUNT) as u64
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn auth_epoch_near_legacy_limit_migrates_without_growing_again() {
        let state_path = temp_state_file("auth-epoch-near-limit");
        let credential_file = state_path.with_file_name("credentials.json");
        let lock_path = auth_mutation_lock_path(&credential_file);
        fs::create_dir_all(lock_path.parent().expect("lock path should have a parent"))
            .expect("lock parent should create");
        let mut legacy = String::new();
        let mut epoch = 0_u64;
        loop {
            let next = epoch + 1;
            let record = format!("{next}\n");
            if legacy.len() + record.len() > AUTH_EPOCH_LOG_LIMIT as usize {
                break;
            }
            legacy.push_str(&record);
            epoch = next;
        }
        fs::write(&lock_path, legacy).expect("near-limit legacy epoch log should write");

        let (_, migrated_epoch) =
            with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                .expect("near-limit legacy epoch should migrate");
        assert_eq!(migrated_epoch, epoch + 1);
        for expected in (migrated_epoch + 1)..=(migrated_epoch + 4) {
            let (_, observed) =
                with_auth_mutation_transaction(&state_path, &credential_file, |_| Ok(()))
                    .expect("fixed epoch slot should continue advancing");
            assert_eq!(observed, expected);
        }
        assert_eq!(
            fs::metadata(&lock_path)
                .expect("auth epoch file should exist")
                .len(),
            AUTH_EPOCH_LOG_LIMIT + (AUTH_EPOCH_SLOT_SIZE * AUTH_EPOCH_SLOT_COUNT) as u64
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn no_op_legacy_sync_keeps_the_auth_epoch_stable() {
        let state_path = temp_state_file("auth-epoch-no-op-sync");
        let credential_file = state_path.with_file_name("credentials.json");
        save_auth_state(&state_path, &test_state()).expect("state should save");
        fs::write(&credential_file, r#"{"cookie":"fresh-cookie"}"#)
            .expect("credential file should write");

        let (sync_result, observed_epoch) =
            sync_bbdown_rust_credentials_from_state_with_epoch(&state_path, &credential_file, None)
                .expect("no-op credential sync should succeed");

        assert!(!sync_result.expect("sync result should be available"));
        assert_eq!(observed_epoch, 0);
        let reopened = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect("auth lock should open");
        assert_eq!(reopened.current_epoch().expect("epoch should read"), 0);
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_mutation_lock_rejects_unknown_regular_file_unchanged() {
        use std::os::unix::fs::PermissionsExt;

        let state_path = temp_state_file("auth-lock-unknown-regular");
        let credential_file = state_path.with_file_name("credentials.json");
        let lock_path = auth_mutation_lock_path(&credential_file);
        fs::create_dir_all(lock_path.parent().expect("lock path should have a parent"))
            .expect("lock parent should create");
        fs::write(&lock_path, b"unrelated user data").expect("unrelated file should write");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644))
            .expect("unrelated permissions should set");

        let error = acquire_auth_reply_file_lock(&state_path, &credential_file)
            .expect_err("unknown regular file must not be adopted as an auth lock");

        assert!(format!("{error:#}").contains("not a recognized legacy epoch log"));
        assert_eq!(fs::read(&lock_path).unwrap(), b"unrelated user data");
        assert_eq!(
            fs::metadata(&lock_path).unwrap().permissions().mode() & 0o777,
            0o644
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn saves_auth_state_with_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_state_file("state-permissions");
        save_auth_state(&path, &test_state()).expect("state should save");

        let mode = fs::metadata(&path)
            .expect("state metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_mutation_lock_never_follows_a_symlink() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let state_path = temp_state_file("auth-lock-symlink");
        let credential_file = state_path.with_file_name("credentials.json");
        let victim = state_path.with_file_name("victim.txt");
        save_auth_state(&state_path, &test_state()).expect("state should save");
        fs::write(&victim, b"victim").expect("victim should write");
        fs::set_permissions(&victim, fs::Permissions::from_mode(0o644))
            .expect("victim permissions should set");
        symlink(&victim, auth_mutation_lock_path(&credential_file))
            .expect("lock symlink should create");

        let error = sync_bbdown_rust_credentials_from_state(&state_path, &credential_file, None)
            .expect_err("symlinked auth lock must be rejected");

        assert!(format!("{error:#}").contains("failed to open BBDown auth lock"));
        assert_eq!(fs::read(&victim).unwrap(), b"victim");
        assert_eq!(
            fs::metadata(&victim).unwrap().permissions().mode() & 0o777,
            0o644
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn auth_mutation_lock_rejects_hard_link_aliases() {
        use std::os::unix::fs::PermissionsExt;

        let state_path = temp_state_file("auth-lock-hard-link");
        let credential_file = state_path.with_file_name("credentials.json");
        let victim = state_path.with_file_name("victim.txt");
        save_auth_state(&state_path, &test_state()).expect("state should save");
        fs::write(&victim, b"victim").expect("victim should write");
        fs::set_permissions(&victim, fs::Permissions::from_mode(0o644))
            .expect("victim permissions should set");
        fs::hard_link(&victim, auth_mutation_lock_path(&credential_file))
            .expect("lock hard link should create");

        let error = sync_bbdown_rust_credentials_from_state(&state_path, &credential_file, None)
            .expect_err("aliased auth lock must be rejected");

        assert!(format!("{error:#}").contains("hard-link aliases"));
        assert_eq!(fs::read(&victim).unwrap(), b"victim");
        assert_eq!(
            fs::metadata(&victim).unwrap().permissions().mode() & 0o777,
            0o644
        );
        if let Some(parent) = state_path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn does_not_chmod_existing_auth_parent_directory() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_state_file("state-existing-parent-permissions");
        let parent = path.parent().expect("state should have parent");
        fs::create_dir_all(parent).expect("parent should be created");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o755))
            .expect("parent permissions should be set");

        save_auth_state(&path, &test_state()).expect("state should save");

        let parent_mode = fs::metadata(parent)
            .expect("parent metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(&path)
            .expect("state metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(parent_mode, 0o755);
        assert_eq!(file_mode, 0o600);

        let _ = fs::remove_dir_all(parent);
    }
}
