use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use bbdown_core::{
    AccessKeyLoginConfig, AccessKeyLoginCredentials, AccessKeyLoginTicket, BiliClient,
    ClientConfig, CredentialHealthReport, CredentialProfileSelection, CredentialSource,
    CredentialStore, Credentials, DanmakuFormat, DownloadMode, DownloadOptions, EndpointConfig,
    MediaHostOptions, MuxOptions, PlayurlMode, RestrictedArea, RestrictedAreaConfig,
    RestrictedAreaProxy, RestrictedAreaProxyKind, RetryPolicy, Selection, StreamSelection,
};

use crate::config::AppConfig;
use crate::router::BilibiliSelection;
use reqwest::redirect::Policy;
use url::Url;

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const BILIBILI_BROWSER_USER_AGENT: &str = "Mozilla/5.0";
const B23_SHORT_LINK_MAX_REDIRECTS: usize = 5;
const DEFAULT_ACCESS_KEY_AUTH_BASE: &str = "https://www.biliplus.com";
const DEFAULT_ACCESS_KEY_CALLBACK_ORIGIN: &str = "https://www.bilibili.com";
const BALH_LOGIN_CREDENTIALS_PREFIX: &str = "balh-login-credentials:";
const LEGACY_GLOBAL_VALUE_FLAGS: &[&str] = &[
    "--api-base",
    "--pgc-base",
    "--intl-base",
    "--comment-base",
    "--passport-base",
    "--tv-api-base",
    "--tv-passport-base",
    "--tv-passport-poll-base",
    "--app-grpc-base",
    "--app-pgc-grpc-base",
    "--playurl-mode",
    "--restricted-area",
    "--restricted-area-proxy",
    "--restricted-api-proxy",
    "--request-timeout-seconds",
];
const LEGACY_ENDPOINT_BASE_FLAGS: &[&str] = &[
    "--api-base",
    "--pgc-base",
    "--intl-base",
    "--comment-base",
    "--passport-base",
    "--tv-api-base",
    "--tv-passport-base",
    "--tv-passport-poll-base",
    "--app-grpc-base",
    "--app-pgc-grpc-base",
];

#[derive(Clone, Debug)]
pub struct CredentialRuntime {
    credential_file: PathBuf,
    selection: CredentialProfileSelection,
}

impl CredentialRuntime {
    pub fn from_config(config: &AppConfig) -> Result<Self> {
        Self::from_credential_file(
            config.bilibili.auth.credential_file.clone(),
            config.bilibili.auth.credential_profile.as_deref(),
        )
    }

    pub fn from_credential_file(
        credential_file: PathBuf,
        credential_profile: Option<&str>,
    ) -> Result<Self> {
        crate::bilibili_auth::validated_credential_store_path(&credential_file)?;
        Ok(Self {
            credential_file,
            selection: credential_profile_selection(credential_profile)?,
        })
    }

    fn store(&self) -> Result<CredentialStore> {
        Ok(CredentialStore::new(
            crate::bilibili_auth::validated_credential_store_path(&self.credential_file)?,
        ))
    }

    pub fn load(&self) -> Result<Credentials> {
        self.store()?
            .load_selected_profile(&self.selection)
            .context("failed to load BBDown credentials")
    }

    pub fn save_merged(&self, credentials: Credentials) -> Result<CredentialSource> {
        let stored = self
            .store()?
            .update_selected_profile(&self.selection, |mut stored| {
                merge_credentials(&mut stored, credentials);
                Ok(stored)
            })
            .context("failed to save BBDown credentials")?;
        Ok(stored.redacted_summary())
    }
}

pub fn credential_runtime(config: &AppConfig) -> Result<CredentialRuntime> {
    CredentialRuntime::from_config(config)
}

pub fn client(config: &AppConfig) -> Result<BiliClient> {
    let credentials = credential_runtime(config)?.load()?;
    Ok(BiliClient::new(client_config(config, credentials)?))
}

pub fn anonymous_client(config: &AppConfig) -> Result<BiliClient> {
    Ok(BiliClient::new(client_config(
        config,
        Credentials::default(),
    )?))
}

pub async fn resolve_b23_short_link(config: &AppConfig, raw_url: &str) -> Result<String> {
    let mut current = Url::parse(raw_url).context("invalid Bilibili short link URL")?;
    ensure!(
        is_b23_short_link_url(&current),
        "Bilibili short link must be an anonymous http(s) b23.tv URL"
    );

    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(request_timeout(config)?)
        .user_agent(BILIBILI_BROWSER_USER_AGENT)
        .build()
        .context("failed to construct Bilibili short-link client")?;

    for redirect_count in 0..=B23_SHORT_LINK_MAX_REDIRECTS {
        let response = client
            .get(current.clone())
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Bilibili short-link request failed"))?;
        if let Some(target) = terminal_b23_short_link_target(&current, response.status())? {
            return Ok(target);
        }
        ensure!(
            redirect_count < B23_SHORT_LINK_MAX_REDIRECTS,
            "Bilibili short link exceeded redirect limit"
        );
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .context("Bilibili short link redirect did not include a valid location")?;
        let next = current
            .join(location)
            .context("Bilibili short link redirect location was invalid")?;
        ensure!(
            is_allowed_b23_redirect_hop(&next),
            "Bilibili short link redirected to an unsupported host"
        );
        current = next;
    }

    bail!("Bilibili short link exceeded redirect limit")
}

fn terminal_b23_short_link_target(
    current: &Url,
    status: reqwest::StatusCode,
) -> Result<Option<String>> {
    if status.is_redirection() {
        return Ok(None);
    }
    if is_bilibili_short_link_target(current) {
        return Ok(Some(current.to_string()));
    }
    bail!("Bilibili short link returned status {status}")
}

fn is_b23_short_link_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case("b23.tv"))
        && url.username().is_empty()
        && url.password().is_none()
}

fn is_bilibili_short_link_target(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.username().is_empty()
        && url.password().is_none()
        && url.host_str().is_some_and(|host| {
            let host = host.to_ascii_lowercase();
            host == "bilibili.com"
                || host.ends_with(".bilibili.com")
                || host == "bilibili.tv"
                || host.ends_with(".bilibili.tv")
        })
}

fn is_allowed_b23_redirect_hop(url: &Url) -> bool {
    is_b23_short_link_url(url) || is_bilibili_short_link_target(url)
}

pub async fn credential_health(config: &AppConfig) -> Result<CredentialHealthReport> {
    Ok(client(config)?.check_credential_health().await)
}

pub fn create_access_key_ticket() -> Result<AccessKeyLoginTicket> {
    Ok(AccessKeyLoginConfig::new(
        DEFAULT_ACCESS_KEY_AUTH_BASE,
        DEFAULT_ACCESS_KEY_CALLBACK_ORIGIN,
    )?
    .ticket()?)
}

pub fn access_key_login_credentials(
    ticket: &AccessKeyLoginTicket,
    input: &str,
) -> Result<Credentials> {
    Ok(parse_access_key_login_input(ticket, input)?.credentials())
}

pub fn looks_like_access_key_login_input(input: &str) -> bool {
    let input = input.trim();
    input.starts_with(BALH_LOGIN_CREDENTIALS_PREFIX)
        || (input.starts_with('{') && input.contains("\"access_key\""))
        || (input.starts_with('{') && input.contains("\"access_token\""))
        || input.contains("access_key=")
        || input.contains("access_token=")
}

pub fn selection(selection: Option<BilibiliSelection>) -> Option<Selection> {
    selection.map(|selection| match selection {
        BilibiliSelection::Latest => Selection::Latest,
        BilibiliSelection::All => Selection::All,
        BilibiliSelection::Page(page) => Selection::Page(page),
    })
}

pub fn download_options(config: &AppConfig) -> Result<DownloadOptions> {
    let mode = download_mode_from_config(config)?;
    Ok(DownloadOptions::new(output_dir(config))
        .with_retry_policy(RetryPolicy::default())
        .with_stream_selection(StreamSelection::default())
        .with_download_idle_timeout(Some(Duration::from_secs(
            config.bot.command_idle_timeout_seconds,
        )))
        .with_download_mode(mode)
        .with_danmaku(config.bilibili.danmaku.enabled)
        .with_danmaku_formats(danmaku_formats(config)?)
        .with_media_hosts(MediaHostOptions::bbdown_cli_default())
        .with_mux(MuxOptions::ffmpeg(config.tools.ffmpeg.clone())))
}

pub fn output_dir(config: &AppConfig) -> PathBuf {
    config.downloads.video_dir.clone()
}

pub(crate) fn validate_legacy_direct_api_config(config: &AppConfig) -> Result<()> {
    validate_legacy_bilibili_args(
        &config.bilibili.extra_args,
        "bilibili.extra_args",
        true,
        true,
    )?;
    validate_legacy_bilibili_args(
        &config.bilibili.global_args,
        "bilibili.global_args",
        false,
        true,
    )?;
    validate_legacy_bilibili_args(
        &config.bilibili.download_args,
        "bilibili.download_args",
        true,
        false,
    )?;
    if !config.bilibili.plan_args.is_empty() {
        bail!(
            "bilibili.plan_args is not supported by the direct bbdown-core downloader; migrate to structured bilibili settings"
        );
    }

    let implicit_config = config.downloads.video_dir.join("BBDown.config");
    match fs::symlink_metadata(&implicit_config) {
        Ok(_) => bail!(
            "legacy BBDown config {} is not supported by the direct bbdown-core downloader; migrate its settings to config.toml or remove the file",
            implicit_config.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect legacy BBDown config {}",
                implicit_config.display()
            )
        }),
    }
}

pub(crate) fn validate_structured_restricted_proxy_config(config: &AppConfig) -> Result<()> {
    validate_structured_restricted_proxy_values(
        &config.bilibili.restricted_area_proxies,
        RestrictedAreaProxyKind::PlayUrl,
        "bilibili.restricted_area_proxies",
    )?;
    validate_structured_restricted_proxy_values(
        &config.bilibili.restricted_api_proxies,
        RestrictedAreaProxyKind::BilibiliApi,
        "bilibili.restricted_api_proxies",
    )
}

fn validate_structured_restricted_proxy_values(
    values: &[String],
    kind: RestrictedAreaProxyKind,
    setting: &str,
) -> Result<()> {
    for value in values.iter().filter(|value| !value.trim().is_empty()) {
        parse_restricted_proxy_spec(value, kind)
            .with_context(|| format!("invalid Bilibili restricted-area proxy in {setting}"))?;
    }
    Ok(())
}

fn validate_legacy_bilibili_args(
    args: &[String],
    setting: &str,
    allow_download_modes: bool,
    allow_global_options: bool,
) -> Result<()> {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if allow_global_options
            && let Some(flag) = LEGACY_GLOBAL_VALUE_FLAGS
                .iter()
                .copied()
                .find(|flag| legacy_arg_matches_value_flag(arg, flag))
        {
            let value = legacy_arg_value(args, index, flag)?;
            index = consume_legacy_value_arg(args, index, flag, setting)?;
            validate_legacy_global_value(flag, value, setting)?;
            continue;
        }
        if allow_download_modes && legacy_arg_matches_value_flag(arg, "--only") {
            let next = consume_legacy_value_arg(args, index, "--only", setting)?;
            let value = legacy_arg_value(args, index, "--only")?;
            parse_download_mode(value)
                .with_context(|| format!("invalid legacy Bilibili download mode in {setting}"))?;
            index = next;
            continue;
        }
        if allow_download_modes
            && ["--audio-only", "--video-only"]
                .iter()
                .any(|flag| legacy_arg_matches_boolean_flag(arg, flag))
        {
            index = consume_legacy_boolean_arg(args, index, setting)?;
            continue;
        }
        bail!(
            "unsupported legacy BBDown option {} in {setting}; migrate it to a direct bbdown-core setting before starting the bot",
            legacy_arg_label(arg)
        );
    }
    Ok(())
}

fn validate_legacy_global_value(flag: &str, value: &str, setting: &str) -> Result<()> {
    let result = if LEGACY_ENDPOINT_BASE_FLAGS.contains(&flag) {
        validate_endpoint_base(value, flag)
    } else {
        match flag {
            "--playurl-mode" => parse_playurl_mode(value).map(|_| ()),
            "--restricted-area" => parse_restricted_area(value).map(|_| ()),
            "--restricted-area-proxy" => {
                validate_legacy_restricted_proxy_values(value, RestrictedAreaProxyKind::PlayUrl)
            }
            "--restricted-api-proxy" => {
                validate_legacy_restricted_proxy_values(value, RestrictedAreaProxyKind::BilibiliApi)
            }
            "--request-timeout-seconds" => parse_request_timeout(value).map(|_| ()),
            _ => unreachable!("validated legacy flag must have a parser"),
        }
    };
    result.with_context(|| format!("invalid legacy BBDown option {flag} in {setting}"))
}

fn validate_legacy_restricted_proxy_values(
    value: &str,
    kind: RestrictedAreaProxyKind,
) -> Result<()> {
    for spec in value.split(',').filter(|spec| !spec.trim().is_empty()) {
        parse_restricted_proxy_spec(spec, kind)?;
    }
    Ok(())
}

fn legacy_arg_matches_value_flag(arg: &str, flag: &str) -> bool {
    arg == flag
        || arg
            .strip_prefix(flag)
            .is_some_and(|suffix| suffix.starts_with('='))
}

fn legacy_arg_matches_boolean_flag(arg: &str, flag: &str) -> bool {
    arg == flag
        || arg
            .strip_prefix(flag)
            .is_some_and(|suffix| suffix.starts_with('=') || suffix.starts_with(':'))
}

fn consume_legacy_value_arg(
    args: &[String],
    index: usize,
    flag: &str,
    setting: &str,
) -> Result<usize> {
    let value = legacy_arg_value(args, index, flag)?;
    ensure!(
        !value.trim().is_empty(),
        "legacy BBDown option {flag} in {setting} requires a non-empty value"
    );
    Ok(if args[index] == flag {
        index + 2
    } else {
        index + 1
    })
}

fn legacy_arg_value<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str> {
    let arg = &args[index];
    if arg == flag {
        return args
            .get(index + 1)
            .map(String::as_str)
            .with_context(|| format!("legacy BBDown option {flag} requires a value"));
    }
    arg.strip_prefix(flag)
        .and_then(|suffix| suffix.strip_prefix('='))
        .with_context(|| format!("invalid legacy BBDown option {flag}"))
}

fn consume_legacy_boolean_arg(args: &[String], index: usize, setting: &str) -> Result<usize> {
    if args[index].starts_with("--audio-only") || args[index].starts_with("--video-only") {
        if (args[index] == "--audio-only" || args[index] == "--video-only")
            && args
                .get(index + 1)
                .is_some_and(|value| parse_bool_token(value).is_some())
        {
            return Ok(index + 2);
        }
        if let Some(value) = args[index].split_once(['=', ':']).map(|(_, value)| value) {
            ensure!(
                parse_bool_token(value).is_some(),
                "legacy BBDown boolean option {} in {setting} must be true, false, 1, or 0",
                legacy_arg_label(&args[index])
            );
        }
        return Ok(index + 1);
    }
    Ok(index + 1)
}

fn legacy_arg_label(arg: &str) -> &str {
    arg.split(['=', ':']).next().unwrap_or("<value>")
}

pub fn download_mode_from_config(config: &AppConfig) -> Result<DownloadMode> {
    let mut mode = DownloadMode::All;
    apply_legacy_download_modes(&config.bilibili.extra_args, &mut mode)?;
    apply_legacy_download_modes(&config.bilibili.download_args, &mut mode)?;
    Ok(mode)
}

fn apply_legacy_download_modes(args: &[String], mode: &mut DownloadMode) -> Result<()> {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if let Some((directive, consumed)) = legacy_download_mode_arg_value(args, index)? {
            match directive {
                LegacyDownloadModeDirective::Set(next) => *mode = next,
                LegacyDownloadModeDirective::Clear(target) if *mode == target => {
                    *mode = DownloadMode::All;
                }
                LegacyDownloadModeDirective::Clear(_) => {}
            }
            index += consumed;
            continue;
        }
        if arg == "--only" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| anyhow::anyhow!("bilibili --only requires a value"))?;
            *mode = parse_download_mode(value)?;
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--only=") {
            *mode = parse_download_mode(value)?;
        }
        index += 1;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyDownloadModeDirective {
    Set(DownloadMode),
    Clear(DownloadMode),
}

fn credential_profile_selection(profile: Option<&str>) -> Result<CredentialProfileSelection> {
    match profile.map(str::trim).filter(|profile| !profile.is_empty()) {
        Some(profile) => CredentialProfileSelection::named(profile)
            .map_err(anyhow::Error::from)
            .context("invalid BBDown credential profile"),
        None => Ok(CredentialProfileSelection::default_profile()),
    }
}

fn client_config(config: &AppConfig, credentials: Credentials) -> Result<ClientConfig> {
    Ok(ClientConfig::new(endpoint_config(config)?, credentials)
        .with_restricted_area(restricted_area_config(config)?)
        .with_playurl_mode(playurl_mode(config)?)
        .with_user_agent(BILIBILI_BROWSER_USER_AGENT)
        .with_request_timeout(request_timeout(config)?))
}

fn playurl_mode(config: &AppConfig) -> Result<PlayurlMode> {
    let mode = config
        .bilibili
        .playurl_mode
        .as_deref()
        .or_else(|| last_bilibili_arg_value(config, "--playurl-mode").map(str::trim));
    mode.map(parse_playurl_mode)
        .transpose()
        .map(|mode| mode.unwrap_or(PlayurlMode::Web))
}

fn parse_playurl_mode(value: &str) -> Result<PlayurlMode> {
    match value.trim() {
        "web" => Ok(PlayurlMode::Web),
        "tv" => Ok(PlayurlMode::Tv),
        "app" => Ok(PlayurlMode::App),
        value => bail!("unsupported bilibili.playurl_mode `{value}`"),
    }
}

fn endpoint_config(config: &AppConfig) -> Result<EndpointConfig> {
    let default_endpoints = EndpointConfig::default();
    let mut endpoints = EndpointConfig::default()
        .with_api_base(legacy_endpoint_base(
            config,
            "--api-base",
            default_endpoints.api_base.as_str(),
        )?)
        .with_pgc_base(legacy_endpoint_base(
            config,
            "--pgc-base",
            default_endpoints.pgc_base.as_str(),
        )?)
        .with_intl_base(legacy_endpoint_base(
            config,
            "--intl-base",
            default_endpoints.intl_base.as_str(),
        )?)
        .with_comment_base(legacy_endpoint_base(
            config,
            "--comment-base",
            default_endpoints.comment_base.as_str(),
        )?)
        .with_passport_base(legacy_endpoint_base(
            config,
            "--passport-base",
            default_endpoints.passport_base.as_str(),
        )?)
        .with_tv_api_base(legacy_endpoint_base(
            config,
            "--tv-api-base",
            default_endpoints.tv_api_base.as_str(),
        )?)
        .with_app_grpc_base(legacy_endpoint_base(
            config,
            "--app-grpc-base",
            default_endpoints.app_grpc_base.as_str(),
        )?)
        .with_app_pgc_grpc_base(legacy_endpoint_base(
            config,
            "--app-pgc-grpc-base",
            default_endpoints.app_pgc_grpc_base.as_str(),
        )?);
    let tv_passport_base = legacy_endpoint_base(
        config,
        "--tv-passport-base",
        default_endpoints.tv_passport_base.as_str(),
    )?;
    let tv_passport_poll_base =
        if let Some(value) = last_bilibili_arg_value(config, "--tv-passport-poll-base") {
            normalized_endpoint_base(value, "--tv-passport-poll-base")?
        } else if let Some(value) = last_bilibili_arg_value(config, "--tv-passport-base") {
            normalized_endpoint_base(value, "--tv-passport-base")?
        } else {
            default_endpoints.tv_passport_poll_base.to_string()
        };
    endpoints = endpoints
        .with_tv_passport_base(tv_passport_base)
        .with_tv_passport_poll_base(tv_passport_poll_base);
    Ok(endpoints)
}

fn legacy_endpoint_base(config: &AppConfig, flag: &str, default: &str) -> Result<String> {
    let value = last_bilibili_arg_value(config, flag).unwrap_or(default);
    normalized_endpoint_base(value, flag)
}

fn normalized_endpoint_base(value: &str, flag: &str) -> Result<String> {
    validate_endpoint_base(value, flag)?;
    Ok(value.trim().to_string())
}

fn validate_endpoint_base(value: &str, flag: &str) -> Result<()> {
    let value = value.trim();
    ensure!(!value.is_empty(), "{flag} must not be empty");
    let parsed = url::Url::parse(value).with_context(|| {
        format!(
            "failed to parse {flag} URL `{}`",
            redact_url_for_error(value)
        )
    })?;
    ensure!(
        matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
        "{flag} must be an absolute http or https URL"
    );
    Ok(())
}

fn request_timeout(config: &AppConfig) -> Result<Duration> {
    match last_bilibili_arg_value(config, "--request-timeout-seconds") {
        Some(value) => parse_request_timeout(value),
        None => Ok(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECONDS)),
    }
}

fn parse_request_timeout(value: &str) -> Result<Duration> {
    let seconds = value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("invalid --request-timeout-seconds value `{value}`"))?;
    ensure!(
        seconds > 0,
        "--request-timeout-seconds must be greater than 0"
    );
    Ok(Duration::from_secs(seconds))
}

fn restricted_area_config(config: &AppConfig) -> Result<RestrictedAreaConfig> {
    let area_hint = config
        .bilibili
        .restricted_area
        .as_deref()
        .or_else(|| last_bilibili_arg_value(config, "--restricted-area").map(str::trim))
        .map(parse_restricted_area)
        .transpose()?;
    let mut proxies = Vec::new();
    for spec in &config.bilibili.restricted_area_proxies {
        if !spec.trim().is_empty() {
            proxies.push(parse_restricted_proxy_spec(
                spec,
                RestrictedAreaProxyKind::PlayUrl,
            )?);
        }
    }
    for spec in &config.bilibili.restricted_api_proxies {
        if !spec.trim().is_empty() {
            proxies.push(parse_restricted_proxy_spec(
                spec,
                RestrictedAreaProxyKind::BilibiliApi,
            )?);
        }
    }
    for spec in bilibili_arg_values(config, "--restricted-area-proxy") {
        for value in spec.split(',').filter(|value| !value.trim().is_empty()) {
            proxies.push(parse_restricted_proxy_spec(
                value,
                RestrictedAreaProxyKind::PlayUrl,
            )?);
        }
    }
    for spec in bilibili_arg_values(config, "--restricted-api-proxy") {
        for value in spec.split(',').filter(|value| !value.trim().is_empty()) {
            proxies.push(parse_restricted_proxy_spec(
                value,
                RestrictedAreaProxyKind::BilibiliApi,
            )?);
        }
    }
    Ok(RestrictedAreaConfig::new(area_hint, proxies))
}

fn parse_restricted_proxy_spec(
    spec: &str,
    kind: RestrictedAreaProxyKind,
) -> Result<RestrictedAreaProxy> {
    let trimmed = spec.trim();
    ensure!(!trimmed.is_empty(), "restricted-area proxy cannot be empty");
    let (area, base_url) = if let Some((area, base_url)) = parse_area_prefixed_proxy(trimmed)? {
        (Some(parse_restricted_area(area)?), base_url.trim())
    } else {
        (None, trimmed)
    };
    ensure!(
        !base_url.is_empty(),
        "restricted-area proxy URL cannot be empty"
    );
    let parsed = url::Url::parse(base_url).with_context(|| {
        format!(
            "failed to parse restricted-area proxy URL `{}`",
            redact_url_for_error(base_url)
        )
    })?;
    ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "restricted-area proxy URL `{}` must use http or https",
        redact_url_for_error(base_url)
    );
    Ok(match kind {
        RestrictedAreaProxyKind::PlayUrl => RestrictedAreaProxy::playurl(base_url, area),
        RestrictedAreaProxyKind::BilibiliApi => RestrictedAreaProxy::bilibili_api(base_url, area),
    })
}

fn parse_area_prefixed_proxy(spec: &str) -> Result<Option<(&str, &str)>> {
    if starts_with_url_scheme(spec) {
        return Ok(None);
    }
    let Some((area, base_url)) = spec.split_once('=') else {
        return Ok(None);
    };
    match area.trim().to_ascii_lowercase().as_str() {
        "cn" | "th" | "hk" | "tw" => Ok(Some((area, base_url))),
        other => bail!("unsupported restricted area `{other}`; expected cn, th, hk, or tw"),
    }
}

fn parse_restricted_area(value: &str) -> Result<RestrictedArea> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cn" => Ok(RestrictedArea::Cn),
        "th" => Ok(RestrictedArea::Th),
        "hk" => Ok(RestrictedArea::Hk),
        "tw" => Ok(RestrictedArea::Tw),
        other => bail!("unsupported restricted area `{other}`; expected cn, th, hk, or tw"),
    }
}

fn danmaku_formats(config: &AppConfig) -> Result<Vec<DanmakuFormat>> {
    config
        .bilibili
        .danmaku_formats
        .iter()
        .map(|format| match format.as_str() {
            "xml" => Ok(DanmakuFormat::Xml),
            "ass" => Ok(DanmakuFormat::Ass),
            value => bail!("unsupported bilibili.danmaku_formats entry `{value}`"),
        })
        .collect()
}

fn parse_access_key_login_input(
    ticket: &AccessKeyLoginTicket,
    input: &str,
) -> Result<AccessKeyLoginCredentials> {
    let input = input.trim();
    ensure!(!input.is_empty(), "access-key login input is empty");
    if input.starts_with(BALH_LOGIN_CREDENTIALS_PREFIX) {
        return Ok(ticket.credentials_from_message(&ticket.message_origin, input)?);
    }
    Ok(AccessKeyLoginCredentials::from_balh_payload(input)?)
}

fn merge_credentials(stored: &mut Credentials, credentials: Credentials) {
    if credentials.cookie.is_some() {
        stored.cookie = credentials.cookie;
    }
    if credentials.access_key.is_some() {
        stored.access_key = credentials.access_key;
    }
    if credentials.tv_access_key.is_some() {
        stored.tv_access_key = credentials.tv_access_key;
    }
}

fn legacy_download_mode_arg_value(
    args: &[String],
    index: usize,
) -> Result<Option<(LegacyDownloadModeDirective, usize)>> {
    let arg = args
        .get(index)
        .context("legacy Bilibili download mode index is out of range")?;
    for (flag, mode) in [
        ("--audio-only", DownloadMode::AudioOnly),
        ("--video-only", DownloadMode::VideoOnly),
    ] {
        if arg == flag {
            if let Some(value) = args.get(index + 1) {
                if value.starts_with("--") {
                    return Ok(Some((LegacyDownloadModeDirective::Set(mode), 1)));
                }
                let enabled = parse_bool_token(value).with_context(|| {
                    format!("legacy BBDown boolean option {flag} must be true, false, 1, or 0")
                })?;
                return Ok(Some((
                    if enabled {
                        LegacyDownloadModeDirective::Set(mode)
                    } else {
                        LegacyDownloadModeDirective::Clear(mode)
                    },
                    2,
                )));
            }
            return Ok(Some((LegacyDownloadModeDirective::Set(mode), 1)));
        }
        let equals_prefix = format!("{flag}=");
        let colon_prefix = format!("{flag}:");
        if let Some(value) = arg
            .strip_prefix(&equals_prefix)
            .or_else(|| arg.strip_prefix(&colon_prefix))
        {
            let enabled = parse_bool_token(value).with_context(|| {
                format!("legacy BBDown boolean option {flag} must be true, false, 1, or 0")
            })?;
            return Ok(Some((
                if enabled {
                    LegacyDownloadModeDirective::Set(mode)
                } else {
                    LegacyDownloadModeDirective::Clear(mode)
                },
                1,
            )));
        }
    }
    Ok(None)
}

fn parse_download_mode(value: &str) -> Result<DownloadMode> {
    Ok(match value.trim().to_ascii_lowercase().as_str() {
        "all" => DownloadMode::All,
        "video" => DownloadMode::VideoOnly,
        "audio" => DownloadMode::AudioOnly,
        "subtitle" | "subtitles" => DownloadMode::SubtitleOnly,
        "danmaku" => DownloadMode::DanmakuOnly,
        "cover" => DownloadMode::CoverOnly,
        other => bail!("unsupported Bilibili download mode `{other}`"),
    })
}

fn parse_bool_token(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

fn last_bilibili_arg_value<'a>(config: &'a AppConfig, flag: &str) -> Option<&'a str> {
    bilibili_arg_values(config, flag).last().copied()
}

fn bilibili_arg_values<'a>(config: &'a AppConfig, flag: &str) -> Vec<&'a str> {
    let mut values = arg_values(&config.bilibili.extra_args, flag);
    values.extend(arg_values(&config.bilibili.global_args, flag));
    values
}

fn arg_values<'a>(args: &'a [String], flag: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == flag
            && let Some(value) = args.get(index + 1)
        {
            values.push(value.as_str());
            index += 2;
            continue;
        }
        let equals_prefix = format!("{flag}=");
        if let Some(value) = arg.strip_prefix(&equals_prefix) {
            values.push(value);
        }
        index += 1;
    }
    values
}

fn starts_with_url_scheme(value: &str) -> bool {
    let Some(scheme_end) = value.find("://") else {
        return false;
    };
    let scheme = &value[..scheme_end];
    scheme
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphabetic)
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn redact_url_for_error(raw: &str) -> String {
    url::Url::parse(raw).map_or_else(
        |_| "<redacted url>".to_string(),
        |mut url| {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_path("");
            url.set_query(None);
            url.set_fragment(None);
            url.to_string().trim_end_matches('/').to_owned()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_runtime_trims_configured_profile_name() {
        let directory = std::env::temp_dir().join(format!(
            "telegram-video-downloader-profile-trim-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let credential_file = directory.join("credentials.json");
        let store = CredentialStore::new(credential_file.clone());
        let mut profiles = bbdown_core::CredentialProfiles::default();
        profiles
            .set_profile(
                "default",
                Credentials::default().with_cookie("SESSDATA=default"),
            )
            .unwrap();
        profiles
            .set_profile(
                "selected",
                Credentials::default().with_cookie("SESSDATA=selected"),
            )
            .unwrap();
        profiles.set_default_profile("default").unwrap();
        store.save_profiles(&profiles).unwrap();

        let runtime =
            CredentialRuntime::from_credential_file(credential_file, Some("  selected  "))
                .expect("trimmed profile should be valid");

        assert_eq!(
            runtime.load().unwrap().cookie.as_deref(),
            Some("SESSDATA=selected")
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn credential_runtime_rejects_replaceable_store_ancestry() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "telegram-video-downloader-runtime-credential-ancestry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let shared_parent = root.join("shared");
        let credential_file = shared_parent.join("credentials").join("credentials.json");
        std::fs::create_dir_all(
            credential_file
                .parent()
                .expect("credential file should have a parent"),
        )
        .unwrap();
        std::fs::set_permissions(&shared_parent, std::fs::Permissions::from_mode(0o777)).unwrap();

        let error = CredentialRuntime::from_credential_file(credential_file, None)
            .expect_err("runtime must reject a credential store another user can replace");

        assert!(format!("{error:#}").contains("permits another user to replace credentials"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn credential_runtime_revalidates_existing_store_leaf_before_each_core_call() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "telegram-video-downloader-runtime-credential-leaf-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let credential_file = directory.join("credentials.json");
        let store = CredentialStore::new(credential_file.clone());
        let mut profiles = bbdown_core::CredentialProfiles::default();
        profiles
            .set_profile(
                "default",
                Credentials::default().with_cookie("SESSDATA=default"),
            )
            .unwrap();
        store.save_profiles(&profiles).unwrap();
        std::fs::set_permissions(&credential_file, std::fs::Permissions::from_mode(0o600)).unwrap();

        let runtime = CredentialRuntime::from_credential_file(credential_file.clone(), None)
            .expect("private credential file should initialize a runtime");

        std::fs::set_permissions(&credential_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        let mode_error = runtime
            .load()
            .expect_err("runtime must reject a credential leaf with group-readable permissions");
        assert!(format!("{mode_error:#}").contains("current-user-owned private single-link"));

        std::fs::set_permissions(&credential_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let alias = directory.join("credentials.alias.json");
        std::fs::hard_link(&credential_file, &alias).unwrap();
        let link_error = runtime
            .load()
            .expect_err("runtime must reject a credential leaf with another hard link");
        assert!(format!("{link_error:#}").contains("current-user-owned private single-link"));

        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn maps_router_selection_to_core_selection() {
        assert_eq!(
            selection(Some(BilibiliSelection::Latest)),
            Some(Selection::Latest)
        );
        assert_eq!(
            selection(Some(BilibiliSelection::All)),
            Some(Selection::All)
        );
        assert_eq!(
            selection(Some(BilibiliSelection::Page(2))),
            Some(Selection::Page(2))
        );
        assert_eq!(selection(None), None);
    }

    #[test]
    fn restricts_b23_short_link_redirect_hops() {
        let b23 = Url::parse("https://b23.tv/abc").unwrap();
        let b23_with_userinfo = Url::parse("https://user:pass@b23.tv/abc").unwrap();
        let bilibili = Url::parse("https://www.bilibili.com/video/BV123?p=2").unwrap();
        let bilibili_intl = Url::parse("https://www.bilibili.tv/en/play/123/456").unwrap();
        let unexpected = Url::parse("https://example.test/redirect").unwrap();

        assert!(is_b23_short_link_url(&b23));
        assert!(!is_b23_short_link_url(&b23_with_userinfo));
        assert!(is_allowed_b23_redirect_hop(&b23));
        assert!(is_allowed_b23_redirect_hop(&bilibili));
        assert!(is_allowed_b23_redirect_hop(&bilibili_intl));
        assert!(!is_allowed_b23_redirect_hop(&unexpected));
    }

    #[test]
    fn preserves_bilibili_short_link_target_after_terminal_page_errors() {
        let target = Url::parse("https://www.bilibili.com/video/BV123?p=2").unwrap();
        assert_eq!(
            terminal_b23_short_link_target(&target, reqwest::StatusCode::FORBIDDEN).unwrap(),
            Some(target.to_string())
        );
        assert_eq!(
            terminal_b23_short_link_target(&target, reqwest::StatusCode::PRECONDITION_FAILED,)
                .unwrap(),
            Some(target.to_string())
        );
        assert_eq!(
            terminal_b23_short_link_target(&target, reqwest::StatusCode::FOUND).unwrap(),
            None
        );

        let b23 = Url::parse("https://b23.tv/abc").unwrap();
        assert!(terminal_b23_short_link_target(&b23, reqwest::StatusCode::OK).is_err());
    }

    #[test]
    fn identifies_access_key_login_input_shapes() {
        assert!(looks_like_access_key_login_input(
            r#"balh-login-credentials: {"access_key":"AK"}"#
        ));
        assert!(looks_like_access_key_login_input(
            "https://www.bilibili.com/callback?access_token=AK&refresh_token=RT"
        ));
        assert!(looks_like_access_key_login_input(
            "#access_key=AK&expires_in=60"
        ));
        assert!(looks_like_access_key_login_input(
            r#"{"access_key":"AK","refresh_token":"RT"}"#
        ));
        assert!(looks_like_access_key_login_input(
            r#"{"access_token":"AK","refresh_token":"RT"}"#
        ));
        assert!(!looks_like_access_key_login_input(
            "https://www.bilibili.com/video/BV123"
        ));
        assert!(!looks_like_access_key_login_input("普通消息"));
    }

    #[test]
    fn legacy_download_mode_false_clears_only_its_matching_mode() {
        let mut config = crate::config::AppConfig::for_test();
        config.bilibili.extra_args = vec!["--audio-only".to_string()];
        config.bilibili.download_args = vec!["--audio-only=false".to_string()];

        assert_eq!(
            download_mode_from_config(&config).unwrap(),
            DownloadMode::All
        );

        config.bilibili.extra_args = vec!["--video-only".to_string()];
        assert_eq!(
            download_mode_from_config(&config).unwrap(),
            DownloadMode::VideoOnly
        );
    }

    #[test]
    fn parses_restricted_proxy_area_prefix() {
        let proxy = parse_restricted_proxy_spec(
            "th=https://proxy.example.test",
            RestrictedAreaProxyKind::BilibiliApi,
        )
        .unwrap();
        assert_eq!(proxy.area, Some(RestrictedArea::Th));
        assert_eq!(proxy.kind, RestrictedAreaProxyKind::BilibiliApi);
        assert_eq!(proxy.base_url, "https://proxy.example.test");
    }

    #[test]
    fn reads_supported_global_args_from_legacy_extra_args() {
        let mut config = crate::config::AppConfig::for_test();
        config.bilibili.extra_args = vec![
            "--api-base".to_string(),
            "https://api.example.test".to_string(),
            "--playurl-mode=app".to_string(),
            "--restricted-area".to_string(),
            "th".to_string(),
            "--request-timeout-seconds".to_string(),
            "7".to_string(),
            "--restricted-area-proxy".to_string(),
            "th=https://proxy.example.test".to_string(),
        ];

        assert_eq!(playurl_mode(&config).unwrap(), PlayurlMode::App);
        assert_eq!(request_timeout(&config).unwrap(), Duration::from_secs(7));
        assert_eq!(
            endpoint_config(&config).unwrap().api_base,
            "https://api.example.test"
        );
        assert_eq!(
            restricted_area_config(&config).unwrap().area_hint,
            Some(RestrictedArea::Th)
        );
        assert_eq!(
            bilibili_arg_values(&config, "--restricted-area-proxy"),
            vec!["th=https://proxy.example.test"]
        );
    }

    #[test]
    fn new_global_args_override_legacy_extra_args() {
        let mut config = crate::config::AppConfig::for_test();
        config.bilibili.extra_args = vec![
            "--api-base=https://legacy-api.example.test".to_string(),
            "--playurl-mode=app".to_string(),
            "--restricted-area=th".to_string(),
            "--request-timeout-seconds=7".to_string(),
        ];
        config.bilibili.global_args = vec![
            "--api-base=https://api.example.test".to_string(),
            "--playurl-mode=tv".to_string(),
            "--restricted-area=hk".to_string(),
            "--request-timeout-seconds=9".to_string(),
        ];

        assert_eq!(playurl_mode(&config).unwrap(), PlayurlMode::Tv);
        assert_eq!(request_timeout(&config).unwrap(), Duration::from_secs(9));
        assert_eq!(
            endpoint_config(&config).unwrap().api_base,
            "https://api.example.test"
        );
        assert_eq!(
            restricted_area_config(&config).unwrap().area_hint,
            Some(RestrictedArea::Hk)
        );
    }

    #[test]
    fn structured_fields_override_legacy_and_global_args() {
        let mut config = crate::config::AppConfig::for_test();
        config.bilibili.extra_args = vec!["--playurl-mode=app".to_string()];
        config.bilibili.global_args = vec![
            "--playurl-mode=tv".to_string(),
            "--restricted-area=hk".to_string(),
        ];
        config.bilibili.playurl_mode = Some("web".to_string());
        config.bilibili.restricted_area = Some("cn".to_string());

        assert_eq!(playurl_mode(&config).unwrap(), PlayurlMode::Web);
        assert_eq!(
            restricted_area_config(&config).unwrap().area_hint,
            Some(RestrictedArea::Cn)
        );
    }

    #[test]
    fn uses_browser_user_agent_for_bilibili_media_requests() {
        let config = crate::config::AppConfig::for_test();
        let client_config = client_config(&config, Credentials::default()).unwrap();

        assert_eq!(client_config.user_agent, BILIBILI_BROWSER_USER_AGENT);
    }
}
