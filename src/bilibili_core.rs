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

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const BILIBILI_BROWSER_USER_AGENT: &str = "Mozilla/5.0";
const DEFAULT_ACCESS_KEY_AUTH_BASE: &str = "https://www.biliplus.com";
const DEFAULT_ACCESS_KEY_CALLBACK_ORIGIN: &str = "https://www.bilibili.com";
const BALH_LOGIN_CREDENTIALS_PREFIX: &str = "balh-login-credentials:";

#[derive(Clone, Debug)]
pub struct CredentialRuntime {
    store: CredentialStore,
    selection: CredentialProfileSelection,
}

impl CredentialRuntime {
    pub fn from_config(config: &AppConfig) -> Result<Self> {
        Ok(Self {
            store: CredentialStore::new(config.bilibili.auth.credential_file.clone()),
            selection: credential_profile_selection(
                config.bilibili.auth.credential_profile.as_deref(),
            )?,
        })
    }

    pub fn load(&self) -> Result<Credentials> {
        self.store
            .load_selected_profile(&self.selection)
            .context("failed to load BBDown credentials")
    }

    pub fn save_merged(&self, credentials: Credentials) -> Result<CredentialSource> {
        let mut stored = self.load()?;
        merge_credentials(&mut stored, credentials);
        self.store
            .save_selected_profile(&self.selection, &stored)
            .context("failed to save BBDown credentials")?;
        Ok(stored.redacted_summary())
    }

    pub fn logout(&self) -> Result<()> {
        match self.selection.profile_name() {
            Some(profile) => {
                self.store
                    .remove_profile(profile)
                    .context("failed to clear BBDown credential profile")?;
            }
            None => {
                let profile = self
                    .store
                    .load_profiles()
                    .context("failed to load BBDown credential profiles")?
                    .default_profile;
                self.store
                    .remove_profile(&profile)
                    .context("failed to clear BBDown default credential profile")?;
            }
        }
        Ok(())
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

pub fn download_mode_from_config(config: &AppConfig) -> Result<DownloadMode> {
    let mut args = legacy_bilibili_extra_args_for_direct_api(&config.bilibili.extra_args);
    args.extend(config.bilibili.download_args.clone());
    let mut mode = DownloadMode::All;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--only" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| anyhow::anyhow!("bilibili --only requires a value"))?;
            mode = parse_download_mode(value)?;
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--only=") {
            mode = parse_download_mode(value)?;
        }
        index += 1;
    }
    Ok(mode)
}

fn credential_profile_selection(profile: Option<&str>) -> Result<CredentialProfileSelection> {
    match profile {
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
        .or_else(|| last_global_arg_value(config, "--playurl-mode").map(str::trim));
    Ok(match mode {
        Some("web") | None => PlayurlMode::Web,
        Some("tv") => PlayurlMode::Tv,
        Some("app") => PlayurlMode::App,
        Some(value) => bail!("unsupported bilibili.playurl_mode `{value}`"),
    })
}

fn endpoint_config(config: &AppConfig) -> Result<EndpointConfig> {
    let default_endpoints = EndpointConfig::default();
    let mut endpoints = EndpointConfig::default()
        .with_api_base(
            last_global_arg_value(config, "--api-base")
                .unwrap_or(default_endpoints.api_base.as_str())
                .to_string(),
        )
        .with_pgc_base(
            last_global_arg_value(config, "--pgc-base")
                .unwrap_or(default_endpoints.pgc_base.as_str())
                .to_string(),
        )
        .with_intl_base(
            last_global_arg_value(config, "--intl-base")
                .unwrap_or(default_endpoints.intl_base.as_str())
                .to_string(),
        )
        .with_comment_base(
            last_global_arg_value(config, "--comment-base")
                .unwrap_or(default_endpoints.comment_base.as_str())
                .to_string(),
        )
        .with_passport_base(
            last_global_arg_value(config, "--passport-base")
                .unwrap_or(default_endpoints.passport_base.as_str())
                .to_string(),
        )
        .with_tv_api_base(
            last_global_arg_value(config, "--tv-api-base")
                .unwrap_or(default_endpoints.tv_api_base.as_str())
                .to_string(),
        )
        .with_app_grpc_base(
            last_global_arg_value(config, "--app-grpc-base")
                .unwrap_or(default_endpoints.app_grpc_base.as_str())
                .to_string(),
        )
        .with_app_pgc_grpc_base(
            last_global_arg_value(config, "--app-pgc-grpc-base")
                .unwrap_or(default_endpoints.app_pgc_grpc_base.as_str())
                .to_string(),
        );
    let tv_passport_base = last_global_arg_value(config, "--tv-passport-base")
        .unwrap_or(default_endpoints.tv_passport_base.as_str())
        .to_string();
    let tv_passport_poll_base = last_global_arg_value(config, "--tv-passport-poll-base")
        .or_else(|| last_global_arg_value(config, "--tv-passport-base"))
        .unwrap_or(default_endpoints.tv_passport_poll_base.as_str())
        .to_string();
    endpoints = endpoints
        .with_tv_passport_base(tv_passport_base)
        .with_tv_passport_poll_base(tv_passport_poll_base);
    Ok(endpoints)
}

fn request_timeout(config: &AppConfig) -> Result<Duration> {
    let seconds = match last_global_arg_value(config, "--request-timeout-seconds") {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("invalid --request-timeout-seconds value `{value}`"))?,
        None => DEFAULT_REQUEST_TIMEOUT_SECONDS,
    };
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
        .or_else(|| last_global_arg_value(config, "--restricted-area").map(str::trim))
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
    for spec in global_arg_values(config, "--restricted-area-proxy") {
        for value in spec.split(',').filter(|value| !value.trim().is_empty()) {
            proxies.push(parse_restricted_proxy_spec(
                value,
                RestrictedAreaProxyKind::PlayUrl,
            )?);
        }
    }
    for spec in global_arg_values(config, "--restricted-api-proxy") {
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

fn legacy_bilibili_extra_args_for_direct_api(args: &[String]) -> Vec<String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if let Some((mode, consumed)) = legacy_download_mode_arg_value(args, index) {
            if let Some(mode) = mode {
                filtered.extend(["--only".to_string(), mode.to_string()]);
            }
            index += consumed;
            continue;
        }
        match arg.as_str() {
            "--only" => {
                if let Some(value) = args.get(index + 1) {
                    filtered.extend(["--only".to_string(), value.clone()]);
                    index += 2;
                } else {
                    filtered.push(arg.clone());
                    index += 1;
                }
            }
            _ if arg.starts_with("--only=") => {
                filtered.push(arg.clone());
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    filtered
}

fn legacy_download_mode_arg_value(
    args: &[String],
    index: usize,
) -> Option<(Option<&'static str>, usize)> {
    for (flag, mode) in [("--audio-only", "audio"), ("--video-only", "video")] {
        let arg = args.get(index)?;
        if arg == flag {
            if let Some(value) = args
                .get(index + 1)
                .and_then(|value| parse_bool_token(value))
            {
                return Some((value.then_some(mode), 2));
            }
            return Some((Some(mode), 1));
        }
        let equals_prefix = format!("{flag}=");
        let colon_prefix = format!("{flag}:");
        if let Some(value) = arg
            .strip_prefix(&equals_prefix)
            .or_else(|| arg.strip_prefix(&colon_prefix))
        {
            return Some((parse_bool_token(value).unwrap_or(true).then_some(mode), 1));
        }
    }
    None
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

fn last_global_arg_value<'a>(config: &'a AppConfig, flag: &str) -> Option<&'a str> {
    global_arg_values(config, flag).last().copied()
}

fn global_arg_values<'a>(config: &'a AppConfig, flag: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut index = 0;
    while index < config.bilibili.global_args.len() {
        let arg = &config.bilibili.global_args[index];
        if arg == flag
            && let Some(value) = config.bilibili.global_args.get(index + 1)
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
    fn maps_router_selection_to_core_selection() {
        assert_eq!(
            selection(Some(BilibiliSelection::Latest)),
            Some(Selection::Latest)
        );
        assert_eq!(
            selection(Some(BilibiliSelection::All)),
            Some(Selection::All)
        );
        assert_eq!(selection(None), None);
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
    fn parses_legacy_download_mode_args() {
        assert_eq!(
            legacy_bilibili_extra_args_for_direct_api(&[
                "--audio-only".to_string(),
                "--video-only=false".to_string(),
                "--cookie".to_string(),
                "SESSDATA=legacy".to_string(),
            ]),
            vec!["--only".to_string(), "audio".to_string()]
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
    fn reads_known_legacy_global_args() {
        let mut config = crate::config::AppConfig::for_test();
        config.bilibili.global_args = vec![
            "--api-base".to_string(),
            "https://api.example.test".to_string(),
            "--playurl-mode=app".to_string(),
            "--restricted-area".to_string(),
            "th".to_string(),
            "--request-timeout-seconds".to_string(),
            "7".to_string(),
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
    }

    #[test]
    fn uses_browser_user_agent_for_bilibili_media_requests() {
        let config = crate::config::AppConfig::for_test();
        let client_config = client_config(&config, Credentials::default()).unwrap();

        assert_eq!(client_config.user_agent, BILIBILI_BROWSER_USER_AGENT);
    }
}
