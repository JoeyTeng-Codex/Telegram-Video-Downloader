use bbdown_core::Input;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRequest {
    Bilibili {
        url: String,
        selection: Option<BilibiliSelection>,
    },
    Youtube {
        url: String,
    },
    Pdf {
        url: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum BilibiliSelection {
    Latest,
    All,
    Page(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    Jobs(Vec<JobRequest>),
    Help,
    PdfUsage,
    BilibiliAuth(BilibiliAuthCommand),
    BilibiliAuthUsage,
    UnsupportedLinks,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BilibiliAuthCommand {
    Login(BilibiliAuthLoginMode),
    Status,
    Logout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BilibiliAuthLoginMode {
    Web,
    Tv,
    AccessKey,
}

impl JobRequest {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Bilibili { .. } => "Bilibili download",
            Self::Youtube { .. } => "YouTube download",
            Self::Pdf { .. } => "PDF capture",
        }
    }

    pub fn requires_bilibili_selection(&self) -> bool {
        match self {
            Self::Bilibili { url, selection } => {
                selection.is_none() && bilibili_url_requires_selection(url)
            }
            Self::Youtube { .. } | Self::Pdf { .. } => false,
        }
    }
}

impl BilibiliSelection {
    pub fn label(self) -> &'static str {
        match self {
            Self::Latest => "latest episode",
            Self::All => "all episodes",
            Self::Page(_) => "selected video page",
        }
    }
}

pub fn route_message(text: &str, auto_pdf_domains: &[String]) -> RouteResult {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return RouteResult::Empty;
    }

    if is_help_command(trimmed) {
        return RouteResult::Help;
    }

    if let Some(args) = bbdown_command_args(trimmed) {
        let mut parts = args.split_whitespace();
        return match (parts.next(), parts.next()) {
            (Some("login"), mode) => match parse_bilibili_auth_login_mode(mode) {
                Some(mode) if parts.next().is_none() => {
                    RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(mode))
                }
                _ => RouteResult::BilibiliAuthUsage,
            },
            (Some("status"), None) => RouteResult::BilibiliAuth(BilibiliAuthCommand::Status),
            (Some("logout"), None) => RouteResult::BilibiliAuth(BilibiliAuthCommand::Logout),
            _ => RouteResult::BilibiliAuthUsage,
        };
    }

    if let Some(pdf_args) = pdf_command_args(trimmed) {
        let jobs: Vec<_> = extract_http_urls(pdf_args)
            .into_iter()
            .map(|url| JobRequest::Pdf { url })
            .collect();
        return if jobs.is_empty() {
            RouteResult::PdfUsage
        } else {
            RouteResult::Jobs(jobs)
        };
    }

    let urls = extract_http_urls(trimmed);
    let jobs: Vec<_> = urls
        .iter()
        .filter_map(|url| classify_url(url, auto_pdf_domains))
        .collect();

    if !jobs.is_empty() {
        RouteResult::Jobs(jobs)
    } else if urls.is_empty() {
        RouteResult::Empty
    } else {
        RouteResult::UnsupportedLinks
    }
}

pub fn extract_http_urls(text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut search_start = 0;

    while let Some(relative_start) = find_next_scheme(&text[search_start..]) {
        let start = search_start + relative_start;
        let end = find_url_end(text, start);
        let candidate = clean_url_candidate(&text[start..end]);

        if let Ok(parsed) = Url::parse(candidate)
            && matches!(parsed.scheme(), "http" | "https")
        {
            urls.push(parsed.to_string());
        }

        search_start = end;
    }

    urls
}

fn classify_url(raw_url: &str, auto_pdf_domains: &[String]) -> Option<JobRequest> {
    classify_bilibili_opus_url(raw_url)
        .or_else(|| classify_video_url(raw_url))
        .or_else(|| classify_auto_pdf_url(raw_url, auto_pdf_domains))
}

fn parse_bilibili_auth_login_mode(mode: Option<&str>) -> Option<BilibiliAuthLoginMode> {
    match mode {
        None | Some("web") | Some("cookie") => Some(BilibiliAuthLoginMode::Web),
        Some("tv") => Some(BilibiliAuthLoginMode::Tv),
        Some("access-key" | "access_key" | "accesskey" | "intl" | "bstar") => {
            Some(BilibiliAuthLoginMode::AccessKey)
        }
        Some(_) => None,
    }
}

fn classify_bilibili_opus_url(raw_url: &str) -> Option<JobRequest> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if !domain_or_subdomain(&host, "bilibili.com") {
        return None;
    }

    let mut segments = url.path_segments()?;
    if !is_bilibili_opus_path(&mut segments) {
        return None;
    }

    let opus_id = segments.next()?;
    if opus_id.is_empty() || !opus_id.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    Some(JobRequest::Pdf {
        url: format!("https://www.bilibili.com/opus/{opus_id}"),
    })
}

fn classify_video_url(raw_url: &str) -> Option<JobRequest> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();

    if is_b23_short_link_url(raw_url) || is_bilibili_video_url(&host, &url) {
        Some(JobRequest::Bilibili {
            url: raw_url.to_string(),
            selection: bilibili_selection_from_url(raw_url),
        })
    } else if is_youtube_host(&host) {
        Some(JobRequest::Youtube {
            url: raw_url.to_string(),
        })
    } else {
        None
    }
}

fn has_bilibili_opus_path(url: &Url) -> bool {
    url.path_segments()
        .is_some_and(|mut segments| is_bilibili_opus_path(&mut segments))
}

fn is_bilibili_video_url(host: &str, url: &Url) -> bool {
    if domain_or_subdomain(host, "bilibili.tv") {
        return is_bilibili_intl_video_url(url);
    }

    if !domain_or_subdomain(host, "bilibili.com") || has_bilibili_opus_path(url) {
        return false;
    }

    is_bilibili_standard_video_url(url)
        || url.path_segments().is_some_and(|mut segments| {
            matches!(
                (segments.next(), segments.next()),
                (Some("bangumi"), Some("play" | "media"))
            )
        })
}

fn is_bilibili_standard_video_url(url: &Url) -> bool {
    let Some(mut segments) = url.path_segments() else {
        return false;
    };
    matches!(
        (segments.next(), segments.next()),
        (Some("video"), Some(id)) if id.starts_with("BV") || id.starts_with("av")
    )
}

fn is_bilibili_intl_video_url(url: &Url) -> bool {
    let Some(mut segments) = url.path_segments() else {
        return false;
    };
    matches!(
        (segments.next(), segments.next(), segments.next(), segments.next()),
        (Some(_locale), Some("play"), Some(_season_id), Some(episode_id))
            if !episode_id.is_empty() && episode_id.chars().all(|ch| ch.is_ascii_digit())
    )
}

pub(crate) fn bilibili_selection_from_url(raw_url: &str) -> Option<BilibiliSelection> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let is_b23_short_link = is_b23_short_link_url(raw_url);
    if !is_b23_short_link
        && (!domain_or_subdomain(&host, "bilibili.com") || !is_bilibili_standard_video_url(&url))
    {
        return None;
    }
    if !is_b23_short_link && has_positive_episode_id(&url) {
        return None;
    }

    url.query_pairs()
        .find(|(name, _)| name == "p")
        .and_then(|(_, value)| value.parse::<u32>().ok())
        .filter(|page| *page > 0)
        .map(BilibiliSelection::Page)
}

pub(crate) fn is_b23_short_link_url(raw_url: &str) -> bool {
    Url::parse(raw_url).ok().is_some_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url
                .host_str()
                .is_some_and(|host| host.eq_ignore_ascii_case("b23.tv"))
            && url.username().is_empty()
            && url.password().is_none()
    })
}

fn has_positive_episode_id(url: &Url) -> bool {
    url.query_pairs().any(|(name, value)| {
        name == "ep_id" && value.parse::<u64>().is_ok_and(|episode_id| episode_id > 0)
    })
}

pub(crate) fn bilibili_url_ep_id_selects_episode(raw_url: &str) -> bool {
    let Ok(url) = Url::parse(raw_url) else {
        return false;
    };
    has_positive_episode_id(&url) && matches!(Input::parse(raw_url), Ok(Input::Episode(_)))
}

fn bilibili_url_requires_selection(raw_url: &str) -> bool {
    let Ok(url) = Url::parse(raw_url) else {
        return false;
    };
    let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
        return false;
    };
    if !domain_or_subdomain(&host, "bilibili.com") {
        return false;
    }
    if has_positive_episode_id(&url) {
        return false;
    }
    let Some(mut segments) = url.path_segments() else {
        return false;
    };
    match (segments.next(), segments.next(), segments.next()) {
        (Some("bangumi"), Some("play"), Some(id)) => id.starts_with("ss"),
        (Some("bangumi"), Some("media"), Some(id)) => id.starts_with("md"),
        _ => false,
    }
}

fn is_bilibili_opus_path<'a, I>(segments: &mut I) -> bool
where
    I: Iterator<Item = &'a str>,
{
    segments.next().is_some_and(|segment| segment == "opus")
}

fn classify_auto_pdf_url(raw_url: &str, auto_pdf_domains: &[String]) -> Option<JobRequest> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if auto_pdf_domains
        .iter()
        .any(|domain| domain_or_subdomain(&host, &domain.to_ascii_lowercase()))
    {
        Some(JobRequest::Pdf {
            url: raw_url.to_string(),
        })
    } else {
        None
    }
}

fn is_help_command(text: &str) -> bool {
    let first = text.split_whitespace().next();
    first == Some("/help") || first.is_some_and(|command| command.starts_with("/help@"))
}

fn pdf_command_args(text: &str) -> Option<&str> {
    let first = text.split_whitespace().next()?;
    if first == "/pdf" || first.starts_with("/pdf@") {
        Some(text[first.len()..].trim())
    } else {
        None
    }
}

fn bbdown_command_args(text: &str) -> Option<&str> {
    let first = text.split_whitespace().next()?;
    if first == "/bbdown" || first.starts_with("/bbdown@") {
        Some(text[first.len()..].trim())
    } else {
        None
    }
}

fn find_next_scheme(text: &str) -> Option<usize> {
    for (offset, _) in text.char_indices() {
        let rest = &text[offset..];
        if rest
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
            || rest
                .get(..7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        {
            return Some(offset);
        }
    }
    None
}

fn find_url_end(text: &str, start: usize) -> usize {
    let mut ascii_bracket_stack = Vec::new();
    for (offset, ch) in text[start..].char_indices() {
        if is_url_boundary(ch) {
            return start + offset;
        }
        if let Some(closer) = matching_ascii_closer(ch) {
            ascii_bracket_stack.push(closer);
            continue;
        }
        if is_ascii_closer(ch) {
            if ascii_bracket_stack
                .last()
                .is_some_and(|closer| *closer == ch)
            {
                ascii_bracket_stack.pop();
            } else {
                return start + offset;
            }
        }
    }

    text.len()
}

fn is_url_boundary(ch: char) -> bool {
    ch.is_whitespace() || ch.is_control() || is_cjk_punctuation(ch) || is_quote_boundary(ch)
}

fn matching_ascii_closer(ch: char) -> Option<char> {
    match ch {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        '<' => Some('>'),
        _ => None,
    }
}

fn is_ascii_closer(ch: char) -> bool {
    matches!(ch, ')' | ']' | '}' | '>')
}

fn is_quote_boundary(ch: char) -> bool {
    matches!(
        ch,
        '"' | '\'' | '\u{2018}' | '\u{2019}' | '\u{201c}' | '\u{201d}'
    )
}

fn clean_url_candidate(candidate: &str) -> &str {
    candidate.trim_end_matches(is_trailing_punctuation)
}

fn is_trailing_punctuation(ch: char) -> bool {
    matches!(ch, '.' | ',' | '!' | '?' | ';' | ':') || is_cjk_punctuation(ch)
}

fn is_cjk_punctuation(ch: char) -> bool {
    matches!(
        ch,
        '\u{3002}'
            | '\u{ff0c}'
            | '\u{ff01}'
            | '\u{ff1f}'
            | '\u{ff1b}'
            | '\u{ff1a}'
            | '\u{3001}'
            | '\u{ff08}'
            | '\u{ff09}'
            | '\u{ff3b}'
            | '\u{ff3d}'
            | '\u{ff5b}'
            | '\u{ff5d}'
            | '\u{3008}'
            | '\u{3009}'
            | '\u{300a}'
            | '\u{300b}'
            | '\u{300c}'
            | '\u{300d}'
            | '\u{300e}'
            | '\u{300f}'
            | '\u{3010}'
            | '\u{3011}'
            | '\u{3014}'
            | '\u{3015}'
    )
}

fn domain_or_subdomain(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn is_youtube_host(host: &str) -> bool {
    host == "youtu.be"
        || domain_or_subdomain(host, "youtube.com")
        || domain_or_subdomain(host, "youtube-nocookie.com")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auto_pdf_domains() -> Vec<String> {
        vec!["mp.weixin.qq.com".to_string()]
    }

    #[test]
    fn routes_bilibili_domains() {
        assert_eq!(
            route_message("https://www.bilibili.com/video/BV123", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string(),
                selection: None
            }])
        );
        assert_eq!(
            route_message("https://b23.tv/abc", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://b23.tv/abc".to_string(),
                selection: None
            }])
        );
        assert_eq!(
            route_message(
                "https://www.bilibili.com/bangumi/play/ep123456",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ep123456".to_string(),
                selection: None
            }])
        );
    }

    #[test]
    fn preserves_bilibili_video_page_selection() {
        assert_eq!(
            route_message(
                "https://www.bilibili.com/video/BV12TRrBcEP8/?p=2&share_source=copy_web",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/?p=2&share_source=copy_web"
                    .to_string(),
                selection: Some(BilibiliSelection::Page(2)),
            }])
        );
        assert_eq!(
            route_message("https://b23.tv/abc?p=3", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://b23.tv/abc?p=3".to_string(),
                selection: Some(BilibiliSelection::Page(3)),
            }])
        );
        assert_eq!(
            bilibili_selection_from_url("https://www.bilibili.com/video/BV12TRrBcEP8/?p=0"),
            None
        );
        assert_eq!(
            bilibili_selection_from_url("https://www.bilibili.com/video/BV12TRrBcEP8/?p=invalid"),
            None
        );
        assert_eq!(
            bilibili_selection_from_url(
                "https://www.bilibili.com/video/BV12TRrBcEP8/?ep_id=456&p=2"
            ),
            None
        );
        assert_eq!(
            bilibili_selection_from_url("https://b23.tv/abc?ep_id=456&p=3"),
            Some(BilibiliSelection::Page(3))
        );
        assert_eq!(
            bilibili_selection_from_url(
                "https://www.bilibili.com/video/BV12TRrBcEP8/?ep_id=invalid&p=2"
            ),
            Some(BilibiliSelection::Page(2))
        );
        assert!(bilibili_url_ep_id_selects_episode(
            "https://www.bilibili.com/video/BV12TRrBcEP8/?ep_id=456&p=2"
        ));
        assert!(!bilibili_url_ep_id_selects_episode(
            "https://space.bilibili.com/123?ep_id=456"
        ));
        assert!(!bilibili_url_ep_id_selects_episode(
            "https://www.bilibili.com/account/dynamic?ep_id=456"
        ));
        assert!(!bilibili_url_ep_id_selects_episode(
            "https://b23.tv/abc?ep_id=456"
        ));
        assert!(!is_b23_short_link_url("https://user:pass@b23.tv/abc"));
    }

    #[test]
    fn routes_bilibili_season_media_and_intl() {
        assert_eq!(
            route_message(
                "https://www.bilibili.com/bangumi/play/ss12345",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: None
            }])
        );
        assert_eq!(
            route_message(
                "https://www.bilibili.com/bangumi/media/md12345",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md12345".to_string(),
                selection: None
            }])
        );
        assert_eq!(
            route_message(
                "https://www.bilibili.tv/en/play/123/456",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.tv/en/play/123/456".to_string(),
                selection: None
            }])
        );
    }

    #[test]
    fn requires_selection_for_bilibili_season_and_media_links() {
        assert!(
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: None
            }
            .requires_bilibili_selection()
        );
        assert!(
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md12345".to_string(),
                selection: None
            }
            .requires_bilibili_selection()
        );
        assert!(
            !JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ep12345".to_string(),
                selection: None
            }
            .requires_bilibili_selection()
        );
        assert!(
            !JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345".to_string(),
                selection: Some(BilibiliSelection::Latest)
            }
            .requires_bilibili_selection()
        );
        assert!(
            !JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345?ep_id=456".to_string(),
                selection: None,
            }
            .requires_bilibili_selection()
        );
        assert!(
            !JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md12345?ep_id=456".to_string(),
                selection: None,
            }
            .requires_bilibili_selection()
        );
        assert!(
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/play/ss12345?ep_id=invalid".to_string(),
                selection: None,
            }
            .requires_bilibili_selection()
        );
    }

    #[test]
    fn routes_non_video_bilibili_to_auto_pdf_or_unsupported() {
        assert_eq!(
            route_message(
                "https://www.bilibili.com/read/cv123",
                &["bilibili.com".to_string()]
            ),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://www.bilibili.com/read/cv123".to_string()
            }])
        );
        assert_eq!(
            route_message("https://space.bilibili.com/123", &auto_pdf_domains()),
            RouteResult::UnsupportedLinks
        );
    }

    #[test]
    fn routes_bilibili_with_non_url_prefix() {
        assert_eq!(
            route_message(
                "Title https://www.bilibili.com/video/BV12TRrBcEP8/?share_source=copy_web&vd_source=abc",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/?share_source=copy_web&vd_source=abc".to_string(),
                selection: None
            }])
        );
    }

    #[test]
    fn routes_bilibili_opus_as_canonical_pdf() {
        assert_eq!(
            route_message(
                "https://m.bilibili.com/opus/1206098216310800386?from_spmid=united.player-video-detail.0.0&share_source=COPY",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://www.bilibili.com/opus/1206098216310800386".to_string()
            }])
        );
        assert_eq!(
            route_message(
                "https://www.bilibili.com/opus/1206098216310800386?spmid=dt.opus-detail.0.0#reply",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://www.bilibili.com/opus/1206098216310800386".to_string()
            }])
        );
    }

    #[test]
    fn leaves_malformed_bilibili_opus_unsupported() {
        assert_eq!(
            route_message(
                "https://www.bilibili.com/opus/not-a-number",
                &auto_pdf_domains()
            ),
            RouteResult::UnsupportedLinks
        );
    }

    #[test]
    fn routes_youtube_domains() {
        for input in [
            "https://youtube.com/watch?v=abc",
            "https://www.youtube.com/watch?v=abc",
            "https://m.youtube.com/watch?v=abc",
            "https://music.youtube.com/watch?v=abc",
            "https://youtu.be/abc",
            "https://www.youtube-nocookie.com/embed/abc",
        ] {
            assert!(matches!(
                route_message(input, &auto_pdf_domains()),
                RouteResult::Jobs(jobs) if matches!(jobs.as_slice(), [JobRequest::Youtube { .. }])
            ));
        }
    }

    #[test]
    fn keeps_youtube_timestamp_query() {
        assert_eq!(
            route_message(
                "Watch https://www.youtube.com/watch?v=PHH1wTDF-1M&t=47s",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M&t=47s".to_string()
            }])
        );
    }

    #[test]
    fn routes_pdf_command_and_auto_domain() {
        assert_eq!(
            route_message("/pdf https://example.com/article", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://example.com/article".to_string()
            }])
        );
        assert_eq!(
            route_message("/pdf", &auto_pdf_domains()),
            RouteResult::PdfUsage
        );
        assert_eq!(
            route_message(
                "https://mp.weixin.qq.com/s?__biz=abc&mid=1&idx=1#rd",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://mp.weixin.qq.com/s?__biz=abc&mid=1&idx=1#rd".to_string()
            }])
        );
    }

    #[test]
    fn routes_help_command() {
        assert_eq!(
            route_message("/help", &auto_pdf_domains()),
            RouteResult::Help
        );
        assert_eq!(
            route_message("/help@DownloaderBot", &auto_pdf_domains()),
            RouteResult::Help
        );
        assert_eq!(
            route_message("/help anything", &auto_pdf_domains()),
            RouteResult::Help
        );
    }

    #[test]
    fn routes_bbdown_auth_commands() {
        assert_eq!(
            route_message("/bbdown login", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(BilibiliAuthLoginMode::Web))
        );
        assert_eq!(
            route_message("/bbdown login web", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(BilibiliAuthLoginMode::Web))
        );
        assert_eq!(
            route_message("/bbdown login tv", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(BilibiliAuthLoginMode::Tv))
        );
        assert_eq!(
            route_message("/bbdown login access-key", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(BilibiliAuthLoginMode::AccessKey))
        );
        assert_eq!(
            route_message("/bbdown login intl", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Login(BilibiliAuthLoginMode::AccessKey))
        );
        assert_eq!(
            route_message("/bbdown status", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Status)
        );
        assert_eq!(
            route_message("/bbdown logout", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Logout)
        );
        assert_eq!(
            route_message("/bbdown@DownloaderBot status", &auto_pdf_domains()),
            RouteResult::BilibiliAuth(BilibiliAuthCommand::Status)
        );
    }

    #[test]
    fn routes_bbdown_auth_usage() {
        assert_eq!(
            route_message("/bbdown", &auto_pdf_domains()),
            RouteResult::BilibiliAuthUsage
        );
        assert_eq!(
            route_message("/bbdown status extra", &auto_pdf_domains()),
            RouteResult::BilibiliAuthUsage
        );
        assert_eq!(
            route_message("/bbdown login unknown", &auto_pdf_domains()),
            RouteResult::BilibiliAuthUsage
        );
        assert_eq!(
            route_message("/bbdown login web extra", &auto_pdf_domains()),
            RouteResult::BilibiliAuthUsage
        );
        assert_eq!(
            route_message("/bbdown unknown", &auto_pdf_domains()),
            RouteResult::BilibiliAuthUsage
        );
    }

    #[test]
    fn leaves_non_whitelisted_web_urls_unsupported() {
        assert_eq!(
            route_message("https://example.com/article", &auto_pdf_domains()),
            RouteResult::UnsupportedLinks
        );
    }

    #[test]
    fn extracts_urls_with_common_wrapping_punctuation() {
        assert_eq!(
            extract_http_urls("read this: <https://example.com/a?b=1>."),
            vec!["https://example.com/a?b=1".to_string()]
        );
    }

    #[test]
    fn extracts_multiple_urls_from_free_text() {
        assert_eq!(
            extract_http_urls(
                "A: https://example.com/a, B: (https://www.youtube.com/watch?v=abc)."
            ),
            vec![
                "https://example.com/a".to_string(),
                "https://www.youtube.com/watch?v=abc".to_string()
            ]
        );
    }

    #[test]
    fn stops_urls_at_cjk_punctuation_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "https://youtu.be/abc\u{ff0c}caption https://example.com/a\u{3002}title"
            ),
            vec![
                "https://youtu.be/abc".to_string(),
                "https://example.com/a".to_string()
            ]
        );
    }

    #[test]
    fn keeps_ascii_punctuation_inside_urls() {
        assert_eq!(
            extract_http_urls("/pdf https://example.com/a(b)/c"),
            vec!["https://example.com/a(b)/c".to_string()]
        );
    }

    #[test]
    fn stops_at_unmatched_ascii_closing_wrapper() {
        assert_eq!(
            extract_http_urls(
                "(https://mp.weixin.qq.com/s?x=1)title (https://youtu.be/abc)caption"
            ),
            vec![
                "https://mp.weixin.qq.com/s?x=1".to_string(),
                "https://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn keeps_balanced_ascii_parentheses_inside_wrapped_urls() {
        assert_eq!(
            extract_http_urls("(https://example.com/a(b)/c)title"),
            vec!["https://example.com/a(b)/c".to_string()]
        );
    }

    #[test]
    fn keeps_balanced_ascii_parentheses_at_url_end() {
        assert_eq!(
            extract_http_urls("https://en.wikipedia.org/wiki/Foo_(bar),"),
            vec!["https://en.wikipedia.org/wiki/Foo_(bar)".to_string()]
        );
    }

    #[test]
    fn handles_fullwidth_wrapped_urls_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "\u{ff08}https://mp.weixin.qq.com/s?x=1\u{ff09}title https://youtu.be/abc\u{ff09}"
            ),
            vec![
                "https://mp.weixin.qq.com/s?x=1".to_string(),
                "https://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn extracts_urls_with_case_insensitive_schemes() {
        assert_eq!(
            extract_http_urls("/pdf HTTPS://example.com HTTP://youtu.be/abc"),
            vec![
                "https://example.com/".to_string(),
                "http://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn stops_quoted_urls_before_caption_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "\u{201c}https://youtu.be/abc\u{201d}caption \"https://mp.weixin.qq.com/s?x=1\"title"
            ),
            vec![
                "https://youtu.be/abc".to_string(),
                "https://mp.weixin.qq.com/s?x=1".to_string()
            ]
        );
    }
}
