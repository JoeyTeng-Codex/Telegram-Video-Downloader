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

pub(crate) fn redact_sensitive_text(text: &str) -> String {
    let mut redacted = redact_url_userinfo(text);
    redacted = redact_flag_line_values(&redacted, "--cookie", "<redacted Bilibili cookie>");
    for flag in ["--access-key", "--access-token", "--refresh-token"] {
        redacted = redact_flag_line_values(&redacted, flag, "<redacted BBDown credential>");
    }
    redacted = redact_bilibili_cookie_lines(&redacted);
    for name in BILIBILI_COOKIE_NAMES {
        redacted = redact_pair_values(&redacted, name, "<redacted>");
    }
    redact_sensitive_lines(&redacted)
}

fn redact_url_userinfo(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        let authority_start = scheme_end + 3;
        let authority_end = rest[authority_start..]
            .find(|ch: char| {
                ch.is_ascii_whitespace()
                    || matches!(ch, '/' | '?' | '#' | '"' | '\'' | '>' | ')' | ']' | '}')
            })
            .map_or(rest.len(), |offset| authority_start + offset);
        let authority = &rest[authority_start..authority_end];
        let Some(userinfo_end) = authority.rfind('@').filter(|index| *index > 0) else {
            redacted.push_str(&rest[..authority_end]);
            rest = &rest[authority_end..];
            continue;
        };

        redacted.push_str(&rest[..authority_start]);
        redacted.push_str("<redacted>@");
        redacted.push_str(&authority[userinfo_end + 1..]);
        rest = &rest[authority_end..];
    }
    redacted.push_str(rest);
    redacted
}

fn redact_sensitive_lines(text: &str) -> String {
    text.lines()
        .map(|line| {
            let lowercase = line.to_ascii_lowercase();
            if lowercase.contains("passport.bilibili.com") && lowercase.contains("qrcode_key=") {
                "<redacted Bilibili login QR URL>"
            } else if lowercase.contains("biliplus.com/login") && lowercase.contains("balh_auth=") {
                "<redacted BBDown access-key authorization URL>"
            } else if lowercase.contains("balh-login-credentials:")
                || contains_bbdown_credential_json(&lowercase)
            {
                "<redacted BBDown access-key callback message>"
            } else if contains_bbdown_credential_parameter(&lowercase) {
                "<redacted BBDown access-key callback URL>"
            } else if contains_authorization_header(&lowercase) {
                "<redacted BBDown credential>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn contains_bbdown_credential_json(line: &str) -> bool {
    ["access_key", "access_token", "refresh_token"]
        .iter()
        .any(|name| line.contains(&format!("\"{name}\"")))
}

fn contains_bbdown_credential_parameter(line: &str) -> bool {
    ["access_key", "access_token", "refresh_token"]
        .iter()
        .any(|name| {
            line.contains(&format!("{name}="))
                || line.contains(&format!("{name}:"))
                || line.contains(&format!("{name}%3d"))
        })
}

fn contains_authorization_header(line: &str) -> bool {
    line.contains("authorization:") && (line.contains("bearer ") || line.contains("identify_v1 "))
}

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

fn redact_pair_values(text: &str, name: &str, replacement: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_bbdown_credentials_from_urls_json_flags_and_headers() {
        let redacted = redact_sensitive_text(
            "request https://api.bilibili.com/x?access_key=APP_ACCESS\n\
             credentials={\"access_token\":\"TV_ACCESS\",\"refresh_token\":\"REFRESH\"}\n\
             --access-key\nCLI_ACCESS\n\
             authorization: identify_v1 HEADER_ACCESS\n\
             safe",
        );

        for secret in [
            "APP_ACCESS",
            "TV_ACCESS",
            "REFRESH",
            "CLI_ACCESS",
            "HEADER_ACCESS",
        ] {
            assert!(!redacted.contains(secret));
        }
        assert!(redacted.contains("<redacted BBDown credential>"));
        assert!(redacted.contains("safe"));
    }

    #[test]
    fn redacts_bilibili_cookie_material() {
        let redacted = redact_sensitive_text(
            "--cookie\nSESSDATA=session; bili_jct=csrf; unknown_cookie=value\nsafe",
        );

        assert!(!redacted.contains("session"));
        assert!(!redacted.contains("csrf"));
        assert!(!redacted.contains("value"));
        assert!(redacted.contains("<redacted Bilibili cookie>"));
        assert!(redacted.contains("safe"));
    }

    #[test]
    fn redacts_proxy_url_userinfo() {
        let redacted = redact_sensitive_text(
            "proxy https://proxy-user:proxy-password@example.test:8443/path safe",
        );

        assert!(!redacted.contains("proxy-user"));
        assert!(!redacted.contains("proxy-password"));
        assert!(redacted.contains("https://<redacted>@example.test:8443/path"));
        assert!(redacted.contains("safe"));
    }
}
