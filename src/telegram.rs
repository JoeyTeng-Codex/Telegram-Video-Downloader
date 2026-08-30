use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::redaction::redact_sensitive_text;

#[derive(Debug, Clone)]
pub struct TelegramClient {
    client: Client,
    token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundTextPolicy {
    RedactCredentials,
    AllowAuthSecret,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub chat: Chat,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub message: Option<Message>,
    pub data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct SendMessageRequest {
    chat_id: i64,
    text: String,
    disable_web_page_preview: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Serialize)]
struct EditMessageTextRequest {
    chat_id: i64,
    message_id: i64,
    text: String,
    disable_web_page_preview: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BotCommand {
    pub command: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
struct SetMyCommandsRequest {
    commands: Vec<BotCommand>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InlineKeyboardButton {
    pub text: String,
    pub callback_data: String,
}

#[derive(Debug, Serialize)]
struct AnswerCallbackQueryRequest {
    callback_query_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    show_alert: bool,
}

impl TelegramClient {
    pub fn new(token: String) -> Self {
        Self {
            client: Client::new(),
            token,
        }
    }

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_seconds: u64,
    ) -> Result<Vec<Update>> {
        let request_timeout = Duration::from_secs(timeout_seconds.saturating_add(10));
        let mut query = vec![("timeout", timeout_seconds.to_string())];
        if let Some(offset) = offset {
            query.push(("offset", offset.to_string()));
        }

        let response = self
            .client
            .get(self.api_url("getUpdates"))
            .query(&query)
            .timeout(request_timeout)
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram getUpdates request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram getUpdates returned HTTP error")?
            .json::<ApiResponse<Vec<Update>>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram getUpdates response")?;

        if response.ok {
            Ok(response.result.unwrap_or_default())
        } else {
            bail!(
                "telegram getUpdates failed: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
    }

    pub async fn send_message(&self, chat_id: i64, text: String) -> Result<i64> {
        self.send_message_payload(chat_id, text, None, OutboundTextPolicy::RedactCredentials)
            .await
    }

    pub async fn send_auth_secret_message(&self, chat_id: i64, text: String) -> Result<i64> {
        self.send_message_payload(chat_id, text, None, OutboundTextPolicy::AllowAuthSecret)
            .await
    }

    pub async fn send_message_with_inline_keyboard(
        &self,
        chat_id: i64,
        text: String,
        reply_markup: InlineKeyboardMarkup,
    ) -> Result<i64> {
        self.send_message_payload(
            chat_id,
            text,
            Some(reply_markup),
            OutboundTextPolicy::RedactCredentials,
        )
        .await
    }

    async fn send_message_payload(
        &self,
        chat_id: i64,
        text: String,
        reply_markup: Option<InlineKeyboardMarkup>,
        policy: OutboundTextPolicy,
    ) -> Result<i64> {
        let text = prepare_outbound_text(text, policy);
        info!(
            chat_id,
            text = %redact_sensitive_text(&text),
            "telegram outbound message"
        );
        let payload = SendMessageRequest {
            chat_id,
            text,
            disable_web_page_preview: true,
            reply_markup,
        };

        let response = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram sendMessage request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram sendMessage returned HTTP error")?
            .json::<ApiResponse<Message>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendMessage response")?;

        telegram_required_result(response, "sendMessage").map(|message| message.message_id)
    }

    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
    ) -> Result<()> {
        self.edit_message_text_payload(chat_id, message_id, text, None)
            .await
    }

    pub async fn edit_message_text_without_inline_keyboard(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
    ) -> Result<()> {
        self.edit_message_text_payload(
            chat_id,
            message_id,
            text,
            Some(InlineKeyboardMarkup {
                inline_keyboard: Vec::new(),
            }),
        )
        .await
    }

    async fn edit_message_text_payload(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> Result<()> {
        let text = redact_sensitive_text(&text);
        info!(
            chat_id,
            message_id,
            text = %redact_sensitive_text(&text),
            "telegram outbound message edit"
        );
        let payload = EditMessageTextRequest {
            chat_id,
            message_id,
            text,
            disable_web_page_preview: true,
            reply_markup,
        };

        let response = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram editMessageText request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram editMessageText returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram editMessageText response")?;

        telegram_optional_result(response, "editMessageText")
    }

    pub async fn set_my_commands(&self, commands: Vec<BotCommand>) -> Result<()> {
        let payload = SetMyCommandsRequest { commands };
        let response = self
            .client
            .post(self.api_url("setMyCommands"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram setMyCommands request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram setMyCommands returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram setMyCommands response")?;

        telegram_optional_result(response, "setMyCommands")
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: String,
        text: String,
    ) -> Result<()> {
        let text = redact_sensitive_text(&text);
        let payload = AnswerCallbackQueryRequest {
            callback_query_id,
            text: (!text.trim().is_empty()).then_some(text),
            show_alert: false,
        };
        let response = self
            .client
            .post(self.api_url("answerCallbackQuery"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram answerCallbackQuery request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram answerCallbackQuery returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram answerCallbackQuery response")?;

        telegram_optional_result(response, "answerCallbackQuery")
    }

    pub async fn send_photo(&self, chat_id: i64, caption: String, png: Vec<u8>) -> Result<()> {
        let caption = redact_sensitive_text(&caption);
        info!(
            chat_id,
            caption = %redact_sensitive_text(&caption),
            image_bytes = png.len(),
            "telegram outbound photo"
        );
        let photo = Part::bytes(png)
            .file_name("bbdown-login.png")
            .mime_str("image/png")
            .context("failed to build Telegram photo part")?;
        let form = Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption)
            .part("photo", photo);

        let response = self
            .client
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram sendPhoto request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram sendPhoto returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendPhoto response")?;

        telegram_optional_result(response, "sendPhoto")
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.token)
    }
}

fn telegram_required_result<T>(response: ApiResponse<T>, method: &str) -> Result<T> {
    if response.ok {
        response
            .result
            .with_context(|| format!("telegram {method} response did not include result"))
    } else {
        bail!(
            "telegram {method} failed: {}",
            response
                .description
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
}

fn telegram_optional_result<T>(response: ApiResponse<T>, method: &str) -> Result<()> {
    if response.ok {
        Ok(())
    } else {
        bail!(
            "telegram {method} failed: {}",
            response
                .description
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
}

impl Chat {
    pub fn is_private(&self) -> bool {
        self.kind.as_deref() == Some("private")
    }
}

fn strip_reqwest_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}

fn prepare_outbound_text(text: String, policy: OutboundTextPolicy) -> String {
    match policy {
        OutboundTextPolicy::RedactCredentials => redact_sensitive_text(&text),
        OutboundTextPolicy::AllowAuthSecret => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_private_chats() {
        assert!(
            Chat {
                id: 1,
                kind: Some("private".to_string())
            }
            .is_private()
        );
        assert!(
            !Chat {
                id: 1,
                kind: Some("group".to_string())
            }
            .is_private()
        );
        assert!(!Chat { id: 1, kind: None }.is_private());
    }

    #[test]
    fn redacts_bilibili_login_qr_urls() {
        assert_eq!(
            redact_sensitive_text(
                "Open:\nhttps://passport.bilibili.com/h5-app/passport/login/scan?qrcode_key=secret"
            ),
            "Open:\n<redacted Bilibili login QR URL>"
        );
        assert_eq!(
            redact_sensitive_text("https://www.bilibili.com/video/BV123"),
            "https://www.bilibili.com/video/BV123"
        );
        assert_eq!(
            redact_sensitive_text(
                "https://www.biliplus.com/login?balh_auth=1&balh_auth_origin=https%3A%2F%2Fwww.bilibili.com"
            ),
            "<redacted BBDown access-key authorization URL>"
        );
        assert_eq!(
            redact_sensitive_text(
                "https://www.bilibili.com/callback?access_token=secret&refresh_token=refresh"
            ),
            "<redacted BBDown access-key callback URL>"
        );
        assert_eq!(
            redact_sensitive_text("https://www.bilibili.com/callback#access_key=secret"),
            "<redacted BBDown access-key callback URL>"
        );
        assert_eq!(
            redact_sensitive_text("balh-login-credentials: {\"access_key\":\"secret\"}"),
            "<redacted BBDown access-key callback message>"
        );
        assert_eq!(
            redact_sensitive_text("{\"access_key\":\"secret\",\"refresh_token\":\"refresh\"}"),
            "<redacted BBDown access-key callback message>"
        );
    }

    #[test]
    fn redacts_credentials_from_standard_payloads_but_preserves_explicit_auth_links() {
        let text = "Authorization link: https://example.test/?access_key=secret".to_string();

        let redacted = prepare_outbound_text(text.clone(), OutboundTextPolicy::RedactCredentials);
        let explicit = prepare_outbound_text(text.clone(), OutboundTextPolicy::AllowAuthSecret);

        assert!(!redacted.contains("secret"));
        assert_eq!(explicit, text);
    }

    #[test]
    fn extracts_required_telegram_result() {
        let result = telegram_required_result(
            ApiResponse {
                ok: true,
                result: Some(123),
                description: None,
            },
            "sendMessage",
        )
        .expect("result should parse");

        assert_eq!(result, 123);
    }

    #[test]
    fn rejects_missing_required_telegram_result() {
        let err = telegram_required_result::<i64>(
            ApiResponse {
                ok: true,
                result: None,
                description: None,
            },
            "sendMessage",
        )
        .expect_err("missing result should fail");

        assert!(err.to_string().contains("did not include result"));
    }
}
