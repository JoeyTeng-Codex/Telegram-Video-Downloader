---
id: 20260618-bbd04f
title: BBDown-rust Bilibili Migration
status: active
created: 2026-06-18
updated: 2026-08-25
branch:
pr:
supersedes: []
superseded_by:
---

# BBDown-rust Bilibili Migration

## Summary
- Bilibili downloads now target the `BBDown-rust` `bbdown-core` crate API instead of the legacy .NET `BBDown` argument surface or the `bbdown` CLI.
- The bot supports Bilibili season/media and intl routing, with Telegram selection prompts for `ss/md` series links.
- BBDown login management now uses direct crate APIs for Web QR, TV QR, access-key handoff, credential health, and logout.

## Current State
- Bilibili preflight uses `BiliClient::plan_download_with_mode` to resolve bvid, aid, cid, and epid identities for duplicate detection.
- Downloads use `BiliClient::download_plan_with_progress`; the bot converts the structured report into its existing NFO, staging, and duplicate-handling flow.
- `/bbdown login [web|tv|access-key]` uses direct QR/access-key APIs; access-key login is a two-step Telegram flow that waits for the callback URL or `balh-login-credentials:` message.
- `/bbdown status` reads `BiliClient::check_credential_health`; `/bbdown logout` clears the selected BBDown-rust credential/profile and removes legacy bot-managed Web cookie state.
- Config exposes structured `playurl_mode`, `restricted_area`, restricted proxy lists, credential profile, and `danmaku_formats`; known legacy `global_args` are translated for endpoint/playurl/restricted/request-timeout compatibility, and `download_args` retains `--only` mode compatibility.
- Bilibili API and media requests use the browser-compatible `Mozilla/5.0` user agent expected by Bilibili media CDNs; application-specific user agents caused reproducible 403 responses after stream planning.
- Rust validation passes; final live Telegram QR/selection validation and full long-video completion remain pending.

## Next Steps
- Add a Telegram command or workflow for `bbdown-core` danmaku update so existing Bilibili downloads can refresh danmaku sidecars without re-downloading media.

## Evidence
- Validation in progress on 2026-06-18: `cargo check` passes after adding the pinned `bbdown-core` git dependency and updating `http` in `Cargo.lock`.
- Auth validation added on 2026-06-18: direct health formatting tests and Telegram secret redaction tests. Live Telegram/Bilibili QR E2E has not been run.
- Internal review evidence: prior `codex-readonly` review found duplicate-overwrite and BBDown-rust legacy/output edge cases; those findings were fixed and covered by tests. Final readonly reruns timed out without a final artifact and were terminated/cleaned up.
- 2026-08-25 403 diagnosis reproduced immediate media-request failures for `BV1XhM96FEM8` and `BV1xhBDBUEse` with the prior `telegram-video-downloader/0.1 bbdown-core` user agent. Short post-fix replays reached `video started` for both 1080p streams and stopped only at the intentional 15-second debug timeout.
- 2026-08-25 validation: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` (194 passed).
