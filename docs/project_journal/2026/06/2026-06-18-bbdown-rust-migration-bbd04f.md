---
id: 20260618-bbd04f
title: BBDown-rust Bilibili Migration
status: completed
created: 2026-06-18
updated: 2026-08-29
branch: wip/bbdown-rust-migration
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
- Config exposes structured `playurl_mode`, `restricted_area`, restricted proxy lists, credential profile, and `danmaku_formats`; known legacy flags in both `extra_args` and `global_args` are translated for endpoint/playurl/restricted/request-timeout compatibility, with new fields taking precedence, and `download_args` retains `--only` mode compatibility.
- Access-key callback tickets remain reserved while an attempt is running, are removed only after credentials save successfully, and become retryable after a parse or save failure while the auth generation and TTL remain valid.
- Bilibili API and media requests use the browser-compatible `Mozilla/5.0` user agent expected by Bilibili media CDNs; application-specific user agents caused reproducible 403 responses after stream planning.
- Direct Bilibili downloads rely on `bbdown-core` request and media idle timeouts instead of the bot's external-command total timeout. Active long downloads are no longer cancelled at the two-hour external-command boundary, while stalled media reads still fail after the configured idle interval.
- Duplicate detection builds one media identity index per incoming job, so Bilibili plan identities reuse filename, NFO, and info JSON reads instead of rescanning sidecars for every candidate ID.
- Bilibili bvid/aid and filename-only cid/epid matches remain duplicate prompts but cannot authorize destructive replacement. Overwrite requires one exact cid/epid match for a single-entry plan that is proven by NFO or info JSON metadata. The move path first acquires the old media and sidecars into an exclusive transaction-owned backup directory, then rebuilds a strict identity index from those acquired files; missing targets, unreadable metadata, identity changes, recreated paths, and new ambiguity are rejected with rollback. FLV concat mux uses a unique transaction-owned temporary list and never removes a pre-existing `ffmpeg-concat.txt`.
- The migration implementation and Rust validation are complete. Live Telegram login and selection flows were exercised during development; a full post-fix long-video completion remains an operational smoke test after deployment.

## Next Steps
- Add a Telegram command or workflow for `bbdown-core` danmaku update so existing Bilibili downloads can refresh danmaku sidecars without re-downloading media.

## Evidence
- Validation in progress on 2026-06-18: `cargo check` passes after adding the pinned `bbdown-core` git dependency and updating `http` in `Cargo.lock`.
- Auth validation added on 2026-06-18: direct health formatting tests and Telegram secret redaction tests. Live Telegram/Bilibili QR E2E has not been run.
- Internal review evidence: prior `codex-readonly` review found duplicate-overwrite and BBDown-rust legacy/output edge cases; those findings were fixed and covered by tests. Final readonly reruns timed out without a final artifact and were terminated/cleaned up.
- 2026-08-25 403 diagnosis reproduced immediate media-request failures for `BV1XhM96FEM8` and `BV1xhBDBUEse` with the prior `telegram-video-downloader/0.1 bbdown-core` user agent. Short post-fix replays reached `video started` for both 1080p streams and stopped only at the intentional 15-second debug timeout.
- 2026-08-25 pre-merge review identified ignored legacy global flags in `extra_args` and premature access-key ticket consumption. The compatibility mapping, precedence rules, failure retry state, and stale-generation behavior now have regression coverage.
- 2026-08-25 follow-up review identified cross-entry Bilibili overwrite risk and repeated sidecar reads for multi-entry plans. Exact cid/epid overwrite authorization, server-side move validation, and a one-pass identity index now have regression coverage.
- 2026-08-25 whole-range review then identified a confirmation-to-replacement race in cached overwrite targets. Media and artifact-only overwrites now revalidate the protected semantic identity immediately before backup; regression tests cover missing, unreadable, changed, and newly ambiguous targets.
- 2026-08-25 validation: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (205 passed), `uv run ruff format --check`, `uv run ruff check`, and `uv run python -m unittest discover -s tests` (20 passed).
- 2026-08-26 formal whole-range review identified three remaining replacement-safety issues: filename-only Bilibili entry IDs could authorize overwrite, the fixed `ffmpeg-concat.txt` path was not transaction-owned, and validation returned a live path that could identify a different object by replacement time. The fixes separate duplicate evidence from destructive overwrite evidence, use exclusive per-run concat files, and validate only after the old media bundle has been acquired into an exclusive backup directory. Regression coverage includes filename-only cid/epid matches, preservation of user-owned concat files, rollback after identity changes, and retained recovery backups when an original path is recreated.
- 2026-08-26 current-head GitHub review identified two final partial-update issues: artifact-only overwrite removed acquired sidecars that had no staged replacement, and successful fresh credential saves were reported as failed when legacy state cleanup failed. Artifact commits now restore every unreplaced acquired sidecar before deleting only replaced backups, while legacy cleanup is best-effort after the new credential store commits. Regression coverage preserves NFO, description, and subtitles during danmaku/cover updates and accepts a deterministic legacy cleanup failure without losing the saved credential.
- 2026-08-26 post-fix local validation: `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (208 passed), `uv run ruff format --check`, `uv run ruff check`, `uv run python -m unittest discover -s tests` (20 passed), project journal validation, and `git diff --check`.
- 2026-08-29 operational logs showed `BV1xhBDBUEse` complete a 610.3 MiB video transfer and begin its audio transfer before the bot cancelled the still-active task exactly 7200 seconds after start. The redundant outer total timeout was removed; `bbdown-core` retains its configured 300-second media idle timeout and independent API request bounds.
- 2026-08-29 validation: `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (208 passed), `uv run ruff format --check`, `uv run ruff check`, and `uv run python -m unittest discover -s tests` (20 passed).
