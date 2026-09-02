# Project State

## Current State
- Telegram 本地下载 bot 已完成 BBDown-rust Bilibili 迁移，Bilibili 下载和登录管理直接使用 `bbdown-core` crate API。当前功能包括全文 URL 扫描、微信文章自动 PDF 白名单、YouTube metadata/封面/字幕/sidecar、Bilibili 番剧/intl 下载、BBDown-rust web/tv/access-key 登录管理、外部命令进度转发、文件活性监控和超时保护。
- 最新 workstream 记录在 `docs/project_journal/2026/06/2026-06-18-bbdown-rust-migration-bbd04f.md`；原始 bot workstream 记录在 `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`。

## Recovery Pointers
- Latest workstream: `docs/project_journal/2026/06/2026-06-18-bbdown-rust-migration-bbd04f.md`
- Base bot workstream: `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`

## Global Blockers
- 暂无 repo-wide blocker；部署后仍应完成一次修复版 Bilibili 长视频下载的端到端 smoke test。

## Notes
- 普通任务进展写入 workstream journal；此文件只保留仓库级恢复入口。
