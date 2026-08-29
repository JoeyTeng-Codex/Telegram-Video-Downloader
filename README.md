# Telegram Local Downloader Bot

这是一个个人本机使用的 Telegram bot。把支持的视频链接发给 bot 后，它会保存到本机下载目录；Bilibili 使用内嵌 `bbdown-core` crate，YouTube/PDF 仍调用本机工具。对图文网页，可以使用 `/pdf URL`，也可以配置自动 PDF 域名让 bot 直接保存 PDF。

## 功能

- 普通消息中的 Bilibili 链接通过 `BBDown-rust` 的 `bbdown-core` crate 解析和下载，保存到视频下载目录，并默认保留 XML/ASS 弹幕 sidecar。
- Bilibili 番剧和 intl 链接走内嵌 plan/download API；`ss/md` 系列入口会先提示选择最新一集或全集。
- 普通消息中的 Bilibili `opus` 文章链接会规范化为 `www.bilibili.com/opus/<id>` 并保存为 PDF。
- 私聊中可以用 `/bbdown login`、`/bbdown status`、`/bbdown logout` 管理 BBDown 使用的 Bilibili 登录态。
- `/help` 会显示 bot 支持的命令；启动时也会向 Telegram 注册 slash command 提示。
- 普通消息中的 YouTube 链接调用 `yt-dlp`，保存到视频下载目录，并尽量写入 metadata、封面、字幕和媒体库 sidecar。
- 普通消息会从整段文本里扫描 HTTP(S) URL；标题、说明和 URL 外层标点会被忽略。
- 视频下载会先写入隐藏 staging 目录，成功后再移动到最终目录；如果可从 URL 识别到本地已有相同 YouTube 或 Bilibili 视频，bot 会先提供两者并存或取消，只有能唯一定位现有文件时才同时提供覆盖按钮。
- `/pdf URL` 调用 uv 管理的 Python Playwright helper，使用系统 Chrome 打印 PDF；`pdf.auto_domains` 里的域名会自动走 PDF。
- 全局并发由配置控制，超出的任务会排队。
- 外部命令会流式采集 stdout/stderr，并监控输出目录文件大小；长时间无输出且无文件增长会自动失败，避免任务一直停在 `Started`。
- 任务开始后会发送一条状态消息，后续下载/混流进度会尽量通过 Telegram edit message 在同一条消息中刷新。

## 配置

复制示例配置并填入 Telegram token：

```sh
cp config.example.toml config.toml
```

`config.toml` 包含本机路径和 token，已经被 `.gitignore` 忽略。示例配置使用 `~` 表示当前用户 home；程序也支持在路径开头使用 `~`、`$HOME` 或 `${HOME}`。默认下载目录是：

- 视频：`~/Movies/Downloads`
- PDF：`~/Documents/Downloads`

`telegram.allowed_chat_ids` 必须配置为允许使用这个 bot 的 chat id。个人私聊通常是你的用户 chat id；群组使用群组 chat id。确实需要临时放开时，可以显式设置 `allow_all_chats = true`。

`pdf.auto_domains` 默认包含 `mp.weixin.qq.com`。Bilibili 视频和 YouTube 链接始终优先按视频处理，不会被 PDF 白名单吞掉；Bilibili `opus` 文章链接会自动走 PDF，并丢弃分享 query 参数。Bilibili `opus` PDF 会使用 archive print 样式隐藏页面导航、目录、分享和反馈控件，保留作者、标题、正文、图片和版权信息。

`video.subtitle_languages` 默认按中文、英文、日语优先。YouTube 会先找人工字幕；如果这些语言没有人工字幕，再使用自动字幕。`write_nfo = true` 会为视频生成同 basename 的 `.nfo`，`keep_sidecars = true` 会让 yt-dlp 保留 `.info.json`、`.description` 和封面 sidecar。

重复视频检测会单次扫描视频文件名与同 basename sidecar，建立媒体 ID 索引后复用。YouTube 使用 URL 中的 video id；Bilibili 会先使用 URL 中的 `BV...` / `av...` / `ep...`，再通过 `bbdown-core` plan API 解析 bvid、aid、cid 和 epid，因此 `b23.tv` 短链和番剧条目也可以在下载前弹出重复选择。Bilibili 的 bvid/aid 可能同时对应多个分 P，只用于提示存在相关文件；只有单条下载计划的 cid/epid 能由 NFO 或 info JSON sidecar 证明并唯一匹配一个现有文件时才允许覆盖，文件名里的 cid/epid 只用于重复提示。全集和歧义匹配不会显示覆盖按钮，服务端也会再次拒绝不安全的覆盖请求。覆盖时，bot 会先把旧媒体及其 sidecar 移入本次事务独占的隐藏备份目录，再从已取得的文件重建严格身份索引；目标缺失、metadata 不可读、身份变化、路径被重新占用或新增歧义都会拒绝覆盖并尝试恢复原文件。检测失败时任务仍走 staging keep-both 移动，避免直接覆盖最终目录里的同名文件。

Bilibili 下载和登录不需要本机 `bbdown` 可执行文件；项目直接依赖 `BBDown-rust` 的 `bbdown-core` crate。`tools.ffmpeg` 仍会传给 crate 用于 mux，bot 负责 NFO、staging 和重复文件处理；FLV concat mux 使用每次任务独占的隐藏临时清单，不会覆盖或清理下载目录里同名的用户文件。

区域受限或 intl 番剧可以配置 `playurl_mode`、`restricted_area`、`restricted_area_proxies`、`restricted_api_proxies`。为兼容旧配置，`bilibili.extra_args` 和 `bilibili.global_args` 里的已知 BBDown-rust 全局项也会被 direct API 读取：endpoint base、`--playurl-mode`、`--restricted-area`、restricted proxy 和 `--request-timeout-seconds`。单值参数按 `extra_args`、`global_args`、结构化字段的顺序覆盖；restricted proxy 参数保持累加。`bilibili.plan_args` 不再用于主路径；`bilibili.download_args` 仅保留 `--only audio|video|subtitle|danmaku|cover` 这类下载模式迁移。

`bilibili.danmaku.enabled = true` 时，bot 会让 `bbdown-core` 写出配置里的弹幕格式，默认是 `.xml` 和 `.ass` sidecar，并让它们跟随 staging、覆盖和两者并存流程移动。后续会接入 `bbdown-core` 的 danmaku update API，用于只更新已有视频的弹幕 sidecar；暂时不做 PGO/PGS 图形字幕预渲染。

`bilibili.auth.credential_file` 是 `bbdown-core` credential 文件，默认写到 `~/.local/state/telegram-video-downloader/bbdown-credentials.json`；可选的 `credential_profile` 会选择同一文件里的 profile。`/bbdown login` 默认等同 `/bbdown login web`，会直接创建并轮询 Web QR；`/bbdown login tv` 会保存 TV 专用 `tv_access_key`；`/bbdown login access-key` 会发送 BiliPlus/BALH 授权 QR 和链接，授权后把 callback URL 或 `balh-login-credentials:` 消息发回同一个私聊即可保存 generic intl/Bstar `access_key`。`/bbdown status` 通过 crate API 检查 cookie、`access_key` 和 `tv_access_key`；`/bbdown logout` 清理当前 credential/profile，并兼容删除旧版 bot Web cookie state。

`bot.progress_update_seconds` 控制进度回复频率，默认 5 秒。YouTube/PDF 外部命令会按这个间隔刷新文件增长快照；Bilibili 会转发 `bbdown-core` 的关键 plan、download 和 mux 阶段。`bot.command_timeout_seconds` 是单个外部命令的总超时；direct Bilibili 下载不受这个总时限约束，而是把 `bot.command_idle_timeout_seconds` 作为媒体读取 idle timeout 传给 `bbdown-core`。Bilibili API 请求仍受独立的 request timeout 约束，bot 调用的 ffmpeg 等外部命令仍受总超时和 idle timeout 约束。

## 运行

```sh
cargo run -- config.toml
```

发送示例：

```text
https://www.bilibili.com/video/BV...
Title https://www.bilibili.com/video/BV...
https://www.bilibili.com/bangumi/play/ss...
https://www.bilibili.tv/en/play/...
https://youtu.be/...
/pdf https://example.com/article
https://mp.weixin.qq.com/s?...
https://m.bilibili.com/opus/1206098216310800386?share_source=COPY
/help
/bbdown login web
/bbdown login tv
/bbdown login access-key
/bbdown status
```

本地重放一条 Telegram 文本、不走真实 Telegram API：

```sh
cargo run -- --replay-message config.toml "Title https://b23.tv/..."
```

`ss/md` 番剧系列入口需要 Telegram inline keyboard 选择，`--replay-message` 不会替你默认选择；本地重放请使用具体 `ep` 链接。

## macOS 自启动

用户级 LaunchAgent 可以让 bot 在当前用户的 launchd session 里自动启动和保活。安装脚本会构建 release binary，并把 plist 写入 `~/Library/LaunchAgents`：

```sh
scripts/launch_agent.sh install
```

常用操作：

```sh
scripts/launch_agent.sh status
scripts/launch_agent.sh restart
scripts/launch_agent.sh logs
scripts/launch_agent.sh uninstall
```

脚本默认使用：

- label：`io.github.telegram-local-downloader.bot`
- config：`./config.toml`
- binary：`./target/release/telegram-video-downloader`
- logs：`~/Library/Logs/TelegramVideoDownloader/`
- launchd domain：`user/$(id -u)`

这些都可以通过环境变量覆盖，例如：

```sh
BOT_LABEL=com.example.telegram-downloader BOT_CONFIG=/path/to/config.toml scripts/launch_agent.sh install
```

## 验证

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
uv run ruff format --check
uv run ruff check
uv run python -m unittest discover -s tests
```
