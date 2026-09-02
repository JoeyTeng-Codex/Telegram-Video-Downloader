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

重复视频检测会单次扫描视频文件名与同 basename sidecar，建立媒体 ID 索引后复用。YouTube 使用 URL 中的 video id；Bilibili 会先使用 URL 中的 `BV...` / `av...` / `ep...`，再通过 `bbdown-core` plan API 解析 bvid、aid、cid 和 epid，因此 `b23.tv` 短链和番剧条目也可以在下载前弹出重复选择。Bilibili 的 bvid/aid 可能同时对应多个分 P，只用于提示存在相关文件；只有单条下载计划的 cid/epid 能由 NFO 或 info JSON sidecar 证明并唯一匹配一个现有文件时才允许覆盖，文件名里的 cid/epid 只用于重复提示。全集和歧义匹配不会显示覆盖按钮，服务端也会再次拒绝不安全的覆盖请求。实际下载使用的 plan 还必须保留确认时的精确 cid/epid，否则任务中止。覆盖时，bot 会先把旧媒体及明确属于同 basename 的 sidecar 移入本次事务独占的隐藏备份目录，再从已取得的文件重建严格身份索引；无法明确归属的裸 `danmaku.*`、`subtitle-*` 或 `cover-*` 文件不会被旧文件清理误删。目标缺失、metadata 不可读、身份变化、路径被重新占用或新增歧义都会拒绝覆盖并尝试恢复原文件。如果单个已完成任务意外产出多个主媒体文件，bot 会保留旧文件并把全部新产物按“两者并存”提交，避免清理 staging 时丢失下载结果。检测失败时任务仍走 staging keep-both 移动，避免直接覆盖最终目录里的同名文件。

覆盖按钮生成时会以 `O_NOFOLLOW` 打开并持有现有媒体文件，直到任务完成，避免确认后删除并复用 inode 的同路径文件继承覆盖授权。旧媒体备份保留原文件名；完整视频覆盖会先提交主媒体，再提交 sidecar，对每个已提交输出执行 `fsync`，然后为其在事务目录创建同 inode 的硬链接锚点，并以“完整写入临时文件后原子交换”的方式把 recovery manifest 从 `acquired` 切换为 `committed`。只有这些输出内容和 committed recovery state 都持久化后才会删除旧备份。旧备份全部安全清理前锚点始终存在，因此输出路径即使被删除也不能把已记录 inode 转让给无关替换文件。最后一次输出验证后，包含 manifest 和全部锚点的事务目录会整体原子移入受控删除隔离区；崩溃恢复不会看到只删除了一部分锚点的 committed 事务。bot 每次启动都会扫描这些受控事务；正常任务还会在持有跨进程输出锁时，把 `.telegram-video-downloader-control` 私有目录里的恢复状态通过原子文件替换写为 dirty，只有任务和恢复都干净结束后才写回 clean。控制目录由绑定根目录和控制目录 inode 的 owner 记录认证，状态文件要求私有权限且硬链接数为 1；升级时旧根目录 marker 只读检测并触发一次恢复扫描，不会被修改。后续任务只在标记不干净时再次递归恢复，避免每次下载前后都完整扫描媒体库。`acquired` 事务只在目标路径仍为空时回滚，路径被占用时保留全部对象供人工处理；v3 `committed` 事务只有在当前输出仍与持久锚点相同、且 manifest 身份一致时才完成清理；没有锚点的旧 v2 committed 事务会 fail closed 并保留备份。视频目录里的持久锁文件会跨 bot、`--replay-message` 和启动恢复进程串行化输出事务。无法识别的旧版备份目录、结构异常的事务和无关的不可读子目录会分别保留或跳过并写入日志，不会误删文件或阻止其他事务恢复。

Bilibili 下载和登录不需要本机 `bbdown` 可执行文件；项目直接依赖 `BBDown-rust` 的 `bbdown-core` crate。bot 会关闭 crate 内置 mux，再通过配置的 `tools.ffmpeg` 执行受统一超时和进程管理约束的 mux，同时负责 NFO、staging 和重复文件处理。外层 Bilibili worker 使用独立进程组且不受外部命令总截止时间约束；worker 内的 ffmpeg 继承该进程组，同时仍保留自己的总超时和 idle timeout。worker 持有由 bot 提供的父进程存活通道；bot 遇到 `SIGTERM` 会正常关闭运行时，异常退出则会关闭通道，worker 随即终止自己的完整进程组。ffmpeg 及 wrapper 后代还会继承一个独立的关闭 fence，只有该 fence 和命令输出都关闭后才允许提交 mux 结果；wrapper 提前退出、父任务显式终止或 command future 被取消时都会清理整个 worker 进程组，因此 mux 不会脱离 worker 继续写文件。mux 前会绑定每个原始流文件，并在输出目录下创建权限为 `0700`、带 fsynced recovery manifest 的任务私有 staging 目录；ffmpeg 通过继承的文件描述符读取这些已绑定输入，并只接触 staging 里的已绑定空输出文件，因此输入路径被替换也不会改变实际读取对象，公开目标路径在 mux 期间保持不存在。ffmpeg 成功后，bot 会 `fsync` 同一输出对象，再通过原子 no-replace rename 发布到最终路径，并在事务目录创建同 inode 的输出锚点。每次删除原始流前都会复验所有输出和锚点；最终验证后才整体提交事务目录。强制终止时，启动恢复会丢弃未发布的 partial mux，或通过锚点恢复已发布输出并继续清理剩余原始流；路径被无关文件占用时会保留事务和锚点供人工处理。身份绑定的文件或目录清理会先把目标整体原子移入私有隔离区，确认仍是预期 device/inode/type 后才删除；隔离区在父目录持久化 terminal tombstone，目录删除后才移除 tombstone，因此 manifest、目录或锚点清理任一步崩溃都能继续。竞态替换对象会恢复或保留，不会被误删。FLV concat mux 使用每次任务独占并通过文件描述符传给 ffmpeg 的隐藏临时清单，不会覆盖或清理下载目录里同名的用户文件。

staging 下载成功后会先持久化身份绑定的 `.retained.json`；Bilibili worker 会在向父进程报告成功前完成这一步。随后发布流程在写入 `.publication.json` 前 `fsync` 每个源文件，再从最深层源目录逐级同步到任务 staging 根目录；发布所需的最终目录也会通过绑定父目录逐层创建并立即持久化。正常发布完成会连同 staging 一起删除完成标记。若一次已完成下载超过 4096 个发布文件，或 recovery manifest 超过 512 KiB，bot 不会删除耗时下载得到的产物，而是保留该标记并返回人工恢复路径。后续启动恢复会校验并保留该目录，把它记录为不需重试的终态，因此不会误删产物或让全局恢复状态长期保持 dirty。

区域受限或 intl 番剧可以配置 `playurl_mode`、`restricted_area`、`restricted_area_proxies`、`restricted_api_proxies`。为兼容旧配置，`bilibili.extra_args` 和 `bilibili.global_args` 里的已知 BBDown-rust 全局项也会被 direct API 读取：endpoint base、`--playurl-mode`、`--restricted-area`、restricted proxy 和 `--request-timeout-seconds`。单值参数按 `extra_args`、`global_args`、结构化字段的顺序覆盖；restricted proxy 参数保持累加。`bilibili.download_args` 仅保留 `--only audio|video|subtitle|danmaku|cover` 和 `--audio-only`/`--video-only` 这类下载模式迁移。其余旧 CLI 参数、任何非空 `bilibili.plan_args`，以及 `video_dir/BBDown.config` 都会在启动时明确报错，必须迁移到结构化 `config.toml` 设置后再运行，避免 direct API 静默忽略旧行为。

`bilibili.danmaku.enabled = true` 时，bot 会让 `bbdown-core` 写出配置里的弹幕格式，默认是 `.xml` 和 `.ass` sidecar，并让它们跟随 staging、覆盖和两者并存流程移动。后续会接入 `bbdown-core` 的 danmaku update API，用于只更新已有视频的弹幕 sidecar；暂时不做 PGO/PGS 图形字幕预渲染。

`bilibili.auth.credential_file` 是 `bbdown-core` credential 文件，默认写到 `~/.local/state/telegram-video-downloader/bbdown-credentials.json`；可选的 `credential_profile` 会选择同一文件里的 profile。`credential_file` 的直接父目录不能是符号链接；需要使用链接目录时，请配置其解析后的真实目录。`/bbdown login` 默认等同 `/bbdown login web`，会直接创建并轮询 Web QR；`/bbdown login tv` 会保存 TV 专用 `tv_access_key`；`/bbdown login access-key` 会发送 BiliPlus/BALH 授权 QR 和链接，授权后把 callback URL 或 `balh-login-credentials:` 消息发回同一个私聊即可保存 generic intl/Bstar `access_key`。`/bbdown status` 通过 crate API 检查 cookie、`access_key` 和 `tv_access_key`；`/bbdown logout` 清理当前 credential/profile，并兼容删除旧版 bot Web cookie state。legacy cookie migration、fresh login 和 logout 使用同一个常驻、`O_NOFOLLOW` 打开的跨进程锁文件。该文件通过两个交替写入、`fsync` 的固定大小槽位保存认证 epoch，并能从旧版有界 append log 自动迁移：登录开始时冻结当前 epoch，写入凭据前必须仍匹配；status、登录成功和 logout 的最终 Telegram 回复会在持有同一跨进程锁时重新核对 epoch。这样并行进程既不能在 logout 后用等待中的登录重新创建凭据，也不能在凭据变化后发送过期的“有效/成功”回复。

`bot.progress_update_seconds` 控制进度回复频率，默认 5 秒。进度通道只保留最新状态，Telegram 按该间隔合并刷新，不会因全集任务的高频事件积压消息。YouTube/PDF 外部命令会刷新文件增长快照；Bilibili 会转发 `bbdown-core` 的关键 plan、download 和 mux 阶段。`bot.command_timeout_seconds` 是单个外部命令的总超时；direct Bilibili 下载不受这个总时限约束，而是把 `bot.command_idle_timeout_seconds` 作为媒体读取 idle timeout 传给 `bbdown-core`。Bilibili API 请求仍受独立的 request timeout 约束，bot 调用的 ffmpeg 等外部命令仍受总超时和 idle timeout 约束。

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
- launchd domain：`gui/$(id -u)`（当前登录的图形会话）

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
