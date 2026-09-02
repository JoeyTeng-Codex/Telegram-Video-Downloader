---
id: 20260902-lae01
title: LaunchAgent Bootstrap Recovery
status: completed
created: 2026-09-02
updated: 2026-09-02
branch: wip/launch-agent-bootstrap
pr:
supersedes: []
superseded_by:
---

# LaunchAgent Bootstrap Recovery

## Summary
- Restored macOS login-start deployment after `scripts/launch_agent.sh install` returned `Bootstrap failed: 5: Input/output error`.

## Current State
- The installer targets the current GUI launchd domain by default: `gui/$(id -u)`.
- Generated plists no longer set `LimitLoadToSessionType=Background`; that key prevented an otherwise valid Telegram downloader plist from bootstrapping in the active GUI session.
- Default-domain operations migrate prior `user/$(id -u)` installations: install/uninstall clean the old service, while status/restart fall back to it until migration. Explicit `BOT_DOMAIN` values remain isolated from that compatibility behavior.

## Evidence
- The generated plist passed `plutil -lint` and its binary, log directory, and plist permissions were readable by the current user.
- A temporary plist with a unique label continued to fail with the session-type key, then bootstrapped successfully after only that key was removed.
- The fixed production installer built the release binary and registered `gui/501/io.github.telegram-local-downloader.bot`; after the startup window, launchd reported `state = running` and the bot emitted a fresh startup marker.
- The checked-in fake-`launchctl` integration test (`scripts/test_launch_agent.sh`) passed: default install cleans the legacy domain and registers GUI, status/restart fall back to legacy, uninstall cleans both domains, and explicit `BOT_DOMAIN` skips migration. It simulates delayed service disappearance after `bootout`, an absent GUI domain, and a query failure with exit code `1`; shutdown waits are bounded and only explicit absent-service/domain diagnostics select fallback. Legacy query failures stop before bootstrap, while legacy cleanup failures roll back the new GUI service. The test uses only temporary files and does not create a second Telegram bot.
