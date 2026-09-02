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

## Evidence
- The generated plist passed `plutil -lint` and its binary, log directory, and plist permissions were readable by the current user.
- A temporary plist with a unique label continued to fail with the session-type key, then bootstrapped successfully after only that key was removed.
- The fixed production installer built the release binary and registered `gui/501/io.github.telegram-local-downloader.bot`; after the startup window, launchd reported `state = running` and the bot emitted a fresh startup marker.
