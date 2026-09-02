#!/usr/bin/env bash
set -euo pipefail

DEFAULT_LABEL="io.github.telegram-local-downloader.bot"

usage() {
  cat <<'EOF'
Usage:
  scripts/launch_agent.sh install [--label LABEL] [--config PATH] [--binary PATH] [--no-build]
  scripts/launch_agent.sh uninstall [--label LABEL]
  scripts/launch_agent.sh restart [--label LABEL]
  scripts/launch_agent.sh status [--label LABEL]
  scripts/launch_agent.sh logs [--label LABEL]

Environment overrides:
  BOT_LABEL       LaunchAgent label. Defaults to io.github.telegram-local-downloader.bot.
  BOT_CONFIG      Config file path. Defaults to ./config.toml.
  BOT_BINARY      Binary path. Defaults to ./target/release/telegram-video-downloader.
  BOT_LOG_DIR     Log directory. Defaults to ~/Library/Logs/TelegramVideoDownloader.
  BOT_DOMAIN      launchd domain. Defaults to gui/$(id -u); bypasses legacy migration when set.
  BOT_DOTNET_ROOT Optional .NET runtime root for BBDown global-tool apphosts.
  BOT_SKIP_BUILD  Set to 1 to skip cargo build during install.
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_dir="$(cd -- "${script_dir}/.." && pwd -P)"

action="${1:-}"
if [[ -z "${action}" ]]; then
  usage
  exit 2
fi
shift

label="${BOT_LABEL:-${DEFAULT_LABEL}}"
config_path="${BOT_CONFIG:-${repo_dir}/config.toml}"
binary_path="${BOT_BINARY:-${repo_dir}/target/release/telegram-video-downloader}"
log_dir="${BOT_LOG_DIR:-${HOME}/Library/Logs/TelegramVideoDownloader}"
default_domain="gui/$(id -u)"
legacy_domain="user/$(id -u)"
domain="${BOT_DOMAIN:-${default_domain}}"
use_default_domain=0
if [[ -z "${BOT_DOMAIN:-}" ]]; then
  use_default_domain=1
fi
dotnet_root="${BOT_DOTNET_ROOT:-}"
skip_build="${BOT_SKIP_BUILD:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label)
      [[ $# -ge 2 ]] || die "--label requires a value"
      label="$2"
      shift 2
      ;;
    --config)
      [[ $# -ge 2 ]] || die "--config requires a value"
      config_path="$2"
      shift 2
      ;;
    --binary)
      [[ $# -ge 2 ]] || die "--binary requires a value"
      binary_path="$2"
      shift 2
      ;;
    --no-build)
      skip_build="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

xml_escape() {
  local value="$1"
  value="${value//&/&amp;}"
  value="${value//</&lt;}"
  value="${value//>/&gt;}"
  value="${value//\"/&quot;}"
  value="${value//\'/&apos;}"
  printf '%s' "${value}"
}

absolute_path() {
  local path="$1"
  local dir
  local base
  case "${path}" in
    [~])
      path="${HOME}"
      ;;
    [~]/*)
      path="${HOME}/${path#\~/}"
      ;;
  esac
  if [[ "${path}" != /* ]]; then
    path="${PWD}/${path}"
  fi
  dir="$(dirname -- "${path}")"
  base="$(basename -- "${path}")"
  printf '%s/%s\n' "$(cd -- "${dir}" && pwd -P)" "${base}"
}

plist_path() {
  printf '%s/Library/LaunchAgents/%s.plist\n' "${HOME}" "${label}"
}

service_name_for_domain() {
  local target_domain="$1"
  printf '%s/%s\n' "${target_domain}" "${label}"
}

service_name() {
  service_name_for_domain "${domain}"
}

legacy_service_name() {
  service_name_for_domain "${legacy_domain}"
}

cleanup_legacy_service() {
  if [[ "${use_default_domain}" == "1" ]]; then
    launchctl bootout "$(legacy_service_name)" >/dev/null 2>&1 || true
  fi
}

active_or_default_service_name() {
  if launchctl print "$(service_name)" >/dev/null 2>&1; then
    service_name
    return
  fi
  if [[ "${use_default_domain}" == "1" ]] \
    && launchctl print "$(legacy_service_name)" >/dev/null 2>&1; then
    legacy_service_name
    return
  fi
  service_name
}

is_dotnet_root() {
  local candidate="$1"
  [[ -d "${candidate}/shared/Microsoft.NETCore.App" || -d "${candidate}/host/fxr" ]]
}

detect_dotnet_root() {
  local candidate

  if [[ -n "${dotnet_root}" ]]; then
    candidate="$(absolute_path "${dotnet_root}")"
    if is_dotnet_root "${candidate}"; then
      printf '%s\n' "${candidate}"
      return
    fi
    die "BOT_DOTNET_ROOT does not look like a .NET runtime root: ${dotnet_root}"
  fi

  if command -v dotnet >/dev/null 2>&1; then
    candidate="$(
      { dotnet --list-runtimes 2>/dev/null || true; } \
        | sed -n 's/.*\[\(.*\)\/shared\/.*/\1/p' \
        | head -n 1
    )"
    if [[ -n "${candidate}" ]] && is_dotnet_root "${candidate}"; then
      absolute_path "${candidate}"
      return
    fi
  fi

  for candidate in "${HOME}/.dotnet" "/usr/local/share/dotnet" "/opt/homebrew/share/dotnet"; do
    if is_dotnet_root "${candidate}"; then
      absolute_path "${candidate}"
      return
    fi
  done
}

write_plist() {
  local output="$1"
  local binary="$2"
  local config="$3"
  local workdir="$4"
  local logs="$5"
  local runtime_root="$6"
  local escaped_label
  local escaped_binary
  local escaped_config
  local escaped_workdir
  local escaped_logs
  local escaped_home
  local escaped_path
  local escaped_dotnet_root
  local dotnet_env_block
  local path_value

  escaped_label="$(xml_escape "${label}")"
  escaped_binary="$(xml_escape "${binary}")"
  escaped_config="$(xml_escape "${config}")"
  escaped_workdir="$(xml_escape "${workdir}")"
  escaped_logs="$(xml_escape "${logs}")"
  escaped_home="$(xml_escape "${HOME}")"
  path_value="/opt/homebrew/bin:${HOME}/.dotnet:${HOME}/.dotnet/tools:/usr/local/share/dotnet:/opt/homebrew/share/dotnet:${HOME}/.local/bin:${HOME}/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
  dotnet_env_block=""
  if [[ -n "${runtime_root}" ]]; then
    escaped_dotnet_root="$(xml_escape "${runtime_root}")"
    escaped_path="$(xml_escape "${runtime_root}:${path_value}")"
    dotnet_env_block="    <key>DOTNET_ROOT</key>
    <string>${escaped_dotnet_root}</string>
    <key>DOTNET_ROOT_ARM64</key>
    <string>${escaped_dotnet_root}</string>"
  else
    escaped_path="$(xml_escape "${path_value}")"
  fi

  # A Background session restriction makes this GUI LaunchAgent fail to bootstrap.
  cat > "${output}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${escaped_label}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${escaped_binary}</string>
    <string>${escaped_config}</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${escaped_workdir}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>${escaped_home}</string>
${dotnet_env_block}
    <key>PATH</key>
    <string>${escaped_path}</string>
    <key>RUST_LOG</key>
    <string>info</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${escaped_logs}/stdout.log</string>

  <key>StandardErrorPath</key>
  <string>${escaped_logs}/stderr.log</string>
</dict>
</plist>
EOF
}

install_agent() {
  local plist
  local temp_plist
  local binary
  local config
  local runtime_root

  [[ -f "${config_path}" ]] || die "config file not found: ${config_path}"

  if [[ "${skip_build}" != "1" ]]; then
    cargo build --release --manifest-path "${repo_dir}/Cargo.toml"
  fi

  [[ -x "${binary_path}" ]] || die "binary is not executable: ${binary_path}"

  plist="$(plist_path)"
  temp_plist="$(mktemp)"
  binary="$(absolute_path "${binary_path}")"
  config="$(absolute_path "${config_path}")"
  if [[ "${log_dir}" != /* ]]; then
    log_dir="${PWD}/${log_dir}"
  fi

  mkdir -p "$(dirname -- "${plist}")" "${log_dir}"
  log_dir="$(cd -- "${log_dir}" && pwd -P)"
  runtime_root="$(detect_dotnet_root)"
  write_plist "${temp_plist}" "${binary}" "${config}" "${repo_dir}" "${log_dir}" "${runtime_root}"
  plutil -lint "${temp_plist}" >/dev/null
  install -m 644 "${temp_plist}" "${plist}"
  rm -f "${temp_plist}"

  cleanup_legacy_service
  launchctl bootout "$(service_name)" >/dev/null 2>&1 || true
  launchctl bootstrap "${domain}" "${plist}"
  launchctl print "$(service_name)"
}

uninstall_agent() {
  local plist
  plist="$(plist_path)"
  launchctl bootout "$(service_name)" >/dev/null 2>&1 || true
  cleanup_legacy_service
  rm -f "${plist}"
}

case "${action}" in
  install)
    install_agent
    ;;
  uninstall)
    uninstall_agent
    ;;
  restart)
    launchctl kickstart -k "$(active_or_default_service_name)"
    ;;
  status)
    launchctl print "$(active_or_default_service_name)"
    ;;
  logs)
    tail -f "${log_dir}/stdout.log" "${log_dir}/stderr.log"
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
