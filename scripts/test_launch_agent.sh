#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/telegram-launch-agent-test.XXXXXX")"
trap 'rm -rf "${test_root}"' EXIT

fake_bin="${test_root}/bin"
fake_log="${test_root}/launchctl.log"
fake_state="${test_root}/launchctl-state"
test_home="${test_root}/home"
label="io.github.telegram-local-downloader.migration-probe"
gui_service="gui/$(id -u)/${label}"
legacy_service="user/$(id -u)/${label}"
custom_service="custom/123/${label}"
mkdir -p "${fake_bin}" "${fake_state}" "${test_home}"
touch "${test_root}/config.toml"

cat > "${test_root}/stub" <<'EOF'
#!/usr/bin/env bash
sleep 600
EOF
chmod 700 "${test_root}/stub"

cat > "${fake_bin}/launchctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${FAKE_LAUNCHCTL_LOG}"

state_file() {
  local target="$1"
  target="${target//\//_}"
  printf '%s/%s\n' "${FAKE_LAUNCHCTL_STATE}" "${target}"
}

missing_service() {
  printf 'Bad request.\nCould not find service "%s"\n' "$1" >&2
  exit 113
}

missing_domain() {
  printf 'Could not find domain for "%s"\n' "$1" >&2
  exit 3
}

target_state=""
case "$1" in
  print)
    target_state="$(state_file "$2")"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "gui-query-fails-code-one" && "$2" == gui/* ]]; then
      printf '%s\n' "simulated GUI query failure" >&2
      exit 1
    fi
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "gui-domain-missing" && "$2" == gui/* ]]; then
      missing_domain "$2"
    fi
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "legacy-query-fails" && "$2" == user/* ]]; then
      printf '%s\n' "simulated legacy query failure" >&2
      exit 42
    fi
    if [[ -e "${target_state}.delay" ]]; then
      remaining="$(<"${target_state}.delay")"
      if (( remaining > 0 )); then
        printf '%s\n' "$((remaining - 1))" > "${target_state}.delay"
      else
        rm -f "${target_state}" "${target_state}.delay"
      fi
    fi
    [[ -e "${target_state}" ]] || missing_service "$2"
    printf '%s\n' "$2"
    ;;
  bootout)
    target_state="$(state_file "$2")"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "legacy-bootout-fails" && "$2" == user/* ]]; then
      printf '%s\n' "simulated legacy bootout failure" >&2
      exit 42
    fi
    [[ -e "${target_state}" ]] || missing_service "$2"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "delayed-bootout" ]]; then
      printf '%s\n' 2 > "${target_state}.delay"
    else
      rm -f "${target_state}"
    fi
    ;;
  bootstrap)
    touch "$(state_file "$2/${FAKE_LAUNCHCTL_LABEL}")"
    ;;
  kickstart)
    target_state="$(state_file "$3")"
    [[ -e "${target_state}" ]] || missing_service "$3"
    ;;
  *)
    printf '%s\n' "unexpected launchctl action: $1" >&2
    exit 64
    ;;
esac
EOF
chmod 700 "${fake_bin}/launchctl"

state_file() {
  local target="$1"
  target="${target//\//_}"
  printf '%s/%s\n' "${fake_state}" "${target}"
}

reset_state() {
  rm -f "${fake_state}"/* "${fake_state}"/*.delay
  local target
  for target in "$@"; do
    touch "$(state_file "${target}")"
  done
}

run_agent() {
  HOME="${test_home}" \
    PATH="${fake_bin}:${PATH}" \
    FAKE_LAUNCHCTL_LOG="${fake_log}" \
    FAKE_LAUNCHCTL_STATE="${fake_state}" \
    FAKE_LAUNCHCTL_LABEL="${label}" \
    FAKE_LAUNCHCTL_MODE="${FAKE_LAUNCHCTL_MODE:-normal}" \
    BOT_LABEL="${label}" \
    BOT_CONFIG="${test_root}/config.toml" \
    BOT_BINARY="${test_root}/stub" \
    BOT_LOG_DIR="${test_root}/logs" \
    "${repo_dir}/scripts/launch_agent.sh" "$@"
}

expect_log() {
  grep -Fqx -- "$1" "${fake_log}"
}

expect_no_log() {
  if grep -Fqx -- "$1" "${fake_log}"; then
    printf 'unexpected launchctl invocation: %s\n' "$1" >&2
    exit 1
  fi
}

empty_log() {
  : > "${fake_log}"
}

reset_state "${legacy_service}"
empty_log
run_agent install --no-build
expect_log "bootout ${legacy_service}"
expect_log "bootstrap gui/$(id -u) ${test_home}/Library/LaunchAgents/${label}.plist"
expect_log "print ${gui_service}"
[[ -e "$(state_file "${gui_service}")" ]]
[[ ! -e "$(state_file "${legacy_service}")" ]]
if grep -Fq "LimitLoadToSessionType" "${test_home}/Library/LaunchAgents/${label}.plist"; then
  printf '%s\n' "generated plist retained the invalid session-type restriction" >&2
  exit 1
fi

reset_state "${legacy_service}"
empty_log
run_agent status >/dev/null
expect_log "print ${gui_service}"
expect_log "print ${legacy_service}"

reset_state "${legacy_service}"
empty_log
run_agent restart
expect_log "kickstart -k ${legacy_service}"

reset_state "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=gui-domain-missing run_agent status >/dev/null
expect_log "print ${gui_service}"
expect_log "print ${legacy_service}"

reset_state "${legacy_service}"
empty_log
if FAKE_LAUNCHCTL_MODE=gui-query-fails-code-one run_agent status >/dev/null 2>&1; then
  printf '%s\n' "status ignored a GUI service query failure with exit code 1" >&2
  exit 1
fi
expect_log "print ${gui_service}"
expect_no_log "print ${legacy_service}"

reset_state "${gui_service}" "${legacy_service}"
empty_log
run_agent uninstall
expect_log "bootout ${gui_service}"
expect_log "bootout ${legacy_service}"
[[ ! -e "$(state_file "${gui_service}")" ]]
[[ ! -e "$(state_file "${legacy_service}")" ]]
if [[ -e "${test_home}/Library/LaunchAgents/${label}.plist" ]]; then
  printf '%s\n' "uninstall retained the generated plist" >&2
  exit 1
fi

reset_state
empty_log
BOT_DOMAIN="custom/123" run_agent install --no-build
expect_log "bootstrap custom/123 ${test_home}/Library/LaunchAgents/${label}.plist"
[[ -e "$(state_file "${custom_service}")" ]]
if grep -Fq "${legacy_service}" "${fake_log}"; then
  printf '%s\n' "explicit BOT_DOMAIN unexpectedly migrated the legacy domain" >&2
  exit 1
fi

empty_log
BOT_DOMAIN="custom/123" run_agent uninstall
expect_log "bootout ${custom_service}"
if grep -Fq "${legacy_service}" "${fake_log}"; then
  printf '%s\n' "explicit BOT_DOMAIN unexpectedly cleaned the legacy domain" >&2
  exit 1
fi

reset_state "${legacy_service}"
empty_log
if FAKE_LAUNCHCTL_MODE=legacy-query-fails run_agent install --no-build >/dev/null 2>&1; then
  printf '%s\n' "install ignored a legacy service query failure" >&2
  exit 1
fi
if grep -Fq "bootstrap gui/$(id -u)" "${fake_log}"; then
  printf '%s\n' "install started a GUI service after legacy query failure" >&2
  exit 1
fi
[[ -e "$(state_file "${legacy_service}")" ]]
[[ ! -e "${test_home}/Library/LaunchAgents/${label}.plist" ]]

reset_state "${legacy_service}"
empty_log
if FAKE_LAUNCHCTL_MODE=legacy-bootout-fails run_agent install --no-build >/dev/null 2>&1; then
  printf '%s\n' "install ignored a legacy service cleanup failure" >&2
  exit 1
fi
expect_log "bootstrap gui/$(id -u) ${test_home}/Library/LaunchAgents/${label}.plist"
expect_log "bootout ${legacy_service}"
expect_log "bootout ${gui_service}"
[[ -e "$(state_file "${legacy_service}")" ]]
[[ ! -e "$(state_file "${gui_service}")" ]]

reset_state "${gui_service}" "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=delayed-bootout run_agent uninstall
[[ ! -e "$(state_file "${gui_service}")" ]]
[[ ! -e "$(state_file "${legacy_service}")" ]]

reset_state "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=gui-domain-missing run_agent uninstall
expect_log "print ${gui_service}"
expect_log "bootout ${legacy_service}"
[[ ! -e "$(state_file "${legacy_service}")" ]]

printf '%s\n' "launch-agent integration test passed"
