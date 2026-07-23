#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${PM_ROBOT_NAS_ROOT:-$SCRIPT_DIR}"
DOCKER="${DOCKER:-/usr/local/bin/docker}"
DOCKER_SUDO="${PM_ROBOT_DOCKER_SUDO:-auto}"
DOCKER_RUNNER_READY=0
DOCKER_RUNNER=()

PROXY_SERVICES="proxy-tunnel-primary proxy-tunnel-secondary proxy-tunnel"
CORE_SERVICES="$PROXY_SERVICES web"
DISCOVERY_SERVICES="discovery-loop rtds-discovery"
SCREEN_SERVICES="wallet-screen-planner wallet-screen-worker-0 wallet-screen-worker-1 wallet-screen-worker-2"
CONTROL_SERVICES="research-control"
HISTORY_SERVICES="wallet-history-worker-0 wallet-history-worker-1 wallet-history-worker-2"
L6_SERVICES="l6-validation-worker"
MAINTENANCE_SERVICES="maintenance-loop"
APP_SERVICES="$CORE_SERVICES $DISCOVERY_SERVICES $SCREEN_SERVICES $CONTROL_SERVICES $HISTORY_SERVICES $L6_SERVICES $MAINTENANCE_SERVICES"

env_file_value() {
  local name="$1"
  local value=""
  if [[ -f "$ROOT/.env" ]]; then
    value="$(awk -v key="$name" 'index($0, key "=") == 1 {sub(/^[^=]*=/, ""); print; exit}' "$ROOT/.env")"
  fi
  value="${value#\"}"
  value="${value%\"}"
  value="${value#\'}"
  value="${value%\'}"
  printf '%s' "$value"
}

storage_setting="${PM_ROBOT_STORAGE_ROOT:-$(env_file_value PM_ROBOT_STORAGE_ROOT)}"
if [[ -z "$storage_setting" ]]; then
  storage_setting="/volume1/poly_data/pmbot"
fi
if [[ "$storage_setting" == "." ]]; then
  STORAGE_ROOT="$ROOT"
elif [[ "$storage_setting" == /* ]]; then
  STORAGE_ROOT="$storage_setting"
else
  STORAGE_ROOT="$ROOT/$storage_setting"
fi

resolve_docker_runner() {
  if [[ "$DOCKER_RUNNER_READY" == "1" ]]; then
    return
  fi
  if [[ "$DOCKER_SUDO" == "always" || "$DOCKER_SUDO" == "1" ]]; then
    DOCKER_RUNNER=(sudo -n "$DOCKER")
  elif [[ "$DOCKER_SUDO" == "never" || "$DOCKER_SUDO" == "0" ]]; then
    DOCKER_RUNNER=("$DOCKER")
  elif "$DOCKER" ps >/dev/null 2>&1; then
    DOCKER_RUNNER=("$DOCKER")
  elif command -v sudo >/dev/null 2>&1 && sudo -n "$DOCKER" ps >/dev/null 2>&1; then
    DOCKER_RUNNER=(sudo -n "$DOCKER")
  else
    DOCKER_RUNNER=("$DOCKER")
  fi
  DOCKER_RUNNER_READY=1
}

docker_cli() {
  resolve_docker_runner
  "${DOCKER_RUNNER[@]}" "$@"
}

compose() {
  if docker_cli compose version >/dev/null 2>&1; then
    docker_cli compose --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" "$@"
    return
  fi
  echo "Docker Compose is not available on this NAS." >&2
  exit 1
}

task_compose() {
  compose run --rm --no-deps task "$@"
}

validate_proxy_config() {
  local primary_host secondary_host key_path known_hosts_path remote_port local_port
  local primary_tunnel_port secondary_tunnel_port
  primary_host="${PM_ROBOT_PROXY_PRIMARY_VPS_HOST:-$(env_file_value PM_ROBOT_PROXY_PRIMARY_VPS_HOST)}"
  secondary_host="${PM_ROBOT_PROXY_SECONDARY_VPS_HOST:-$(env_file_value PM_ROBOT_PROXY_SECONDARY_VPS_HOST)}"
  key_path="${PM_ROBOT_VPS_KEY_PATH:-$(env_file_value PM_ROBOT_VPS_KEY_PATH)}"
  known_hosts_path="${PM_ROBOT_VPS_KNOWN_HOSTS_PATH:-$(env_file_value PM_ROBOT_VPS_KNOWN_HOSTS_PATH)}"
  remote_port="${PM_ROBOT_PROXY_REMOTE_PORT:-$(env_file_value PM_ROBOT_PROXY_REMOTE_PORT)}"
  local_port="${PM_ROBOT_PROXY_LOCAL_PORT:-$(env_file_value PM_ROBOT_PROXY_LOCAL_PORT)}"
  primary_tunnel_port="${PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT:-$(env_file_value PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT)}"
  secondary_tunnel_port="${PM_ROBOT_PROXY_SECONDARY_TUNNEL_PORT:-$(env_file_value PM_ROBOT_PROXY_SECONDARY_TUNNEL_PORT)}"
  if [[ -z "$primary_host" ]]; then
    echo "PM_ROBOT_PROXY_PRIMARY_VPS_HOST is required before starting proxy-dependent services." >&2
    exit 2
  fi
  if [[ -z "$secondary_host" ]]; then
    echo "PM_ROBOT_PROXY_SECONDARY_VPS_HOST is required before starting proxy-dependent services." >&2
    exit 2
  fi
  if [[ -z "$key_path" ]]; then
    key_path="$ROOT/ssh/id_ed25519_pmrobot_vps"
  elif [[ "$key_path" == /ssh/* ]]; then
    key_path="$ROOT/ssh/$(basename "$key_path")"
  elif [[ "$key_path" != /* ]]; then
    key_path="$ROOT/$key_path"
  fi
  if [[ ! -r "$key_path" ]]; then
    echo "VPS tunnel key is missing or unreadable: $key_path" >&2
    exit 2
  fi
  if [[ -z "$known_hosts_path" ]]; then
    known_hosts_path="$ROOT/ssh/known_hosts"
  elif [[ "$known_hosts_path" == /ssh/* ]]; then
    known_hosts_path="$ROOT/ssh/$(basename "$known_hosts_path")"
  elif [[ "$known_hosts_path" != /* ]]; then
    known_hosts_path="$ROOT/$known_hosts_path"
  fi
  if [[ ! -r "$known_hosts_path" ]]; then
    echo "VPS known_hosts is missing or unreadable: $known_hosts_path" >&2
    exit 2
  fi
  local_port="${local_port:-18082}"
  remote_port="${remote_port:-18081}"
  primary_tunnel_port="${primary_tunnel_port:-18083}"
  secondary_tunnel_port="${secondary_tunnel_port:-18084}"
  for port_value in \
    "$local_port" \
    "$remote_port" \
    "$primary_tunnel_port" \
    "$secondary_tunnel_port"; do
    if [[ ! "$port_value" =~ ^[0-9]+$ ]] || (( port_value < 1 || port_value > 65535 )); then
      echo "Proxy port must be an integer in 1..65535: $port_value" >&2
      exit 2
    fi
  done
  if [[ "$local_port" == "$primary_tunnel_port" ||
        "$local_port" == "$secondary_tunnel_port" ||
        "$primary_tunnel_port" == "$secondary_tunnel_port" ]]; then
    echo "Proxy listener and tunnel ports must be distinct." >&2
    exit 2
  fi
}

ensure_layout() {
  mkdir -p \
    "$STORAGE_ROOT/data" \
    "$STORAGE_ROOT/data/parquet" \
    "$STORAGE_ROOT/logs" \
    "$STORAGE_ROOT/reports" \
    "$STORAGE_ROOT/backups" \
    "$ROOT/config" \
    "$ROOT/ssh"
  if [[ ! -f "$ROOT/.env" && -f "$ROOT/env.example" ]]; then
    cp "$ROOT/env.example" "$ROOT/.env"
    echo "Created $ROOT/.env from env.example"
  fi
}

start_group() {
  compose up -d --no-build --remove-orphans "$@"
}

restart_group() {
  compose up -d --no-build --remove-orphans "$@"
  compose restart "$@"
}

stop_group() {
  compose stop "$@"
}

runtime_summary() {
  local db_path="$STORAGE_ROOT/data/pm_robot.sqlite"
  if [[ ! -f "$db_path" ]]; then
    printf '%s\n' '{"database":"missing"}'
    return
  fi
  PM_ROBOT_STATUS_DB="$db_path" python3 - <<'PY'
import json
import os
import sqlite3
from pathlib import Path

path = Path(os.environ["PM_ROBOT_STATUS_DB"])
payload = {
    "database": str(path.name),
    "database_bytes": path.stat().st_size,
    "levels": {},
    "active_jobs": {},
    "history_artifacts": {},
}
conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
try:
    conn.row_factory = sqlite3.Row
    try:
        payload["levels"] = {
            str(row["level"]): int(row["wallets"])
            for row in conn.execute(
                "SELECT level, COUNT(*) AS wallets FROM wallet_levels GROUP BY level ORDER BY level"
            )
        }
    except sqlite3.Error:
        payload["levels"] = {}
    try:
        payload["active_jobs"] = {
            str(row["job_type"]): int(row["jobs"])
            for row in conn.execute(
                """
                SELECT job_type, COUNT(*) AS jobs
                FROM pipeline_jobs
                WHERE status IN ('queued', 'running')
                  AND job_type IN ('wallet_recent_screen', 'wallet_history_collect', 'wallet_l6_validate')
                GROUP BY job_type
                """
            )
        }
    except sqlite3.Error:
        payload["active_jobs"] = {}
    try:
        payload["history_artifacts"] = {
            str(row["history_depth"]): int(row["artifacts"])
            for row in conn.execute(
                """
                SELECT history_depth, COUNT(*) AS artifacts
                FROM wallet_history_artifacts
                WHERE status = 'active'
                GROUP BY history_depth
                """
            )
        }
    except sqlite3.Error:
        payload["history_artifacts"] = {}
finally:
    conn.close()
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
}

usage() {
  cat <<'EOF'
Usage: ./pmrobot-nas.sh COMMAND

Core:
  init                 Create persistent directories and .env when missing
  build                Build the two local images
  migrate              Apply SQLite migrations
  up | down | restart  Manage the complete L0-L6 discovery stack
  status               Show containers and compact L0-L6/queue state
  logs [service]       Follow logs (all active services by default)
  task ARGS...         Run a one-off pm-robot CLI command

Groups:
  web-up|web-down|web-restart
  discovery-up|discovery-down|discovery-restart
  screen-up|screen-down|screen-restart
  research-control-up|research-control-down|research-control-restart
  history-up|history-down|history-restart
  l6-up|l6-down|l6-restart
  maintenance-up|maintenance-down|maintenance-restart
  proxy-up|proxy-down|proxy-restart
EOF
}

cmd="${1:-status}"
shift || true
cd "$ROOT"

case "$cmd" in
  init)
    ensure_layout
    ;;
  build)
    ensure_layout
    compose build proxy-tunnel-primary web
    ;;
  migrate)
    ensure_layout
    task_compose migrate
    ;;
  up)
    ensure_layout
    validate_proxy_config
    task_compose migrate
    start_group $APP_SERVICES
    ;;
  down)
    stop_group $APP_SERVICES
    ;;
  restart)
    validate_proxy_config
    restart_group $APP_SERVICES
    ;;
  status|runtime-status)
    compose ps $APP_SERVICES
    runtime_summary
    ;;
  logs)
    if [[ "$#" -gt 0 ]]; then
      compose logs --tail "${PM_ROBOT_LOG_TAIL:-200}" -f "$@"
    else
      compose logs --tail "${PM_ROBOT_LOG_TAIL:-200}" -f $APP_SERVICES
    fi
    ;;
  task)
    task_compose "$@"
    ;;
  web-up) start_group web ;;
  web-down) stop_group web ;;
  web-restart) restart_group web ;;
  discovery-up) validate_proxy_config; start_group $DISCOVERY_SERVICES ;;
  discovery-down) stop_group $DISCOVERY_SERVICES ;;
  discovery-restart) validate_proxy_config; restart_group $DISCOVERY_SERVICES ;;
  screen-up) validate_proxy_config; start_group $SCREEN_SERVICES ;;
  screen-down) stop_group $SCREEN_SERVICES ;;
  screen-restart) validate_proxy_config; restart_group $SCREEN_SERVICES ;;
  research-control-up) start_group $CONTROL_SERVICES ;;
  research-control-down) stop_group $CONTROL_SERVICES ;;
  research-control-restart) restart_group $CONTROL_SERVICES ;;
  history-up) validate_proxy_config; start_group $HISTORY_SERVICES ;;
  history-down) stop_group $HISTORY_SERVICES ;;
  history-restart) validate_proxy_config; restart_group $HISTORY_SERVICES ;;
  l6-up) validate_proxy_config; start_group $L6_SERVICES ;;
  l6-down) stop_group $L6_SERVICES ;;
  l6-restart) validate_proxy_config; restart_group $L6_SERVICES ;;
  maintenance-up) start_group $MAINTENANCE_SERVICES ;;
  maintenance-down) stop_group $MAINTENANCE_SERVICES ;;
  maintenance-restart) restart_group $MAINTENANCE_SERVICES ;;
  proxy-up) validate_proxy_config; start_group $PROXY_SERVICES ;;
  proxy-down) stop_group $PROXY_SERVICES ;;
  proxy-restart) validate_proxy_config; restart_group $PROXY_SERVICES ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: $cmd" >&2
    usage >&2
    exit 2
    ;;
esac
