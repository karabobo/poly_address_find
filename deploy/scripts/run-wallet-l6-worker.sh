#!/usr/bin/env bash
set -euo pipefail

# A systemd oneshot must report the worker's real summary instead of always
# publishing a healthy heartbeat after a zero process exit.
ROOT="${PM_ROBOT_HOME:-/opt/pm-robot}"
PYTHON="${PM_ROBOT_PYTHON:-$ROOT/.venv/bin/python}"
ENV_FILE="${PM_ROBOT_ENV_FILE:-$ROOT/.env}"
ARCHIVE_DIR="${PM_ROBOT_ARCHIVE_DIR:-$ROOT/data/parquet}"
SHARD_INDEX="${PM_ROBOT_WALLET_L6_SHARD_INDEX:-0}"
SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"
WORKER_LIMIT="${PM_ROBOT_WALLET_L6_WORKER_LIMIT:-1}"
LEASE_SECONDS="${PM_ROBOT_WALLET_L6_LEASE_SECONDS:-1800}"
SLEEP_SECONDS="${PM_ROBOT_WALLET_L6_REQUEST_SLEEP:-0.05}"
WORKER_ID="${PM_ROBOT_WALLET_L6_WORKER_ID:-wallet-l6-worker}"
HEARTBEAT_NAME="loop_wallet_l6_validation_worker"

output_file="$(mktemp)"
trap 'rm -f "$output_file"' EXIT

heartbeat() {
  local status="$1"
  local error="${2:-}"
  "$PYTHON" -m pm_robot.cli --env "$ENV_FILE" runtime-heartbeat \
    --name "$HEARTBEAT_NAME" \
    --status "$status" \
    --error "$error" >/dev/null
}

if ! "$PYTHON" -m pm_robot.cli --env "$ENV_FILE" wallet-l6-worker \
    --archive-dir "$ARCHIVE_DIR" \
    --shard-index "$SHARD_INDEX" \
    --shard-count "$SHARD_COUNT" \
    --limit "$WORKER_LIMIT" \
    --lease-seconds "$LEASE_SECONDS" \
    --sleep "$SLEEP_SECONDS" \
    --worker-id "$WORKER_ID" >"$output_file"; then
  cat "$output_file"
  heartbeat failed "L6 validation worker command failed"
  exit 1
fi

cat "$output_file"
if ! worker_status="$("$PYTHON" -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
status = str(payload.get("status", ""))
if status not in {"ok", "partial"}:
    raise ValueError("unsupported L6 worker status")
print(status)
' "$output_file")"; then
  heartbeat failed "L6 validation worker returned invalid JSON"
  exit 1
fi

case "$worker_status" in
  ok)
    heartbeat ok
    ;;
  partial)
    heartbeat partial "L6 validation worker reported a partial cycle"
    ;;
esac
