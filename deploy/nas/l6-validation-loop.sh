#!/usr/bin/env sh
set -eu

# L6 is intentionally low-volume: it independently verifies only current L5/L6 wallets.
INTERVAL="${PM_ROBOT_WALLET_L6_WORKER_INTERVAL:-300}"
ACTIVE_INTERVAL="${PM_ROBOT_WALLET_L6_ACTIVE_INTERVAL:-60}"
RUN_ONCE="${PM_ROBOT_WALLET_L6_RUN_ONCE:-0}"
SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"
SHARD_INDEX="${PM_ROBOT_WALLET_L6_SHARD_INDEX:-0}"
WORKER_LIMIT="${PM_ROBOT_WALLET_L6_WORKER_LIMIT:-1}"
LEASE_SECONDS="${PM_ROBOT_WALLET_L6_LEASE_SECONDS:-1800}"
SLEEP_SECONDS="${PM_ROBOT_WALLET_L6_REQUEST_SLEEP:-0.05}"
ARCHIVE_DIR="${PM_ROBOT_ARCHIVE_DIR:-/app/data/parquet}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_WALLET_L6_WORKER_ID:-nas-wallet-l6-${HOSTNAME_VALUE}}"
HEARTBEAT_NAME="loop_wallet_l6_validation_worker"

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$HEARTBEAT_NAME" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

while true; do
  sleep_interval="$INTERVAL"
  command_status="failed"
  jobs_attempted=0
  command_output=""

  echo "$(date -Iseconds) L6 validation worker: start"
  if command_output="$(python -m pm_robot.cli --env /app/.env wallet-l6-worker \
      --archive-dir "$ARCHIVE_DIR" \
      --shard-index "$SHARD_INDEX" \
      --shard-count "$SHARD_COUNT" \
      --limit "$WORKER_LIMIT" \
      --lease-seconds "$LEASE_SECONDS" \
      --sleep "$SLEEP_SECONDS" \
      --worker-id "$WORKER_ID")"; then
    printf '%s\n' "$command_output"
    worker_state=""
    if worker_state="$(printf '%s' "$command_output" | python -c '
import json
import sys

payload = json.load(sys.stdin)
status = str(payload.get("status", ""))
jobs_attempted = payload.get("jobs_attempted")
if status not in {"ok", "partial"}:
    raise ValueError("unsupported L6 validation worker status")
if isinstance(jobs_attempted, bool) or not isinstance(jobs_attempted, int) or jobs_attempted < 0:
    raise ValueError("jobs_attempted must be a nonnegative integer")
print(status, jobs_attempted)
' 2>/dev/null)"; then
      command_status="${worker_state%% *}"
      jobs_attempted="${worker_state#* }"
      if [ "$jobs_attempted" -gt 0 ]; then
        sleep_interval="$ACTIVE_INTERVAL"
      fi
    else
      command_status="invalid"
      echo "$(date -Iseconds) L6 validation worker returned invalid JSON" >&2
    fi
  else
    if [ -n "$command_output" ]; then
      printf '%s\n' "$command_output"
    fi
    echo "$(date -Iseconds) L6 validation worker failed" >&2
  fi

  if [ "$command_status" = "ok" ]; then
    runtime_heartbeat ok
  elif [ "$command_status" = "partial" ]; then
    runtime_heartbeat partial
  else
    runtime_heartbeat failed "L6 validation worker failed or returned invalid summary"
  fi

  echo "$(date -Iseconds) L6 validation worker: next poll in ${sleep_interval}s (status=${command_status}, jobs=${jobs_attempted})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
