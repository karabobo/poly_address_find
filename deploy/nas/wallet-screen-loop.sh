#!/usr/bin/env sh
set -eu

MODE="${PM_ROBOT_WALLET_SCREEN_MODE:-worker}"
PLANNER_INTERVAL="${PM_ROBOT_WALLET_SCREEN_PLANNER_INTERVAL:-180}"
WORKER_INTERVAL="${PM_ROBOT_WALLET_SCREEN_WORKER_INTERVAL:-60}"
ACTIVE_INTERVAL="${PM_ROBOT_WALLET_SCREEN_ACTIVE_INTERVAL:-30}"
ACTIVE_MAX_INTERVAL="${PM_ROBOT_WALLET_SCREEN_ACTIVE_MAX_INTERVAL:-300}"
ACTIVE_BACKOFF_STEP="${PM_ROBOT_WALLET_SCREEN_ACTIVE_BACKOFF_STEP:-30}"
LOCK_BUSY_INTERVAL="${PM_ROBOT_CONTROL_PLANE_LOCK_BUSY_INTERVAL:-120}"
LOCK_STALE_SECONDS="${PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS:-21600}"
RUN_ONCE="${PM_ROBOT_WALLET_SCREEN_RUN_ONCE:-0}"
CONTROL_LOCK_PATH="${PM_ROBOT_CONTROL_PLANE_LOCK_PATH:-}"
CONTROL_LOCK_DIR="${PM_ROBOT_CONTROL_PLANE_LOCK_DIR:-}"
RUN_LOCKED_SCRIPT="${PM_ROBOT_RUN_LOCKED_SCRIPT:-}"
CONTROL_LOCK_TOKEN=""
PLANNER_LIMIT="${PM_ROBOT_WALLET_SCREEN_PLANNER_LIMIT:-24}"
MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_SCREEN_MAX_ACTIVE_JOBS:-72}"
RESCREEN_AFTER_SECONDS="${PM_ROBOT_WALLET_SCREEN_RESCREEN_AFTER_SECONDS:-604800}"
SHARD_COUNT="${PM_ROBOT_WALLET_SCREEN_SHARD_COUNT:-3}"
SHARD_INDEX="${PM_ROBOT_WALLET_SCREEN_SHARD_INDEX:-}"
WORKER_LIMIT="${PM_ROBOT_WALLET_SCREEN_WORKER_LIMIT:-2}"
LEASE_SECONDS="${PM_ROBOT_WALLET_SCREEN_LEASE_SECONDS:-600}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_WALLET_SCREEN_WORKER_ID:-nas-wallet-screen-${SHARD_INDEX:-planner}-${HOSTNAME_VALUE}}"
HEARTBEAT_NAME="${PM_ROBOT_WALLET_SCREEN_HEARTBEAT_NAME:-}"

case "$MODE" in
  planner)
    INTERVAL="$PLANNER_INTERVAL"
    if [ -z "$HEARTBEAT_NAME" ]; then
      HEARTBEAT_NAME="loop_wallet_screen_planner"
    fi
    ;;
  worker)
    INTERVAL="$WORKER_INTERVAL"
    if [ -z "$SHARD_INDEX" ]; then
      echo "PM_ROBOT_WALLET_SCREEN_SHARD_INDEX is required in worker mode" >&2
      exit 2
    fi
    if [ -z "$HEARTBEAT_NAME" ]; then
      HEARTBEAT_NAME="loop_wallet_screen_worker_${SHARD_INDEX}"
    fi
    ;;
  *)
    echo "PM_ROBOT_WALLET_SCREEN_MODE must be planner or worker" >&2
    exit 2
    ;;
esac

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$HEARTBEAT_NAME" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

resolve_control_lock_paths() {
  if [ -z "$CONTROL_LOCK_PATH" ]; then
    if [ -d /app/data ] || mkdir -p /app/data 2>/dev/null; then
      CONTROL_LOCK_PATH="/app/data/pm_robot.control_plane.lock"
    else
      CONTROL_LOCK_PATH="/tmp/pm_robot.control_plane.lock"
    fi
  fi
  if [ -z "$CONTROL_LOCK_DIR" ]; then
    CONTROL_LOCK_DIR="${CONTROL_LOCK_PATH}.d"
  fi
}

resolve_run_locked_script() {
  if [ -n "$RUN_LOCKED_SCRIPT" ]; then
    if [ -x "$RUN_LOCKED_SCRIPT" ]; then
      printf '%s\n' "$RUN_LOCKED_SCRIPT"
    fi
    return 0
  elif [ -x /app/deploy/scripts/run_locked.sh ]; then
    printf '%s\n' /app/deploy/scripts/run_locked.sh
  elif [ -x ./deploy/scripts/run_locked.sh ]; then
    printf '%s\n' ./deploy/scripts/run_locked.sh
  fi
}

control_lock_owner_field() {
  field="$1"
  owner_path="$2"
  sed -n "s/^${field}=//p" "$owner_path" 2>/dev/null | sed -n '1p'
}

release_control_lock() {
  owner_path="${CONTROL_LOCK_DIR}/owner"
  if [ -n "$CONTROL_LOCK_TOKEN" ] && [ "$(control_lock_owner_field token "$owner_path")" = "$CONTROL_LOCK_TOKEN" ]; then
    rm -f "$owner_path" 2>/dev/null || true
    rmdir "$CONTROL_LOCK_DIR" 2>/dev/null || true
  fi
}

reclaim_stale_control_lock() {
  owner_path="${CONTROL_LOCK_DIR}/owner"
  owner_token="$(control_lock_owner_field token "$owner_path")"
  owner_pid="$(control_lock_owner_field pid "$owner_path")"
  owner_started="$(control_lock_owner_field started_at "$owner_path")"
  owner_host="$(control_lock_owner_field host "$owner_path")"
  current_host="$(hostname 2>/dev/null || echo unknown)"
  now="$(python -c 'import time; print(int(time.time()))' 2>/dev/null || echo 0)"

  case "$owner_pid:$owner_started:$now:$LOCK_STALE_SECONDS" in
    *[!0-9:]*) return 1 ;;
  esac
  [ -n "$owner_token" ] || return 1
  [ "$owner_host" = "$current_host" ] || return 1
  [ "$now" -ge "$owner_started" ] || return 1
  [ $((now - owner_started)) -ge "$LOCK_STALE_SECONDS" ] || return 1
  owner_state="$(python -c '
import os
import sys

try:
    os.kill(int(sys.argv[1]), 0)
except ProcessLookupError:
    print("dead")
except (PermissionError, OSError):
    print("unknown")
else:
    print("alive")
' "$owner_pid" 2>/dev/null || echo unknown)"
  [ "$owner_state" = "dead" ] || return 1
  [ "$(control_lock_owner_field token "$owner_path")" = "$owner_token" ] || return 1

  stale_path="${CONTROL_LOCK_DIR}.stale.$$.$now"
  if mv "$CONTROL_LOCK_DIR" "$stale_path" 2>/dev/null; then
    echo "$(date -Iseconds) control-plane stale lock reclaimed: task=wallet-screen-planner lock=${CONTROL_LOCK_DIR} owner=${owner_token}" >&2
    rm -f "${stale_path}/owner" 2>/dev/null || true
    rmdir "$stale_path" 2>/dev/null || true
    return 0
  fi
  return 1
}

acquire_mkdir_control_lock() {
  attempt=0
  while [ "$attempt" -lt 2 ]; do
    if mkdir "$CONTROL_LOCK_DIR" 2>/dev/null; then
      started_at="$(python -c 'import time; print(int(time.time()))')"
      owner_host="$(hostname 2>/dev/null || echo unknown)"
      CONTROL_LOCK_TOKEN="${owner_host}:$$:${started_at}"
      owner_tmp="${CONTROL_LOCK_DIR}/owner.tmp.$$"
      {
        printf 'token=%s\n' "$CONTROL_LOCK_TOKEN"
        printf 'pid=%s\n' "$$"
        printf 'started_at=%s\n' "$started_at"
        printf 'host=%s\n' "$owner_host"
      } >"$owner_tmp"
      mv "$owner_tmp" "${CONTROL_LOCK_DIR}/owner"
      return 0
    fi
    [ "$attempt" -eq 0 ] || return 1
    reclaim_stale_control_lock || return 1
    attempt=$((attempt + 1))
  done
  return 1
}

run_control_locked() {
  resolve_control_lock_paths
  lock_parent="$(dirname "$CONTROL_LOCK_PATH")"
  mkdir -p "$lock_parent" 2>/dev/null || true
  run_locked="$(resolve_run_locked_script || true)"
  if [ -n "$run_locked" ] && command -v bash >/dev/null 2>&1 && command -v flock >/dev/null 2>&1; then
    PM_ROBOT_LOCK="$CONTROL_LOCK_PATH" \
      PM_ROBOT_LOCK_WAIT=0 \
      PM_ROBOT_LOCK_TIMEOUT_EXIT=75 \
      PM_ROBOT_TASK_NAME="${PM_ROBOT_TASK_NAME:-wallet-screen-planner}" \
      "$run_locked" "$@"
    return $?
  fi

  if acquire_mkdir_control_lock; then
    trap 'release_control_lock' EXIT INT TERM
    "$@"
    status=$?
    release_control_lock
    trap - EXIT INT TERM
    return "$status"
  fi
  echo "$(date -Iseconds) control-plane lock busy: task=wallet-screen-planner lock=${CONTROL_LOCK_DIR}" >&2
  return 75
}

active_sleep_interval() {
  streak="$1"
  interval=$(( ACTIVE_INTERVAL + (streak - 1) * ACTIVE_BACKOFF_STEP ))
  if [ "$interval" -gt "$ACTIVE_MAX_INTERVAL" ]; then
    interval="$ACTIVE_MAX_INTERVAL"
  fi
  printf '%s\n' "$interval"
}

run_planner_once() {
  command_status="failed"
  work_count=0
  command_output=""

  echo "$(date -Iseconds) wallet screen planner: start"
  if command_output="$(python -m pm_robot.cli --env /app/.env wallet-screen-plan \
      --limit "$PLANNER_LIMIT" \
      --max-active-jobs "$MAX_ACTIVE_JOBS" \
      --rescreen-after-seconds "$RESCREEN_AFTER_SECONDS" \
      --shard-count "$SHARD_COUNT")"; then
    printf '%s\n' "$command_output"
    planner_state=""
    if planner_state="$(printf '%s' "$command_output" | python -c '
import json
import sys

payload = json.load(sys.stdin)
status = str(payload.get("status", ""))
jobs_enqueued = payload.get("jobs_enqueued")
active_jobs = payload.get("active_jobs")
throttled = payload.get("throttled")
if status != "ok":
    raise ValueError("unsupported wallet screen planner status")
for value in (jobs_enqueued, active_jobs):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("wallet screen planner counters must be nonnegative integers")
if not isinstance(throttled, bool):
    raise ValueError("wallet screen planner throttled must be boolean")
print(status, jobs_enqueued, active_jobs, int(throttled))
' 2>/dev/null)"; then
      command_status="${planner_state%% *}"
      remaining_state="${planner_state#* }"
      work_count="${remaining_state%% *}"
      echo "$(date -Iseconds) wallet screen planner: ok"
    else
      command_status="invalid"
      summary_preview="$(printf '%.160s' "$command_output" | tr '\n\r' '  ')"
      echo "$(date -Iseconds) wallet screen planner: invalid JSON summary; output=${summary_preview}" >&2
    fi
  else
    if [ -n "$command_output" ]; then
      printf '%s\n' "$command_output"
    fi
    echo "$(date -Iseconds) wallet screen planner: failed" >&2
  fi
  echo "__PM_ROBOT_SCREEN_RESULT ${command_status} ${work_count}"
}

if [ "${1:-}" = "__planner_once" ]; then
  run_planner_once
  exit 0
fi

active_streak=0
while true; do
  sleep_interval="$INTERVAL"
  command_status="failed"
  work_count=0
  command_output=""

  if [ "$MODE" = "planner" ]; then
    planner_output=""
    planner_status=0
    if planner_output="$(run_control_locked "$0" __planner_once)"; then
      result_line="$(printf '%s\n' "$planner_output" | sed -n 's/^__PM_ROBOT_SCREEN_RESULT //p' | tail -n 1)"
      printf '%s\n' "$planner_output" | sed '/^__PM_ROBOT_SCREEN_RESULT /d'
      if [ -n "$result_line" ]; then
        command_status="${result_line%% *}"
        work_count="${result_line##* }"
      else
        command_status="invalid"
        work_count=0
      fi
      if [ "$work_count" -gt 0 ] 2>/dev/null; then
        active_streak=$((active_streak + 1))
        sleep_interval="$(active_sleep_interval "$active_streak")"
      else
        active_streak=0
      fi
    else
      planner_status=$?
      if [ -n "$planner_output" ]; then
        printf '%s\n' "$planner_output"
      fi
      active_streak=0
      if [ "$planner_status" -eq 75 ]; then
        command_status="skipped"
        sleep_interval="$LOCK_BUSY_INTERVAL"
        echo "$(date -Iseconds) wallet screen planner: control-plane busy; backing off ${sleep_interval}s" >&2
      else
        echo "$(date -Iseconds) wallet screen planner: lock wrapper failed" >&2
      fi
    fi
  else
    echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
    if command_output="$(python -m pm_robot.cli --env /app/.env wallet-screen-worker \
        --shard-index "$SHARD_INDEX" \
        --shard-count "$SHARD_COUNT" \
        --limit "$WORKER_LIMIT" \
        --lease-seconds "$LEASE_SECONDS" \
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
    raise ValueError("unsupported wallet screen worker status")
if isinstance(jobs_attempted, bool) or not isinstance(jobs_attempted, int) or jobs_attempted < 0:
    raise ValueError("wallet screen worker jobs_attempted must be a nonnegative integer")
print(status, jobs_attempted)
' 2>/dev/null)"; then
        command_status="${worker_state%% *}"
        work_count="${worker_state#* }"
        if [ "$work_count" -gt 0 ]; then
          # A successful worker drains bounded jobs; sustained work should not
          # slow the queue consumer. Planner-only backoff still limits writes.
          sleep_interval="$ACTIVE_INTERVAL"
        else
          sleep_interval="$WORKER_INTERVAL"
        fi
        echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: ok"
      else
        command_status="invalid"
        active_streak=0
        summary_preview="$(printf '%.160s' "$command_output" | tr '\n\r' '  ')"
        echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: invalid JSON summary; output=${summary_preview}" >&2
      fi
    else
      if [ -n "$command_output" ]; then
        printf '%s\n' "$command_output"
      fi
      active_streak=0
      echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: failed" >&2
    fi
  fi

  if [ "$command_status" = "ok" ]; then
    runtime_heartbeat ok
  elif [ "$command_status" = "partial" ]; then
    runtime_heartbeat partial
  elif [ "$command_status" = "skipped" ]; then
    # A non-blocking control-plane lock skip is expected contention, not a loop failure.
    runtime_heartbeat ok "wallet screen ${MODE} skipped because control-plane lock is busy"
  else
    runtime_heartbeat failed "wallet screen ${MODE} failed or returned invalid summary"
  fi

  echo "$(date -Iseconds) wallet screen ${MODE}: next poll in ${sleep_interval}s (status=${command_status}, work=${work_count})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
