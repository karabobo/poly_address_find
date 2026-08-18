#!/usr/bin/env sh
set -eu

# This loop owns only local control-plane decisions. Network history reads are
# executed by sharded wallet-history workers.
INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_INTERVAL:-180}"
ACTIVE_INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL:-30}"
ACTIVE_MAX_INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_ACTIVE_MAX_INTERVAL:-300}"
ACTIVE_BACKOFF_STEP="${PM_ROBOT_RESEARCH_CONTROL_ACTIVE_BACKOFF_STEP:-30}"
LOCK_BUSY_INTERVAL="${PM_ROBOT_CONTROL_PLANE_LOCK_BUSY_INTERVAL:-120}"
LOCK_STALE_SECONDS="${PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS:-21600}"
RUN_ONCE="${PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE:-0}"
CONTROL_LOCK_PATH="${PM_ROBOT_CONTROL_PLANE_LOCK_PATH:-}"
CONTROL_LOCK_DIR="${PM_ROBOT_CONTROL_PLANE_LOCK_DIR:-}"
SHARD_COUNT="${PM_ROBOT_WALLET_HISTORY_SHARD_COUNT:-3}"
HISTORY_LIMIT="${PM_ROBOT_WALLET_HISTORY_PLANNER_LIMIT:-12}"
HISTORY_MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_HISTORY_MAX_ACTIVE_JOBS:-36}"
LIGHT_REFRESH_SECONDS="${PM_ROBOT_WALLET_HISTORY_LIGHT_REFRESH_SECONDS:-2592000}"
DEEP_REFRESH_SECONDS="${PM_ROBOT_WALLET_HISTORY_DEEP_REFRESH_SECONDS:-604800}"
MIN_COHORT_SIZE="${PM_ROBOT_WALLET_LEVEL_MIN_COHORT_SIZE:-20}"
TIMEOUT_MIN_COHORT_SIZE="${PM_ROBOT_WALLET_LEVEL_TIMEOUT_MIN_COHORT_SIZE:-5}"
MAX_WAIT_SECONDS="${PM_ROBOT_WALLET_LEVEL_MAX_WAIT_SECONDS:-3600}"
L3_FRACTION="${PM_ROBOT_WALLET_LEVEL_L3_FRACTION:-0.25}"
L4_FRACTION="${PM_ROBOT_WALLET_LEVEL_L4_FRACTION:-0.20}"
L5_FRACTION="${PM_ROBOT_WALLET_LEVEL_L5_FRACTION:-0.10}"
L3_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L3_MAX_PROMOTIONS:-12}"
L4_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L4_MAX_PROMOTIONS:-6}"
L5_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L5_MAX_PROMOTIONS:-2}"
L6_LIMIT="${PM_ROBOT_WALLET_L6_PLANNER_LIMIT:-5}"
L6_MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_L6_MAX_ACTIVE_JOBS:-10}"
L6_SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"
L6_REFRESH_SECONDS="${PM_ROBOT_WALLET_L6_REFRESH_SECONDS:-1209600}"
RUN_LOCKED_SCRIPT="${PM_ROBOT_RUN_LOCKED_SCRIPT:-}"
CONTROL_LOCK_TOKEN=""

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
    echo "$(date -Iseconds) control-plane stale lock reclaimed: task=research-control lock=${CONTROL_LOCK_DIR} owner=${owner_token}" >&2
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
      PM_ROBOT_TASK_NAME="${PM_ROBOT_TASK_NAME:-research-control}" \
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
  echo "$(date -Iseconds) control-plane lock busy: task=research-control lock=${CONTROL_LOCK_DIR}" >&2
  return 75
}

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_wallet_history_planner \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_wallet_level_control \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

json_history_jobs_enqueued() {
  python -c '
import json
import sys

summary = json.loads(sys.argv[1])
if summary.get("status") != "ok":
    raise ValueError("unsupported history summary status")
value = summary.get("jobs_enqueued")
if isinstance(value, bool) or not isinstance(value, int) or value < 0:
    raise ValueError("history jobs_enqueued must be a nonnegative integer")
print(value)
' "$1"
}

json_history_status() {
  python -c '
import json
import sys

summary = json.loads(sys.argv[1])
status = summary.get("status")
if status not in {"ok", "warming_up"}:
    raise ValueError("unsupported history summary status")
print(status)
' "$1"
}

json_selection_l6_counter_sum() {
  python -c '
import json
import sys

selection = json.loads(sys.argv[1])
l6 = json.loads(sys.argv[2])
if selection.get("status") != "ok" or l6.get("status") != "ok":
    raise ValueError("unsupported control summary status")
values = [
    selection.get("promoted_l3"),
    selection.get("promoted_l4"),
    selection.get("promoted_l5"),
    l6.get("jobs_enqueued"),
]
if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
    raise ValueError("control counters must be nonnegative integers")
print(sum(values))
' "$1" "$2"
}

active_sleep_interval() {
  streak="$1"
  interval=$(( ACTIVE_INTERVAL + (streak - 1) * ACTIVE_BACKOFF_STEP ))
  if [ "$interval" -gt "$ACTIVE_MAX_INTERVAL" ]; then
    interval="$ACTIVE_MAX_INTERVAL"
  fi
  printf '%s\n' "$interval"
}

run_control_once() {
  cycle_status="failed"
  work_count=0
  selection_output=""
  history_output=""
  l6_output=""

  echo "$(date -Iseconds) wallet history planning: start"
  history_ok=0
  history_work_count=0
  if history_output="$(python -m pm_robot.cli --env /app/.env wallet-history-plan \
      --limit "$HISTORY_LIMIT" \
      --max-active-jobs "$HISTORY_MAX_ACTIVE_JOBS" \
      --light-refresh-seconds "$LIGHT_REFRESH_SECONDS" \
      --deep-refresh-seconds "$DEEP_REFRESH_SECONDS" \
      --shard-count "$SHARD_COUNT")"; then
    history_ok=1
    printf '%s\n' "$history_output"
  else
    [ -z "$history_output" ] || printf '%s\n' "$history_output" >&2
    echo "$(date -Iseconds) wallet history planning failed" >&2
  fi

  if [ "$history_ok" -ne 1 ]; then
    runtime_heartbeat partial "wallet history planning failed"
    echo "__PM_ROBOT_CONTROL_RESULT partial 0"
    return 0
  fi

  if ! history_status="$(json_history_status "$history_output" 2>/dev/null)"; then
    runtime_heartbeat partial "wallet history planning returned invalid summary"
    echo "__PM_ROBOT_CONTROL_RESULT invalid 0"
    return 0
  fi

  if [ "$history_status" = "warming_up" ]; then
    runtime_heartbeat ok
    echo "__PM_ROBOT_CONTROL_RESULT warming 0"
    return 0
  fi

  if ! history_work_count="$(json_history_jobs_enqueued "$history_output" 2>/dev/null)"; then
    runtime_heartbeat partial "wallet history planning returned invalid counter"
    echo "__PM_ROBOT_CONTROL_RESULT invalid 0"
    return 0
  fi

  echo "$(date -Iseconds) wallet level control: start"
  selection_ok=0
  if selection_output="$(python -m pm_robot.cli --env /app/.env wallet-level-select \
      --min-cohort-size "$MIN_COHORT_SIZE" \
      --timeout-min-cohort-size "$TIMEOUT_MIN_COHORT_SIZE" \
      --max-wait-seconds "$MAX_WAIT_SECONDS" \
      --l3-fraction "$L3_FRACTION" \
      --l4-fraction "$L4_FRACTION" \
      --l5-fraction "$L5_FRACTION" \
      --l3-max-promotions "$L3_MAX_PROMOTIONS" \
      --l4-max-promotions "$L4_MAX_PROMOTIONS" \
      --l5-max-promotions "$L5_MAX_PROMOTIONS")"; then
    selection_ok=1
    printf '%s\n' "$selection_output"
  else
    echo "$(date -Iseconds) wallet level selection failed" >&2
  fi

  l6_ok=0
  if l6_output="$(python -m pm_robot.cli --env /app/.env wallet-l6-plan \
      --limit "$L6_LIMIT" \
      --max-active-jobs "$L6_MAX_ACTIVE_JOBS" \
      --shard-count "$L6_SHARD_COUNT" \
      --refresh-seconds "$L6_REFRESH_SECONDS")"; then
    l6_ok=1
    printf '%s\n' "$l6_output"
  else
    echo "$(date -Iseconds) L6 validation planning failed" >&2
  fi

  if [ "$selection_ok" -eq 1 ] && [ "$l6_ok" -eq 1 ]; then
    if parsed_work_count="$(json_selection_l6_counter_sum "$selection_output" "$l6_output" 2>/dev/null)"; then
      # History ingestion is continuous. Count it as work, but never let it
      # starve level selection or independent L6 validation planning.
      work_count=$((history_work_count + parsed_work_count))
      cycle_status="ok"
      runtime_heartbeat ok
    else
      cycle_status="invalid"
      work_count=0
      runtime_heartbeat partial "wallet level control returned invalid summaries"
    fi
  else
    work_count=0
    runtime_heartbeat partial "wallet level selection or L6 planning failed"
  fi
  echo "__PM_ROBOT_CONTROL_RESULT ${cycle_status} ${work_count}"
}

if [ "${1:-}" = "__control_once" ]; then
  run_control_once
  exit 0
fi

active_streak=0
while true; do
  sleep_interval="$INTERVAL"
  cycle_status="failed"
  work_count=0

  control_output=""
  control_status=0
  if control_output="$(run_control_locked "$0" __control_once)"; then
    result_line="$(printf '%s\n' "$control_output" | sed -n 's/^__PM_ROBOT_CONTROL_RESULT //p' | tail -n 1)"
    printf '%s\n' "$control_output" | sed '/^__PM_ROBOT_CONTROL_RESULT /d'
    if [ -n "$result_line" ]; then
      cycle_status="${result_line%% *}"
      work_count="${result_line##* }"
    else
      cycle_status="invalid"
      work_count=0
    fi
    if [ "$cycle_status" = "warming" ] || [ "$work_count" -gt 0 ] 2>/dev/null; then
      active_streak=$((active_streak + 1))
      sleep_interval="$(active_sleep_interval "$active_streak")"
    else
      active_streak=0
    fi
  else
    control_status=$?
    if [ -n "$control_output" ]; then
      printf '%s\n' "$control_output"
    fi
    active_streak=0
    if [ "$control_status" -eq 75 ]; then
      cycle_status="skipped"
      sleep_interval="$LOCK_BUSY_INTERVAL"
      # A non-blocking control-plane lock skip is expected mutual exclusion.
      # Keep both planner heartbeats fresh without masking an actual command failure.
      runtime_heartbeat ok "wallet level control skipped because control-plane lock is busy"
      echo "$(date -Iseconds) wallet level control: control-plane busy; backing off ${sleep_interval}s" >&2
    else
      runtime_heartbeat partial "wallet level control lock wrapper failed"
    fi
  fi

  echo "$(date -Iseconds) wallet level control: next cycle in ${sleep_interval}s (status=${cycle_status}, work=${work_count})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
