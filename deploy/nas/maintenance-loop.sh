#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_MAINTENANCE_INTERVAL:-900}"
START_DELAY="${PM_ROBOT_MAINTENANCE_START_DELAY:-300}"
RUN_ONCE="${PM_ROBOT_MAINTENANCE_RUN_ONCE:-0}"
WAL_CHECKPOINT="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-passive}"
WAL_CHECKPOINT_TIMEOUT_MS="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT_TIMEOUT_MS:-250}"
REPORT_PATH="${PM_ROBOT_MAINTENANCE_REPORT_PATH:-/app/reports/maintenance_status.json}"
FAILED_JOB_COOLDOWN_SECONDS="${PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS:-21600}"
STALE_HEARTBEAT_SECONDS="${PM_ROBOT_MAINTENANCE_STALE_HEARTBEAT_SECONDS:-21600}"
KEEP_BACKUPS="${PM_ROBOT_MAINTENANCE_KEEP_BACKUPS:-0}"
RUNTIME_HEARTBEAT_DAYS="${PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS:-30}"
CLEANUP_BATCH_LIMIT="${PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT:-500}"
L0_RETENTION_DAYS="${PM_ROBOT_MAINTENANCE_L0_RETENTION_DAYS:-7}"
L0_CLEANUP_BATCH_LIMIT="${PM_ROBOT_MAINTENANCE_L0_CLEANUP_BATCH_LIMIT:-20000}"
LIGHT_INTERVAL_SECONDS="${PM_ROBOT_MAINTENANCE_LIGHT_INTERVAL_SECONDS:-21600}"
LIGHT_RUN_NOW="${PM_ROBOT_MAINTENANCE_LIGHT_RUN_NOW:-0}"
LIGHT_CLEANUP_ENABLED="${PM_ROBOT_MAINTENANCE_LIGHT_CLEANUP_ENABLED:-0}"
LIGHT_STAMP_PATH="${PM_ROBOT_MAINTENANCE_LIGHT_STAMP_PATH:-}"
HISTORY_GC_ENABLED="${PM_ROBOT_WALLET_HISTORY_GC_ENABLED:-0}"
HISTORY_GC_MIN_AGE_SECONDS="${PM_ROBOT_WALLET_HISTORY_GC_MIN_AGE_SECONDS:-2592000}"
HISTORY_GC_KEEP_PER_WALLET="${PM_ROBOT_WALLET_HISTORY_GC_KEEP_PER_WALLET:-1}"
HISTORY_GC_LIMIT="${PM_ROBOT_WALLET_HISTORY_GC_LIMIT:-500}"
HISTORY_AUDIT_ENABLED="${PM_ROBOT_WALLET_HISTORY_AUDIT_ENABLED:-1}"
HISTORY_AUDIT_VERIFY_CHECKSUMS="${PM_ROBOT_WALLET_HISTORY_AUDIT_VERIFY_CHECKSUMS:-0}"
HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS="${PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS:-604800}"
HISTORY_AUDIT_ORPHAN_LIMIT="${PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_LIMIT:-500}"
HISTORY_AUDIT_DELETE_ORPHANS="${PM_ROBOT_WALLET_HISTORY_AUDIT_DELETE_ORPHANS:-0}"
HEAVY_ALLOWED_HOURS="${PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS-2-5}"
HEAVY_INTERVAL_SECONDS="${PM_ROBOT_WALLET_HISTORY_HEAVY_INTERVAL_SECONDS:-86400}"
HEAVY_RUN_NOW="${PM_ROBOT_WALLET_HISTORY_HEAVY_RUN_NOW:-0}"
HEAVY_ON_BACKLOG_ZERO="${PM_ROBOT_WALLET_HISTORY_HEAVY_ON_BACKLOG_ZERO:-1}"
HEAVY_STAMP_PATH="${PM_ROBOT_WALLET_HISTORY_HEAVY_STAMP_PATH:-}"
CONTROL_LOCK_PATH="${PM_ROBOT_CONTROL_PLANE_LOCK_PATH:-}"
CONTROL_LOCK_DIR="${PM_ROBOT_CONTROL_PLANE_LOCK_DIR:-}"
LOCK_BUSY_INTERVAL="${PM_ROBOT_CONTROL_PLANE_LOCK_BUSY_INTERVAL:-120}"
LOCK_STALE_SECONDS="${PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS:-21600}"
RUN_LOCKED_SCRIPT="${PM_ROBOT_RUN_LOCKED_SCRIPT:-}"
CONTROL_LOCK_TOKEN=""

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  attempt=1
  while [ "$attempt" -le 3 ]; do
    if python -m pm_robot.cli --env /app/.env runtime-heartbeat \
      --name loop_maintenance \
      --status "$status" \
      --error "$error" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    [ "$attempt" -le 3 ] && sleep 1
  done
  return 0
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
  if [ -z "$HEAVY_STAMP_PATH" ]; then
    HEAVY_STAMP_PATH="$(dirname "$CONTROL_LOCK_PATH")/.wallet_history_heavy_maintenance.last"
  fi
  if [ -z "$LIGHT_STAMP_PATH" ]; then
    LIGHT_STAMP_PATH="$(dirname "$CONTROL_LOCK_PATH")/.maintenance_light_cleanup.last"
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
    echo "$(date -Iseconds) control-plane stale lock reclaimed: task=maintenance-loop lock=${CONTROL_LOCK_DIR} owner=${owner_token}" >&2
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
      PM_ROBOT_TASK_NAME="${PM_ROBOT_TASK_NAME:-maintenance-loop}" \
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
  echo "$(date -Iseconds) control-plane lock busy: task=maintenance-loop lock=${CONTROL_LOCK_DIR}" >&2
  return 75
}

allowed_hour_now() {
  python -c '
import sys
import time

spec = sys.argv[1]
hour = time.localtime().tm_hour
allowed = set()
for part in spec.split(","):
    part = part.strip()
    if not part:
        continue
    if "-" in part:
        start_s, end_s = part.split("-", 1)
        start = int(start_s)
        end = int(end_s)
        if start <= end:
            allowed.update(range(start, end + 1))
        else:
            allowed.update(range(start, 24))
            allowed.update(range(0, end + 1))
    else:
        allowed.add(int(part))
print("1" if hour in allowed else "0")
' "$HEAVY_ALLOWED_HOURS"
}

heavy_due_by_interval() {
  resolve_control_lock_paths
  now="$(python -c 'import time; print(int(time.time()))')"
  last=0
  if [ -f "$HEAVY_STAMP_PATH" ]; then
    last="$(cat "$HEAVY_STAMP_PATH" 2>/dev/null || echo 0)"
  fi
  case "$last" in
    ''|*[!0-9]*) last=0 ;;
  esac
  [ $((now - last)) -ge "$HEAVY_INTERVAL_SECONDS" ]
}

mark_heavy_done() {
  resolve_control_lock_paths
  stamp_parent="$(dirname "$HEAVY_STAMP_PATH")"
  mkdir -p "$stamp_parent" 2>/dev/null || true
  python -c 'import time; print(int(time.time()))' >"${HEAVY_STAMP_PATH}.tmp.$$" 2>/dev/null && mv "${HEAVY_STAMP_PATH}.tmp.$$" "$HEAVY_STAMP_PATH" 2>/dev/null || true
}

light_cleanup_due_by_interval() {
  resolve_control_lock_paths
  if [ "$LIGHT_RUN_NOW" = "1" ]; then
    return 0
  fi
  now="$(python -c 'import time; print(int(time.time()))')"
  last=0
  if [ -f "$LIGHT_STAMP_PATH" ]; then
    last="$(cat "$LIGHT_STAMP_PATH" 2>/dev/null || echo 0)"
  fi
  case "$last" in
    ''|*[!0-9]*) last=0 ;;
  esac
  [ $((now - last)) -ge "$LIGHT_INTERVAL_SECONDS" ]
}

mark_light_cleanup_done() {
  resolve_control_lock_paths
  stamp_parent="$(dirname "$LIGHT_STAMP_PATH")"
  mkdir -p "$stamp_parent" 2>/dev/null || true
  python -c 'import time; print(int(time.time()))' >"${LIGHT_STAMP_PATH}.tmp.$$" 2>/dev/null && mv "${LIGHT_STAMP_PATH}.tmp.$$" "$LIGHT_STAMP_PATH" 2>/dev/null || true
}

queue_active_from_report() {
  python -c '
import json
import sys

try:
    payload = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    print("1")
    raise SystemExit(0)

value = payload.get("wallet_history_screen_active")
if value is None:
    queue = payload.get("pipeline_queue_health")
    if not isinstance(queue, dict):
        queue = (
            payload.get("research_readiness", {})
            .get("metrics", {})
            .get("queue_health")
        )
    if not isinstance(queue, dict):
        print("1")
        raise SystemExit(0)
    value = queue.get("wallet_history_screen_active")
    if value is None:
        value = queue.get("active", 0)
try:
    active = int(value)
except Exception:
    active = 1
print("1" if active > 0 else "0")
' "$1"
}

maintenance_queue_state() {
  report_parent="$(dirname "$REPORT_PATH")"
  mkdir -p "$report_parent" 2>/dev/null || true
  preflight_tmp="${REPORT_PATH}.preflight.$$"
  if ! python -m pm_robot.cli --env /app/.env maintenance-preflight >"$preflight_tmp"; then
    rm -f "$preflight_tmp" || true
    echo "$(date -Iseconds) maintenance loop: skipped reason=preflight_unavailable" >&2
    runtime_heartbeat ok "maintenance skipped: preflight unavailable"
    printf 'unavailable\n'
    return 0
  fi
  if [ "$(queue_active_from_report "$preflight_tmp")" = "1" ]; then
    rm -f "$preflight_tmp" || true
    printf 'active\n'
    return 0
  fi
  rm -f "$preflight_tmp" || true
  printf 'inactive\n'
}

backlog_is_zero() {
  python -c '
import json
import sys

try:
    payload = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    print("0")
    raise SystemExit(0)

queue = payload.get("pipeline_queue_health")
if not isinstance(queue, dict):
    queue = (
        payload.get("research_readiness", {})
        .get("metrics", {})
        .get("queue_health")
    )
if not isinstance(queue, dict):
    print("0")
    raise SystemExit(0)

busy_keys = (
    "queued",
    "running",
    "active",
    "expired_running",
    "queued_exhausted",
    "failed_missing_error",
)
total = 0
for key in busy_keys:
    value = queue.get(key, 0)
    if isinstance(value, bool):
        total += int(value)
    elif isinstance(value, int):
        total += max(value, 0)
print("1" if total == 0 else "0")
' "$REPORT_PATH"
}

should_run_heavy_history() {
  if [ "$HEAVY_RUN_NOW" = "1" ]; then
    echo "forced"
    return 0
  fi
  if ! heavy_due_by_interval; then
    echo "not_due"
    return 1
  fi
  if [ "$HEAVY_ON_BACKLOG_ZERO" = "1" ] && [ "$(backlog_is_zero)" = "1" ]; then
    echo "backlog_zero"
    return 0
  fi
  if [ "$(allowed_hour_now)" = "1" ]; then
    echo "scheduled_low_peak"
    return 0
  fi
  echo "not_due"
  return 1
}

run_maintenance_once() {
  active_queue="${1:-0}"
  echo "$(date -Iseconds) maintenance loop: start"
  maintenance_ok=1
  report_tmp="${REPORT_PATH}.tmp.$$"
  light_cleanup_due=0

  if [ "$WAL_CHECKPOINT" != "passive" ]; then
    echo "$(date -Iseconds) maintenance loop: forcing passive WAL checkpoint; requested=${WAL_CHECKPOINT}" >&2
    WAL_CHECKPOINT="passive"
  fi

  if [ "$LIGHT_CLEANUP_ENABLED" != "1" ]; then
    echo "$(date -Iseconds) maintenance light cleanup: skipped reason=disabled"
  elif [ "$active_queue" = "1" ]; then
    echo "$(date -Iseconds) maintenance light cleanup: skipped reason=active_queue"
  elif light_cleanup_due_by_interval; then
    light_cleanup_due=1
    echo "$(date -Iseconds) maintenance light cleanup: start reason=due"
  else
    echo "$(date -Iseconds) maintenance light cleanup: skipped reason=not_due"
  fi

  if ! mkdir -p "$(dirname "$REPORT_PATH")"; then
    echo "$(date -Iseconds) maintenance loop: report directory unavailable" >&2
    maintenance_ok=0
  else
    set -- \
      --reset-stale-jobs \
      --failed-job-cooldown-seconds "$FAILED_JOB_COOLDOWN_SECONDS" \
      --reset-stale-heartbeats \
      --stale-heartbeat-seconds "$STALE_HEARTBEAT_SECONDS" \
      --heartbeat-days "$RUNTIME_HEARTBEAT_DAYS" \
      --keep-backups "$KEEP_BACKUPS" \
      --wal-checkpoint "$WAL_CHECKPOINT" \
      --wal-checkpoint-timeout-ms "$WAL_CHECKPOINT_TIMEOUT_MS"
    if [ "$light_cleanup_due" -eq 1 ]; then
      set -- "$@" \
        --cleanup-batch-limit "$CLEANUP_BATCH_LIMIT" \
        --l0-retention-days "$L0_RETENTION_DAYS" \
        --l0-cleanup-batch-limit "$L0_CLEANUP_BATCH_LIMIT"
    else
      set -- "$@" --skip-cleanup
    fi
    if ! python -m pm_robot.cli --env /app/.env maintenance "$@" >"$report_tmp"; then
      maintenance_ok=0
      rm -f "$report_tmp" || true
    elif ! cat "$report_tmp" || ! mv "$report_tmp" "$REPORT_PATH"; then
      echo "$(date -Iseconds) maintenance loop: could not write report" >&2
      maintenance_ok=0
      rm -f "$report_tmp" || true
    fi
  fi

  heavy_reason="not_due"
  if [ "$maintenance_ok" -eq 1 ] && [ "$active_queue" = "1" ]; then
    heavy_reason="not_due"
    echo "$(date -Iseconds) wallet history heavy maintenance: skipped reason=active_queue"
  elif [ "$maintenance_ok" -eq 1 ]; then
    if heavy_reason="$(should_run_heavy_history)"; then
      echo "$(date -Iseconds) wallet history heavy maintenance: start reason=${heavy_reason}"
    else
      echo "$(date -Iseconds) wallet history heavy maintenance: skipped reason=${heavy_reason}"
    fi
  fi

  if [ "$maintenance_ok" -eq 1 ] && [ "$heavy_reason" != "not_due" ] && [ "$HISTORY_AUDIT_ENABLED" = "1" ]; then
    echo "$(date -Iseconds) wallet history audit: start"
    set -- \
      --orphan-min-age-seconds "$HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS" \
      --orphan-limit "$HISTORY_AUDIT_ORPHAN_LIMIT"
    if [ "$HISTORY_AUDIT_VERIFY_CHECKSUMS" = "1" ]; then
      set -- "$@" --verify-checksums
    fi
    if [ "$HISTORY_AUDIT_DELETE_ORPHANS" = "1" ]; then
      set -- "$@" --delete-orphans
    fi
    if audit_output="$(python -m pm_robot.cli --env /app/.env wallet-history-audit "$@")"; then
      printf '%s\n' "$audit_output"
    else
      if [ -n "${audit_output:-}" ]; then
        printf '%s\n' "$audit_output"
      fi
      echo "$(date -Iseconds) wallet history audit: failed" >&2
      maintenance_ok=0
    fi
  fi

  if [ "$maintenance_ok" -eq 1 ] && [ "$heavy_reason" != "not_due" ] && [ "$HISTORY_GC_ENABLED" = "1" ]; then
    echo "$(date -Iseconds) wallet history GC: start"
    if gc_output="$(python -m pm_robot.cli --env /app/.env wallet-history-gc \
        --min-age-seconds "$HISTORY_GC_MIN_AGE_SECONDS" \
        --keep-per-wallet "$HISTORY_GC_KEEP_PER_WALLET" \
        --limit "$HISTORY_GC_LIMIT" \
        --execute)"; then
      printf '%s\n' "$gc_output"
    else
      if [ -n "${gc_output:-}" ]; then
        printf '%s\n' "$gc_output"
      fi
      echo "$(date -Iseconds) wallet history GC: failed" >&2
      maintenance_ok=0
    fi
  fi

  if [ "$maintenance_ok" -eq 1 ]; then
    if [ "$light_cleanup_due" -eq 1 ]; then
      mark_light_cleanup_done
    fi
    if [ "$heavy_reason" != "not_due" ]; then
      mark_heavy_done
    fi
    echo "$(date -Iseconds) maintenance loop: ok"
    runtime_heartbeat ok
  else
    echo "$(date -Iseconds) maintenance loop: failed" >&2
    runtime_heartbeat failed "maintenance, wallet history audit, or GC failed"
  fi
}

if [ "${1:-}" = "__maintenance_once" ]; then
  run_maintenance_once "${2:-0}"
  exit 0
fi

if [ "$START_DELAY" -gt 0 ]; then
  # Publish liveness before the intentional startup delay so deployments do
  # not look unhealthy while destructive maintenance remains deferred.
  runtime_heartbeat ok
  echo "$(date -Iseconds) maintenance loop: initial delay ${START_DELAY}s"
  sleep "$START_DELAY"
fi

while true; do
  maintenance_output=""
  maintenance_status=0
  queue_state="$(maintenance_queue_state)"
  if [ "$queue_state" = "unavailable" ]; then
    maintenance_status=0
  elif maintenance_output="$(run_control_locked "$0" __maintenance_once "$([ "$queue_state" = "active" ] && echo 1 || echo 0)")"; then
    printf '%s\n' "$maintenance_output"
  else
    maintenance_status=$?
    if [ -n "$maintenance_output" ]; then
      printf '%s\n' "$maintenance_output"
    fi
    if [ "$maintenance_status" -eq 75 ]; then
      echo "$(date -Iseconds) maintenance loop: control-plane busy; backing off ${LOCK_BUSY_INTERVAL}s" >&2
      # A busy control lock is expected while planners own the write window.
      # Keep liveness current without treating the skipped cycle as a failure.
      runtime_heartbeat ok "maintenance skipped: control-plane lock busy"
    else
      echo "$(date -Iseconds) maintenance loop: lock wrapper failed" >&2
      runtime_heartbeat failed "maintenance lock wrapper failed"
    fi
  fi

  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  if [ "$maintenance_status" -eq 75 ]; then
    sleep "$LOCK_BUSY_INTERVAL"
  else
    sleep "$INTERVAL"
  fi
done
