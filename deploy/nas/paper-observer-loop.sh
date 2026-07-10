#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PAPER_OBSERVER_LOOP_INTERVAL:-60}"
PAPER_ACTIVITY_WALLET_LIMIT="${PM_ROBOT_PAPER_OBSERVER_ACTIVITY_WALLET_LIMIT:-${PM_ROBOT_PAPER_ACTIVITY_WALLET_LIMIT:-10}}"
PAPER_ACTIVITY_PAGE_LIMIT="${PM_ROBOT_PAPER_OBSERVER_ACTIVITY_PAGE_LIMIT:-${PM_ROBOT_PAPER_ACTIVITY_PAGE_LIMIT:-50}}"
PAPER_ACTIVITY_MAX_EVENTS="${PM_ROBOT_PAPER_OBSERVER_ACTIVITY_MAX_EVENTS:-${PM_ROBOT_PAPER_ACTIVITY_MAX_EVENTS:-50}}"
PAPER_ACTIVITY_SLEEP="${PM_ROBOT_PAPER_OBSERVER_ACTIVITY_SLEEP:-${PM_ROBOT_PAPER_ACTIVITY_SLEEP:-0.1}}"
PAPER_OBSERVER_PREVIEW_LIMIT="${PM_ROBOT_PAPER_OBSERVER_PREVIEW_LIMIT:-50}"
PAPER_OBSERVER_MAX_SIGNAL_AGE_SEC="${PM_ROBOT_PAPER_OBSERVER_MAX_SIGNAL_AGE_SEC:-21600}"
PAPER_OBSERVER_MAX_ACTIONABLE_SIGNAL_AGE_SEC="${PM_ROBOT_PAPER_OBSERVER_MAX_ACTIONABLE_SIGNAL_AGE_SEC:-300}"
PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC="${PM_ROBOT_PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC:-$PAPER_OBSERVER_MAX_ACTIONABLE_SIGNAL_AGE_SEC}"
PAPER_OBSERVER_EVALUATION_LIMIT="${PM_ROBOT_PAPER_OBSERVER_EVALUATION_LIMIT:-25}"
PAPER_OBSERVER_MAX_STAKE_USD="${PM_ROBOT_PAPER_OBSERVER_MAX_STAKE_USD:-40}"

runtime_heartbeat() {
  name="$1"
  status="${2:-ok}"
  error="${3:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$name" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

while true; do
  echo "$(date -Iseconds) paper observer loop: refresh paper-stage activity start"
  if python -m pm_robot.cli --env /app/.env ingest-activity \
      --paper-stage-only \
      --wallet-limit "$PAPER_ACTIVITY_WALLET_LIMIT" \
      --page-limit "$PAPER_ACTIVITY_PAGE_LIMIT" \
      --max-events-per-wallet "$PAPER_ACTIVITY_MAX_EVENTS" \
      --sleep "$PAPER_ACTIVITY_SLEEP"; then
    echo "$(date -Iseconds) paper observer loop: refresh paper-stage activity ok"
    runtime_heartbeat loop_paper_observer_activity ok
  else
    echo "$(date -Iseconds) paper observer loop: refresh paper-stage activity failed" >&2
    runtime_heartbeat loop_paper_observer_activity failed "paper-stage activity refresh failed from paper observer loop"
  fi

  echo "$(date -Iseconds) paper observer loop: export preview start"
  if python -m pm_robot.cli --env /app/.env paper-observer-preview \
      --out /app/reports/paper_observer_preview.json \
      --limit "$PAPER_OBSERVER_PREVIEW_LIMIT" \
      --max-signal-age-sec "$PAPER_OBSERVER_MAX_SIGNAL_AGE_SEC"; then
    echo "$(date -Iseconds) paper observer loop: export preview ok"
    runtime_heartbeat loop_paper_observer_preview ok
  else
    echo "$(date -Iseconds) paper observer loop: export preview failed" >&2
    runtime_heartbeat loop_paper_observer_preview failed "paper-observer-preview failed from paper observer loop"
  fi

  echo "$(date -Iseconds) paper observer loop: evaluate quotes start"
  if python -m pm_robot.cli --env /app/.env paper-observer-evaluate \
      --out /app/reports/paper_observer_evaluation.json \
      --limit "$PAPER_OBSERVER_EVALUATION_LIMIT" \
      --max-stake-usd "$PAPER_OBSERVER_MAX_STAKE_USD" \
      --max-signal-age-sec "$PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC" \
      --max-actionable-signal-age-sec "$PAPER_OBSERVER_MAX_ACTIONABLE_SIGNAL_AGE_SEC" \
      --persist; then
    echo "$(date -Iseconds) paper observer loop: evaluate quotes ok"
    runtime_heartbeat loop_paper_observer_evaluation ok
  else
    echo "$(date -Iseconds) paper observer loop: evaluate quotes failed" >&2
    runtime_heartbeat loop_paper_observer_evaluation failed "paper-observer-evaluate failed from paper observer loop"
  fi

  sleep "$INTERVAL"
done
