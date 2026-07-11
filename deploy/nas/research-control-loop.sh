#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_INTERVAL:-${PM_ROBOT_SCORE_LOOP_INTERVAL:-300}}"
LEGACY_MIN_SCORE="${PM_ROBOT_ELIGIBILITY_REPAIR_MIN_SCORE:-${PM_ROBOT_COPYABILITY_MIN_SCORE:-40}}"
MIN_SCORE="${PM_ROBOT_RESEARCH_MIN_SCORE:-$LEGACY_MIN_SCORE}"
STATE_LIMIT="${PM_ROBOT_PIPELINE_STATE_LIMIT:-25}"
STATE_COMMIT_EVERY="${PM_ROBOT_PIPELINE_STATE_COMMIT_EVERY:-5}"
REPAIR_LIMIT="${PM_ROBOT_ELIGIBILITY_REPAIR_LIMIT:-100}"
SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
WALLET_LIGHT_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT:-30}"
WALLET_MEDIUM_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT:-20}"
WALLET_DEEP_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT:-5}"
WALLET_MAX_ACTIVE_JOBS="${PM_ROBOT_PIPELINE_PLANNER_MAX_ACTIVE_JOBS:-240}"
COPYABILITY_LIMIT="${PM_ROBOT_COPYABILITY_PLANNER_LIMIT:-50}"
COPYABILITY_MAX_ACTIVE_JOBS="${PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS:-50}"
COPYABILITY_MIN_ACTIVITY_EVENTS="${PM_ROBOT_COPYABILITY_MIN_ACTIVITY_EVENTS:-25}"
COPYABILITY_SHARD_COUNT="${PM_ROBOT_COPYABILITY_SHARD_COUNT:-1}"
COPYABILITY_RESCAN_SECONDS="${PM_ROBOT_COPYABILITY_RESCAN_SECONDS:-21600}"
FEATURE_LIMIT="${PM_ROBOT_SCORE_FEATURE_LIMIT:-80}"
FEATURE_MIN_ACTIVITY_EVENTS="${PM_ROBOT_SCORE_MIN_ACTIVITY_EVENTS:-25}"
FEATURE_COMMIT_EVERY="${PM_ROBOT_SCORE_FEATURE_COMMIT_EVERY:-10}"
EVIDENCE_PROMOTION_LIMIT="${PM_ROBOT_EVIDENCE_PROMOTION_LIMIT:-80}"
SCORE_LIMIT="${PM_ROBOT_SCORE_LIMIT:-300}"
POLICY_PATH="${PM_ROBOT_POLICY_PATH:-/app/config/leader_scoring_policy.json}"
PAPER_HANDOFF_LIMIT="${PM_ROBOT_PAPER_HANDOFF_LIMIT:-250}"
BUSY_TIMEOUT_SECONDS="${PM_ROBOT_RESEARCH_BUSY_TIMEOUT_SECONDS:-15}"
PLANNER_LOCK_ATTEMPTS="${PM_ROBOT_RESEARCH_PLANNER_LOCK_ATTEMPTS:-4}"
PLANNER_LOCK_SLEEP_SECONDS="${PM_ROBOT_RESEARCH_PLANNER_LOCK_SLEEP_SECONDS:-1}"

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
  echo "$(date -Iseconds) research control: ordered cycle start"
  if python -m pm_robot.cli --env /app/.env pipeline-cycle \
      --execute-plan \
      --continue-on-error \
      --heartbeat-prefix loop_research_control_step \
      --no-diagnostics \
      --busy-timeout-seconds "$BUSY_TIMEOUT_SECONDS" \
      --planner-lock-attempts "$PLANNER_LOCK_ATTEMPTS" \
      --planner-lock-sleep-seconds "$PLANNER_LOCK_SLEEP_SECONDS" \
      --min-score "$MIN_SCORE" \
      --state-limit "$STATE_LIMIT" \
      --state-stale-only \
      --state-commit-every "$STATE_COMMIT_EVERY" \
      --repair-limit "$REPAIR_LIMIT" \
      --shard-count "$SHARD_COUNT" \
      --wallet-light-limit "$WALLET_LIGHT_LIMIT" \
      --wallet-medium-limit "$WALLET_MEDIUM_LIMIT" \
      --wallet-deep-limit "$WALLET_DEEP_LIMIT" \
      --wallet-max-active-jobs "$WALLET_MAX_ACTIVE_JOBS" \
      --copyability-limit "$COPYABILITY_LIMIT" \
      --copyability-max-active-jobs "$COPYABILITY_MAX_ACTIVE_JOBS" \
      --copyability-min-activity-events "$COPYABILITY_MIN_ACTIVITY_EVENTS" \
      --copyability-shard-count "$COPYABILITY_SHARD_COUNT" \
      --copyability-rescan-seconds "$COPYABILITY_RESCAN_SECONDS" \
      --feature-limit "$FEATURE_LIMIT" \
      --feature-min-activity-events "$FEATURE_MIN_ACTIVITY_EVENTS" \
      --feature-commit-every "$FEATURE_COMMIT_EVERY" \
      --evidence-promotion-limit "$EVIDENCE_PROMOTION_LIMIT" \
      --score-limit "$SCORE_LIMIT" \
      --policy "$POLICY_PATH"; then
    echo "$(date -Iseconds) research control: ordered cycle ok"
    runtime_heartbeat loop_research_control ok
  else
    echo "$(date -Iseconds) research control: ordered cycle partial; later phases used committed data" >&2
    runtime_heartbeat loop_research_control partial "one or more isolated pipeline-cycle phases failed"
  fi

  # Handoff freshness must not depend on every planning phase succeeding.
  echo "$(date -Iseconds) research control: export paper handoff start"
  if python -m pm_robot.cli --env /app/.env paper-handoff-export \
      --out /app/reports/paper_handoff.json \
      --csv-out /app/reports/paper_handoff.csv \
      --limit "$PAPER_HANDOFF_LIMIT"; then
    echo "$(date -Iseconds) research control: export paper handoff ok"
    runtime_heartbeat loop_score_paper_handoff ok
  else
    echo "$(date -Iseconds) research control: export paper handoff failed" >&2
    runtime_heartbeat loop_score_paper_handoff failed "paper-handoff-export failed from research control"
  fi
  sleep "$INTERVAL"
done
