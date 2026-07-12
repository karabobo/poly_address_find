#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${PM_ROBOT_NAS_ROOT:-$SCRIPT_DIR}"

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
if [[ -z "$storage_setting" || "$storage_setting" == "." ]]; then
  STORAGE_ROOT="$ROOT"
elif [[ "$storage_setting" == /* ]]; then
  STORAGE_ROOT="$storage_setting"
else
  STORAGE_ROOT="$ROOT/$storage_setting"
fi
DATA_DIR="$STORAGE_ROOT/data"
REPORTS_DIR="$STORAGE_ROOT/reports"
DOCKER="${DOCKER:-/usr/local/bin/docker}"
DOCKER_SUDO="${PM_ROBOT_DOCKER_SUDO:-auto}"
DOCKER_RUNNER_READY=0
DOCKER_RUNNER=()
WATCHDOG_DISABLED_FILE="${PM_ROBOT_WATCHDOG_DISABLED_FILE:-$ROOT/.watchdog-disabled}"
CORE_SERVICES="proxy-tunnel web"
DISCOVERY_SERVICES="discovery-loop rtds-discovery"
CONTROL_SERVICES="research-control"
PIPELINE_SERVICES="pipeline-worker-0 pipeline-worker-1 pipeline-worker-2"
COPYABILITY_SERVICES="copyability-worker-0 copyability-worker-1"
SCORE_SERVICES="$CONTROL_SERVICES"
PAPER_OBSERVER_SERVICES="paper-observer-loop"
MAINTENANCE_SERVICES="maintenance-loop"
BACKUP_SERVICES="backup-loop"
APP_SERVICES="web $DISCOVERY_SERVICES $CONTROL_SERVICES $PIPELINE_SERVICES $COPYABILITY_SERVICES $PAPER_OBSERVER_SERVICES $MAINTENANCE_SERVICES"
RESEARCH_SERVICES="$CORE_SERVICES $DISCOVERY_SERVICES $CONTROL_SERVICES $PIPELINE_SERVICES $COPYABILITY_SERVICES $PAPER_OBSERVER_SERVICES $MAINTENANCE_SERVICES"
EXECUTION_SERVICES="paper-runner-loop paper-settle-loop publish-loop"

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
    docker_cli compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
    return
  fi
  echo "Docker Compose is not available on this NAS." >&2
  exit 1
}

task_compose() {
  compose --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" run --rm --no-deps "$@"
}

execution_compose() {
  compose -f docker-compose.yml -f docker-compose.execution.yml --profile execution "$@"
}

compose_restart_services() {
  # Source and config are bind-mounted; a restart must never force an image rebuild.
  compose up -d --no-deps --no-build --no-recreate "$@"
  compose restart "$@"
}

execution_compose_restart_services() {
  execution_compose up -d --no-deps --no-build --no-recreate "$@"
  execution_compose restart "$@"
}

remove_legacy_control_containers() {
  local container
  for container in pm-robot-pipeline-planner pm-robot-copyability-planner pm-robot-score-loop; do
    if docker_cli inspect "$container" >/dev/null 2>&1; then
      echo "Removing legacy control container: $container"
      docker_cli rm -f "$container" >/dev/null
    fi
  done
}

execution_preflight() {
  execution_ps_json=""
  execution_ps_error=""
  if execution_ps_json="$(execution_compose ps --format json 2>&1)"; then
    execution_ps_error=""
  else
    execution_ps_error="$execution_ps_json"
    execution_ps_json=""
  fi
  PYTHONPATH="$ROOT/app/src" \
    PM_ROBOT_RUNTIME_ROOT="$ROOT" \
    PM_ROBOT_STORAGE_ROOT_PATH="$STORAGE_ROOT" \
    PM_ROBOT_EXECUTION_SERVICES="$EXECUTION_SERVICES" \
    PM_ROBOT_EXECUTION_COMPOSE_PS_JSON="$execution_ps_json" \
    PM_ROBOT_EXECUTION_COMPOSE_PS_ERROR="$execution_ps_error" \
    python3 - <<'PY'
import json

from pm_robot.execution.preflight import execution_preflight_from_env

print(json.dumps(execution_preflight_from_env(), ensure_ascii=False, indent=2))
PY
}

cmd="${1:-status}"
shift || true

cd "$ROOT"

runtime_status() {
  service_ps_json=""
  service_ps_error=""
  if service_ps_json="$(compose ps --format json 2>&1)"; then
    service_ps_error=""
  else
    service_ps_error="$service_ps_json"
    service_ps_json=""
  fi
  PYTHONPATH="$ROOT/app/src" \
    PM_ROBOT_RUNTIME_ROOT="$ROOT" \
    PM_ROBOT_STORAGE_ROOT_PATH="$STORAGE_ROOT" \
    PM_ROBOT_EXPECTED_SERVICES="$RESEARCH_SERVICES" \
    PM_ROBOT_COMPOSE_PS_JSON="$service_ps_json" \
    PM_ROBOT_COMPOSE_PS_ERROR="$service_ps_error" \
    python3 - <<'PY'
from pathlib import Path
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request

from pm_robot.config import RobotSettings
from pm_robot.orchestration.pipeline_audit import address_quality_report
from pm_robot.web import _runtime_build_info, dashboard_data

root = Path(os.environ["PM_ROBOT_RUNTIME_ROOT"])
storage_root = Path(os.environ["PM_ROBOT_STORAGE_ROOT_PATH"])
db_path = storage_root / "data" / "pm_robot.sqlite"
settings = RobotSettings(db_path=db_path, execution_mode="research")
source = _runtime_build_info()
try:
    dashboard = dashboard_data(settings, include_pair_quality=False)
    production_readiness = dashboard.get("production_readiness") or {}
    copyability_lane = dashboard.get("copyability_lane") or {}
    score_policy = dashboard.get("score_policy") or {}
    storage_maintenance = dashboard.get("storage_maintenance") or {}
    retention_cycle = storage_maintenance.get("retention_cycle") or {}
except Exception as exc:
    production_readiness = {"error": type(exc).__name__, "message": str(exc)[:160]}
    copyability_lane = {"error": type(exc).__name__, "message": str(exc)[:160]}
    score_policy = {"error": type(exc).__name__, "message": str(exc)[:160]}
    storage_maintenance = {"error": type(exc).__name__, "message": str(exc)[:160]}
    retention_cycle = {"error": type(exc).__name__, "message": str(exc)[:160]}


def env_value(name):
    env_path = root / ".env"
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    prefix = f"{name}="
    for line in lines:
        if line.startswith(prefix):
            return line[len(prefix):].strip().strip("\"'")
    return ""


def count_rows(conn, sql):
    try:
        return [dict(row) for row in conn.execute(sql)]
    except sqlite3.Error as exc:
        return [{"error": str(exc)}]


def source_mounts_report(root):
    compose_path = root / "docker-compose.yml"
    try:
        text = compose_path.read_text(encoding="utf-8")
    except OSError as exc:
        return {"configured": False, "error": type(exc).__name__, "message": str(exc)[:160]}
    mounts = {
        "./app/src:/app/src:ro": text.count("./app/src:/app/src:ro"),
        "./app/deploy:/app/deploy:ro": text.count("./app/deploy:/app/deploy:ro"),
        "./app/scripts:/app/scripts:ro": text.count("./app/scripts:/app/scripts:ro"),
        "./config:/app/config:ro": text.count("./config:/app/config:ro"),
    }
    return {
        "configured": all(count > 0 for count in mounts.values()),
        "mount_counts": mounts,
    }


def service_monitor_report():
    expected = [item for item in os.environ.get("PM_ROBOT_EXPECTED_SERVICES", "").split() if item]
    raw = os.environ.get("PM_ROBOT_COMPOSE_PS_JSON", "")
    error = os.environ.get("PM_ROBOT_COMPOSE_PS_ERROR", "")
    if not raw:
        return {
            "available": False,
            "state": "unverified",
            "expected": expected,
            "running": [],
            "missing": [],
            "not_running": [],
            "error": error[:240],
            "action": "./pmrobot-nas.sh status",
        }
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {
            "available": False,
            "state": "parse_error",
            "expected": expected,
            "running": [],
            "missing": [],
            "not_running": [],
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
            "action": "./pmrobot-nas.sh status",
        }
    by_service = {str(row.get("Service") or ""): row for row in rows if row.get("Service")}
    running = sorted(
        service
        for service, row in by_service.items()
        if str(row.get("State") or "").lower() == "running"
    )
    missing = [service for service in expected if service not in by_service]
    not_running = [
        {
            "service": service,
            "state": str(by_service[service].get("State") or ""),
            "status": str(by_service[service].get("Status") or ""),
            "exit_code": by_service[service].get("ExitCode"),
        }
        for service in expected
        if service in by_service and str(by_service[service].get("State") or "").lower() != "running"
    ]
    ok = not missing and not not_running
    return {
        "available": True,
        "state": "ok" if ok else "incomplete",
        "expected": expected,
        "running": running,
        "missing": missing,
        "not_running": not_running,
        "action": "" if ok else "./pmrobot-nas.sh runtime-ensure",
    }


def deployment_status(
    source,
    api,
    recent_copyability_runs,
    source_mounts,
    copyability_lane,
    service_monitor,
    paper_handoff=None,
    paper_observer=None,
    paper_observer_evaluation=None,
):
    return _deployment_status(
        source,
        api,
        recent_copyability_runs,
        source_mounts,
        copyability_lane,
        service_monitor,
        paper_handoff,
        paper_observer,
        paper_observer_evaluation,
    )


def _deployment_status(
    source,
    api,
    recent_copyability_runs,
    source_mounts,
    copyability_lane,
    service_monitor,
    paper_handoff,
    paper_observer,
    paper_observer_evaluation,
):
    actions = []
    web_current = bool(api.get("matches_source"))
    if api.get("auth_required"):
        web = {"state": "auth_unverified", "action": ""}
    elif web_current:
        web = {"state": "current", "action": ""}
    elif api.get("reachable"):
        web = {"state": "outdated", "action": "./pmrobot-nas.sh web-restart"}
        actions.append({
            "service": "web",
            "command": "./pmrobot-nas.sh web-restart",
            "reason": "running web API does not expose the current source fingerprint",
        })
    else:
        web = {"state": "unreachable", "action": "./pmrobot-nas.sh web-up"}
        actions.append({
            "service": "web",
            "command": "./pmrobot-nas.sh web-up",
            "reason": "web API is not reachable from the NAS host",
        })

    latest_copyability_type = ""
    for row in recent_copyability_runs:
        if row.get("error"):
            continue
        latest_copyability_type = str(row.get("ingest_type") or "")
        break
    legacy_copyability_type = latest_copyability_type in {
        "copyability_evidence_worker_0",
        "copyability_evidence_worker_1",
        "copyability_evidence_worker_2",
    }
    queued_copyability = int(copyability_lane.get("queued") or 0)
    running_copyability = int(copyability_lane.get("running") or 0)
    copyability_progress = {
        "completed_1h": int(copyability_lane.get("completed_1h") or 0),
        "completed_6h": int(copyability_lane.get("completed_6h") or 0),
        "completed_24h": int(copyability_lane.get("completed_24h") or 0),
        "recent_rate_per_hour": float(copyability_lane.get("recent_rate_per_hour") or 0),
        "eta_label": str(copyability_lane.get("eta_label") or ""),
        "latest_completed_at": int(copyability_lane.get("latest_completed_at") or 0),
    }
    recent_copyability_progress = copyability_progress["completed_1h"] > 0 or copyability_progress["recent_rate_per_hour"] > 0
    if queued_copyability and not running_copyability and not recent_copyability_progress:
        copyability = {
            "state": "idle_with_queued_jobs",
            "latest_run_type": latest_copyability_type,
            "legacy_run_type": legacy_copyability_type,
            "queued": queued_copyability,
            "running": running_copyability,
            **copyability_progress,
            "action": "./pmrobot-nas.sh copyability-restart-when-idle",
        }
        actions.append({
            "service": "copyability",
            "command": "./pmrobot-nas.sh copyability-restart-when-idle",
            "reason": "copyability queue has queued jobs but no running worker",
        })
    elif queued_copyability and not running_copyability:
        copyability = {
            "state": "recent_progress_no_running",
            "latest_run_type": latest_copyability_type,
            "legacy_run_type": legacy_copyability_type,
            "queued": queued_copyability,
            "running": running_copyability,
            **copyability_progress,
            "action": "",
        }
    elif legacy_copyability_type:
        copyability = {
            "state": "legacy_worker_run_type",
            "latest_run_type": latest_copyability_type,
            "legacy_run_type": True,
            "queued": queued_copyability,
            "running": running_copyability,
            **copyability_progress,
            "action": "./pmrobot-nas.sh copyability-restart-when-idle",
        }
        actions.append({
            "service": "copyability",
            "command": "./pmrobot-nas.sh copyability-restart-when-idle",
            "reason": "latest copyability run still uses the legacy shard-only run name",
        })
    elif latest_copyability_type:
        copyability = {
            "state": "current_or_restarted",
            "latest_run_type": latest_copyability_type,
            "legacy_run_type": False,
            "queued": queued_copyability,
            "running": running_copyability,
            **copyability_progress,
            "action": "",
        }
    else:
        copyability = {
            "state": "no_recent_runs",
            "latest_run_type": "",
            "legacy_run_type": False,
            "queued": queued_copyability,
            "running": running_copyability,
            **copyability_progress,
            "action": "",
        }
    if paper_handoff is None:
        paper_handoff = {}
    paper_handoff_state = str(paper_handoff.get("state") or "")
    if paper_handoff_state == "missing":
        paper_handoff = {
            **paper_handoff,
            "action": paper_handoff.get("action") or "./pmrobot-nas.sh shell python -m pm_robot.cli --env /app/.env paper-handoff-export --out /app/reports/paper_handoff.json --csv-out /app/reports/paper_handoff.csv",
        }
        actions.append({
            "service": "paper_handoff",
            "command": paper_handoff["action"],
            "reason": "paper handoff export file is missing",
        })
    elif paper_handoff_state == "stale":
        paper_handoff = {
            **paper_handoff,
            "action": paper_handoff.get("action") or "./pmrobot-nas.sh research-control-restart",
        }
        actions.append({
            "service": "paper_handoff",
            "command": paper_handoff["action"],
            "reason": "paper handoff export is stale; research-control should refresh it after scoring",
        })
    if paper_observer is None:
        paper_observer = {}
    paper_observer_state = str(paper_observer.get("state") or "")
    if paper_observer_state == "missing":
        paper_observer = {
            **paper_observer,
            "action": paper_observer.get("action") or "./pmrobot-nas.sh observer-restart",
        }
        actions.append({
            "service": "paper_observer_preview",
            "command": paper_observer["action"],
            "reason": "paper observer preview export file is missing",
        })
    elif paper_observer_state == "stale":
        paper_observer = {
            **paper_observer,
            "action": paper_observer.get("action") or "./pmrobot-nas.sh observer-restart",
        }
        actions.append({
            "service": "paper_observer_preview",
            "command": paper_observer["action"],
            "reason": "paper observer preview export is stale; paper-observer-loop should refresh it",
        })
    if paper_observer_evaluation is None:
        paper_observer_evaluation = {}
    paper_observer_evaluation_state = str(paper_observer_evaluation.get("state") or "")
    if paper_observer_evaluation_state == "missing":
        paper_observer_evaluation = {
            **paper_observer_evaluation,
            "action": paper_observer_evaluation.get("action") or "./pmrobot-nas.sh observer-restart",
        }
        actions.append({
            "service": "paper_observer_evaluation",
            "command": paper_observer_evaluation["action"],
            "reason": "paper observer quote evaluation export file is missing",
        })
    elif paper_observer_evaluation_state == "stale":
        paper_observer_evaluation = {
            **paper_observer_evaluation,
            "action": paper_observer_evaluation.get("action") or "./pmrobot-nas.sh observer-restart",
        }
        actions.append({
            "service": "paper_observer_evaluation",
            "command": paper_observer_evaluation["action"],
            "reason": "paper observer quote evaluation export is stale; paper-observer-loop should refresh it",
        })
    return {
        "source_fingerprint": source.get("source_fingerprint"),
        "source_delivery": source.get("source_delivery"),
        "source_root": source.get("source_root"),
        "source_mounts": source_mounts,
        "service_monitor": service_monitor,
        "web": web,
        "copyability": copyability,
        "paper_handoff": paper_handoff,
        "paper_observer_preview": paper_observer,
        "paper_observer_evaluation": paper_observer_evaluation,
        "actions": actions,
    }


def paper_handoff_file_report(root):
    json_path = storage_root / "reports" / "paper_handoff.json"
    csv_path = storage_root / "reports" / "paper_handoff.csv"
    now = int(time.time())
    base = {
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "json_exists": json_path.exists(),
        "csv_exists": csv_path.exists(),
        "state": "missing",
        "action": "./pmrobot-nas.sh shell python -m pm_robot.cli --env /app/.env paper-handoff-export --out /app/reports/paper_handoff.json --csv-out /app/reports/paper_handoff.csv",
    }
    if not json_path.exists():
        return base
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            **base,
            "json_exists": True,
            "state": "invalid",
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
        }
    generated_at = int(payload.get("generated_at") or 0)
    mtime = int(json_path.stat().st_mtime)
    age_seconds = now - (generated_at or mtime)
    stale = age_seconds > 3600
    return {
        **base,
        "json_exists": True,
        "csv_exists": csv_path.exists(),
        "state": "stale" if stale else "current",
        "action": "./pmrobot-nas.sh research-control-restart" if stale else "",
        "schema_version": payload.get("schema_version") or "",
        "generated_at": generated_at,
        "age_seconds": max(0, age_seconds),
        "candidate_count": int(payload.get("candidate_count") or 0),
        "visible_wallet_count": int(payload.get("visible_wallet_count") or 0),
        "stage_counts": payload.get("stage_counts") or [],
        "state_counts": payload.get("state_counts") or [],
    }


def paper_observer_file_report(root):
    json_path = storage_root / "reports" / "paper_observer_preview.json"
    now = int(time.time())
    base = {
        "json_path": str(json_path),
        "json_exists": json_path.exists(),
        "state": "missing",
        "action": "./pmrobot-nas.sh observer-restart",
    }
    if not json_path.exists():
        return base
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            **base,
            "json_exists": True,
            "state": "invalid",
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
        }
    generated_at = int(payload.get("generated_at") or 0)
    mtime = int(json_path.stat().st_mtime)
    age_seconds = now - (generated_at or mtime)
    stale = age_seconds > 3600
    return {
        **base,
        "json_exists": True,
        "state": "stale" if stale else "current",
        "action": "./pmrobot-nas.sh observer-restart" if stale else "",
        "schema_version": payload.get("schema_version") or "",
        "generated_at": generated_at,
        "age_seconds": max(0, age_seconds),
        "max_signal_age_sec": int(payload.get("max_signal_age_sec") or 0),
        "paper_stage_wallets": int(payload.get("paper_stage_wallets") or 0),
        "signals_seen": int(payload.get("signals_seen") or 0),
        "recent_buy_events": int(payload.get("recent_buy_events") or 0),
        "latest_buy_age_sec": payload.get("latest_buy_age_sec"),
        "no_signal_reason": payload.get("no_signal_reason") or "",
        "window_diagnostics": payload.get("window_diagnostics") or [],
    }


def paper_observer_evaluation_file_report(root):
    json_path = storage_root / "reports" / "paper_observer_evaluation.json"
    now = int(time.time())
    base = {
        "json_path": str(json_path),
        "json_exists": json_path.exists(),
        "state": "missing",
        "action": "./pmrobot-nas.sh observer-restart",
    }
    if not json_path.exists():
        return base
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            **base,
            "json_exists": True,
            "state": "invalid",
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
        }
    generated_at = int(payload.get("generated_at") or 0)
    mtime = int(json_path.stat().st_mtime)
    age_seconds = now - (generated_at or mtime)
    stale = age_seconds > 3600
    return {
        **base,
        "json_exists": True,
        "state": "stale" if stale else "current",
        "action": "./pmrobot-nas.sh observer-restart" if stale else "",
        "schema_version": payload.get("schema_version") or "",
        "generated_at": generated_at,
        "age_seconds": max(0, age_seconds),
        "max_signal_age_sec": int(payload.get("max_signal_age_sec") or 0),
        "max_actionable_signal_age_sec": int(payload.get("max_actionable_signal_age_sec") or 0),
        "max_stake_usd": float(payload.get("max_stake_usd") or 0),
        "signals_seen": int(payload.get("signals_seen") or 0),
        "quotes_attempted": int(payload.get("quotes_attempted") or 0),
        "quotes_succeeded": int(payload.get("quotes_succeeded") or 0),
        "accepted_signals": int(payload.get("accepted_signals") or 0),
        "actionable_signals": int(payload.get("actionable_signals") or 0),
        "rejected_signals": int(payload.get("rejected_signals") or 0),
        "stale_signal_rejections": int(payload.get("stale_signal_rejections") or 0),
        "quote_error_signals": int(payload.get("quote_error_signals") or 0),
        "actionable_rate_pct": float(payload.get("actionable_rate_pct") or 0),
        "average_slippage_bps": payload.get("average_slippage_bps"),
        "average_latency_ms": payload.get("average_latency_ms"),
    }


token = env_value("PM_ROBOT_UI_TOKEN")
request = urllib.request.Request(
    "http://127.0.0.1:8787/api/runtime",
    headers={"X-PM-Robot-Token": token} if token else {},
)
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        data = json.loads(response.read().decode("utf-8"))
    api_runtime = data.get("runtime") or {}
    api = {
        "reachable": True,
        "health": data.get("health") or (data.get("ops_health") or {}).get("health"),
        "has_runtime": bool(api_runtime),
        "source_fingerprint": api_runtime.get("source_fingerprint") or "",
        "source_delivery": api_runtime.get("source_delivery") or "",
        "matches_source": bool(api_runtime) and api_runtime.get("source_fingerprint") == source["source_fingerprint"],
    }
except (OSError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
    if isinstance(exc, urllib.error.HTTPError) and exc.code in {401, 403}:
        api = {
            "reachable": True,
            "health": "auth_required",
            "has_runtime": False,
            "source_fingerprint": "",
            "source_delivery": "",
            "matches_source": False,
            "auth_required": True,
            "message": "web API requires PM_ROBOT_UI_TOKEN for source fingerprint verification",
        }
    elif isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
        api = {
            "reachable": True,
            "health": "runtime_endpoint_missing",
            "has_runtime": False,
            "source_fingerprint": "",
            "source_delivery": "",
            "matches_source": False,
            "message": "running web API does not expose /api/runtime",
        }
    else:
        api = {"reachable": False, "error": type(exc).__name__, "message": str(exc)[:160]}

recent_copyability_runs = []
try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        recent_copyability_runs = count_rows(
            conn,
            """
            SELECT ingest_type, status, started_at, finished_at, error
            FROM ingest_runs
            WHERE ingest_type LIKE 'copyability_evidence_worker%'
            ORDER BY started_at DESC
            LIMIT 8
            """,
        )
        db = {
            "path": str(db_path),
            "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "address_quality": address_quality_report(conn),
            "candidate_stage": count_rows(
                conn,
                "SELECT candidate_stage, COUNT(*) AS count FROM candidate_wallets GROUP BY candidate_stage ORDER BY count DESC",
            ),
            "pipeline_jobs": count_rows(
                conn,
                "SELECT job_type, status, COUNT(*) AS count FROM pipeline_jobs GROUP BY job_type, status ORDER BY job_type, status",
            ),
        }
    finally:
        conn.close()
except sqlite3.Error as exc:
    db = {"path": str(db_path), "error": str(exc)}

service_monitor = service_monitor_report()
paper_handoff = paper_handoff_file_report(root)
paper_observer = paper_observer_file_report(root)
paper_observer_evaluation = paper_observer_evaluation_file_report(root)
deployment = _deployment_status(
    source,
    api,
    recent_copyability_runs,
    source_mounts_report(root),
    copyability_lane,
    service_monitor,
    paper_handoff,
    paper_observer,
    paper_observer_evaluation,
)
if service_monitor.get("available") and service_monitor.get("state") != "ok":
    deployment["actions"].append({
        "service": "research_services",
        "command": "./pmrobot-nas.sh runtime-ensure",
        "reason": "one or more research/scoring containers are missing or not running",
    })

print(json.dumps({
    "mode": "research/scoring",
    "source": source,
    "api": api,
    "deployment": deployment,
    "production_readiness": production_readiness,
    "score_policy": score_policy,
    "retention": {
        "health": storage_maintenance.get("state", ""),
        "next_action": storage_maintenance.get("next_action", ""),
        "available": retention_cycle.get("available", False),
        "fresh": retention_cycle.get("fresh", False),
        "age_seconds": retention_cycle.get("age_seconds"),
        "state": retention_cycle.get("state", ""),
        "backlog_wallets": (retention_cycle.get("backlog_after") or {}).get("total_wallets", 0),
        "backlog_activity_rows": (retention_cycle.get("backlog_after") or {}).get("total_activity_rows", 0),
        "deleted_activity_rows": retention_cycle.get("deleted_activity_rows", 0),
        "eligible_rows_added": retention_cycle.get("eligible_rows_added", 0),
        "net_backlog_change_rows": retention_cycle.get("net_backlog_change_rows", 0),
        "forecast_rate_per_hour": retention_cycle.get("forecast_rate_per_hour", 0),
        "forecast_eta_hours": retention_cycle.get("forecast_eta_hours"),
        "forecast_basis": retention_cycle.get("forecast_basis", ""),
        "yielded_to_research": retention_cycle.get("yielded_to_research", False),
        "duration_seconds": retention_cycle.get("duration_seconds", 0),
        "batches_completed": retention_cycle.get("batches_completed", 0),
        "error": retention_cycle.get("error", ""),
    },
    "db": db,
}, ensure_ascii=False, sort_keys=True, indent=2))
PY
}

bounded_positive_int() {
  value="${1:-}"
  fallback="${2:-1}"
  max_value="${3:-5}"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    value="$fallback"
  fi
  if [[ "$value" -lt 1 ]]; then
    value=1
  fi
  if [[ "$value" -gt "$max_value" ]]; then
    value="$max_value"
  fi
  echo "$value"
}

host_copyability_drain_once() {
  limit="$(bounded_positive_int "${1:-1}" 1 5)"
  worker_id="manual-host-drain-$(date +%s)"
  # Keep manual CLI runs on the same Python and dependency image as production.
  task_compose task \
    --db /app/data/pm_robot.sqlite \
    copyability-worker \
    --shard-index 0 \
    --shard-count 1 \
    --limit "$limit" \
    --lease-seconds "${PM_ROBOT_COPYABILITY_WORKER_LEASE_SECONDS:-7200}" \
    --worker-id "$worker_id" \
    --max-leader-events "${PM_ROBOT_COPYABILITY_MAX_LEADER_EVENTS:-3000}" \
    --max-followers-per-event "${PM_ROBOT_COPYABILITY_MAX_FOLLOWERS_PER_EVENT:-200}" \
    --prefer-scan-mode "${PM_ROBOT_COPYABILITY_PREFER_SCAN_MODE:-}"
}

host_materialize_once() {
  limit="$(bounded_positive_int "${1:-50}" 50 500)"
  task_compose task \
    --db /app/data/pm_robot.sqlite \
    materialize-features \
    --limit "$limit"
}

host_score_once() {
  limit="$(bounded_positive_int "${1:-50}" 50 500)"
  task_compose \
    -e PM_ROBOT_POLICY_PATH=/app/config/leader_scoring_policy.json \
    task \
    --db /app/data/pm_robot.sqlite \
    build-review \
    --incremental \
    --limit "$limit" \
    --no-import-csv \
    --out /app/reports/manual_incremental_review.csv
}

host_policy_rescore_once() {
  score_limit="$(bounded_positive_int "${1:-250}" 250 500)"
  feature_limit="${2:-0}"
  if [[ "$feature_limit" =~ ^[0-9]+$ && "$feature_limit" -gt 0 ]]; then
    feature_limit="$(bounded_positive_int "$feature_limit" 1 500)"
    echo "materializing wallet features with limit=${feature_limit}"
    host_materialize_once "$feature_limit"
  else
    echo "skipping feature materialization; policy rescore uses existing wallet features"
  fi
  echo "running incremental scoring with policy freshness check, limit=${score_limit}"
  host_score_once "$score_limit"
}

copyability_queue_counts() {
  PM_ROBOT_HOST_DB_PATH="$DATA_DIR/pm_robot.sqlite" python3 - <<'PY'
import os
import sqlite3

conn = sqlite3.connect(os.environ["PM_ROBOT_HOST_DB_PATH"])
queued, running = conn.execute(
    """
    SELECT
        SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END)
    FROM pipeline_jobs
    WHERE job_type = 'copyability_evidence'
    """
).fetchone()
print(f"{int(queued or 0)} {int(running or 0)}")
PY
}

pipeline_running_job_counts() {
  PM_ROBOT_HOST_DB_PATH="$DATA_DIR/pm_robot.sqlite" python3 - <<'PY'
import os
import sqlite3

conn = sqlite3.connect(os.environ["PM_ROBOT_HOST_DB_PATH"])
row = conn.execute(
    """
    SELECT
        SUM(CASE WHEN job_type = 'wallet_evidence_backfill' THEN 1 ELSE 0 END),
        SUM(CASE WHEN job_type = 'copyability_evidence' THEN 1 ELSE 0 END),
        COUNT(*)
    FROM pipeline_jobs
    WHERE status = 'running'
    """
).fetchone()
print(f"{int(row[0] or 0)} {int(row[1] or 0)} {int(row[2] or 0)}")
PY
}

host_recover_once() {
  copyability_limit="$(bounded_positive_int "${1:-2}" 2 5)"
  score_limit="$(bounded_positive_int "${2:-50}" 50 500)"
  feature_limit="$(bounded_positive_int "${3:-$score_limit}" "$score_limit" 500)"
  read -r queued running < <(copyability_queue_counts)
  echo "copyability queue: queued=${queued} running=${running}"
  if [[ "$queued" -gt 0 && "$running" -eq 0 ]]; then
    echo "draining up to ${copyability_limit} copyability job(s) through host runtime"
    host_copyability_drain_once "$copyability_limit"
  else
    echo "copyability drain skipped"
  fi
  echo "materializing wallet features with limit=${feature_limit}"
  host_materialize_once "$feature_limit"
  echo "running incremental scoring with limit=${score_limit}"
  host_score_once "$score_limit"
}

wal_truncate_window() {
  timeout_seconds="$(bounded_positive_int "${1:-300}" 300 3600)"
  task_name="pm-robot-wal-truncate-task"
  echo "WAL maintenance window: stopping research/scoring app services."
  compose stop $APP_SERVICES
  docker_cli rm -f "$task_name" >/dev/null 2>&1 || true
  set +e
  checkpoint_container="$(compose run -d --name "$task_name" task maintenance --skip-cleanup --reset-stale-jobs --reset-stale-ingest-runs --stale-ingest-run-seconds "${PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS:-21600}" --wal-checkpoint truncate 2>&1)"
  start_status=$?
  set -e
  timed_out=0
  checkpoint_status="$start_status"
  if [[ "$start_status" -eq 0 ]]; then
    deadline=$(( $(date +%s) + timeout_seconds ))
    while [[ "$(docker_cli inspect -f '{{.State.Running}}' "$task_name" 2>/dev/null)" == "true" ]]; do
      if [[ "$(date +%s)" -ge "$deadline" ]]; then
        timed_out=1
        echo "WAL maintenance window: truncate exceeded ${timeout_seconds}s; stopping checkpoint task." >&2
        docker_cli stop "$task_name" >/dev/null 2>&1 || true
        break
      fi
      sleep 5
    done
    checkpoint_status="$(docker_cli inspect -f '{{.State.ExitCode}}' "$task_name" 2>/dev/null || echo 1)"
    docker_cli logs "$task_name" 2>/dev/null || true
    docker_cli rm -f "$task_name" >/dev/null 2>&1 || true
  else
    echo "$checkpoint_container" >&2
  fi
  echo "WAL maintenance window: restarting research/scoring services."
  compose up -d --no-deps --no-recreate $RESEARCH_SERVICES
  if [[ "$timed_out" -eq 1 ]]; then
    return 124
  fi
  return "$checkpoint_status"
}

wal_truncate_when_idle() {
  wait_timeout_seconds="$(bounded_positive_int "${1:-7200}" 7200 86400)"
  checkpoint_timeout_seconds="$(bounded_positive_int "${2:-900}" 900 3600)"
  poll_seconds="$(bounded_positive_int "${3:-30}" 30 600)"
  deadline=$(( $(date +%s) + wait_timeout_seconds ))
  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    read -r evidence_running copyability_running total_running < <(pipeline_running_job_counts)
    if [[ "$total_running" == "0" ]]; then
      echo "WAL maintenance idle guard: no running pipeline jobs; starting truncate window."
      wal_truncate_window "$checkpoint_timeout_seconds"
      return $?
    fi
    echo "WAL maintenance idle guard: ${total_running} running job(s) (${evidence_running} evidence, ${copyability_running} copyability); waiting ${poll_seconds}s."
    sleep "$poll_seconds"
  done
  read -r evidence_running copyability_running total_running < <(pipeline_running_job_counts)
  echo "Timed out waiting for WAL maintenance idle window: ${total_running} running job(s) remain (${evidence_running} evidence, ${copyability_running} copyability)." >&2
  return 1
}

runtime_watchdog_once() {
  if [[ -f "$WATCHDOG_DISABLED_FILE" ]]; then
    echo "runtime watchdog: disabled by $WATCHDOG_DISABLED_FILE"
    return 0
  fi
  running_services="$(compose ps --services --filter status=running)"
  missing_services=()
  for service in $RESEARCH_SERVICES; do
    if ! grep -Fxq "$service" <<<"$running_services"; then
      missing_services+=("$service")
    fi
  done
  if [[ "${#missing_services[@]}" -eq 0 ]]; then
    echo "runtime watchdog: ok; all research/scoring services are running"
    return 0
  fi
  echo "runtime watchdog: starting missing/stopped research/scoring service(s): ${missing_services[*]}"
  compose up -d --no-deps --no-recreate "${missing_services[@]}"
}

watchdog_status() {
  if [[ -f "$WATCHDOG_DISABLED_FILE" ]]; then
    echo "runtime watchdog: disabled"
    echo "disabled_file: $WATCHDOG_DISABLED_FILE"
    sed -n '1,5p' "$WATCHDOG_DISABLED_FILE" 2>/dev/null || true
  else
    echo "runtime watchdog: enabled"
    echo "disabled_file: $WATCHDOG_DISABLED_FILE"
  fi
}

watchdog_disable() {
  mkdir -p "$(dirname "$WATCHDOG_DISABLED_FILE")"
  {
    echo "disabled_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "disabled_by=$(id -un)"
    if [[ "$#" -gt 0 ]]; then
      echo "reason=$*"
    fi
  } > "$WATCHDOG_DISABLED_FILE"
  watchdog_status
}

watchdog_enable() {
  rm -f "$WATCHDOG_DISABLED_FILE"
  watchdog_status
}

case "$cmd" in
  up|down|restart|app-restart|runtime-ensure|watchdog-once|web-up|web-restart|discovery-up|discovery-down|discovery-restart|research-control-up|research-control-down|research-control-restart|pipeline-up|pipeline-down|pipeline-restart|copyability-up|copyability-down|copyability-restart|score-up|score-down|score-restart|observer-up|observer-down|observer-restart|maintenance-up|maintenance-down|maintenance-restart|backup-up|backup-down|backup-restart|execution-up|execution-down|execution-restart)
    remove_legacy_control_containers
    ;;
esac

case "$cmd" in
  up)
    compose up -d --build $RESEARCH_SERVICES
    ;;
  web-up)
    compose up -d --build $CORE_SERVICES
    ;;
  web-restart)
    compose_restart_services web
    ;;
  pipeline-up)
    echo "Starting the shared research control loop and wallet evidence workers."
    compose up -d --build $CONTROL_SERVICES $PIPELINE_SERVICES
    ;;
  copyability-up)
    echo "Starting copyability workers; research-control owns queue admission."
    compose up -d --build $COPYABILITY_SERVICES
    ;;
  discovery-up)
    compose up -d --build $DISCOVERY_SERVICES
    ;;
  discovery-down)
    compose stop $DISCOVERY_SERVICES
    ;;
  pipeline-down)
    echo "Stopping wallet evidence workers and shared research-control."
    echo "This also pauses wallet/copyability admission, feature refresh, scoring, and handoff export."
    compose stop $CONTROL_SERVICES $PIPELINE_SERVICES
    ;;
  copyability-down)
    echo "Stopping copyability workers only; research-control may admit jobs up to the configured waterline."
    compose stop $COPYABILITY_SERVICES
    ;;
  research-control-up)
    compose up -d --build $CONTROL_SERVICES
    ;;
  research-control-down)
    echo "Stopping shared research-control pauses queue admission, feature refresh, scoring, and handoff export."
    compose stop $CONTROL_SERVICES
    ;;
  research-control-restart)
    compose_restart_services $CONTROL_SERVICES
    ;;
  score-up)
    echo "score-up is a compatibility alias for research-control-up."
    echo "The shared control loop also owns wallet/copyability admission and handoff export."
    compose up -d --build $SCORE_SERVICES
    ;;
  score-down)
    echo "score-down is a compatibility alias for research-control-down."
    echo "Stopping it also pauses wallet/copyability admission and handoff export."
    compose stop $SCORE_SERVICES
    ;;
  observer-up)
    compose up -d --build $PAPER_OBSERVER_SERVICES
    ;;
  observer-down)
    compose stop $PAPER_OBSERVER_SERVICES
    ;;
  maintenance-up)
    compose up -d --build $MAINTENANCE_SERVICES
    ;;
  maintenance-down)
    compose stop $MAINTENANCE_SERVICES
    ;;
  backup-up)
    compose up -d --build $BACKUP_SERVICES
    ;;
  backup-down)
    compose stop $BACKUP_SERVICES
    ;;
  execution-up)
    echo "Starting the opt-in execution profile: paper-run, paper-settle, and publish loops."
    echo "This is outside the default NAS research/scoring stack."
    execution_compose up -d --build $EXECUTION_SERVICES
    ;;
  execution-down)
    execution_compose stop $EXECUTION_SERVICES
    ;;
  execution-restart)
    echo "Restarting the opt-in execution profile."
    execution_compose_restart_services $EXECUTION_SERVICES
    ;;
  execution-status)
    echo "pm-robot execution profile is opt-in; default research/scoring commands do not start these services."
    execution_compose ps $EXECUTION_SERVICES
    ;;
  execution-preflight)
    execution_preflight
    ;;
  execution-logs)
    service="${2:-paper-runner-loop}"
    execution_compose logs --tail="${1:-200}" -f "$service"
    ;;
  down)
    compose down
    ;;
  restart)
    compose_restart_services $RESEARCH_SERVICES
    ;;
  app-restart)
    compose_restart_services $APP_SERVICES
    ;;
  runtime-ensure)
    compose up -d --no-deps --no-recreate $RESEARCH_SERVICES
    ;;
  watchdog-once)
    runtime_watchdog_once
    ;;
  watchdog-status)
    watchdog_status
    ;;
  watchdog-disable)
    watchdog_disable "$@"
    ;;
  watchdog-enable)
    watchdog_enable
    ;;
  discovery-restart)
    compose_restart_services $DISCOVERY_SERVICES
    ;;
  pipeline-restart)
    compose_restart_services $CONTROL_SERVICES $PIPELINE_SERVICES
    ;;
  copyability-restart)
    compose_restart_services $COPYABILITY_SERVICES
    ;;
  copyability-ensure-workers)
    compose up -d --no-deps --no-recreate copyability-worker-0 copyability-worker-1
    ;;
  copyability-restart-when-idle)
    timeout_seconds="${1:-7200}"
    poll_seconds="${2:-15}"
    compose up -d --no-deps --no-recreate copyability-worker-0 copyability-worker-1
    deadline=$(( $(date +%s) + timeout_seconds ))
    while [[ "$(date +%s)" -lt "$deadline" ]]; do
      running_count="$(PM_ROBOT_HOST_DB_PATH="$DATA_DIR/pm_robot.sqlite" python3 - <<'PY'
import os
import sqlite3

conn = sqlite3.connect(os.environ["PM_ROBOT_HOST_DB_PATH"])
print(conn.execute(
    "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ? AND status = ?",
    ("copyability_evidence", "running"),
).fetchone()[0])
PY
)"
      if [[ "$running_count" == "0" ]]; then
        compose up -d --no-deps --force-recreate copyability-worker-0 copyability-worker-1
        echo "copyability workers restarted after the copyability queue became idle."
        exit 0
      fi
      echo "copyability workers still have $running_count running job(s); waiting ${poll_seconds}s."
      sleep "$poll_seconds"
    done
    echo "Timed out waiting for copyability workers to become idle." >&2
    exit 1
    ;;
  score-restart)
    echo "score-restart is a compatibility alias for research-control-restart."
    echo "The shared control loop also owns wallet/copyability admission and handoff export."
    compose_restart_services $SCORE_SERVICES
    ;;
  observer-restart)
    compose_restart_services $PAPER_OBSERVER_SERVICES
    ;;
  maintenance-restart)
    compose_restart_services $MAINTENANCE_SERVICES
    ;;
  backup-restart)
    compose_restart_services $BACKUP_SERVICES
    ;;
  logs)
    service="${2:-web}"
    compose logs --tail="${1:-200}" -f "$service"
    ;;
  status)
    echo "pm-robot NAS mode: research/scoring only; read-only paper observer is allowed, paper trading/publish loops are not started here."
    compose ps
    ;;
  runtime-status)
    runtime_status
    ;;
  copyability-drain-once)
    host_copyability_drain_once "${1:-1}"
    ;;
  materialize-once)
    host_materialize_once "${1:-50}"
    ;;
  score-once)
    host_score_once "${1:-50}"
    ;;
  policy-rescore-once)
    host_policy_rescore_once "${1:-250}" "${2:-}"
    ;;
  recover-once)
    host_recover_once "${1:-2}" "${2:-50}" "${3:-}"
    ;;
  wal-truncate-window)
    wal_truncate_window "${1:-300}"
    ;;
  wal-truncate-when-idle)
    wal_truncate_when_idle "${1:-7200}" "${2:-900}" "${3:-30}"
    ;;
  migrate)
    compose run --rm task migrate
    ;;
  health)
    compose run --rm task health
    ;;
  app-status)
    compose run --rm task status
    ;;
  maintenance)
    compose run --rm task maintenance "$@"
    ;;
  backup-now)
    compose run --rm task backup
    ;;
  pipeline-jobs)
    compose run --rm task wallet-pipeline-jobs "$@"
    ;;
  pipeline-audit)
    compose run --rm task pipeline-audit "$@"
    ;;
  copyability-jobs)
    compose run --rm task copyability-jobs "$@"
    ;;
  shell)
    compose run --rm --entrypoint /bin/sh task
    ;;
  *)
    echo "Usage: $0 {up|web-up|web-restart|discovery-up|discovery-down|research-control-up|research-control-down|research-control-restart|pipeline-up|pipeline-down|copyability-up|copyability-down|score-up|score-down|observer-up|observer-down|maintenance-up|maintenance-down|backup-up|backup-down|backup-restart|backup-now|execution-up|execution-down|execution-restart|execution-status|execution-preflight|execution-logs [tail] [service]|down|restart|app-restart|runtime-ensure|watchdog-once|watchdog-status|watchdog-disable [reason]|watchdog-enable|discovery-restart|pipeline-restart|copyability-restart|copyability-ensure-workers|copyability-restart-when-idle [timeout_seconds] [poll_seconds]|score-restart|observer-restart|maintenance-restart|logs [tail] [service]|status|runtime-status|copyability-drain-once [limit<=5]|materialize-once [limit<=500]|score-once [limit<=500]|policy-rescore-once [score_limit<=500] [optional_feature_limit<=500]|recover-once [copyability_limit<=5] [score_limit<=500] [feature_limit<=500]|wal-truncate-window [checkpoint_timeout_seconds]|wal-truncate-when-idle [wait_timeout_seconds] [checkpoint_timeout_seconds] [poll_seconds]|migrate|health|app-status|maintenance|pipeline-jobs|pipeline-audit|copyability-jobs|shell}" >&2
    exit 2
    ;;
esac
