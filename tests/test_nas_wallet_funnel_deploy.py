import json
import os
from pathlib import Path
import re
import subprocess
import sys

from pm_robot.ops import ACTIVE_RESEARCH_RUNTIME_EVENTS

COMPOSE_PATH = Path("deploy/nas/docker-compose.yml")


def _service_block(name: str) -> str:
    text = COMPOSE_PATH.read_text(encoding="utf-8")
    marker = f"  {name}:\n"
    start = text.index(marker)
    match = re.search(r"^  [a-zA-Z0-9_-]+:\s*$", text[start + len(marker) :], re.MULTILINE)
    if match is None:
        return text[start:]
    return text[start : start + len(marker) + match.start()]


def test_nas_required_heartbeats_match_the_active_research_runtime_contract():
    env_text = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    line = next(
        line
        for line in env_text.splitlines()
        if line.startswith("PM_ROBOT_REQUIRED_RUNTIME_HEARTBEATS=")
    )
    configured = tuple(value for value in line.split("=", 1)[1].split(",") if value)

    assert set(configured) == set(ACTIVE_RESEARCH_RUNTIME_EVENTS)
    assert len(configured) == len(ACTIVE_RESEARCH_RUNTIME_EVENTS)


def test_nas_active_services_run_only_the_l0_l6_discovery_funnel():
    active_text = COMPOSE_PATH.read_text(encoding="utf-8")

    for service in {
        "proxy-tunnel",
        "proxy-tunnel-primary",
        "proxy-tunnel-secondary",
        "web",
        "research-control",
        "discovery-loop",
        "rtds-discovery",
        "wallet-screen-planner",
        "wallet-screen-worker-0",
        "wallet-screen-worker-1",
        "wallet-screen-worker-2",
        "wallet-history-worker-0",
        "wallet-history-worker-1",
        "wallet-history-worker-2",
        "l6-validation-worker",
        "maintenance-loop",
    }:
        assert f"  {service}:\n" in active_text
    for obsolete in (
        "pipeline-worker-",
        "copyability-worker",
        "validation-observer",
        "paper-runner",
        "paper-settle",
        "publish-loop",
    ):
        assert obsolete not in active_text


def test_nas_active_services_restart_after_container_manager_restart():
    for service in {
        "proxy-tunnel",
        "proxy-tunnel-primary",
        "proxy-tunnel-secondary",
        "web",
        "research-control",
        "discovery-loop",
        "rtds-discovery",
        "wallet-screen-planner",
        "wallet-screen-worker-0",
        "wallet-screen-worker-1",
        "wallet-screen-worker-2",
        "wallet-history-worker-0",
        "wallet-history-worker-1",
        "wallet-history-worker-2",
        "l6-validation-worker",
        "maintenance-loop",
    }:
        assert "restart: always" in _service_block(service)


def test_nas_control_loop_plans_history_relative_selection_and_l6_validation():
    loop = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")

    assert "wallet-level-select" in loop
    assert "wallet-history-plan" in loop
    assert "wallet-l6-plan" in loop
    assert "--name loop_wallet_history_planner" in loop
    assert "--name loop_wallet_level_control" in loop
    assert "wallet-pipeline-plan" not in loop
    assert "pipeline-cycle" not in loop
    assert "materialize-features" not in loop
    assert "copyability" not in loop.lower()
    assert "score-database" not in loop
    assert "paper" not in loop.lower()


def test_nas_rtds_loop_is_discovery_only():
    loop = Path("deploy/nas/rtds-discovery-loop.sh").read_text(encoding="utf-8")

    assert "discover-rtds" in loop
    assert "--min-trade-usdc" in loop
    for obsolete in ("validation", "watch-min-score", "copyability", "paper"):
        assert obsolete not in loop.lower()


def test_nas_history_workers_are_sharded_direct_to_parquet():
    loop = Path("deploy/nas/wallet-history-loop.sh").read_text(encoding="utf-8")

    for shard in range(3):
        service = _service_block(f"wallet-history-worker-{shard}")
        assert f'PM_ROBOT_WALLET_HISTORY_SHARD_INDEX: "{shard}"' in service
        assert "/app/deploy/nas/wallet-history-loop.sh" in service
        assert "proxy-tunnel:" in service
        assert "condition: service_healthy" in service
    assert "wallet-history-worker" in loop
    assert 'WORKER_LIMIT="${PM_ROBOT_WALLET_HISTORY_WORKER_LIMIT:-1}"' in loop
    assert 'LEASE_SECONDS="${PM_ROBOT_WALLET_HISTORY_LEASE_SECONDS:-1800}"' in loop
    assert 'ARCHIVE_DIR="${PM_ROBOT_ARCHIVE_DIR:-/app/data/parquet}"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_history_worker_${SHARD_INDEX}"' in loop


def test_nas_l6_worker_is_single_low_volume_network_worker():
    loop = Path("deploy/nas/l6-validation-loop.sh").read_text(encoding="utf-8")
    service = _service_block("l6-validation-worker")

    assert "/app/deploy/nas/l6-validation-loop.sh" in service
    assert "proxy-tunnel:" in service
    assert "condition: service_healthy" in service
    assert 'SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"' in loop
    assert 'WORKER_LIMIT="${PM_ROBOT_WALLET_L6_WORKER_LIMIT:-1}"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_l6_validation_worker"' in loop
    assert "wallet-l6-worker" in loop


def test_nas_screen_loop_records_unique_planner_and_worker_heartbeats():
    loop = Path("deploy/nas/wallet-screen-loop.sh").read_text(encoding="utf-8")

    assert 'HEARTBEAT_NAME="loop_wallet_screen_planner"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_screen_worker_${SHARD_INDEX}"' in loop
    assert '--name "$HEARTBEAT_NAME"' in loop


def test_nas_env_documents_bounded_new_queue_defaults():
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")

    assert "PM_ROBOT_WALLET_SCREEN_MAX_ACTIVE_JOBS=72" in env
    assert "PM_ROBOT_WALLET_HISTORY_PLANNER_LIMIT=12" in env
    assert "PM_ROBOT_WALLET_HISTORY_MAX_ACTIVE_JOBS=36" in env
    assert "PM_ROBOT_WALLET_HISTORY_WORKER_LIMIT=1" in env
    assert "PM_ROBOT_WALLET_L6_MAX_ACTIVE_JOBS=10" in env
    assert "PM_ROBOT_WALLET_L6_WORKER_LIMIT=1" in env
    assert "PM_ROBOT_WALLET_L6_REFRESH_SECONDS=1209600" in env
    assert "PM_ROBOT_WALLET_LEVEL_MIN_COHORT_SIZE=20" in env
    assert "PM_ROBOT_WALLET_LEVEL_TIMEOUT_MIN_COHORT_SIZE=5" in env
    assert "PM_ROBOT_WALLET_LEVEL_MAX_WAIT_SECONDS=3600" in env
    assert "PM_ROBOT_REQUIRED_RUNTIME_HEARTBEATS=" in env
    assert "loop_wallet_screen_planner" in env
    assert "loop_wallet_screen_worker_0" in env
    assert "loop_wallet_level_control" in env
    assert "loop_wallet_history_worker_2" in env
    assert "loop_wallet_l6_validation_worker" in env
    assert "PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_SECONDS=900" in env
    assert (
        "PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_OVERRIDES="
        "loop_discovery_leaderboard:7200,loop_discovery_activity:7200"
    ) in env


def test_nas_helper_manages_only_discovery_funnel_services():
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    assert 'HISTORY_SERVICES="wallet-history-worker-0 wallet-history-worker-1 wallet-history-worker-2"' in helper
    assert 'SCREEN_SERVICES="wallet-screen-planner wallet-screen-worker-0 wallet-screen-worker-1 wallet-screen-worker-2"' in helper
    assert 'L6_SERVICES="l6-validation-worker"' in helper
    assert 'PROXY_SERVICES="proxy-tunnel-primary proxy-tunnel-secondary proxy-tunnel"' in helper
    assert "--remove-orphans" in helper
    assert "--no-build" in helper
    assert "validate_proxy_config" in helper
    assert "PM_ROBOT_PROXY_PRIMARY_VPS_HOST is required" in helper
    assert "PM_ROBOT_PROXY_SECONDARY_VPS_HOST is required" in helper
    assert "VPS tunnel key is missing or unreadable" in helper
    assert "VPS known_hosts is missing or unreadable" in helper
    assert '[[ "$key_path" == /ssh/* ]]' in helper
    assert '[[ "$known_hosts_path" == /ssh/* ]]' in helper
    for obsolete in (
        "copyability",
        "validation-observer",
        "paper-run",
        "paper-settle",
        "publish-leaders",
        "pipeline-worker-",
    ):
        assert obsolete not in helper.lower()


def test_nas_proxy_failover_has_two_checked_tunnels_and_gates_network_workers():
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    haproxy = Path("deploy/nas/haproxy-proxy.cfg").read_text(encoding="utf-8")
    tunnel_script = Path("deploy/nas/vps-http-proxy-tunnel.sh").read_text(encoding="utf-8")

    primary = _service_block("proxy-tunnel-primary")
    secondary = _service_block("proxy-tunnel-secondary")
    proxy = _service_block("proxy-tunnel")
    assert "build:" in primary
    assert "build:" not in secondary
    for tunnel in (primary, secondary):
        assert "image: pm-robot:ssh-tunnel" in tunnel
        assert "network_mode: host" in tunnel
        assert "healthcheck:" in tunnel
        assert "CONNECT {host}:{port} HTTP/1.1" in tunnel
        assert "PM_ROBOT_PROXY_HEALTHCHECK_TARGET" in tunnel
        assert "PM_ROBOT_PROXY_LOCAL_HOST: 127.0.0.1" in tunnel
        assert "PM_ROBOT_VPS_KNOWN_HOSTS_PATH: /ssh/known_hosts" in tunnel
    assert "PM_ROBOT_PROXY_PRIMARY_VPS_HOST" in primary
    assert "PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT" in primary
    assert "PM_ROBOT_PROXY_SECONDARY_VPS_HOST" in secondary
    assert "PM_ROBOT_PROXY_SECONDARY_TUNNEL_PORT" in secondary
    assert "image: haproxy:3.0-alpine" in proxy
    assert "./app/deploy/nas/haproxy-proxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" in proxy
    assert "proxy-tunnel-primary:" in proxy
    assert "proxy-tunnel-secondary:" in proxy
    assert proxy.count("condition: service_started") == 2
    assert "tcp-check send-binary" in haproxy
    assert "server primary" in haproxy
    assert "server secondary" in haproxy
    assert "backup" in haproxy
    assert "nbsrv(proxy_backends)" in haproxy
    assert "default-server inter 5s fall 2 rise 2" in haproxy
    assert "HostKeyAlgorithms=ssh-ed25519" in tunnel_script
    assert "StrictHostKeyChecking=yes" in tunnel_script
    assert 'UserKnownHostsFile="$KNOWN_HOSTS_PATH"' in tunnel_script
    for variable in (
        "PM_ROBOT_PROXY_PRIMARY_VPS_USER=",
        "PM_ROBOT_PROXY_PRIMARY_VPS_HOST=",
        "PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT=18083",
        "PM_ROBOT_PROXY_SECONDARY_VPS_USER=",
        "PM_ROBOT_PROXY_SECONDARY_VPS_HOST=",
        "PM_ROBOT_PROXY_SECONDARY_TUNNEL_PORT=18084",
    ):
        assert variable in env
    assert "PM_ROBOT_VPS_HOST=" not in env
    for service_name in (
        "discovery-loop",
        "rtds-discovery",
        "wallet-screen-worker-0",
        "wallet-screen-worker-1",
        "wallet-screen-worker-2",
        "wallet-history-worker-0",
        "wallet-history-worker-1",
        "wallet-history-worker-2",
        "l6-validation-worker",
    ):
        block = _service_block(service_name)
        assert "proxy-tunnel:" in block
        assert "condition: service_healthy" in block
    assert "deploy/nas/vps-http-connect-proxy.py" not in compose


def test_nas_storage_mounts_are_host_agnostic_and_persistent():
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    for container_path in ("data", "logs", "backups", "reports"):
        assert (
            f"${{PM_ROBOT_STORAGE_ROOT:-/volume1/poly_data/pmbot}}/"
            f"{container_path}:/app/{container_path}"
        ) in compose
    assert "PM_ROBOT_STORAGE_ROOT=/volume1/poly_data/pmbot" in env
    assert 'storage_setting="${PM_ROBOT_STORAGE_ROOT:-' in helper
    assert 'storage_setting="/volume1/poly_data/pmbot"' in helper
    for forbidden in ("/Users/", "192.168.", "172.16."):
        assert forbidden not in compose
        assert forbidden not in env
        assert forbidden not in helper


def test_nas_maintenance_audits_before_parquet_gc_without_legacy_retention_cycle():
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/maintenance-loop.sh").read_text(encoding="utf-8")

    assert "PM_ROBOT_ARCHIVE_DIR=/app/data/parquet" in env
    assert "PM_ROBOT_WALLET_HISTORY_GC_ENABLED=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_GC_MIN_AGE_SECONDS=2592000" in env
    assert "PM_ROBOT_WALLET_HISTORY_GC_KEEP_PER_WALLET=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_ENABLED=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_VERIFY_CHECKSUMS=0" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS=604800" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_DELETE_ORPHANS=1" in env
    assert "wallet-history-audit" in loop
    assert "wallet-history-gc" in loop
    assert loop.index("wallet-history-audit") < loop.index("wallet-history-gc")
    assert "--execute" in loop
    assert "PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS=30" in env
    assert "--heartbeat-days" in loop
    assert "--pipeline-job-days" not in loop
    assert "retention-cycle" not in loop
    assert "PM_ROBOT_RETENTION_" not in env


def test_nas_full_database_backup_is_explicit_cli_only():
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")

    assert "  backup-loop:\n" not in compose
    assert "manual-backup" not in compose
    assert "PM_ROBOT_SCHEDULED_BACKUP_" not in env
    assert "PM_ROBOT_BACKUP_INTERVAL" not in env
    assert "PM_ROBOT_BACKUP_START_DELAY" not in env
    assert "  task:\n" in compose


def test_nas_shell_entrypoints_parse():
    checks = (
        ("bash", "deploy/nas/pmrobot-nas.sh"),
        ("sh", "deploy/nas/research-control-loop.sh"),
        ("sh", "deploy/nas/wallet-screen-loop.sh"),
        ("sh", "deploy/nas/wallet-history-loop.sh"),
        ("sh", "deploy/nas/l6-validation-loop.sh"),
        ("sh", "deploy/nas/discovery-loop.sh"),
        ("sh", "deploy/nas/rtds-discovery-loop.sh"),
        ("sh", "deploy/nas/maintenance-loop.sh"),
    )
    for shell, script in checks:
        result = subprocess.run(
            [shell, "-n", script],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"{script}: {result.stderr}"


def _fake_runtime(tmp_path: Path) -> tuple[Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    call_log = tmp_path / "calls.jsonl"
    fake_python = fake_bin / "python"
    fake_python.write_text(
        f"""#!{sys.executable}
import json
import os
import pathlib
import sys

args = sys.argv[1:]
if args and args[0] == "-c":
    sys.argv = ["-c", *args[2:]]
    exec(args[1], {{"__name__": "__main__"}})
    raise SystemExit(0)
with pathlib.Path(os.environ["CALL_LOG"]).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args) + "\\n")
if "wallet-level-select" in args:
    print(json.dumps({{
        "cohorts_processed": 1,
        "decisions_written": 2,
        "promoted_l3": 1,
        "promoted_l4": 0,
        "promoted_l5": 0,
        "status": "ok",
    }}))
elif "wallet-history-plan" in args:
    print(json.dumps({{
        "targets_seen": 2,
        "jobs_enqueued": 2,
        "active_jobs": 0,
        "max_active_jobs": 36,
        "throttled": False,
        "status": "ok",
    }}))
elif "wallet-l6-plan" in args:
    print(json.dumps({{
        "targets_seen": 1,
        "jobs_enqueued": 1,
        "active_jobs": 0,
        "max_active_jobs": 10,
        "status": "ok",
    }}))
elif "wallet-history-worker" in args:
    print(json.dumps({{
        "jobs_attempted": 1,
        "jobs_succeeded": 1,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "light_completed": 1,
        "deep_completed": 0,
        "rows_archived": 75,
        "status": "ok",
        "error": "",
    }}))
elif "wallet-l6-worker" in args:
    print(json.dumps({{
        "jobs_attempted": 1,
        "jobs_succeeded": 1,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "validations_passed": 1,
        "validations_warned": 0,
        "validations_failed": 0,
        "promoted_l6": 1,
        "status": "ok",
        "error": "",
    }}))
elif "maintenance" in args:
    print(json.dumps({{"ok": True, "status": "ok"}}))
elif "wallet-history-audit" in args:
    print(json.dumps({{"status": "ok", "orphan_files_deleted": 0}}))
elif "wallet-history-gc" in args:
    print(json.dumps({{"status": "ok", "files_deleted": 0}}))
elif "runtime-heartbeat" in args:
    pass
else:
    raise SystemExit("unexpected command")
""",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    fake_date = fake_bin / "date"
    fake_date.write_text("#!/bin/sh\nprintf '%s\\n' '2026-07-15T00:00:00Z'\n", encoding="utf-8")
    fake_date.chmod(0o755)
    fake_hostname = fake_bin / "hostname"
    fake_hostname.write_text("#!/bin/sh\nprintf '%s\\n' 'test-nas'\n", encoding="utf-8")
    fake_hostname.chmod(0o755)
    return fake_bin, call_log


def test_nas_control_loop_runs_selection_history_and_l6_plan_once(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE": "1",
    }

    result = subprocess.run(
        ["sh", "deploy/nas/research-control-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    commands = [args[args.index("--env") + 2] for args in calls if "--env" in args]
    assert commands[:3] == ["wallet-level-select", "wallet-history-plan", "wallet-l6-plan"]
    control_loop = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")
    assert "PM_ROBOT_WALLET_LEVEL_POLICY_VERSION" not in control_loop
    assert "--policy-version" not in control_loop
    assert "status=ok, work=4" in result.stdout


def test_nas_history_loop_runs_only_its_assigned_shard_once(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    archive_dir = tmp_path / "parquet"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_HISTORY_RUN_ONCE": "1",
        "PM_ROBOT_WALLET_HISTORY_SHARD_INDEX": "2",
        "PM_ROBOT_ARCHIVE_DIR": str(archive_dir),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/wallet-history-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    worker_call = next(args for args in calls if "wallet-history-worker" in args)
    assert worker_call[worker_call.index("--shard-index") + 1] == "2"
    assert worker_call[worker_call.index("--archive-dir") + 1] == str(archive_dir)
    assert "status=ok, jobs=1" in result.stdout


def test_nas_l6_loop_runs_one_bounded_worker_once(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    archive_dir = tmp_path / "parquet"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_L6_RUN_ONCE": "1",
        "PM_ROBOT_ARCHIVE_DIR": str(archive_dir),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/l6-validation-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    worker_call = next(args for args in calls if "wallet-l6-worker" in args)
    assert worker_call[worker_call.index("--shard-count") + 1] == "1"
    assert worker_call[worker_call.index("--limit") + 1] == "1"
    assert worker_call[worker_call.index("--archive-dir") + 1] == str(archive_dir)
    assert "status=ok, jobs=1" in result.stdout


def test_nas_maintenance_runs_artifact_audit_before_gc(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/maintenance-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    commands = [args[args.index("--env") + 2] for args in calls if "--env" in args]
    assert commands[:3] == ["maintenance", "wallet-history-audit", "wallet-history-gc"]
    maintenance_call = next(args for args in calls if "maintenance" in args)
    assert "--heartbeat-days" in maintenance_call
    assert "--pipeline-job-days" not in maintenance_call
    audit_call = next(args for args in calls if "wallet-history-audit" in args)
    assert "--delete-orphans" in audit_call
    assert "--verify-checksums" not in audit_call
    assert report_path.is_file()
    assert "maintenance loop: ok" in result.stdout
