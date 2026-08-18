import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from pm_robot.ops import ACTIVE_RESEARCH_RUNTIME_EVENTS
from pm_robot.research.current_elite import current_high_confidence_l6_manifest_checksum

COMPOSE_PATH = Path("deploy/nas/docker-compose.yml")


def _with_hcl6_checksum(manifest: dict) -> dict:
    manifest = dict(manifest)
    manifest["manifest_checksum"] = current_high_confidence_l6_manifest_checksum(manifest)
    return manifest


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
    assert 'BATCH_SIZE="${PM_ROBOT_RTDS_BATCH_SIZE:-1000}"' in loop
    assert 'FLUSH_INTERVAL="${PM_ROBOT_RTDS_FLUSH_INTERVAL:-60}"' in loop
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
    assert (
        'START_STAGGER_SECONDS="${PM_ROBOT_WALLET_HISTORY_START_STAGGER_SECONDS:-7}"'
        in loop
    )
    assert "start_delay=$((SHARD_INDEX * START_STAGGER_SECONDS))" in loop
    assert 'ARCHIVE_DIR="${PM_ROBOT_ARCHIVE_DIR:-/app/data/parquet}"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_history_worker_${SHARD_INDEX}"' in loop


def test_nas_l6_worker_is_single_low_volume_network_worker():
    loop = Path("deploy/nas/l6-validation-loop.sh").read_text(encoding="utf-8")
    handoff = Path("deploy/nas/l6-handoff-push.sh").read_text(encoding="utf-8")
    service = _service_block("l6-validation-worker")

    assert "/app/deploy/nas/l6-validation-loop.sh" in service
    assert "proxy-tunnel:" in service
    assert "condition: service_healthy" in service
    assert 'SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"' in loop
    assert 'WORKER_LIMIT="${PM_ROBOT_WALLET_L6_WORKER_LIMIT:-1}"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_l6_validation_worker"' in loop
    assert "wallet-l6-worker" in loop
    assert loop.index('runtime_heartbeat "$command_status"') < loop.index(
        'if export_output="$(python -m pm_robot.cli'
    )
    assert 'EXPORT_PATH="${PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH:-/app/data/exports/current_high_confidence_l6.json}"' in loop
    assert "export-high-confidence-l6" in loop
    assert '--out "$EXPORT_PATH"' in loop
    assert "/app/deploy/nas/l6-handoff-push.sh" in loop
    assert "./ssh:/app/ssh:ro" in service
    assert 'PM_ROBOT_L6_HANDOFF_ENABLED:-0' in handoff
    assert "BatchMode=yes" in handoff
    assert "HostKeyAlgorithms=ssh-ed25519" in handoff
    assert "StrictHostKeyChecking=yes" in handoff
    assert "canonical_manifest_json" in handoff
    assert "manifest_checksum(manifest)" in handoff
    assert "L6 handoff manifest_checksum mismatch" in handoff
    assert 'if handoff_status == "ready" and (' in handoff
    assert 'L6 handoff must remain research-only' in handoff
    assert 'PM_ROBOT_L6_HANDOFF_REFRESH_SECONDS:-21600' in handoff
    assert 'pmrobot-l6-upload < "$MANIFEST_PATH"' in handoff


def test_nas_image_installs_dependencies_before_copying_churned_source():
    dockerfile = Path("deploy/nas/Dockerfile").read_text(encoding="utf-8")

    assert dockerfile.index("RUN apt-get update") < dockerfile.index("COPY src /app/src")


def test_nas_l6_handoff_pushes_changed_manifest_once(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_ssh = fake_bin / "ssh"
    fake_ssh.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$SSH_ARGS\"\n"
        "cat > \"$SSH_RECEIVED\"\n",
        encoding="utf-8",
    )
    fake_ssh.chmod(0o755)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            _with_hcl6_checksum(
                {
                    "schema_version": 3,
                    "source": "pm_robot.current_high_confidence_l6",
                    "handoff_status": "ready",
                    "replace_active_set_allowed": True,
                    "automatic_trading_activation": False,
                    "research_only": True,
                    "not_for_trading": True,
                    "generated_at": 100,
                    "source_version": "hcl6:v3:100:first",
                    "manifest_checksum": "first",
                    "candidates": [
                        {
                            "wallet": "0x1111111111111111111111111111111111111111",
                            "research_score": 90,
                            "evidence_updated_at": 90,
                            "validated_at": 95,
                        }
                    ],
                }
            )
        )
        + "\n",
        encoding="utf-8",
    )
    first_manifest_bytes = manifest.read_bytes()
    key = tmp_path / "id_ed25519"
    key.write_text("test-key\n", encoding="utf-8")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 test\n", encoding="utf-8")
    state = tmp_path / "state" / "handoff.sha256"
    received = tmp_path / "received.json"
    args_log = tmp_path / "ssh.args"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "SSH_ARGS": str(args_log),
        "SSH_RECEIVED": str(received),
        "PM_ROBOT_L6_HANDOFF_ENABLED": "1",
        "PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH": str(manifest),
        "PM_ROBOT_L6_HANDOFF_STATE_PATH": str(state),
        "PM_ROBOT_L6_HANDOFF_HOST": "203.0.113.10",
        "PM_ROBOT_L6_HANDOFF_IDENTITY_FILE": str(key),
        "PM_ROBOT_L6_HANDOFF_KNOWN_HOSTS_FILE": str(known_hosts),
    }

    first = subprocess.run(
        ["sh", "deploy/nas/l6-handoff-push.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    updated = json.loads(manifest.read_text(encoding="utf-8"))
    updated["generated_at"] = 200
    updated["source_version"] = "hcl6:v3:200:second"
    updated["manifest_checksum"] = current_high_confidence_l6_manifest_checksum(updated)
    manifest.write_text(json.dumps(updated) + "\n", encoding="utf-8")
    second = subprocess.run(
        ["sh", "deploy/nas/l6-handoff-push.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    os.utime(state, (0, 0))
    refreshed = subprocess.run(
        ["sh", "deploy/nas/l6-handoff-push.sh"],
        env={**env, "PM_ROBOT_L6_HANDOFF_REFRESH_SECONDS": "1"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert refreshed.returncode == 0, refreshed.stderr
    assert received.read_bytes() == manifest.read_bytes()
    assert first_manifest_bytes != received.read_bytes()
    assert len(args_log.read_text(encoding="utf-8").splitlines()) == 3
    assert "StrictHostKeyChecking=yes" in args_log.read_text(encoding="utf-8")
    assert len(state.read_text(encoding="utf-8").strip()) == 64


def test_nas_l6_handoff_rejects_manifest_checksum_mismatch_before_ssh(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_ssh = fake_bin / "ssh"
    fake_ssh.write_text(
        "#!/bin/sh\n"
        "echo ssh-should-not-run >&2\n"
        "exit 99\n",
        encoding="utf-8",
    )
    fake_ssh.chmod(0o755)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "source": "pm_robot.current_high_confidence_l6",
                "handoff_status": "degraded",
                "replace_active_set_allowed": False,
                "automatic_trading_activation": False,
                "research_only": True,
                "not_for_trading": True,
                "candidates": [],
                "manifest_checksum": "0" * 64,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    key = tmp_path / "id_ed25519"
    key.write_text("test-key\n", encoding="utf-8")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 test\n", encoding="utf-8")

    result = subprocess.run(
        ["sh", "deploy/nas/l6-handoff-push.sh"],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
            "PM_ROBOT_L6_HANDOFF_ENABLED": "1",
            "PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH": str(manifest),
            "PM_ROBOT_L6_HANDOFF_STATE_PATH": str(tmp_path / "state" / "handoff.sha256"),
            "PM_ROBOT_L6_HANDOFF_HOST": "203.0.113.10",
            "PM_ROBOT_L6_HANDOFF_IDENTITY_FILE": str(key),
            "PM_ROBOT_L6_HANDOFF_KNOWN_HOSTS_FILE": str(known_hosts),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "L6 handoff manifest_checksum mismatch" in result.stderr
    assert "ssh-should-not-run" not in result.stderr


def test_nas_l6_handoff_pushes_degraded_manifest_to_revoke_execution_readiness(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_ssh = fake_bin / "ssh"
    fake_ssh.write_text(
        "#!/bin/sh\n"
        "cat > \"$SSH_RECEIVED\"\n",
        encoding="utf-8",
    )
    fake_ssh.chmod(0o755)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            _with_hcl6_checksum(
                {
                "schema_version": 3,
                "source": "pm_robot.current_high_confidence_l6",
                "handoff_status": "degraded",
                "replace_active_set_allowed": False,
                "automatic_trading_activation": False,
                "research_only": True,
                "not_for_trading": True,
                "candidates": [],
                }
            )
        )
        + "\n",
        encoding="utf-8",
    )
    key = tmp_path / "id_ed25519"
    key.write_text("test-key\n", encoding="utf-8")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 test\n", encoding="utf-8")
    state = tmp_path / "state" / "handoff.sha256"
    received = tmp_path / "received.json"

    result = subprocess.run(
        ["sh", "deploy/nas/l6-handoff-push.sh"],
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
            "SSH_RECEIVED": str(received),
            "PM_ROBOT_L6_HANDOFF_ENABLED": "1",
            "PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH": str(manifest),
            "PM_ROBOT_L6_HANDOFF_STATE_PATH": str(state),
            "PM_ROBOT_L6_HANDOFF_HOST": "203.0.113.10",
            "PM_ROBOT_L6_HANDOFF_IDENTITY_FILE": str(key),
            "PM_ROBOT_L6_HANDOFF_KNOWN_HOSTS_FILE": str(known_hosts),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert received.read_bytes() == manifest.read_bytes()
    assert len(state.read_text(encoding="utf-8").strip()) == 64


def test_nas_screen_loop_records_unique_planner_and_worker_heartbeats():
    loop = Path("deploy/nas/wallet-screen-loop.sh").read_text(encoding="utf-8")

    assert 'HEARTBEAT_NAME="loop_wallet_screen_planner"' in loop
    assert 'HEARTBEAT_NAME="loop_wallet_screen_worker_${SHARD_INDEX}"' in loop
    assert '--name "$HEARTBEAT_NAME"' in loop
    assert "run_control_locked" in loop
    assert '"$0" __planner_once' in loop
    assert "wallet screen worker" in loop


def test_nas_env_documents_bounded_new_queue_defaults():
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")

    assert "PM_ROBOT_WALLET_SCREEN_MAX_ACTIVE_JOBS=72" in env
    assert "PM_ROBOT_CONTROL_PLANE_LOCK_PATH=/app/data/pm_robot.control_plane.lock" in env
    assert "PM_ROBOT_CONTROL_PLANE_LOCK_BUSY_INTERVAL=120" in env
    assert "PM_ROBOT_WALLET_HISTORY_PLANNER_LIMIT=12" in env
    assert "PM_ROBOT_WALLET_HISTORY_MAX_ACTIVE_JOBS=36" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_MAX_INTERVAL=300" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_BACKOFF_STEP=30" in env
    assert "PM_ROBOT_WALLET_SCREEN_ACTIVE_MAX_INTERVAL=300" in env
    assert "PM_ROBOT_WALLET_SCREEN_ACTIVE_BACKOFF_STEP=30" in env
    assert "PM_ROBOT_WALLET_HISTORY_WORKER_LIMIT=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_START_STAGGER_SECONDS=7" in env
    assert "PM_ROBOT_WALLET_L6_MAX_ACTIVE_JOBS=10" in env
    assert "PM_ROBOT_WALLET_L6_WORKER_LIMIT=1" in env
    assert "PM_ROBOT_WALLET_L6_REFRESH_SECONDS=1209600" in env
    assert "PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH=/app/data/exports/current_high_confidence_l6.json" in env
    assert "PM_ROBOT_L6_HANDOFF_ENABLED=0" in env
    assert "PM_ROBOT_L6_HANDOFF_HOST=203.0.113.10" in env
    assert "PM_ROBOT_L6_HANDOFF_IDENTITY_FILE=/app/ssh/polyhermes-l6-handoff-ed25519" in env
    assert "PM_ROBOT_WALLET_LEVEL_MIN_COHORT_SIZE=20" in env
    assert "PM_ROBOT_WALLET_LEVEL_TIMEOUT_MIN_COHORT_SIZE=5" in env
    assert "PM_ROBOT_WALLET_LEVEL_MAX_WAIT_SECONDS=3600" in env
    assert "PM_ROBOT_RTDS_BATCH_SIZE=1000" in env
    assert "PM_ROBOT_RTDS_FLUSH_INTERVAL=60" in env
    assert "PM_ROBOT_RTDS_L0_BUFFER_TTL_SECONDS=86400" in env
    assert "PM_ROBOT_RTDS_L0_BUFFER_MAX_WALLETS=50000" in env
    assert "PM_ROBOT_RTDS_PERSIST_COOLDOWN_SECONDS=300" in env
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
    assert "PM_ROBOT_PROXY_PRIMARY_ENABLED" in helper
    assert "At least one VPS proxy tunnel must be enabled" in helper
    assert "PM_ROBOT_PROXY_SECONDARY_VPS_HOST is required" in helper
    assert "VPS tunnel key is missing or unreadable" in helper
    assert "VPS known_hosts is missing or unreadable" in helper
    assert '[[ "$key_path" == /ssh/* ]]' in helper
    assert '[[ "$known_hosts_path" == /ssh/* ]]' in helper
    assert "export-high-confidence-l6" in helper
    assert "/app/data/exports/current_high_confidence_l6.json" in helper
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
    assert "PM_ROBOT_PROXY_TUNNEL_ENABLED: ${PM_ROBOT_PROXY_PRIMARY_ENABLED:-0}" in primary
    assert "PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT" in primary
    assert "PM_ROBOT_PROXY_SECONDARY_VPS_HOST" in secondary
    assert "PM_ROBOT_PROXY_TUNNEL_ENABLED: ${PM_ROBOT_PROXY_SECONDARY_ENABLED:-1}" in secondary
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
    assert "VPS HTTP proxy tunnel is intentionally disabled" in tunnel_script
    for variable in (
        "PM_ROBOT_PROXY_PRIMARY_VPS_USER=",
        "PM_ROBOT_PROXY_PRIMARY_ENABLED=0",
        "PM_ROBOT_PROXY_PRIMARY_VPS_HOST=",
        "PM_ROBOT_PROXY_PRIMARY_TUNNEL_PORT=18083",
        "PM_ROBOT_PROXY_SECONDARY_VPS_USER=",
        "PM_ROBOT_PROXY_SECONDARY_ENABLED=1",
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
    assert "PM_ROBOT_WALLET_HISTORY_GC_ENABLED=0" in env
    assert "PM_ROBOT_WALLET_HISTORY_GC_MIN_AGE_SECONDS=2592000" in env
    assert "PM_ROBOT_WALLET_HISTORY_GC_KEEP_PER_WALLET=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_ENABLED=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_VERIFY_CHECKSUMS=0" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS=604800" in env
    assert "PM_ROBOT_WALLET_HISTORY_AUDIT_DELETE_ORPHANS=0" in env
    assert "PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS=2-5" in env
    assert "PM_ROBOT_WALLET_HISTORY_HEAVY_INTERVAL_SECONDS=86400" in env
    assert "PM_ROBOT_WALLET_HISTORY_HEAVY_ON_BACKLOG_ZERO=1" in env
    assert "PM_ROBOT_WALLET_HISTORY_HEAVY_RUN_NOW=0" in env
    assert "PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS=21600" in env
    assert "wallet-history-audit" in loop
    assert "wallet-history-gc" in loop
    assert loop.index("wallet-history-audit") < loop.index("wallet-history-gc")
    assert "should_run_heavy_history" in loop
    assert "backlog_is_zero" in loop
    assert 'LIGHT_INTERVAL_SECONDS="${PM_ROBOT_MAINTENANCE_LIGHT_INTERVAL_SECONDS:-21600}"' in loop
    assert "maintenance_queue_state" in loop
    assert "maintenance-preflight" in loop
    assert " pm_robot.cli --env /app/.env status" not in loop
    assert "wallet_history_screen_active" in loop
    assert "--skip-cleanup" in loop
    assert "--reset-stale-heartbeats" in loop
    assert "--execute" in loop
    assert "PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS=30" in env
    assert "PM_ROBOT_MAINTENANCE_LIGHT_CLEANUP_ENABLED=0" in env
    assert "PM_ROBOT_MAINTENANCE_L0_RETENTION_DAYS=7" in env
    assert "PM_ROBOT_MAINTENANCE_L0_CLEANUP_BATCH_LIMIT=20000" in env
    assert "--heartbeat-days" in loop
    assert "--l0-retention-days" in loop
    assert "--l0-cleanup-batch-limit" in loop
    assert "--pipeline-job-days" not in loop
    assert "--wal-checkpoint \"$WAL_CHECKPOINT\"" in loop
    assert "wal-truncate" not in loop.lower()
    assert "vacuum" not in loop.lower()
    assert "truncate" not in loop.lower()
    assert "retention-cycle" not in loop
    assert "PM_ROBOT_RETENTION_" not in env
    assert "TRUNCATE" not in env
    assert "VACUUM" not in env


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
known_commands = (
    "wallet-level-select",
    "wallet-history-plan",
    "wallet-l6-plan",
    "wallet-screen-plan",
    "wallet-screen-worker",
    "wallet-history-worker",
    "wallet-l6-reconcile",
    "wallet-l6-worker",
    "export-high-confidence-l6",
    "wallet-history-audit",
    "wallet-history-gc",
    "runtime-heartbeat",
    "maintenance-preflight",
    "maintenance",
)
command = next((name for name in known_commands if name in args), "")
if command and os.environ.get("FAKE_FAIL_COMMAND") == command:
    raise SystemExit(42)
if command and os.environ.get("FAKE_INVALID_COMMAND") == command:
    print(json.dumps({{"status": "bad", "jobs_enqueued": 0}}))
    raise SystemExit(0)
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
        "jobs_enqueued": int(os.environ.get("FAKE_HISTORY_JOBS", "2")),
        "active_jobs": 0,
        "max_active_jobs": 36,
        "throttled": False,
        "status": os.environ.get("FAKE_HISTORY_STATUS", "ok"),
    }}))
elif "wallet-l6-plan" in args:
    print(json.dumps({{
        "targets_seen": 1,
        "jobs_enqueued": 1,
        "active_jobs": 0,
        "max_active_jobs": 10,
        "status": "ok",
    }}))
elif "wallet-screen-plan" in args:
    print(json.dumps({{
        "jobs_enqueued": 2,
        "active_jobs": 0,
        "throttled": False,
        "status": "ok",
    }}))
elif "wallet-screen-worker" in args:
    print(json.dumps({{
        "jobs_attempted": 1,
        "jobs_succeeded": 1,
        "jobs_failed": 0,
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
elif "wallet-l6-reconcile" in args:
    print(json.dumps({{
        "historical_l6": 4,
        "current_valid_l6": 4,
        "retained_l6": 4,
        "reclassified_l5": 0,
        "reclassified_l2": 0,
        "dry_run": False,
        "status": "ok",
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
elif "export-high-confidence-l6" in args:
    print(json.dumps({{
        "output": args[args.index("--out") + 1],
        "candidate_count": 1,
        "automatic_trading_activation": False,
    }}))
elif "maintenance" in args:
    print(json.dumps({{
        "ok": True,
        "status": "ok",
        "pipeline_queue_health": {{
            "queued": int(os.environ.get("FAKE_MAINTENANCE_QUEUED", "1")),
            "running": 0,
            "wallet_history_screen_active": int(os.environ.get("FAKE_STATUS_HISTORY_SCREEN_ACTIVE", "0")),
        }},
    }}))
elif "maintenance-preflight" in args:
    print(json.dumps({{
        "ok": True,
        "wallet_history_screen_active": int(os.environ.get("FAKE_STATUS_HISTORY_SCREEN_ACTIVE", "0")),
        "defer_maintenance": int(os.environ.get("FAKE_STATUS_HISTORY_SCREEN_ACTIVE", "0")) > 0,
    }}))
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
    fake_date.write_text(
        "#!/bin/sh\n"
        "if [ \"${1:-}\" = \"+%s\" ]; then\n"
        "  printf '%s\\n' '1784073600'\n"
        "else\n"
        "  printf '%s\\n' '2026-07-15T00:00:00Z'\n"
        "fi\n",
        encoding="utf-8",
    )
    fake_date.chmod(0o755)
    fake_hostname = fake_bin / "hostname"
    fake_hostname.write_text("#!/bin/sh\nprintf '%s\\n' 'test-nas'\n", encoding="utf-8")
    fake_hostname.chmod(0o755)
    fake_flock = fake_bin / "flock"
    fake_flock.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_flock.chmod(0o755)
    return fake_bin, call_log


def test_nas_control_loop_does_not_starve_selection_and_l6_when_history_enqueues_work(
    tmp_path,
):
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
    assert commands[:3] == ["wallet-history-plan", "wallet-level-select", "wallet-l6-plan"]
    control_loop = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")
    assert "PM_ROBOT_WALLET_LEVEL_POLICY_VERSION" not in control_loop
    assert "--policy-version" not in control_loop
    assert "status=ok, work=4" in result.stdout


def test_nas_control_loop_runs_history_selection_and_l6_when_history_is_empty(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_HISTORY_JOBS": "0",
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
    assert commands[:3] == ["wallet-history-plan", "wallet-level-select", "wallet-l6-plan"]
    assert "status=ok, work=2" in result.stdout


def test_nas_control_loop_marks_invalid_history_partial_without_downstream(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_INVALID_COMMAND": "wallet-history-plan",
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
    assert commands[:2] == ["wallet-history-plan", "runtime-heartbeat"]
    assert "wallet-level-select" not in commands
    assert "wallet-l6-plan" not in commands
    assert "status=invalid, work=0" in result.stdout


def test_nas_control_loop_marks_history_failure_partial_without_downstream(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_FAIL_COMMAND": "wallet-history-plan",
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
    assert commands[:2] == ["wallet-history-plan", "runtime-heartbeat"]
    assert "wallet-level-select" not in commands
    assert "wallet-l6-plan" not in commands
    assert "wallet history planning failed" in result.stderr
    assert "status=partial, work=0" in result.stdout


def test_nas_control_loop_treats_history_warmup_as_active_non_error_without_downstream(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_HISTORY_JOBS": "0",
        "FAKE_HISTORY_STATUS": "warming_up",
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
    assert commands[:2] == ["wallet-history-plan", "runtime-heartbeat"]
    assert "wallet-level-select" not in commands
    assert "wallet-l6-plan" not in commands
    assert "wallet history planning failed" not in result.stderr
    assert "status=warming, work=0" in result.stdout
    assert "next cycle in 30s" in result.stdout


def test_nas_control_loop_marks_downstream_invalid_partial(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_HISTORY_JOBS": "0",
        "FAKE_INVALID_COMMAND": "wallet-level-select",
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
    assert commands[:3] == ["wallet-history-plan", "wallet-level-select", "wallet-l6-plan"]
    assert "status=invalid, work=0" in result.stdout


def test_nas_control_lock_fallback_never_reclaims_a_live_owner(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    lock_path = tmp_path / "control.lock"
    lock_dir = tmp_path / "control.lock.d"
    lock_dir.mkdir()
    owner_token = f"test-nas:{os.getpid()}:1"
    (lock_dir / "owner").write_text(
        f"token={owner_token}\npid={os.getpid()}\nstarted_at=1\nhost=test-nas\n",
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_HISTORY_JOBS": "0",
        "PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE": "1",
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(lock_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(lock_dir),
        "PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS": "0",
        "PM_ROBOT_RUN_LOCKED_SCRIPT": str(tmp_path / "disabled-run-locked"),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/research-control-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert lock_dir.is_dir()
    assert (lock_dir / "owner").read_text(encoding="utf-8").startswith(
        f"token={owner_token}\n"
    )
    assert "control-plane lock busy" in result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    assert not any("wallet-history-plan" in args for args in calls)
    assert not any("wallet-level-select" in args for args in calls)
    assert not any("wallet-l6-plan" in args for args in calls)
    assert all("runtime-heartbeat" in args for args in calls)


def test_nas_control_loop_reports_busy_control_lock_as_healthy_skip(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    busy_lock = tmp_path / "busy.lock.d"
    busy_lock.mkdir()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE": "1",
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(busy_lock),
        "PM_ROBOT_RUN_LOCKED_SCRIPT": str(tmp_path / "disabled-run-locked"),
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
    assert not any("wallet-history-plan" in args for args in calls)
    heartbeats = [args for args in calls if "runtime-heartbeat" in args]
    assert len(heartbeats) == 2
    for heartbeat in heartbeats:
        assert heartbeat[heartbeat.index("--status") + 1] == "ok"
        assert "control-plane lock is busy" in heartbeat[heartbeat.index("--error") + 1]
    assert "status=skipped, work=0" in result.stdout


def test_nas_control_lock_fallback_reclaims_only_stale_dead_owner(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    lock_path = tmp_path / "control.lock"
    lock_dir = tmp_path / "control.lock.d"
    lock_dir.mkdir()
    (lock_dir / "owner").write_text(
        "token=test-nas:99999999:1\npid=99999999\nstarted_at=1\nhost=test-nas\n",
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_HISTORY_JOBS": "0",
        "PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE": "1",
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(lock_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(lock_dir),
        "PM_ROBOT_CONTROL_PLANE_LOCK_STALE_SECONDS": "1",
        "PM_ROBOT_RUN_LOCKED_SCRIPT": str(tmp_path / "disabled-run-locked"),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/research-control-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not lock_dir.exists()
    assert "control-plane stale lock reclaimed" in result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    assert any("wallet-level-select" in args for args in calls)


def test_nas_screen_planner_uses_control_lock_without_blocking_workers(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_SCREEN_RUN_ONCE": "1",
        "PM_ROBOT_WALLET_SCREEN_MODE": "planner",
    }

    result = subprocess.run(
        ["sh", "deploy/nas/wallet-screen-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    commands = [args[args.index("--env") + 2] for args in calls if "--env" in args]
    assert commands[0] == "wallet-screen-plan"
    assert "status=ok, work=2" in result.stdout


def test_nas_screen_planner_reports_busy_control_lock_as_healthy_skip(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    busy_lock = tmp_path / "busy.lock.d"
    busy_lock.mkdir()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_SCREEN_RUN_ONCE": "1",
        "PM_ROBOT_WALLET_SCREEN_MODE": "planner",
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(busy_lock),
        "PM_ROBOT_RUN_LOCKED_SCRIPT": str(tmp_path / "disabled-run-locked"),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/wallet-screen-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    assert not any("wallet-screen-plan" in args for args in calls)
    heartbeat = next(args for args in calls if "runtime-heartbeat" in args)
    assert heartbeat[heartbeat.index("--status") + 1] == "ok"
    assert "control-plane lock is busy" in heartbeat[heartbeat.index("--error") + 1]
    assert "status=skipped, work=0" in result.stdout


def test_nas_screen_worker_runs_without_control_lock_when_lock_is_busy(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    busy_lock = tmp_path / "busy.lock.d"
    busy_lock.mkdir()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_SCREEN_RUN_ONCE": "1",
        "PM_ROBOT_WALLET_SCREEN_MODE": "worker",
        "PM_ROBOT_WALLET_SCREEN_SHARD_INDEX": "1",
        "PM_ROBOT_WALLET_SCREEN_ACTIVE_INTERVAL": "17",
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(busy_lock),
    }

    result = subprocess.run(
        ["sh", "deploy/nas/wallet-screen-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    worker_call = next(args for args in calls if "wallet-screen-worker" in args)
    assert worker_call[worker_call.index("--shard-index") + 1] == "1"
    assert "status=ok, work=1" in result.stdout
    assert "next poll in 17s" in result.stdout


def test_nas_screen_worker_does_not_apply_planner_backoff():
    loop = Path("deploy/nas/wallet-screen-loop.sh").read_text(encoding="utf-8")
    worker_branch = loop.split('echo "$(date -Iseconds) wallet screen worker', 1)[1]

    assert 'sleep_interval="$ACTIVE_INTERVAL"' in worker_branch
    assert 'active_sleep_interval "$active_streak"' not in worker_branch


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
    export_path = tmp_path / "current_high_confidence_l6.json"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_WALLET_L6_RUN_ONCE": "1",
        "PM_ROBOT_ARCHIVE_DIR": str(archive_dir),
        "PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH": str(export_path),
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
    export_call = next(args for args in calls if "export-high-confidence-l6" in args)
    assert export_call[export_call.index("--out") + 1] == str(export_path)
    assert "status=ok, jobs=1" in result.stdout


def test_nas_maintenance_light_cycle_skips_history_audit_and_gc_by_default(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(tmp_path / "control.lock"),
        "PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS": "",
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
    assert commands == ["maintenance-preflight", "maintenance", "runtime-heartbeat"]
    maintenance_call = next(args for args in calls if "maintenance" in args)
    assert "--skip-cleanup" in maintenance_call
    assert report_path.is_file()
    assert "maintenance light cleanup: skipped reason=disabled" in result.stdout
    assert "wallet history heavy maintenance: skipped reason=not_due" in result.stdout
    assert "maintenance loop: ok" in result.stdout


def test_nas_maintenance_publishes_liveness_before_start_delay(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o755)
    report_path = tmp_path / "maintenance.json"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "300",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(tmp_path / "control.lock"),
        "PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS": "",
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
    assert commands == [
        "runtime-heartbeat",
        "maintenance-preflight",
        "maintenance",
        "runtime-heartbeat",
    ]
    assert "maintenance loop: initial delay 300s" in result.stdout


def test_nas_maintenance_reports_busy_control_lock_as_healthy_skip(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    busy_lock = tmp_path / "busy.lock.d"
    busy_lock.mkdir()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(tmp_path / "maintenance.json"),
        "PM_ROBOT_CONTROL_PLANE_LOCK_DIR": str(busy_lock),
        "PM_ROBOT_RUN_LOCKED_SCRIPT": str(tmp_path / "disabled-run-locked"),
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
    assert not any("maintenance" in args and "maintenance-preflight" not in args for args in calls)
    heartbeat = next(args for args in calls if "runtime-heartbeat" in args)
    assert heartbeat[heartbeat.index("--status") + 1] == "ok"
    assert "control-plane lock busy" in heartbeat[heartbeat.index("--error") + 1]
    assert "control-plane busy; backing off" in result.stderr


def test_nas_maintenance_runs_light_cycle_when_history_or_screen_queue_is_active(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    lock_path = tmp_path / "control.lock"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_STATUS_HISTORY_SCREEN_ACTIVE": "3",
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(lock_path),
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
    assert commands == ["maintenance-preflight", "maintenance", "runtime-heartbeat"]
    maintenance_call = next(args for args in calls if "maintenance" in args)
    assert "--skip-cleanup" in maintenance_call
    assert report_path.exists()
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock_handle, fcntl.LOCK_UN)
    assert not lock_path.with_suffix(".lock.d").exists()
    assert "maintenance light cleanup: skipped reason=disabled" in result.stdout
    assert "wallet history heavy maintenance: skipped reason=active_queue" in result.stdout
    assert "maintenance loop: ok" in result.stdout


def test_nas_maintenance_uses_fast_recovery_between_full_light_cleanup_windows(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    stamp_path = tmp_path / "light.last"
    stamp_path.write_text("4102444800\n", encoding="utf-8")
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
        "PM_ROBOT_MAINTENANCE_LIGHT_STAMP_PATH": str(stamp_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(tmp_path / "control.lock"),
        "PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS": "",
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
    assert commands == ["maintenance-preflight", "maintenance", "runtime-heartbeat"]
    maintenance_call = next(args for args in calls if "maintenance" in args)
    assert "--skip-cleanup" in maintenance_call
    assert "--reset-stale-jobs" in maintenance_call
    assert "--reset-stale-heartbeats" in maintenance_call
    assert "--l0-retention-days" not in maintenance_call
    assert "--cleanup-batch-limit" not in maintenance_call
    assert "maintenance light cleanup: skipped reason=disabled" in result.stdout
    assert "maintenance loop: ok" in result.stdout


def test_nas_maintenance_runs_artifact_audit_before_gc_when_forced(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(tmp_path / "control.lock"),
        "PM_ROBOT_WALLET_HISTORY_HEAVY_RUN_NOW": "1",
        "PM_ROBOT_MAINTENANCE_LIGHT_CLEANUP_ENABLED": "1",
        "PM_ROBOT_WALLET_HISTORY_GC_ENABLED": "1",
        "PM_ROBOT_WALLET_HISTORY_AUDIT_DELETE_ORPHANS": "1",
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
    assert commands[:4] == [
        "maintenance-preflight",
        "maintenance",
        "wallet-history-audit",
        "wallet-history-gc",
    ]
    maintenance_call = next(args for args in calls if "maintenance" in args)
    assert "--heartbeat-days" in maintenance_call
    assert "--l0-retention-days" in maintenance_call
    assert "--l0-cleanup-batch-limit" in maintenance_call
    assert "--pipeline-job-days" not in maintenance_call
    assert "--wal-checkpoint" in maintenance_call
    assert maintenance_call[maintenance_call.index("--wal-checkpoint") + 1] == "passive"
    audit_call = next(args for args in calls if "wallet-history-audit" in args)
    assert "--delete-orphans" in audit_call
    assert "--verify-checksums" not in audit_call
    assert report_path.is_file()
    assert "maintenance loop: ok" in result.stdout


def test_nas_maintenance_zero_backlog_reuses_stamp_and_skips_second_heavy_cycle(tmp_path):
    fake_bin, call_log = _fake_runtime(tmp_path)
    report_path = tmp_path / "maintenance.json"
    stamp_path = tmp_path / "heavy.last"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "CALL_LOG": str(call_log),
        "FAKE_MAINTENANCE_QUEUED": "0",
        "PM_ROBOT_MAINTENANCE_RUN_ONCE": "1",
        "PM_ROBOT_MAINTENANCE_START_DELAY": "0",
        "PM_ROBOT_MAINTENANCE_REPORT_PATH": str(report_path),
        "PM_ROBOT_CONTROL_PLANE_LOCK_PATH": str(tmp_path / "control.lock"),
        "PM_ROBOT_WALLET_HISTORY_HEAVY_ALLOWED_HOURS": "",
        "PM_ROBOT_WALLET_HISTORY_HEAVY_INTERVAL_SECONDS": "86400",
        "PM_ROBOT_WALLET_HISTORY_HEAVY_STAMP_PATH": str(stamp_path),
    }

    first = subprocess.run(
        ["sh", "deploy/nas/maintenance-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    second = subprocess.run(
        ["sh", "deploy/nas/maintenance-loop.sh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert stamp_path.is_file()
    assert "reason=backlog_zero" in first.stdout
    assert "wallet history heavy maintenance: skipped reason=not_due" in second.stdout
    calls = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    commands = [args[args.index("--env") + 2] for args in calls if "--env" in args]
    assert commands.count("maintenance-preflight") == 2
    assert commands.count("maintenance") == 2
    assert commands.count("wallet-history-audit") == 1
    assert commands.count("wallet-history-gc") == 0
