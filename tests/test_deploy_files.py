import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


WRITER_SERVICES = {
    "pm-robot-activity.service",
    "pm-robot-backup.service",
    "pm-robot-copy-backtest.service",
    "pm-robot-copy-graph.service",
    "pm-robot-discover-activity.service",
    "pm-robot-discover.service",
    "pm-robot-evidence-planner.service",
    "pm-robot-gamma-paper.service",
    "pm-robot-gamma.service",
    "pm-robot-ingest.service",
    "pm-robot-maintenance.service",
    "pm-robot-materialize-features.service",
    "pm-robot-paper-settle.service",
    "pm-robot-paper.service",
    "pm-robot-publish.service",
    "pm-robot-score.service",
    "pm-robot-trade-role.service",
}


def _nas_deployment_status_function():
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    start = helper.index("def deployment_status(")
    end = helper.index("\ntoken = env_value", start)
    namespace: dict[str, object] = {}
    exec(helper[start:end], namespace)
    return namespace["deployment_status"]


def test_deploy_files_exist():
    required = [
        ".github/workflows/ci.yml",
        "deploy/env.example",
        "deploy/install.sh",
        "deploy/scripts/health_check.sh",
        "deploy/scripts/backup.sh",
        "deploy/scripts/gdrive_backup.sh",
        "deploy/systemd/pm-robot-health.service",
        "deploy/systemd/pm-robot-health.timer",
        "deploy/systemd/pm-robot-ingest.service",
        "deploy/systemd/pm-robot-ingest.timer",
        "deploy/systemd/pm-robot-score.service",
        "deploy/systemd/pm-robot-score.timer",
        "deploy/systemd/pm-robot-publish.service",
        "deploy/systemd/pm-robot-publish.timer",
        "deploy/systemd/pm-robot-discover-activity.service",
        "deploy/systemd/pm-robot-discover-activity.timer",
        "deploy/systemd/pm-robot-evidence-backfill.service",
        "deploy/systemd/pm-robot-evidence-backfill.timer",
        "deploy/systemd/pm-robot-evidence-planner.service",
        "deploy/systemd/pm-robot-evidence-planner.timer",
        "deploy/systemd/pm-robot-evidence-worker@.service",
        "deploy/systemd/pm-robot-evidence-worker@.timer",
        "deploy/systemd/pm-robot-materialize-features.service",
        "deploy/systemd/pm-robot-materialize-features.timer",
        "deploy/systemd/pm-robot-trade-role.service",
        "deploy/systemd/pm-robot-trade-role.timer",
        "deploy/systemd/pm-robot-gamma-paper.service",
        "deploy/systemd/pm-robot-gamma-paper.timer",
        "deploy/systemd/pm-robot-web.service",
        "deploy/systemd/pm-robot-backup.service",
        "deploy/systemd/pm-robot-backup.timer",
        "deploy/systemd/pm-robot-gdrive-backup.service",
        "deploy/systemd/pm-robot-gdrive-backup.timer",
        "deploy/nas/Dockerfile.tunnel",
        "deploy/nas/vps-http-connect-proxy.py",
        "deploy/nas/vps-http-proxy-tunnel.sh",
        "deploy/nas/discovery-loop.sh",
        "deploy/nas/rtds-discovery-loop.sh",
        "deploy/nas/research-control-loop.sh",
        "deploy/nas/wallet-pipeline-worker-loop.sh",
        "deploy/nas/copyability-worker-loop.sh",
        "deploy/nas/paper-observer-loop.sh",
        "deploy/nas/maintenance-loop.sh",
        "deploy/nas/backup-loop.sh",
        "deploy/nas/pmrobot-vps-http-proxy.service",
    ]
    for item in required:
        assert Path(item).exists(), item


def test_nas_retention_prune_is_bounded_and_staggered():
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/maintenance-loop.sh").read_text(encoding="utf-8")

    assert "PM_ROBOT_MAINTENANCE_START_DELAY=300" in env
    assert 'START_DELAY="${PM_ROBOT_MAINTENANCE_START_DELAY:-300}"' in loop
    assert 'sleep "$START_DELAY"' in loop
    assert "PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT=500" in env
    assert "PM_ROBOT_RETENTION_PRUNE_BATCHES=6" in env
    assert "PM_ROBOT_RETENTION_PRUNE_LIMIT=20" in env
    assert "PM_ROBOT_RETENTION_PRUNE_MAX_ACTIVITY_ROWS=5000" in env
    assert "PM_ROBOT_RETENTION_PRUNE_BATCH_DELAY=10" in env
    assert "PM_ROBOT_RETENTION_CONTROL_LOCK_TIMEOUT=0" in env
    assert "PM_ROBOT_RETENTION_CATCHUP_PASSES=4" in env
    assert "PM_ROBOT_RETENTION_CATCHUP_DELAY=60" in env
    assert 'CLEANUP_BATCH_LIMIT="${PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT:-500}"' in loop
    assert 'PRUNE_BATCHES="${PM_ROBOT_RETENTION_PRUNE_BATCHES:-6}"' in loop
    assert 'PRUNE_LIMIT="${PM_ROBOT_RETENTION_PRUNE_LIMIT:-20}"' in loop
    assert 'PRUNE_MAX_ACTIVITY_ROWS="${PM_ROBOT_RETENTION_PRUNE_MAX_ACTIVITY_ROWS:-5000}"' in loop
    assert 'PRUNE_BATCH_DELAY="${PM_ROBOT_RETENTION_PRUNE_BATCH_DELAY:-10}"' in loop
    assert 'PRUNE_CONTROL_LOCK_TIMEOUT="${PM_ROBOT_RETENTION_CONTROL_LOCK_TIMEOUT:-0}"' in loop
    assert 'PRUNE_CATCHUP_PASSES="${PM_ROBOT_RETENTION_CATCHUP_PASSES:-4}"' in loop
    assert 'PRUNE_CATCHUP_DELAY="${PM_ROBOT_RETENTION_CATCHUP_DELAY:-60}"' in loop
    assert '--cleanup-batch-limit "$CLEANUP_BATCH_LIMIT"' in loop
    assert "retention-cycle" in loop
    assert '--batches "$PRUNE_BATCHES"' in loop
    assert '--max-activity-rows "$PRUNE_MAX_ACTIVITY_ROWS"' in loop
    assert '--batch-delay-seconds "$PRUNE_BATCH_DELAY"' in loop
    assert '--cycle-interval-seconds "$INTERVAL"' in loop
    assert '--control-lock-timeout-seconds "$PRUNE_CONTROL_LOCK_TIMEOUT"' in loop
    assert '--previous-report "$PRUNE_REPORT_PATH"' in loop
    assert '--report-path "$PRUNE_REPORT_PATH"' in loop
    assert 'mv "$PRUNE_REPORT_TMP" "$PRUNE_REPORT_PATH"' not in loop
    assert "inflow_outpacing_cleanup|yielded_to_research" in loop
    assert 'sleep "$PRUNE_CATCHUP_DELAY"' in loop
    assert "--skip-cleanup" not in loop


def test_canonical_docs_describe_current_research_pipeline_only():
    architecture = Path("docs/research_pipeline_architecture.md").read_text(encoding="utf-8")
    probe = Path("docs/github_activity_probe.md").read_text(encoding="utf-8")

    assert "canonical architecture reference" in architecture
    assert "research/scoring" in architecture
    assert "observed_wallets" in architecture
    assert "wallet_processing_state (L0-L3 evidence truth)" in architecture
    assert "pipeline_jobs[job_type=wallet_evidence_backfill]" in architecture
    assert "Copyability is a separate evidence lane" in architecture
    assert "one control-plane owner" in architecture
    assert "never writes `pipeline_jobs`" in architecture
    assert "There is no L4" in architecture
    assert "does not submit real orders" in architecture
    assert "manual supplemental discovery probe, not a runtime" in probe


def test_github_workflows_do_not_auto_deploy_or_duplicate_discovery():
    workflows = Path(".github/workflows")
    ci = (workflows / "ci.yml").read_text(encoding="utf-8")
    probe = (workflows / "polymarket-activity-probe.yml").read_text(encoding="utf-8")

    assert not (workflows / "deploy-vps.yml").exists()
    assert "python -m pytest" in ci
    assert "workflow_dispatch:" in probe
    assert "schedule:" not in probe
    assert "cron:" not in probe


def test_systemd_units_are_paper_safe():
    for path in Path("deploy/systemd").glob("*.service"):
        text = path.read_text(encoding="utf-8")
        assert "PM_ROBOT_LIVE_ENABLED" not in text
        assert "PM_ROBOT_CANARY" not in text
        assert "EnvironmentFile=/opt/pm-robot/.env" in text


def test_all_local_sqlite_writer_services_use_global_lock():
    unit_dir = Path("deploy/systemd")
    for name in WRITER_SERVICES:
        text = (unit_dir / name).read_text(encoding="utf-8")
        assert "deploy/scripts/run_locked.sh" in text, name


def test_evidence_workers_use_internal_queue_leases():
    service = Path("deploy/systemd/pm-robot-evidence-worker@.service").read_text(encoding="utf-8")
    assert "run_locked.sh" not in service
    assert "wallet-pipeline-worker" in service
    assert "--shard-index %i" in service


def test_systemd_evidence_services_use_v2_wallet_pipeline():
    planner = Path("deploy/systemd/pm-robot-evidence-planner.service").read_text(encoding="utf-8")
    worker = Path("deploy/systemd/pm-robot-evidence-worker@.service").read_text(encoding="utf-8")
    score = Path("deploy/systemd/pm-robot-score.service").read_text(encoding="utf-8")
    legacy = Path("deploy/systemd/pm-robot-evidence-backfill.service").read_text(encoding="utf-8")

    assert "wallet-pipeline-state" in planner
    assert "--stale-only" in planner
    assert "wallet-pipeline-plan" in planner
    assert "wallet-pipeline-worker" in worker
    assert "--max-active-jobs 240" in planner
    assert "wallet-pipeline-state" not in score
    assert "wallet-pipeline-plan" not in score
    assert "paper-handoff-export" in score
    assert "/opt/pm-robot/reports/paper_handoff.json" in score
    assert "ingest-activity --paper-stage-only" in score
    assert "paper-observer-preview" in score
    assert "/opt/pm-robot/reports/paper_observer_preview.json" in score
    assert "paper-observer-evaluate" in score
    assert "/opt/pm-robot/reports/paper_observer_evaluation.json" in score
    assert "--persist" in score
    assert "legacy evidence-backfill is disabled" in legacy
    assert "evidence-backfill-plan" not in planner
    assert "evidence-backfill-worker" not in worker


def test_systemd_wallet_pipeline_control_plane_has_one_owner():
    owners = []
    for path in Path("deploy/systemd").glob("*.service"):
        text = path.read_text(encoding="utf-8")
        if "wallet-pipeline-plan" in text:
            owners.append(path.name)

    assert owners == ["pm-robot-evidence-planner.service"]


def test_health_check_is_low_frequency_and_not_writer_locked():
    service = Path("deploy/systemd/pm-robot-health.service").read_text(encoding="utf-8")
    timer = Path("deploy/systemd/pm-robot-health.timer").read_text(encoding="utf-8")
    assert "run_locked.sh" not in service
    assert "OnUnitActiveSec=5min" in timer


def test_gdrive_backup_is_streaming_and_not_writer_locked():
    service = Path("deploy/systemd/pm-robot-gdrive-backup.service").read_text(encoding="utf-8")
    script = Path("deploy/scripts/gdrive_backup.sh").read_text(encoding="utf-8")
    assert "run_locked.sh" not in service
    assert "backup-sql-dump" in script
    assert "rclone rcat" in script


def test_nas_proxy_tunnel_is_containerized():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    dockerfile = Path("deploy/nas/Dockerfile.tunnel").read_text(encoding="utf-8")
    tunnel = Path("deploy/nas/vps-http-proxy-tunnel.sh").read_text(encoding="utf-8")

    assert "proxy-tunnel:" in compose
    assert "container_name: pm-robot-proxy-tunnel" in compose
    assert "network_mode: host" in compose
    assert "PM_ROBOT_PROXY_LOCAL_HOST" in compose
    assert "openssh-client" in dockerfile
    assert "vps-http-proxy-tunnel.sh" in dockerfile
    assert "-g \\" in tunnel
    assert 'PM_ROBOT_PROXY_LOCAL_HOST:-0.0.0.0' in tunnel
    assert "/ssh/id_ed25519_pmrobot_vps" in tunnel
    assert "/logs/vps-http-proxy-tunnel.log" in tunnel
    assert ".".join(("172", "19", "0", "1")) not in tunnel
    assert "/" + "volume" + "1/" not in tunnel


def test_nas_deploy_files_do_not_embed_host_specific_paths_or_bridge_ips():
    paths = (
        "deploy/nas/env.example",
        "deploy/nas/docker-compose.yml",
        "deploy/nas/vps-http-proxy-tunnel.sh",
        "deploy/nas/reverse-tunnel.sh",
        "deploy/nas/pmrobot-nas.sh",
    )
    text = "\n".join(Path(path).read_text(encoding="utf-8") for path in paths)

    assert "/" + "volume" + "1/" not in text
    assert ".".join(("172", "19", "0", "1")) not in text
    assert "PM_ROBOT_NAS_ROOT" in text


def test_nas_research_control_runs_ordered_cycle_with_sharded_workers():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    control = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")
    worker = Path("deploy/nas/wallet-pipeline-worker-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    assert "research-control:" in compose
    assert "container_name: pm-robot-research-control" in compose
    assert "pipeline-worker-0:" in compose
    assert "pipeline-worker-1:" in compose
    assert "pipeline-worker-2:" in compose
    assert "research-control-loop.sh" in compose
    assert "wallet-pipeline-worker-loop.sh" in compose
    assert "pipeline-planner:" not in compose
    assert "copyability-planner:" not in compose
    assert "score-loop:" not in compose
    assert "PM_ROBOT_PIPELINE_SHARD_INDEX: \"0\"" in compose
    assert "PM_ROBOT_PIPELINE_SHARD_INDEX: \"1\"" in compose
    assert "PM_ROBOT_PIPELINE_SHARD_INDEX: \"2\"" in compose
    assert "CONTROL_SERVICES=\"research-control\"" in helper
    assert "remove_legacy_control_containers" in helper
    assert "pm-robot-pipeline-planner pm-robot-copyability-planner pm-robot-score-loop" in helper
    assert 'docker_cli rm -f "$container"' in helper
    assert "pipeline-jobs" in helper
    assert "pipeline-audit" in helper
    assert "runtime-status" in helper
    assert "PM_ROBOT_PIPELINE_WORKER_LIMIT=6" in env
    assert "PM_ROBOT_PIPELINE_WORKER_INTERVAL=60" in env
    assert "PM_ROBOT_PIPELINE_WORKER_ACTIVE_INTERVAL=5" in env
    assert "PM_ROBOT_PIPELINE_WORKER_PAGE_LIMIT=500" in env
    assert "PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS=1800" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_INTERVAL=300" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL=60" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_CATCHUP_BURST_LIMIT=4" in env
    assert "PM_ROBOT_PIPELINE_STATE_LIMIT=25" in env
    assert "PM_ROBOT_PIPELINE_STATE_COMMIT_EVERY=5" in env
    assert "PM_ROBOT_PIPELINE_PLANNER_MAX_ACTIVE_JOBS=240" in env
    assert "PM_ROBOT_ELIGIBILITY_REPAIR_LIMIT=100" in env
    assert "PM_ROBOT_RESEARCH_MIN_SCORE=40" in env
    assert "PM_ROBOT_RESEARCH_CONTROL_LOCK_TIMEOUT_SECONDS=120" in env
    assert "pipeline-cycle" in control
    assert "--execute-plan" in control
    assert "--continue-on-error" in control
    assert "--heartbeat-prefix loop_research_control_step" in control
    assert "--no-diagnostics" in control
    assert '--busy-timeout-seconds "$BUSY_TIMEOUT_SECONDS"' in control
    assert '--control-lock-timeout-seconds "$CONTROL_LOCK_TIMEOUT_SECONDS"' in control
    assert '--planner-lock-attempts "$PLANNER_LOCK_ATTEMPTS"' in control
    assert '--planner-lock-sleep-seconds "$PLANNER_LOCK_SLEEP_SECONDS"' in control
    assert "--state-stale-only" in control
    assert "PM_ROBOT_PIPELINE_STATE_LIMIT:-25" in control
    assert "PM_ROBOT_PIPELINE_STATE_COMMIT_EVERY:-5" in control
    assert "PM_ROBOT_RESEARCH_BUSY_TIMEOUT_SECONDS:-15" in control
    assert "PM_ROBOT_RESEARCH_CONTROL_LOCK_TIMEOUT_SECONDS:-120" in control
    assert "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL:-60" in control
    assert "PM_ROBOT_RESEARCH_CONTROL_CATCHUP_BURST_LIMIT:-4" in control
    assert "PM_ROBOT_RESEARCH_PLANNER_LOCK_ATTEMPTS:-4" in control
    assert '--wallet-max-active-jobs "$WALLET_MAX_ACTIVE_JOBS"' in control
    assert '--copyability-max-active-jobs "$COPYABILITY_MAX_ACTIVE_JOBS"' in control
    assert '--copyability-shard-count "$COPYABILITY_SHARD_COUNT"' in control
    assert "paper-handoff-export" in control
    assert "loop_research_control" in control
    assert "wallet-pipeline-worker" in worker
    assert "PM_ROBOT_PIPELINE_WORKER_INTERVAL:-60" in worker
    assert "PM_ROBOT_PIPELINE_WORKER_ACTIVE_INTERVAL:-5" in worker
    assert "PM_ROBOT_PIPELINE_WORKER_PAGE_LIMIT:-500" in worker
    assert 'worker_state="$(printf' in worker
    assert 'sleep_interval="$ACTIVE_INTERVAL"' in worker
    assert 'sleep_interval="$INTERVAL"' in worker
    assert "--priority-aging-seconds \"$PRIORITY_AGING_SECONDS\"" in worker
    assert 'control_state="$(printf' in control
    assert 'sleep_interval="$ACTIVE_INTERVAL"' in control
    assert 'sleep_interval="$INTERVAL"' in control
    assert "sleep \"$sleep_interval\"" in control
    assert "--scoring-only" in control
    assert "sleep \"$sleep_interval\"" in worker


@pytest.mark.parametrize(
    ("summary", "expected_interval"),
    [
        (
            {
                "ok": True,
                "steps": [
                    {
                        "name": "materialize_features",
                        "data": {"wallets_attempted": 80},
                    },
                    {
                        "name": "incremental_score",
                        "data": {"score_candidates_considered": 25},
                    },
                ],
            },
            60,
        ),
        (
            {
                "ok": True,
                "steps": [
                    {
                        "name": "materialize_features",
                        "data": {"wallets_attempted": 12},
                    },
                    {
                        "name": "incremental_score",
                        "data": {"score_candidates_considered": 300},
                    },
                ],
            },
            60,
        ),
        (
            {
                "ok": True,
                "steps": [
                    {
                        "name": "materialize_features",
                        "data": {"wallets_attempted": 12},
                    },
                    {
                        "name": "incremental_score",
                        "data": {"score_candidates_considered": 40},
                    },
                ],
            },
            300,
        ),
    ],
)
def test_nas_research_control_selects_backlog_or_idle_interval(
    tmp_path,
    summary,
    expected_interval,
):
    result = _run_nas_research_control_once(tmp_path, json.dumps(summary))

    assert result.returncode == 0, result.stderr
    assert f"next cycle in {expected_interval}s (status=ok" in result.stdout


def test_nas_research_control_uses_idle_interval_for_malformed_summary(tmp_path):
    result = _run_nas_research_control_once(tmp_path, "not-json")

    assert result.returncode == 0
    assert "invalid JSON summary; using idle interval" in result.stderr
    assert "next cycle in 300s (status=invalid" in result.stdout


def test_nas_research_control_rejects_incomplete_success_summary(tmp_path):
    result = _run_nas_research_control_once(tmp_path, '{"ok": true, "steps": []}')

    assert result.returncode == 0
    assert "invalid JSON summary; using idle interval" in result.stderr
    assert "next cycle in 300s (status=invalid" in result.stdout


def test_nas_research_control_uses_idle_interval_after_failed_cycle(tmp_path):
    result = _run_nas_research_control_once(tmp_path, '{"ok": false}', exit_code=1)

    assert result.returncode == 0
    assert "ordered cycle partial" in result.stderr
    assert "next cycle in 300s (status=failed" in result.stdout


def test_nas_research_control_switches_saturated_followup_to_scoring_only(tmp_path):
    saturated = _research_control_summary(features_attempted=80, scores_considered=300)
    idle = _research_control_summary(features_attempted=4, scores_considered=12)

    result, commands, sleeps = _run_nas_research_control_sequence(
        tmp_path,
        [(saturated, 0), (idle, 0)],
    )

    assert result.returncode == 73, result.stderr
    assert len(commands) == 2
    assert "--scoring-only" not in commands[0].split()
    assert "--scoring-only" in commands[1].split()
    assert sleeps == ["60", "300"]
    assert "mode=full, next_mode=scoring_only" in result.stdout
    assert "mode=scoring_only, next_mode=full" in result.stdout


def test_nas_research_control_forces_full_cycle_after_bounded_scoring_burst(tmp_path):
    saturated = _research_control_summary(features_attempted=80, scores_considered=300)
    idle = _research_control_summary(features_attempted=1, scores_considered=2)

    result, commands, sleeps = _run_nas_research_control_sequence(
        tmp_path,
        [(saturated, 0), (saturated, 0), (saturated, 0), (idle, 0)],
        burst_limit=2,
    )

    assert result.returncode == 73, result.stderr
    assert ["--scoring-only" in command.split() for command in commands] == [
        False,
        True,
        True,
        False,
    ]
    assert sleeps == ["60", "60", "60", "300"]
    assert result.stdout.count("mode=scoring_only, next_mode=full") == 1


@pytest.mark.parametrize(
    ("catchup_case", "catchup_exit", "expected_status"),
    [
        ("idle", 0, "ok"),
        ("failed", 1, "failed"),
        ("invalid", 0, "invalid"),
    ],
)
def test_nas_research_control_recovers_idle_failed_or_invalid_catchup_to_full_cycle(
    tmp_path,
    catchup_case,
    catchup_exit,
    expected_status,
):
    saturated = _research_control_summary(features_attempted=80, scores_considered=300)
    idle = _research_control_summary(features_attempted=0, scores_considered=0)
    catchup_output = {
        "idle": _research_control_summary(features_attempted=3, scores_considered=5),
        "failed": '{"ok": false}',
        "invalid": "not-json",
    }[catchup_case]

    result, commands, sleeps = _run_nas_research_control_sequence(
        tmp_path,
        [(saturated, 0), (catchup_output, catchup_exit), (idle, 0)],
    )

    assert result.returncode == 73, result.stderr
    assert ["--scoring-only" in command.split() for command in commands] == [False, True, False]
    assert sleeps == ["60", "300", "300"]
    assert (
        f"next cycle in 300s (status={expected_status}, mode=scoring_only, next_mode=full"
        in result.stdout
    )


def _research_control_summary(*, features_attempted, scores_considered):
    return json.dumps(
        {
            "ok": True,
            "steps": [
                {
                    "name": "materialize_features",
                    "data": {"wallets_attempted": features_attempted},
                },
                {
                    "name": "incremental_score",
                    "data": {"score_candidates_considered": scores_considered},
                },
            ],
        }
    )


def _run_nas_research_control_sequence(tmp_path, cycles, *, burst_limit=4):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sequence_dir = tmp_path / "sequence"
    sequence_dir.mkdir()
    for index, (output, exit_code) in enumerate(cycles, start=1):
        (sequence_dir / f"{index}.stdout").write_text(output + "\n", encoding="utf-8")
        (sequence_dir / f"{index}.exit").write_text(str(exit_code), encoding="utf-8")

    counter_path = tmp_path / "control-counter"
    args_log = tmp_path / "control-args.log"
    sleep_log = tmp_path / "sleep.log"
    python_wrapper = fake_bin / "python"
    python_wrapper.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"-m\" ]; then\n"
        "  case \" $* \" in\n"
        "    *\" pipeline-cycle --execute-plan \"*)\n"
        "      cycle=0\n"
        "      if [ -f \"$FAKE_CONTROL_COUNTER\" ]; then cycle=$(cat \"$FAKE_CONTROL_COUNTER\"); fi\n"
        "      cycle=$((cycle + 1))\n"
        "      printf '%s\\n' \"$cycle\" > \"$FAKE_CONTROL_COUNTER\"\n"
        "      printf '%s\\n' \"$*\" >> \"$FAKE_CONTROL_ARGS_LOG\"\n"
        "      cat \"$FAKE_CONTROL_SEQUENCE_DIR/$cycle.stdout\"\n"
        "      exit $(cat \"$FAKE_CONTROL_SEQUENCE_DIR/$cycle.exit\")\n"
        "      ;;\n"
        "    *\" paper-handoff-export \"*)\n"
        "      printf '%s\\n' '{\"ok\": true}'\n"
        "      exit 0\n"
        "      ;;\n"
        "    *\" runtime-heartbeat \"*)\n"
        "      exit 0\n"
        "      ;;\n"
        "  esac\n"
        "fi\n"
        f'exec "{sys.executable}" "$@"\n',
        encoding="utf-8",
    )
    python_wrapper.chmod(0o755)
    date_wrapper = fake_bin / "date"
    date_wrapper.write_text(
        "#!/bin/sh\nprintf '%s\\n' '2026-07-12T09:00:00+08:00'\n",
        encoding="utf-8",
    )
    date_wrapper.chmod(0o755)
    sleep_wrapper = fake_bin / "sleep"
    sleep_wrapper.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$1\" >> \"$FAKE_SLEEP_LOG\"\n"
        "cycle=$(cat \"$FAKE_CONTROL_COUNTER\")\n"
        "if [ \"$cycle\" -ge \"$FAKE_CONTROL_MAX_CYCLES\" ]; then exit 73; fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    sleep_wrapper.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "FAKE_CONTROL_SEQUENCE_DIR": str(sequence_dir),
        "FAKE_CONTROL_COUNTER": str(counter_path),
        "FAKE_CONTROL_ARGS_LOG": str(args_log),
        "FAKE_CONTROL_MAX_CYCLES": str(len(cycles)),
        "FAKE_SLEEP_LOG": str(sleep_log),
        "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL": "60",
        "PM_ROBOT_RESEARCH_CONTROL_INTERVAL": "300",
        "PM_ROBOT_RESEARCH_CONTROL_CATCHUP_BURST_LIMIT": str(burst_limit),
        "PM_ROBOT_SCORE_FEATURE_LIMIT": "80",
        "PM_ROBOT_SCORE_LIMIT": "300",
    }
    result = subprocess.run(
        ["sh", "deploy/nas/research-control-loop.sh"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    commands = args_log.read_text(encoding="utf-8").splitlines()
    sleeps = sleep_log.read_text(encoding="utf-8").splitlines()
    return result, commands, sleeps


def _run_nas_research_control_once(tmp_path, control_output, *, exit_code=0):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    python_wrapper = fake_bin / "python"
    python_wrapper.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"-m\" ]; then\n"
        "  case \" $* \" in\n"
        "    *\" pipeline-cycle --execute-plan \"*)\n"
        "      printf '%s\\n' \"$FAKE_CONTROL_OUTPUT\"\n"
        "      exit \"${FAKE_CONTROL_EXIT:-0}\"\n"
        "      ;;\n"
        "    *\" paper-handoff-export \"*)\n"
        "      printf '%s\\n' '{\"ok\": true}'\n"
        "      exit 0\n"
        "      ;;\n"
        "    *\" runtime-heartbeat \"*)\n"
        "      exit 0\n"
        "      ;;\n"
        "  esac\n"
        "fi\n"
        f'exec "{sys.executable}" "$@"\n',
        encoding="utf-8",
    )
    python_wrapper.chmod(0o755)
    date_wrapper = fake_bin / "date"
    date_wrapper.write_text(
        "#!/bin/sh\nprintf '%s\\n' '2026-07-12T09:00:00+08:00'\n",
        encoding="utf-8",
    )
    date_wrapper.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "FAKE_CONTROL_OUTPUT": control_output,
        "FAKE_CONTROL_EXIT": str(exit_code),
        "PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE": "1",
        "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL": "60",
        "PM_ROBOT_RESEARCH_CONTROL_INTERVAL": "300",
        "PM_ROBOT_SCORE_FEATURE_LIMIT": "80",
        "PM_ROBOT_SCORE_LIMIT": "300",
    }
    return subprocess.run(
        ["sh", "deploy/nas/research-control-loop.sh"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


@pytest.mark.parametrize(
    ("summary", "expected_interval", "expected_status"),
    [
        ({"status": "ok", "jobs_attempted": 3}, 5, "ok"),
        ({"status": "ok", "jobs_attempted": 0}, 60, "ok"),
        ({"status": "partial", "jobs_attempted": 1}, 60, "partial"),
    ],
)
def test_nas_wallet_worker_loop_selects_busy_or_idle_interval(
    tmp_path,
    summary,
    expected_interval,
    expected_status,
):
    result = _run_nas_wallet_worker_once(tmp_path, json.dumps(summary))

    assert result.returncode == 0, result.stderr
    assert (
        f"next poll in {expected_interval}s "
        f"(status={expected_status}, jobs_attempted={summary['jobs_attempted']})"
    ) in result.stdout


def test_nas_wallet_worker_loop_reports_malformed_summary(tmp_path):
    result = _run_nas_wallet_worker_once(tmp_path, "not-json")

    assert result.returncode == 0
    assert "invalid JSON summary; using idle interval" in result.stderr
    assert "next poll in 60s (status=invalid, jobs_attempted=0)" in result.stdout


def _run_nas_wallet_worker_once(tmp_path, worker_output):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    python_wrapper = fake_bin / "python"
    python_wrapper.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"-m\" ]; then\n"
        "  printf '%s\\n' \"$FAKE_WORKER_OUTPUT\"\n"
        "  exit \"${FAKE_WORKER_EXIT:-0}\"\n"
        "fi\n"
        f'exec "{sys.executable}" "$@"\n',
        encoding="utf-8",
    )
    python_wrapper.chmod(0o755)
    date_wrapper = fake_bin / "date"
    date_wrapper.write_text(
        "#!/bin/sh\nprintf '%s\\n' '2026-07-12T09:00:00+08:00'\n",
        encoding="utf-8",
    )
    date_wrapper.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "FAKE_WORKER_OUTPUT": worker_output,
        "PM_ROBOT_PIPELINE_SHARD_INDEX": "0",
        "PM_ROBOT_PIPELINE_WORKER_RUN_ONCE": "1",
        "PM_ROBOT_PIPELINE_WORKER_ACTIVE_INTERVAL": "5",
        "PM_ROBOT_PIPELINE_WORKER_INTERVAL": "60",
    }
    return subprocess.run(
        ["sh", "deploy/nas/wallet-pipeline-worker-loop.sh"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_nas_copyability_planner_enforces_active_queue_waterline():
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    control = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")

    assert "PM_ROBOT_COPYABILITY_PLANNER_LIMIT=50" in env
    assert "PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS=50" in env
    assert "PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS:-50" in control
    assert '--copyability-max-active-jobs "$COPYABILITY_MAX_ACTIVE_JOBS"' in control


def test_nas_helper_exposes_shared_control_lifecycle_and_cleans_legacy_loops():
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    cleanup_gate = helper.split('case "$cmd" in', 2)[1]

    for command in (
        "up",
        "down",
        "research-control-up",
        "research-control-down",
        "research-control-restart",
        "pipeline-up",
        "pipeline-down",
        "copyability-up",
        "copyability-down",
        "score-up",
        "score-down",
        "score-restart",
    ):
        assert command in cleanup_gate
    assert "remove_legacy_control_containers" in cleanup_gate
    assert "score-up is a compatibility alias for research-control-up" in helper
    assert "score-down is a compatibility alias for research-control-down" in helper
    assert "score-restart is a compatibility alias for research-control-restart" in helper
    assert "This also pauses wallet/copyability admission" in helper
    assert "research-control may admit jobs up to the configured waterline" in helper


def test_nas_copyability_workers_have_stable_unique_ids():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")

    assert compose.count('PM_ROBOT_COPYABILITY_WORKER_ID: "nas-copyability-worker-0"') == 1
    assert compose.count('PM_ROBOT_COPYABILITY_WORKER_ID: "nas-copyability-worker-1"') == 1


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_helper_auto_uses_passwordless_sudo_for_synology_docker():
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")

    assert "DOCKER_SUDO=\"${PM_ROBOT_DOCKER_SUDO:-auto}\"" in helper
    assert "resolve_docker_runner()" in helper
    assert '"$DOCKER" ps >/dev/null 2>&1' in helper
    assert 'sudo -n "$DOCKER" ps >/dev/null 2>&1' in helper
    assert "DOCKER_RUNNER=(sudo -n \"$DOCKER\")" in helper
    assert "docker_cli compose version" in helper
    assert "docker_cli compose \"$@\"" in helper
    assert "docker_cli inspect" in helper
    assert "docker_cli logs" in helper
    assert "docker_cli rm -f \"$task_name\"" in helper
    assert "exec sudo -E" not in helper
    assert "falls back to `sudo -n /usr/local/bin/docker`" in readme
    assert "PM_ROBOT_DOCKER_SUDO=always" in readme
    assert "PM_ROBOT_DOCKER_SUDO=never" in readme


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_copyability_queue_runs_two_workers_on_one_conservative_queue():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    control = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")
    worker = Path("deploy/nas/copyability-worker-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    assert "research-control:" in compose
    assert "copyability-worker-0:" in compose
    assert "copyability-worker-1:" in compose
    assert "copyability-worker-2:" not in compose
    assert "research-control-loop.sh" in compose
    assert "copyability-worker-loop.sh" in compose
    assert compose.count("PM_ROBOT_COPYABILITY_SHARD_INDEX: \"0\"") == 2
    assert "PM_ROBOT_COPYABILITY_WORKER_ID: \"nas-copyability-worker-1\"" in compose
    assert "pipeline-cycle" in control
    assert "copyability-worker" in worker
    assert "copyability-up" in helper
    assert "copyability-jobs" in helper
    assert "copyability-ensure-workers" in helper
    assert "copyability-restart-when-idle" in helper
    assert "copyability-drain-once" in helper
    assert "materialize-once" in helper
    assert "score-once" in helper
    assert "policy-rescore-once" in helper
    assert "recover-once" in helper
    assert "copyability_queue_counts" in helper
    assert "host_policy_rescore_once" in helper
    assert "host_recover_once" in helper
    assert "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ? AND status = ?" in helper
    assert "COPYABILITY_SERVICES=\"copyability-worker-0 copyability-worker-1\"" in helper
    assert "compose up -d --no-deps --no-recreate copyability-worker-0 copyability-worker-1" in helper
    assert "force-recreate copyability-worker-0 copyability-worker-1" in helper
    assert "compose build copyability-worker-0 copyability-worker-1" not in helper
    assert "PM_ROBOT_COPYABILITY_SHARD_COUNT=1" in env
    assert "PM_ROBOT_COPYABILITY_WORKER_INTERVAL=30" in env
    assert "PM_ROBOT_COPYABILITY_WORKER_LIMIT=1" in env
    assert "PM_ROBOT_COPYABILITY_WORKER_LEASE_SECONDS=7200" in env
    assert "PM_ROBOT_COPYABILITY_MAX_LEADER_EVENTS=3000" in env
    assert "PM_ROBOT_COPYABILITY_WORKER_INTERVAL:-30" in worker
    assert "PM_ROBOT_COPYABILITY_WORKER_LEASE_SECONDS:-7200" in worker
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")
    assert "two conservative workers on shard `0`" in readme
    assert "longer lease than L1/L2/L3" in readme
    assert "copyability-ensure-workers" in readme
    assert "--no-recreate" in readme
    assert "copyability-restart-when-idle" in readme


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_runtime_is_research_scoring_only():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    compact_readme = " ".join(readme.split())

    assert "research/scoring" in readme
    assert "not run paper trading, settlement, publish" in readme
    assert "paper_candidate" in readme
    assert "research/scoring only" in helper
    assert "PAPER_OBSERVER_SERVICES=\"paper-observer-loop\"" in helper
    assert "RESEARCH_SERVICES=\"$CORE_SERVICES $DISCOVERY_SERVICES $CONTROL_SERVICES $PIPELINE_SERVICES $COPYABILITY_SERVICES $PAPER_OBSERVER_SERVICES $MAINTENANCE_SERVICES\"" in helper
    assert "runtime-ensure" in helper
    assert "compose up -d --no-deps --no-recreate $RESEARCH_SERVICES" in helper
    assert "watchdog-once" in helper
    assert "watchdog-status" in helper
    assert "watchdog-disable" in helper
    assert "watchdog-enable" in helper
    assert "runtime_watchdog_once" in helper
    assert "WATCHDOG_DISABLED_FILE" in helper
    assert "runtime watchdog: disabled by" in helper
    assert "starting missing/stopped research/scoring service(s)" in helper
    assert "runtime-status" in readme
    assert "does not require sudo" in readme
    assert "/api/runtime" in readme
    assert "auth_unverified" in readme
    assert "source fingerprint verification is needed" in compact_readme
    assert "exec sudo -E" not in helper
    assert "host_copyability_drain_once" in helper
    assert "host_materialize_once" in helper
    assert "host_score_once" in helper
    assert 'PM_ROBOT_POLICY_PATH="$ROOT/config/leader_scoring_policy.json"' in helper
    assert "skipping feature materialization; policy rescore uses existing wallet features" in helper
    assert "matches_source" in helper
    assert "/api/runtime" in helper
    assert "runtime_endpoint_missing" in helper
    assert 'data.get("health")' in helper
    assert "production_readiness" in helper
    assert "deployment_status" in helper
    assert "service_monitor_report" in helper
    assert "auth_unverified" in helper
    assert "HTTPError" in helper
    assert "web API requires PM_ROBOT_UI_TOKEN" in helper
    assert "PM_ROBOT_EXPECTED_SERVICES" in helper
    assert "PM_ROBOT_COMPOSE_PS_JSON" in helper
    assert "research/scoring containers are missing or not running" in helper
    assert "legacy_worker_run_type" in helper
    assert "idle_with_queued_jobs" in helper
    assert "recent_progress_no_running" in helper
    assert "recent_copyability_progress" in helper
    assert "copyability queue has queued jobs but no running worker" in helper
    assert "copyability_progress" in helper
    assert "completed_1h" in helper
    assert "recent_rate_per_hour" in helper
    assert "eta_label" in helper
    assert '"./config:/app/config:ro"' in helper
    assert "include_pair_quality=False" in helper
    assert "fallback_command" not in helper
    assert "fallback_action" not in helper
    assert "copyability-restart-when-idle" in readme
    assert "recent_progress_no_running" in readme
    assert "does not ask for a restart" in readme
    assert "runtime-ensure" in readme
    assert "watchdog-once" in readme
    assert "watchdog-disable" in readme
    assert "watchdog-enable" in readme
    assert "watchdog-status" in readme
    assert "runtime-watchdog.log" in readme
    assert "proxy tunnel, web console, discovery, evidence pipeline, copyability, scoring, paper observer" in compact_readme
    assert "copyability-drain-once" in readme
    assert "materialize-once" in readme
    assert "score-once" in readme
    assert "only re-scores existing wallet features" in readme
    assert "PM_ROBOT_NAS_ROOT" in helper
    assert "recover-once" in readme
    assert "break-glass maintenance tools" in readme
    assert "outside the production\narchitecture" in readme
    assert "deployment actions" in readme
    assert "paper-runner" not in compose
    assert "publish-loop" not in compose


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_execution_profile_is_manual_opt_in():
    main_compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    execution_compose = Path("deploy/nas/docker-compose.execution.yml").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    preflight = Path("src/pm_robot/execution/preflight.py").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    paper_runner = Path("deploy/nas/paper-runner-loop.sh").read_text(encoding="utf-8")
    paper_settle = Path("deploy/nas/paper-settle-loop.sh").read_text(encoding="utf-8")
    publish = Path("deploy/nas/publish-loop.sh").read_text(encoding="utf-8")

    assert "paper-runner-loop" not in main_compose
    assert "publish-loop" not in main_compose
    assert "profiles:" in execution_compose
    assert "- execution" in execution_compose
    assert "paper-runner-loop:" in execution_compose
    assert "paper-settle-loop:" in execution_compose
    assert "publish-loop:" in execution_compose
    assert "/app/deploy/nas/paper-runner-loop.sh" in execution_compose
    assert "/app/deploy/nas/paper-settle-loop.sh" in execution_compose
    assert "/app/deploy/nas/publish-loop.sh" in execution_compose
    assert "EXECUTION_SERVICES=\"paper-runner-loop paper-settle-loop publish-loop\"" in helper
    assert "execution_compose()" in helper
    assert "docker-compose.execution.yml" in helper
    assert "execution-up)" in helper
    assert "execution-status)" in helper
    assert "execution-preflight)" in helper
    assert "execution-down)" in helper
    assert "execution-logs)" in helper
    assert "execution_preflight()" in helper
    assert "execution_preflight" in helper
    assert "from pm_robot.execution.preflight import execution_preflight_from_env" in helper
    assert "ready_to_start_execution" in preflight
    assert "waiting_fresh_buy_signal" in preflight
    assert "paper_signal_evaluations" in preflight
    assert "recent_paper_stage_buy" in preflight
    assert "paper_stage_orders" in preflight
    assert "paper_stage_recent_orders" in preflight
    assert "preflight is read-only" in preflight
    assert "list[" not in helper
    assert "dict[" not in helper
    assert "up)" in helper
    assert "compose up -d --build $RESEARCH_SERVICES" in helper
    assert "execution profile is opt-in" in readme
    assert "not part of the default research/scoring stack" in readme
    assert "execution-preflight" in readme
    assert "read-only check" in readme
    assert "Paper 到正式缺口" in readme
    assert "PM_ROBOT_PAPER_RUN_MAX_SIGNAL_AGE_SEC=300" in env
    assert "PM_ROBOT_WRITE_LOCK_PATH=/app/data/pm_robot.write.lock" in env
    assert "paper-run" in paper_runner
    assert "--max-signal-age-sec \"$MAX_SIGNAL_AGE_SEC\"" in paper_runner
    assert "runtime_heartbeat loop_paper_runner" in paper_runner
    assert "PM_ROBOT_LOCK=\"$LOCK_PATH\"" in paper_runner
    assert "paper-settle" in paper_settle
    assert "runtime_heartbeat loop_paper_settle" in paper_settle
    assert "publish-leaders" in publish
    assert "/app/reports/published_leaders.json" in publish
    assert "runtime_heartbeat loop_publish" in publish


def test_nas_runtime_status_does_not_restart_copyability_during_recent_progress():
    deployment_status = _nas_deployment_status_function()

    deployment = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [{"ingest_type": "copyability_evidence_worker_0_nas_copyability_0_abc", "error": ""}],
        {"configured": True},
        {
            "queued": 8,
            "running": 0,
            "completed_1h": 3,
            "completed_6h": 10,
            "completed_24h": 10,
            "recent_rate_per_hour": 12.5,
            "eta_label": "38 分钟",
            "latest_completed_at": 1_800_000_000,
        },
        {"state": "ok"},
    )

    assert deployment["copyability"]["state"] == "recent_progress_no_running"
    assert deployment["copyability"]["action"] == ""
    assert deployment["paper_handoff"] == {}
    assert deployment["paper_observer_preview"] == {}
    assert deployment["actions"] == []


def test_nas_runtime_status_restarts_copyability_only_when_stale_idle():
    deployment_status = _nas_deployment_status_function()

    deployment = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [{"ingest_type": "copyability_evidence_worker_0_nas_copyability_0_abc", "error": ""}],
        {"configured": True},
        {
            "queued": 8,
            "running": 0,
            "completed_1h": 0,
            "completed_6h": 0,
            "completed_24h": 4,
            "recent_rate_per_hour": 0,
            "eta_label": "",
            "latest_completed_at": 1_799_000_000,
        },
        {"state": "ok"},
    )

    assert deployment["copyability"]["state"] == "idle_with_queued_jobs"
    assert deployment["copyability"]["action"] == "./pmrobot-nas.sh copyability-restart-when-idle"
    assert deployment["actions"] == [
        {
            "service": "copyability",
            "command": "./pmrobot-nas.sh copyability-restart-when-idle",
            "reason": "copyability queue has queued jobs but no running worker",
        }
    ]


def test_nas_runtime_status_reports_paper_handoff_export_health():
    deployment_status = _nas_deployment_status_function()

    current = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {
            "state": "current",
            "candidate_count": 2,
            "visible_wallet_count": 2,
            "age_seconds": 60,
        },
    )
    missing = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "missing"},
    )
    stale = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "stale", "age_seconds": 7200},
    )

    assert current["paper_handoff"]["candidate_count"] == 2
    assert current["actions"] == []
    assert missing["actions"] == [
        {
            "service": "paper_handoff",
            "command": "./pmrobot-nas.sh shell python -m pm_robot.cli --env /app/.env paper-handoff-export --out /app/reports/paper_handoff.json --csv-out /app/reports/paper_handoff.csv",
            "reason": "paper handoff export file is missing",
        }
    ]
    assert stale["paper_handoff"]["action"] == "./pmrobot-nas.sh research-control-restart"
    assert stale["actions"] == [
        {
            "service": "paper_handoff",
            "command": "./pmrobot-nas.sh research-control-restart",
            "reason": "paper handoff export is stale; research-control should refresh it after scoring",
        }
    ]


def test_nas_runtime_status_reports_paper_observer_preview_export_health():
    deployment_status = _nas_deployment_status_function()

    current = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {
            "state": "current",
            "paper_stage_wallets": 2,
            "signals_seen": 0,
            "recent_buy_events": 0,
            "no_signal_reason": "latest_buy_outside_window",
        },
    )
    missing = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {"state": "missing"},
    )
    stale = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {"state": "stale", "age_seconds": 7200},
    )

    assert current["paper_observer_preview"]["paper_stage_wallets"] == 2
    assert current["paper_observer_preview"]["signals_seen"] == 0
    assert current["actions"] == []
    assert missing["actions"] == [
        {
            "service": "paper_observer_preview",
            "command": "./pmrobot-nas.sh observer-restart",
            "reason": "paper observer preview export file is missing",
        }
    ]
    assert stale["paper_observer_preview"]["action"] == "./pmrobot-nas.sh observer-restart"
    assert stale["actions"] == [
        {
            "service": "paper_observer_preview",
            "command": "./pmrobot-nas.sh observer-restart",
            "reason": "paper observer preview export is stale; paper-observer-loop should refresh it",
        }
    ]


def test_nas_runtime_status_reports_paper_observer_evaluation_export_health():
    deployment_status = _nas_deployment_status_function()

    current = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {"state": "current"},
        {
            "state": "current",
            "signals_seen": 10,
            "quotes_attempted": 10,
            "accepted_signals": 8,
            "actionable_signals": 2,
            "stale_signal_rejections": 6,
            "actionable_rate_pct": 20.0,
            "max_actionable_signal_age_sec": 300,
        },
    )
    missing = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {"state": "current"},
        {"state": "missing"},
    )
    stale = deployment_status(
        {"source_fingerprint": "abc", "source_delivery": "local", "source_root": "/app/src"},
        {"matches_source": True},
        [],
        {"configured": True},
        {"queued": 0, "running": 0},
        {"state": "ok"},
        {"state": "current"},
        {"state": "current"},
        {"state": "stale", "age_seconds": 7200},
    )

    assert current["paper_observer_evaluation"]["accepted_signals"] == 8
    assert current["paper_observer_evaluation"]["actionable_signals"] == 2
    assert current["paper_observer_evaluation"]["stale_signal_rejections"] == 6
    assert current["paper_observer_evaluation"]["actionable_rate_pct"] == 20.0
    assert current["actions"] == []
    assert missing["actions"] == [
        {
            "service": "paper_observer_evaluation",
            "command": "./pmrobot-nas.sh observer-restart",
            "reason": "paper observer quote evaluation export file is missing",
        }
    ]
    assert stale["paper_observer_evaluation"]["action"] == "./pmrobot-nas.sh observer-restart"
    assert stale["actions"] == [
        {
            "service": "paper_observer_evaluation",
            "command": "./pmrobot-nas.sh observer-restart",
            "reason": "paper observer quote evaluation export is stale; paper-observer-loop should refresh it",
        }
    ]


@pytest.mark.skipif(
    not all(
        Path(path).exists()
        for path in ("deploy/ubuntu-vm/README.md", "deploy/README.md", "deploy/nas/README.md")
    ),
    reason="README files are intentionally excluded",
)
def test_ubuntu_vm_doc_keeps_docker_compose_and_research_boundary():
    doc = Path("deploy/ubuntu-vm/README.md").read_text(encoding="utf-8")
    deploy_readme = Path("deploy/README.md").read_text(encoding="utf-8")
    nas_readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")
    compact_doc = " ".join(doc.split())

    assert "Docker Compose as" in doc
    assert "application runtime" in doc
    assert "Do not migrate this project into a loose set of host Python processes" in compact_doc
    assert "This remains `research/scoring`" in doc
    assert "paper_candidate" in doc
    assert "paper trading loops" in doc
    assert "publish loops" in doc
    assert "live trading or external execution handoff" in doc
    assert "service monitor `ok`" in doc
    assert "pm-robot-research" in doc
    assert "pm-robot-execution" in doc
    assert "deploy/ubuntu-vm/README.md" in deploy_readme
    assert "Ubuntu should be the host boundary" in deploy_readme
    assert "deploy/ubuntu-vm/README.md" in nas_readme
    assert "not a switch to unmanaged host Python" in nas_readme


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_discovery_loop_runs_high_quality_sources_without_queue_planning():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/discovery-loop.sh").read_text(encoding="utf-8")
    rtds_loop = Path("deploy/nas/rtds-discovery-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")

    assert "discovery-loop:" in compose
    assert "container_name: pm-robot-discovery-loop" in compose
    assert "discovery-loop.sh" in compose
    assert "PM_ROBOT_DISCOVERY_LEADERBOARD_CATEGORIES" in env
    assert "PM_ROBOT_DISCOVERY_ACTIVITY_MIN_TRADE_FILTER_USDC=500" in env
    assert "discover-leaderboard" in loop
    assert "--categories" in loop
    assert "--v1-pages" in loop
    assert "discover-activity" in loop
    assert "--min-trade-filter-usdc" in loop
    assert "wallet-pipeline-state" not in loop
    assert "wallet-pipeline-plan" not in loop
    assert "sleep \"$INTERVAL\"" in loop
    assert "rtds-discovery:" in compose
    assert "container_name: pm-robot-rtds-discovery" in compose
    assert "rtds-discovery-loop.sh" in compose
    assert "PM_ROBOT_RTDS_MIN_TRADE_USDC=500" in env
    assert "PM_ROBOT_RTDS_WATCH_MIN_SCORE=65" in env
    assert "discover-rtds" in rtds_loop
    assert "--min-trade-usdc" in rtds_loop
    assert "--watch-min-score" in rtds_loop
    assert "PM_ROBOT_RTDS_ENDPOINT" in rtds_loop
    assert "DISCOVERY_SERVICES=\"discovery-loop rtds-discovery\"" in helper
    assert "discovery-up" in helper
    assert "discovery-restart" in helper
    assert "persists matching real-time trades for wallets that are already in the paper" in readme
    assert "near-paper `needs_manual_review` wallets" in readme
    assert "RTDS rows for all other wallets are not bulk-saved" in readme


def test_nas_research_control_runs_planning_features_and_scoring_in_one_cycle():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/research-control-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    assert "research-control:" in compose
    assert "container_name: pm-robot-research-control" in compose
    assert "research-control-loop.sh" in compose
    assert "SCORE_SERVICES=\"$CONTROL_SERVICES\"" in helper
    assert "research-control-up" in helper
    assert "research-control-down" in helper
    assert "research-control-restart" in helper
    assert "score-up" in helper
    assert "PM_ROBOT_RESEARCH_CONTROL_INTERVAL=300" in env
    assert "PM_ROBOT_RESEARCH_BUSY_TIMEOUT_SECONDS=15" in env
    assert "PM_ROBOT_RESEARCH_PLANNER_LOCK_ATTEMPTS=4" in env
    assert "PM_ROBOT_RESEARCH_PLANNER_LOCK_SLEEP_SECONDS=1" in env
    assert "PM_ROBOT_SCORE_FEATURE_LIMIT=80" in env
    assert "PM_ROBOT_SCORE_FEATURE_COMMIT_EVERY=10" in env
    assert "PM_ROBOT_SCORE_LIMIT=300" in env
    assert "PM_ROBOT_PAPER_ACTIVITY_WALLET_LIMIT=10" in env
    assert "PM_ROBOT_PAPER_OBSERVER_MAX_SIGNAL_AGE_SEC=21600" in env
    assert "PM_ROBOT_PAPER_OBSERVER_MAX_ACTIONABLE_SIGNAL_AGE_SEC=300" in env
    assert "PM_ROBOT_PAPER_OBSERVER_EVALUATION_LIMIT=25" in env
    assert "PM_ROBOT_PAPER_OBSERVER_MAX_STAKE_USD=40" in env
    assert "pipeline-cycle" in loop
    assert "--execute-plan" in loop
    assert "--continue-on-error" in loop
    assert "--heartbeat-prefix loop_research_control_step" in loop
    assert "--no-diagnostics" in loop
    assert "--state-stale-only" in loop
    assert "PM_ROBOT_SCORE_FEATURE_LIMIT:-80" in loop
    assert "PM_ROBOT_SCORE_FEATURE_COMMIT_EVERY:-10" in loop
    assert '--feature-commit-every "$FEATURE_COMMIT_EVERY"' in loop
    assert '--score-limit "$SCORE_LIMIT"' in loop
    assert "paper-handoff-export" in loop
    assert "/app/reports/paper_handoff.json" in loop
    assert "/app/reports/paper_handoff.csv" in loop
    assert "loop_score_paper_handoff" in loop
    assert loop.index("paper-handoff-export") > loop.index("one or more isolated pipeline-cycle phases failed")
    assert "paper-observer-preview" not in loop
    assert "paper-observer-evaluate" not in loop
    assert "loop_score_paper_observer_preview" not in loop
    assert "loop_score_paper_observer_evaluation" not in loop
    assert "PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL:-60" in loop
    assert "sleep \"$sleep_interval\"" in loop


def test_nas_wallet_pipeline_control_plane_has_one_owner():
    owners = []
    for path in Path("deploy/nas").glob("*-loop.sh"):
        if "pipeline-cycle" in path.read_text(encoding="utf-8"):
            owners.append(path.name)

    assert owners == ["research-control-loop.sh"]


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_paper_observer_loop_runs_fast_readonly_quote_evaluation():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/paper-observer-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")

    assert "paper-observer-loop:" in compose
    assert "container_name: pm-robot-paper-observer-loop" in compose
    assert "paper-observer-loop.sh" in compose
    assert "depends_on:" in compose
    assert "proxy-tunnel" in compose
    assert "PAPER_OBSERVER_SERVICES=\"paper-observer-loop\"" in helper
    assert "observer-up" in helper
    assert "observer-down" in helper
    assert "observer-restart" in helper
    assert "PM_ROBOT_PAPER_OBSERVER_LOOP_INTERVAL=60" in env
    assert "PM_ROBOT_PAPER_OBSERVER_PREVIEW_LIMIT=50" in env
    assert "PM_ROBOT_PAPER_OBSERVER_ACTIVITY_WALLET_LIMIT=10" in env
    assert "PM_ROBOT_PAPER_OBSERVER_ACTIVITY_PAGE_LIMIT=50" in env
    assert "PM_ROBOT_PAPER_OBSERVER_ACTIVITY_MAX_EVENTS=50" in env
    assert "PM_ROBOT_PAPER_OBSERVER_ACTIVITY_SLEEP=0.1" in env
    assert "PM_ROBOT_PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC=300" in env
    assert "ingest-activity" in loop
    assert "--paper-stage-only" in loop
    assert "paper-observer-preview" in loop
    assert "/app/reports/paper_observer_preview.json" in loop
    assert "paper-observer-evaluate" in loop
    assert "/app/reports/paper_observer_evaluation.json" in loop
    assert "PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC" in loop
    assert '--max-signal-age-sec "$PAPER_OBSERVER_EVALUATION_MAX_SIGNAL_AGE_SEC"' in loop
    assert "--max-actionable-signal-age-sec" in loop
    assert "--persist" in loop
    assert "paper_orders" not in loop
    assert "publish-leaders" not in loop
    assert "loop_paper_observer_activity" in loop
    assert "loop_paper_observer_preview" in loop
    assert "loop_paper_observer_evaluation" in loop
    assert "sleep \"$INTERVAL\"" in loop
    assert "read-only paper observer" in readme
    assert "60 seconds" in readme


@pytest.mark.skipif(not Path("deploy/nas/README.md").exists(), reason="README files are intentionally excluded")
def test_nas_maintenance_loop_runs_lightweight_storage_and_queue_repair():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/maintenance-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")
    readme = Path("deploy/nas/README.md").read_text(encoding="utf-8")
    systemd_service = Path("deploy/systemd/pm-robot-maintenance.service").read_text(encoding="utf-8")

    assert "maintenance-loop:" in compose
    assert "container_name: pm-robot-maintenance-loop" in compose
    assert "maintenance-loop.sh" in compose
    assert "MAINTENANCE_SERVICES=\"maintenance-loop\"" in helper
    assert "maintenance-up" in helper
    assert "maintenance-restart" in helper
    assert "PM_ROBOT_MAINTENANCE_INTERVAL=900" in env
    assert "PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT=passive" in env
    assert "PM_ROBOT_MAINTENANCE_REPORT_PATH=/app/reports/maintenance_status.json" in env
    assert "PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS=21600" in env
    assert "PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS=21600" in env
    assert "PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS=30" in env
    assert "PM_ROBOT_RETENTION_ARCHIVE_ENABLED=0" in env
    assert "PM_ROBOT_ARCHIVE_DIR=/app/data/parquet" in env
    assert "PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT=500" in env
    assert "--cleanup-batch-limit \"$CLEANUP_BATCH_LIMIT\"" in loop
    assert "--skip-cleanup" not in loop
    assert "--reset-stale-jobs" in loop
    assert "--failed-job-cooldown-seconds \"$FAILED_JOB_COOLDOWN_SECONDS\"" in loop
    assert "--reset-stale-ingest-runs" in loop
    assert "--stale-ingest-run-seconds \"$STALE_INGEST_RUN_SECONDS\"" in loop
    assert "--runtime-heartbeat-days \"$RUNTIME_HEARTBEAT_DAYS\"" in loop
    assert "--wal-checkpoint \"$WAL_CHECKPOINT\"" in loop
    assert "PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-passive" in loop
    assert 'REPORT_PATH="${PM_ROBOT_MAINTENANCE_REPORT_PATH:-/app/reports/maintenance_status.json}"' in loop
    assert 'REPORT_TMP="${REPORT_PATH}.tmp.$$"' in loop
    assert 'if ! mkdir -p "$(dirname "$REPORT_PATH")"' in loop
    assert 'elif ! mv "$REPORT_TMP" "$REPORT_PATH"' in loop
    assert "could not publish checkpoint report" in loop
    assert "PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS:-21600" in loop
    assert "PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS:-21600" in loop
    assert "PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS:-30" in loop
    assert "PM_ROBOT_RETENTION_ARCHIVE_ENABLED:-0" in loop
    assert 'archive_args="--archive"' in loop
    assert '"$archive_args"' in loop
    assert "--reset-stale-jobs" in systemd_service
    assert "--failed-job-cooldown-seconds 21600" in systemd_service
    assert "--reset-stale-ingest-runs" in systemd_service
    assert "wal_truncate_window" in helper
    assert "wal-truncate-window" in helper
    assert "pipeline_running_job_counts" in helper
    assert "wal_truncate_when_idle" in helper
    assert "wal-truncate-when-idle" in helper
    assert "WAL maintenance idle guard" in helper
    assert "no running pipeline jobs" in helper
    assert "compose stop $APP_SERVICES" in helper
    assert "timeout_seconds" in helper
    assert "truncate exceeded" in helper
    assert "--reset-stale-ingest-runs" in helper
    assert "--wal-checkpoint truncate" in helper
    assert "compose up -d --no-deps --no-recreate $RESEARCH_SERVICES" in helper
    assert "maintenance-loop" in readme
    assert "wal-truncate-window" in readme
    assert "wal-truncate-when-idle" in readme
    assert "waits until `pipeline_jobs` has no running wallet-evidence or" in readme
    assert "stale-run recovery" in readme
    assert "older duplicate `running` rows" in readme
    assert "explicit WAL shrink path" in readme
    assert "300 second timeout" in readme
    assert "WAL shrinking is" in readme


def test_nas_research_stack_keeps_full_database_backups_manual_only():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    loop = Path("deploy/nas/backup-loop.sh").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    assert "backup-loop:" in compose
    assert "container_name: pm-robot-backup-loop" in compose
    assert "backup-loop.sh" in compose
    assert "manual-backup" in compose
    assert "BACKUP_SERVICES=\"backup-loop\"" in helper
    assert "backup-up" in helper
    assert "backup-down" in helper
    assert "backup-restart" in helper
    assert "backup-now" in helper
    research_services = helper.split('RESEARCH_SERVICES="', 1)[1].split('"', 1)[0]
    app_services = helper.split('APP_SERVICES="', 1)[1].split('"', 1)[0]
    assert "$BACKUP_SERVICES" not in research_services
    assert "$BACKUP_SERVICES" not in app_services
    assert "PM_ROBOT_SCHEDULED_BACKUP_ENABLED=0" in env
    assert "PM_ROBOT_BACKUP_INTERVAL=86400" in env
    assert "PM_ROBOT_BACKUP_START_DELAY=600" in env
    assert "PM_ROBOT_MAINTENANCE_KEEP_BACKUPS=0" in env
    assert "PM_ROBOT_BACKUP_INTERVAL:-86400" in loop
    assert "PM_ROBOT_BACKUP_START_DELAY:-600" in loop
    assert 'BACKUP_DIR="${PM_ROBOT_BACKUP_DIR:-/app/backups}"' in loop
    assert "next_backup_delay_seconds" in loop
    assert "schedule calculation failed; backing up now" in loop
    assert "runtime_heartbeat ok 0" in loop
    assert "python -m pm_robot.cli --env /app/.env backup" in loop
    assert "loop_backup" in loop
    assert "runtime-heartbeat" in loop


def test_nas_persistent_storage_can_live_outside_application_root():
    compose = Path("deploy/nas/docker-compose.yml").read_text(encoding="utf-8")
    execution = Path("deploy/nas/docker-compose.execution.yml").read_text(
        encoding="utf-8"
    )
    env = Path("deploy/nas/env.example").read_text(encoding="utf-8")
    helper = Path("deploy/nas/pmrobot-nas.sh").read_text(encoding="utf-8")

    for container_path in ("data", "logs", "backups", "reports"):
        mount = f"${{PM_ROBOT_STORAGE_ROOT:-.}}/{container_path}:/app/{container_path}"
        assert mount in compose
        assert mount in execution
    assert "${PM_ROBOT_STORAGE_ROOT:-.}/logs:/logs" in compose
    assert "PM_ROBOT_STORAGE_ROOT=" in env
    assert 'storage_setting="${PM_ROBOT_STORAGE_ROOT:-' in helper
    assert 'DATA_DIR="$STORAGE_ROOT/data"' in helper
    assert 'REPORTS_DIR="$STORAGE_ROOT/reports"' in helper
    assert helper.count('PM_ROBOT_STORAGE_ROOT_PATH="$STORAGE_ROOT"') >= 2
    assert '$ROOT/data/pm_robot.sqlite' not in helper
