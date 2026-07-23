import os
from pathlib import Path
import shlex
import subprocess
import sys


SYSTEMD_DIR = Path("deploy/systemd")
INSTALL_SCRIPT = Path("deploy/install.sh")
L6_WORKER_SCRIPT = Path("deploy/scripts/run-wallet-l6-worker.sh")

EXPECTED_UNITS = {
    "pm-robot-discover-activity.service",
    "pm-robot-discover-activity.timer",
    "pm-robot-discover.service",
    "pm-robot-discover.timer",
    "pm-robot-health.service",
    "pm-robot-health.timer",
    "pm-robot-maintenance.service",
    "pm-robot-maintenance.timer",
    "pm-robot-wallet-history-planner.service",
    "pm-robot-wallet-history-planner.timer",
    "pm-robot-wallet-history-worker@.service",
    "pm-robot-wallet-history-worker@.timer",
    "pm-robot-wallet-level-select.service",
    "pm-robot-wallet-level-select.timer",
    "pm-robot-wallet-l6-planner.service",
    "pm-robot-wallet-l6-planner.timer",
    "pm-robot-wallet-l6-worker.service",
    "pm-robot-wallet-l6-worker.timer",
    "pm-robot-wallet-screen-planner.service",
    "pm-robot-wallet-screen-planner.timer",
    "pm-robot-wallet-screen-worker@.service",
    "pm-robot-wallet-screen-worker@.timer",
    "pm-robot-web.service",
}


def _read(name: str) -> str:
    return (SYSTEMD_DIR / name).read_text(encoding="utf-8")


def test_systemd_directory_contains_only_the_l0_l6_wallet_funnel():
    actual = {path.name for path in SYSTEMD_DIR.iterdir() if path.is_file()}

    assert actual == EXPECTED_UNITS


def test_systemd_wallet_funnel_uses_bounded_nas_defaults():
    screen_plan = _read("pm-robot-wallet-screen-planner.service")
    screen_worker = _read("pm-robot-wallet-screen-worker@.service")
    history_plan = _read("pm-robot-wallet-history-planner.service")
    history_worker = _read("pm-robot-wallet-history-worker@.service")
    level_select = _read("pm-robot-wallet-level-select.service")
    l6_plan = _read("pm-robot-wallet-l6-planner.service")
    l6_worker = _read("pm-robot-wallet-l6-worker.service")
    l6_worker_script = L6_WORKER_SCRIPT.read_text(encoding="utf-8")

    assert (
        "wallet-screen-plan --limit 24 --max-active-jobs 72 "
        "--rescreen-after-seconds 604800 --shard-count 3"
    ) in screen_plan
    assert "wallet-screen-worker --shard-index %i --shard-count 3 --limit 2 --lease-seconds 600" in screen_worker
    assert (
        "wallet-history-plan --limit 12 --max-active-jobs 36 "
        "--light-refresh-seconds 2592000 --deep-refresh-seconds 604800 "
        "--shard-count 3"
    ) in history_plan
    assert "wallet-history-worker --shard-index %i --shard-count 3 --limit 1 --lease-seconds 1800" in history_worker
    assert "wallet-level-select --min-cohort-size 20 --max-wait-seconds 3600" in level_select
    assert "--policy-version" not in level_select
    assert "wallet-l6-plan --limit 5 --max-active-jobs 10 --shard-count 1" in l6_plan
    assert "ExecStart=/opt/pm-robot/deploy/scripts/run-wallet-l6-worker.sh" in l6_worker
    assert "ExecStartPost=" not in l6_worker
    assert "wallet-l6-worker" in l6_worker_script
    assert 'SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"' in l6_worker_script
    assert 'WORKER_LIMIT="${PM_ROBOT_WALLET_L6_WORKER_LIMIT:-1}"' in l6_worker_script
    assert 'LEASE_SECONDS="${PM_ROBOT_WALLET_L6_LEASE_SECONDS:-1800}"' in l6_worker_script
    assert "heartbeat partial" in l6_worker_script
    assert 'heartbeat ok' in l6_worker_script


def test_systemd_wallet_funnel_timers_are_continuous_and_non_overlapping():
    timer_names = sorted(name for name in EXPECTED_UNITS if name.endswith(".timer"))

    for name in timer_names:
        text = _read(name)
        assert "[Timer]" in text
        assert "Persistent=true" in text
        assert "OnUnitInactiveSec=" in text or "OnCalendar=" in text
        assert "OnUnitActiveSec=" not in text
        assert "[Install]" in text
        assert "WantedBy=timers.target" in text


def test_install_removes_units_absent_from_current_manifest_before_enabling_funnel():
    text = INSTALL_SCRIPT.read_text(encoding="utf-8")

    assert '"$ROOT/.venv/bin/python" -m pm_robot.cli --env "$ROOT/.env" migrate' in text
    assert "for installed_path in /etc/systemd/system/pm-robot-*.service" in text
    assert 'if [[ ! -f "$ROOT/deploy/systemd/$installed_unit" ]]' in text
    assert 'systemctl disable --now "$installed_unit"' in text
    assert 'rm -f "$installed_path"' in text
    assert text.index('pm_robot.cli --env "$ROOT/.env" migrate') < text.index("systemctl daemon-reload")
    assert text.index('systemctl disable --now "$installed_unit"') < text.index("systemctl daemon-reload")
    for active_timer in (
        "pm-robot-health.timer",
        "pm-robot-discover.timer",
        "pm-robot-discover-activity.timer",
        "pm-robot-wallet-screen-planner.timer",
        "pm-robot-wallet-screen-worker@0.timer",
        "pm-robot-wallet-screen-worker@1.timer",
        "pm-robot-wallet-screen-worker@2.timer",
        "pm-robot-wallet-level-select.timer",
        "pm-robot-wallet-history-planner.timer",
        "pm-robot-wallet-history-worker@0.timer",
        "pm-robot-wallet-history-worker@1.timer",
        "pm-robot-wallet-history-worker@2.timer",
        "pm-robot-wallet-l6-planner.timer",
        "pm-robot-wallet-l6-worker.timer",
        "pm-robot-maintenance.timer",
    ):
        assert f"systemctl enable --now {active_timer}" in text

    assert "systemctl enable --now pm-robot-backup.timer" not in text
    assert "systemctl enable --now pm-robot-gdrive-backup.timer" not in text
    assert "systemctl enable --now pm-robot-score.timer" not in text
    assert "systemctl enable --now pm-robot-materialize-features.timer" not in text
    assert "backups are explicit cli operations" in text.lower()


def test_systemd_deployment_is_marked_as_non_nas_server_install():
    readme = Path("deploy/README.md").read_text(encoding="utf-8")
    env = Path("deploy/env.example").read_text(encoding="utf-8")
    maintenance = _read("pm-robot-maintenance.service")

    assert "# Non-NAS Linux Server Deployment" in readme
    assert "not the Synology NAS Compose stack" in readme
    assert "PM_ROBOT_REQUIRED_RUNTIME_HEARTBEATS=" in env
    assert "PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_SECONDS=21600" in env
    assert "--heartbeat-days 30" in maintenance
    assert "--pipeline-job-days" not in maintenance


def test_systemd_maintenance_command_matches_current_cli(tmp_path: Path):
    maintenance = _read("pm-robot-maintenance.service")
    exec_start = next(
        line.removeprefix("ExecStart=")
        for line in maintenance.splitlines()
        if line.startswith("ExecStart=")
    )
    command = shlex.split(exec_start)
    module_index = command.index("pm_robot.cli")
    cli_args = command[module_index + 1 :]

    db_path = tmp_path / "pm_robot.sqlite"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            (
                f"PM_ROBOT_HOME={tmp_path}",
                f"PM_ROBOT_DB_PATH={db_path}",
                f"PM_ROBOT_LOG_DIR={tmp_path / 'logs'}",
                f"PM_ROBOT_BACKUP_DIR={tmp_path / 'backups'}",
                f"PM_ROBOT_ARCHIVE_DIR={tmp_path / 'parquet'}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    cli_args[cli_args.index("/opt/pm-robot/.env")] = str(env_path)

    process_env = os.environ.copy()
    process_env["PYTHONPATH"] = str(Path("src").resolve())
    migrate = subprocess.run(
        [
            sys.executable,
            "-m",
            "pm_robot.cli",
            "--env",
            str(env_path),
            "--db",
            str(db_path),
            "migrate",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=process_env,
    )
    assert migrate.returncode == 0, migrate.stderr

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pm_robot.cli",
            *cli_args,
            "--db",
            str(db_path),
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=process_env,
    )

    assert result.returncode == 0, result.stderr


def test_vps_proxy_server_lives_outside_nas_deploy_tree():
    assert not Path("deploy/nas/vps-http-connect-proxy.py").exists()
    assert not Path("deploy/nas/pmrobot-vps-http-proxy.service").exists()
    assert not Path("deploy/nas/reverse-tunnel.sh").exists()
    assert not Path("deploy/nas/vps-loopback-bridge.py").exists()

    proxy_server = Path("deploy/vps/vps-http-connect-proxy.py")
    proxy_service = Path("deploy/vps/pmrobot-vps-http-proxy.service")
    assert proxy_server.is_file()
    assert proxy_service.is_file()
    assert "/opt/pm-robot/deploy/vps/vps-http-connect-proxy.py" in proxy_service.read_text(encoding="utf-8")


def test_systemd_units_have_no_legacy_commands_user_paths_or_secrets():
    unit_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(SYSTEMD_DIR.iterdir())
        if path.is_file()
    )

    for forbidden in (
        "wallet-pipeline",
        "evidence-backfill",
        "materialize-features",
        "build-review",
        "copyability",
        "copy-graph",
        "copy-backtest",
        "paper-run",
        "paper-settle",
        "publish-leaders",
        "ingest-trade-roles",
    ):
        assert forbidden not in unit_text

    deployment_text = INSTALL_SCRIPT.read_text(encoding="utf-8") + unit_text
    for forbidden in (
        "/Users/",
        "PASSWORD=",
        "PRIVATE_KEY=",
    ):
        assert forbidden not in deployment_text


def test_systemd_service_and_timer_files_have_required_sections():
    for path in sorted(SYSTEMD_DIR.iterdir()):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        assert "[Unit]" in text, path
        if path.suffix == ".service":
            assert "[Service]" in text, path
            assert "ExecStart=" in text, path
        elif path.suffix == ".timer":
            assert "[Timer]" in text, path
            service_name = path.name.removesuffix(".timer") + ".service"
            assert (SYSTEMD_DIR / service_name).is_file(), path


def test_install_script_parses_as_bash():
    result = subprocess.run(
        ["bash", "-n", str(INSTALL_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_systemd_l6_worker_wrapper_parses_as_bash():
    result = subprocess.run(
        ["bash", "-n", str(L6_WORKER_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_systemd_l6_worker_wrapper_preserves_partial_heartbeat(tmp_path: Path):
    fake_source = tmp_path / "fake-source"
    package = fake_source / "pm_robot"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "cli.py").write_text(
        """
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if "wallet-l6-worker" in args:
    print(json.dumps({"status": os.environ["FAKE_WORKER_STATUS"], "jobs_attempted": 1}))
    raise SystemExit(0)
if "runtime-heartbeat" in args:
    Path(os.environ["FAKE_HEARTBEAT_LOG"]).write_text(" ".join(args), encoding="utf-8")
    raise SystemExit(0)
raise SystemExit(2)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    root = tmp_path / "pm-robot"
    root.mkdir()
    (root / ".env").write_text("", encoding="utf-8")
    heartbeat_log = tmp_path / "heartbeat.log"
    env = os.environ.copy()
    env.update(
        {
            "PM_ROBOT_HOME": str(root),
            "PM_ROBOT_PYTHON": sys.executable,
            "PYTHONPATH": str(fake_source),
            "FAKE_WORKER_STATUS": "partial",
            "FAKE_HEARTBEAT_LOG": str(heartbeat_log),
        }
    )

    result = subprocess.run(
        [str(L6_WORKER_SCRIPT.resolve())],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    heartbeat = heartbeat_log.read_text(encoding="utf-8")
    assert "--status partial" in heartbeat
    assert "--status ok" not in heartbeat
