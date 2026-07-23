import subprocess
import sys
from pathlib import Path

import pytest

from pm_robot.cli import main


RETIRED_CLI_COMMANDS = (
    "activity-coverage",
    "backtest-copy-stream",
    "build-review",
    "copyability-jobs",
    "copyability-plan",
    "copyability-worker",
    "evidence-backfill",
    "evidence-backfill-jobs",
    "evidence-backfill-plan",
    "evidence-backfill-status",
    "evidence-backfill-worker",
    "paper-run",
    "paper-readiness",
    "paper-settle",
    "paper-handoff-export",
    "publish-leaders",
    "published-leaders",
    "execution-preflight",
    "paper-realtime-audit",
    "gamma-cache",
    "import-features",
    "ingest-activity",
    "ingest-gamma-markets",
    "ingest-positions",
    "ingest-trade-roles",
    "materialize-features",
    "mine-copy-graph",
    "paper-observer-evaluate",
    "paper-observer-preview",
    "paper-observer-settle",
    "pipeline-audit",
    "pipeline-cycle",
    "pipeline-smoothness",
    "prioritize-backfill",
    "prune-evidence",
    "research-stage-reconcile",
    "retention-cycle",
    "rtds-watch-audit",
    "validation-observer-preview",
    "validation-observer-evaluate",
    "validation-observer-settle",
    "wallet-pipeline-jobs",
    "wallet-pipeline-plan",
    "wallet-pipeline-state",
    "wallet-pipeline-worker",
    "wallet-registry",
    "archive-status",
    "archive-wallet",
    "archive-wallet-activity",
    "compact-evidence",
)


@pytest.mark.parametrize("command", RETIRED_CLI_COMMANDS)
def test_retired_cli_entrypoints_are_unknown_before_db_access(
    tmp_path,
    monkeypatch,
    capsys,
    command,
):
    db_path = tmp_path / "must_not_be_created.sqlite"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            command,
        ],
    )

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "invalid choice" in captured.err
    assert command in captured.err
    assert not db_path.exists()


@pytest.mark.parametrize(
    "command",
    (
        "execution-up",
        "execution-down",
        "execution-restart",
        "execution-status",
        "execution-preflight",
        "execution-logs",
    ),
)
def test_nas_execution_profile_commands_are_unknown(command):
    result = subprocess.run(
        ["bash", "deploy/nas/pmrobot-nas.sh", command],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 2
    assert "Unknown command" in result.stderr


@pytest.mark.parametrize(
    "script",
    (
        "deploy/nas/paper-runner-loop.sh",
        "deploy/nas/paper-settle-loop.sh",
        "deploy/nas/publish-loop.sh",
    ),
)
def test_nas_execution_loop_scripts_are_absent(script):
    assert not Path(script).exists()


def test_execution_compose_overlay_is_absent():
    assert not Path("deploy/nas/docker-compose.execution.yml").exists()


def test_systemd_execution_units_are_absent():
    retired_units = (
        "deploy/systemd/pm-robot-paper.service",
        "deploy/systemd/pm-robot-paper-settle.service",
        "deploy/systemd/pm-robot-publish.service",
        "deploy/systemd/pm-robot-materialize-features.service",
    )

    assert all(not Path(unit).exists() for unit in retired_units)
