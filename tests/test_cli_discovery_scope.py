import re
import sys
from types import SimpleNamespace

import pytest

from pm_robot.cli import main


SUPPORTED_COMMANDS = (
    "migrate",
    "runtime-heartbeat",
    "import-addresses",
    "import-polydata",
    "discover-leaderboard",
    "discover-activity",
    "discover-rtds",
    "wallet-screen-plan",
    "wallet-screen-worker",
    "wallet-history-plan",
    "wallet-history-worker",
    "wallet-history-gc",
    "wallet-history-audit",
    "wallet-level-select",
    "wallet-l6-plan",
    "wallet-l6-worker",
    "pipeline-jobs",
    "health",
    "status",
    "backup",
    "backup-sql-dump",
    "maintenance",
    "web",
)

RETIRED_HELP_TERMS = (
    "candidate_stage",
    "copyability",
    "needs_manual_review",
    "observer",
    "paper",
    "publish",
    "validation trial",
    "wallet-pipeline",
)

RETIRED_RTDS_OPTIONS = (
    "--validation-min-trade-usdc",
    "--paper-min-trade-usdc",
    "--watch-min-score",
)

RETIRED_ACTIVITY_OPTIONS = (
    "--min-trades",
    "--min-usdc-volume",
    "--out",
    "--no-db-write",
)


@pytest.mark.parametrize("command", SUPPORTED_COMMANDS)
def test_supported_discovery_and_operations_commands_have_help(
    monkeypatch,
    capsys,
    command,
):
    monkeypatch.setattr(sys, "argv", ["pm-robot", command, "--help"])

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
    assert command in capsys.readouterr().out


def test_top_level_help_describes_only_discovery_scope(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["pm-robot", "--help"])

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
    help_text = capsys.readouterr().out.lower()
    command_choices = re.search(r"\{([^}]+)\}", help_text)
    assert command_choices is not None
    assert set(command_choices.group(1).split(",")) == set(SUPPORTED_COMMANDS)
    for retired_term in RETIRED_HELP_TERMS:
        assert retired_term not in help_text


def test_selection_policy_version_is_not_a_runtime_override(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "must_not_be_created.sqlite"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--db",
            str(db_path),
            "wallet-level-select",
            "--policy-version",
            "ad-hoc-policy",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
    assert not db_path.exists()


@pytest.mark.parametrize("option", RETIRED_RTDS_OPTIONS)
def test_retired_rtds_options_fail_before_db_access(
    tmp_path,
    monkeypatch,
    capsys,
    option,
):
    db_path = tmp_path / "must_not_be_created.sqlite"
    monkeypatch.setattr(
        sys,
        "argv",
        ["pm-robot", "--db", str(db_path), "discover-rtds", option, "1"],
    )

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
    assert not db_path.exists()


def test_discover_rtds_defaults_to_broad_ingress(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "robot.sqlite"
    received = {}

    def fake_discovery(_conn, **kwargs):
        received.update(kwargs)
        return SimpleNamespace(status="ok")

    monkeypatch.setattr(
        "pm_robot.cli.run_rtds_activity_discovery",
        fake_discovery,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["pm-robot", "--db", str(db_path), "discover-rtds"],
    )

    assert main() == 0
    assert received["min_trade_usdc"] == 1.0
    assert "validation_min_trade_usdc" not in received
    assert "paper_min_trade_usdc" not in received
    assert "watch_min_score" not in received
    assert '"status": "ok"' in capsys.readouterr().out


@pytest.mark.parametrize("option", RETIRED_ACTIVITY_OPTIONS)
def test_discover_activity_has_no_export_or_duplicate_screening_options(
    tmp_path,
    monkeypatch,
    capsys,
    option,
):
    db_path = tmp_path / "must_not_be_created.sqlite"
    argv = ["pm-robot", "--db", str(db_path), "discover-activity", option]
    if option != "--no-db-write":
        argv.append("1")
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
    assert not db_path.exists()
