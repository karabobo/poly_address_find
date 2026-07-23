import json
from types import SimpleNamespace


def test_wallet_screen_plan_cli_exposes_queue_waterline(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-screen-plan",
            "--limit",
            "5",
            "--max-active-jobs",
            "7",
            "--shard-count",
            "3",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "targets_seen": 0,
        "jobs_enqueued": 0,
        "active_jobs": 0,
        "max_active_jobs": 7,
        "throttled": False,
        "status": "ok",
    }


def test_wallet_screen_worker_cli_accepts_a_shard(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-screen-worker",
            "--shard-index",
            "2",
            "--shard-count",
            "3",
            "--limit",
            "2",
            "--lease-seconds",
            "600",
            "--worker-id",
            "screen-cli-test",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "jobs_attempted": 0,
        "jobs_succeeded": 0,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "promoted_l2": 0,
        "status": "ok",
        "error": "",
    }


def test_wallet_history_plan_cli_exposes_queue_waterline(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-history-plan",
            "--limit",
            "4",
            "--max-active-jobs",
            "9",
            "--shard-count",
            "3",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "targets_seen": 0,
        "jobs_enqueued": 0,
        "active_jobs": 0,
        "max_active_jobs": 9,
        "throttled": False,
        "status": "ok",
    }


def test_wallet_history_worker_cli_accepts_archive_and_shard(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    archive_dir = tmp_path / "parquet"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-history-worker",
            "--archive-dir",
            str(archive_dir),
            "--shard-index",
            "1",
            "--shard-count",
            "3",
            "--limit",
            "2",
            "--worker-id",
            "history-cli-test",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "jobs_attempted": 0,
        "jobs_succeeded": 0,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "light_completed": 0,
        "deep_completed": 0,
        "rows_archived": 0,
        "status": "ok",
        "error": "",
    }


def test_wallet_level_select_cli_runs_without_absolute_score_threshold(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    received = {}

    def fake_reconcile(_conn, **kwargs):
        received.update(kwargs)
        return SimpleNamespace(
            cohorts_processed=0,
            decisions_written=0,
            promoted_l3=0,
            promoted_l4=0,
            promoted_l5=0,
            status="ok",
        )

    monkeypatch.setattr(
        "pm_robot.orchestration.wallet_level_selection.reconcile_wallet_level_selections",
        fake_reconcile,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-level-select",
            "--min-cohort-size",
            "20",
            "--max-wait-seconds",
            "3600",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "cohorts_processed": 0,
        "decisions_written": 0,
        "promoted_l3": 0,
        "promoted_l4": 0,
        "promoted_l5": 0,
        "status": "ok",
    }
    assert received["timeout_min_cohort_size"] == 5
