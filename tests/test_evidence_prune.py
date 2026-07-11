import json
import time

import pytest

from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import (
    _previous_retention_cycle,
    _prune_wallet_evidence_batch,
    _retention_cycle_lock_key,
    _retention_database_identity,
    build_wallet_registry,
    prune_low_value_evidence,
    run_retention_cycle,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    enqueue_pipeline_job,
    persist_score,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


def _settings(db_path):
    return RobotSettings(db_path=db_path, execution_mode="research")


def _activity(idx: int) -> dict:
    return {
        "timestamp": 1_000 + idx,
        "conditionId": "condition-1",
        "eventSlug": "event-1",
        "slug": "market-1",
        "asset": "asset-1",
        "outcome": "YES",
        "type": "TRADE",
        "side": "BUY",
        "price": 0.5,
        "size": 10,
        "usdcSize": 5,
        "transactionHash": f"0x{idx:064x}",
    }


def _seed_candidate(conn, wallet: str, *, materialized: bool, roi: float = -0.1) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            extra={"feature_materializer_version": "test"} if materialized else {},
        ),
    )
    persist_wallet_activity(conn, wallet, [_activity(idx) for idx in range(5)], ingested_at=2_000)
    conn.execute(
        """
        INSERT INTO paper_wallet_quality(
            wallet, orders, open_positions, settled_positions, gamma_marked_positions,
            fallback_marked_positions, mark_coverage, settled_cost_usd, settled_pnl_usd,
            settled_roi, total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
        ) VALUES (?, 5, 0, 2, 2, 0, 1.0, 100, ?, ?, ?, ?, 0, '[]', 2000)
        """,
        (wallet, roi * 100, roi, roi * 100, roi),
    )
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=0,
            stage=CandidateStage.NEEDS_DATA,
            reason="missing_required_score_components",
            components={},
            penalties={},
        ),
        policy_version="test",
    )
    conn.commit()


def test_prune_evidence_dry_run_and_execute_low_value_only(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    low = "0x" + "1" * 40
    high = "0x" + "2" * 40
    unmaterialized = "0x" + "3" * 40
    try:
        run_migrations(conn)
        _seed_candidate(conn, low, materialized=True, roi=-0.2)
        _seed_candidate(conn, high, materialized=True, roi=0.5)
        _seed_candidate(conn, unmaterialized, materialized=False, roi=-0.2)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (low,),
        )
        conn.commit()
    finally:
        conn.close()

    dry = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=True,
    )
    conn = connect(db_path)
    try:
        assert dry["wallets"] == [low]
        assert dry["deleted"]["wallet_activity"] == 3
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 5
    finally:
        conn.close()

    executed = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=False,
    )
    conn = connect(db_path)
    try:
        assert executed["wallets"] == [low]
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT trade_count FROM wallet_activity_watermarks WHERE address = ?",
            (low,),
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (high,)
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (unmaterialized,),
        ).fetchone()[0] == 5
        registry = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()

    build_wallet_registry(_settings(db_path))
    conn = connect(db_path)
    try:
        rebuilt = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert rebuilt["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()


def test_retention_cycle_aggregates_batches_and_backlog(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    wallets = ["0x" + str(index) * 40 for index in range(1, 4)]
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
                (wallet,),
            )
        conn.commit()
    finally:
        conn.close()

    identity_before = _retention_database_identity(db_path)
    result = run_retention_cycle(
        _settings(db_path),
        batches=2,
        limit=1,
        max_activity_rows=5,
        batch_delay_seconds=0,
        cycle_interval_seconds=900,
        dry_run=False,
        archive=False,
    )

    assert result["ok"] is True
    assert result["state"] == "draining"
    assert result["batches_completed"] == 2
    assert result["deleted_activity_rows"] == 10
    assert result["planned"]["wallet_activity"] == 10
    assert result["backlog_before"]["total_activity_rows"] == 15
    assert result["backlog_after"]["total_activity_rows"] == 5
    assert result["eligible_rows_added"] == 0
    assert result["net_backlog_change_rows"] == -10
    assert result["gross_rate_per_hour"] > 0
    assert result["net_rate_per_hour"] > 0
    assert result["net_eta_hours"] is not None
    assert result["database_identity"]["database_id"] == identity_before["database_id"]
    assert result["database_identity"]["mutation_generation"] == (
        identity_before["mutation_generation"] + 2
    )

    conn = connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM wallet_activity").fetchone()[0] == 5
    finally:
        conn.close()


def test_retention_cycle_dry_run_reports_plan_without_deleting(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "4" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    result = run_retention_cycle(
        _settings(db_path),
        batches=3,
        limit=1,
        max_activity_rows=5,
        batch_delay_seconds=0,
        dry_run=True,
        archive=False,
    )

    assert result["state"] == "dry_run"
    assert result["batches_completed"] == 1
    assert result["planned"]["wallet_activity"] == 5
    assert result["deleted_activity_rows"] == 0
    assert result["deleted"]["wallet_activity"] == 0
    assert result["backlog_before"]["total_activity_rows"] == 5
    assert result["backlog_after"]["total_activity_rows"] == 5

    conn = connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM wallet_activity").fetchone()[0] == 5
    finally:
        conn.close()


def test_retention_cycle_uses_previous_report_to_measure_interval_inflow(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallets = ["0x" + char * 40 for char in ("5", "6")]
    conn = connect(db_path)
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
                (wallet,),
            )
        conn.commit()
    finally:
        conn.close()
    previous_report = tmp_path / "retention.json"
    database_identity = _retention_database_identity(db_path)
    previous_report.write_text(
        json.dumps(
            {
                "ok": True,
                "dry_run": False,
                "finished_at": int(time.time()) - 900,
                "database_identity": database_identity,
                "backlog_after": {
                    "generated_at": int(time.time()) - 900,
                    "terminal_wallets": 1,
                    "terminal_activity_rows": 5,
                    "needs_data_wallets": 0,
                    "needs_data_activity_rows": 0,
                    "total_wallets": 1,
                    "total_activity_rows": 5,
                },
            }
        ),
        encoding="utf-8",
    )

    result = run_retention_cycle(
        _settings(db_path),
        batches=1,
        limit=1,
        max_activity_rows=5,
        batch_delay_seconds=0,
        cycle_interval_seconds=900,
        dry_run=False,
        archive=False,
        previous_report_path=previous_report,
    )

    assert result["rate_basis"] == "previous_cycle"
    assert result["backlog_before_source"] == "previous_cycle"
    assert result["backlog_before"]["total_activity_rows"] == 5
    assert result["deleted_activity_rows"] == 5
    assert result["eligible_rows_added"] == 5
    assert result["net_backlog_change_rows"] == 0
    assert result["net_rate_per_hour"] == 0
    assert result["state"] == "inflow_outpacing_cleanup"


def test_retention_cycle_rejects_stale_or_wrong_database_baseline(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "7" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()
    database_identity = _retention_database_identity(db_path)
    wrong_database_identity = dict(database_identity)
    wrong_database_identity["database_id"] = "f" * 32
    previous_report = tmp_path / "retention.json"
    previous_report.write_text(
        json.dumps(
            {
                "ok": True,
                "dry_run": False,
                "finished_at": int(time.time()) - 4_000,
                "database_identity": wrong_database_identity,
                "backlog_after": {
                    "generated_at": int(time.time()) - 4_000,
                    "terminal_wallets": 1,
                    "terminal_activity_rows": 5,
                    "needs_data_wallets": 0,
                    "needs_data_activity_rows": 0,
                    "total_wallets": 1,
                    "total_activity_rows": 5,
                },
            }
        ),
        encoding="utf-8",
    )

    result = run_retention_cycle(
        _settings(db_path),
        batches=0,
        dry_run=False,
        previous_report_path=previous_report,
    )

    assert result["rate_basis"] == "configured_interval"
    assert result["backlog_before_source"] == "live_snapshot"


def test_retention_cycle_skips_when_retention_writer_is_active(
    tmp_path,
    monkeypatch,
):
    class BusyGuard:
        def __enter__(self):
            raise TimeoutError("retention cycle is already running")

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr(
        "pm_robot.ops.database_access_guard",
        lambda *args, **kwargs: BusyGuard(),
    )

    report_path = tmp_path / "retention.json"
    report_path.write_text('{"state":"existing"}\n', encoding="utf-8")
    result = run_retention_cycle(
        _settings(tmp_path / "robot.sqlite"),
        report_path=report_path,
    )

    assert result == {
        "ok": True,
        "dry_run": True,
        "state": "already_running",
        "skipped": True,
    }
    assert json.loads(report_path.read_text(encoding="utf-8")) == {
        "state": "existing"
    }


def test_retention_cycle_does_not_hide_internal_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "pm_robot.ops._run_retention_cycle_locked",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("database timeout")),
    )

    with pytest.raises(TimeoutError, match="database timeout"):
        run_retention_cycle(_settings(tmp_path / "robot.sqlite"))


def test_direct_prune_acquires_control_lock_inside_retention_lock(
    tmp_path,
    monkeypatch,
):
    events: list[str] = []

    class RecordingGuard:
        def __init__(self, name):
            self.name = name

        def __enter__(self):
            events.append(f"{self.name}_enter")

        def __exit__(self, exc_type, exc, traceback):
            events.append(f"{self.name}_exit")
            return False

    monkeypatch.setattr(
        "pm_robot.ops.database_access_guard",
        lambda *args, **kwargs: RecordingGuard("retention"),
    )
    monkeypatch.setattr(
        "pm_robot.ops.database_control_plane_guard",
        lambda *args, **kwargs: RecordingGuard("control"),
    )
    monkeypatch.setattr(
        "pm_robot.ops._prune_low_value_evidence_locked",
        lambda *args, **kwargs: events.append("prune") or {"ok": True},
    )

    result = prune_low_value_evidence(
        _settings(tmp_path / "robot.sqlite"),
        dry_run=False,
    )

    assert result == {"ok": True}
    assert events == [
        "retention_enter",
        "control_enter",
        "prune",
        "control_exit",
        "retention_exit",
    ]


def test_direct_prune_dry_run_does_not_take_control_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "pm_robot.ops.database_control_plane_guard",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected lock")),
    )
    monkeypatch.setattr(
        "pm_robot.ops._prune_low_value_evidence_locked",
        lambda *args, **kwargs: {"ok": True},
    )

    assert prune_low_value_evidence(
        _settings(tmp_path / "robot.sqlite"),
        dry_run=True,
    ) == {"ok": True}


def test_retention_cycle_yields_before_batch_when_research_control_is_active(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "7" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    class BusyGuard:
        def __enter__(self):
            raise TimeoutError("research control active")

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr(
        "pm_robot.ops.database_control_plane_guard",
        lambda *args, **kwargs: BusyGuard(),
    )

    result = run_retention_cycle(
        _settings(db_path),
        batches=2,
        batch_delay_seconds=0,
        dry_run=False,
    )

    assert result["ok"] is True
    assert result["state"] == "yielded_to_research"
    assert result["yielded_to_research"] is True
    assert result["yielded_batch"] == 1
    assert result["batches_completed"] == 0
    assert result["deleted_activity_rows"] == 0


def test_retention_cycle_releases_control_lock_between_batches(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "robot.sqlite"
    wallets = ["0x" + digit * 40 for digit in ("8", "9")]
    conn = connect(db_path)
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_candidate(conn, wallet, materialized=True, roi=-0.2)
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
                (wallet,),
            )
        conn.commit()
    finally:
        conn.close()

    events: list[str] = []

    class RecordingGuard:
        def __enter__(self):
            events.append("enter")

        def __exit__(self, exc_type, exc, traceback):
            events.append("exit")
            return False

    monkeypatch.setattr(
        "pm_robot.ops.database_control_plane_guard",
        lambda *args, **kwargs: RecordingGuard(),
    )

    result = run_retention_cycle(
        _settings(db_path),
        batches=2,
        limit=1,
        max_activity_rows=5,
        batch_delay_seconds=0,
        dry_run=False,
    )

    assert result["batches_completed"] == 2
    assert events == ["enter", "exit", "enter", "exit"]


def test_retention_cycle_does_not_hide_timeout_inside_control_lock(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        "pm_robot.ops._prune_low_value_evidence_locked",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("prune timeout")),
    )

    with pytest.raises(TimeoutError, match="prune timeout"):
        run_retention_cycle(
            _settings(tmp_path / "robot.sqlite"),
            batches=1,
            dry_run=False,
        )


def test_retention_lock_key_is_canonical(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert _retention_cycle_lock_key(tmp_path / "robot.sqlite") == (
        _retention_cycle_lock_key(tmp_path.relative_to(tmp_path) / "robot.sqlite")
    )


def test_retention_cycle_atomically_publishes_report(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    report_path = tmp_path / "reports" / "retention.json"

    result = run_retention_cycle(
        _settings(db_path),
        batches=0,
        dry_run=True,
        report_path=report_path,
    )

    assert json.loads(report_path.read_text(encoding="utf-8")) == result
    assert list(report_path.parent.glob(".retention.json.tmp.*")) == []


def test_retention_cycle_publishes_report_before_releasing_lock(
    tmp_path,
    monkeypatch,
):
    events: list[str] = []

    class RecordingGuard:
        def __enter__(self):
            events.append("lock_enter")

        def __exit__(self, exc_type, exc, traceback):
            events.append("lock_exit")
            return False

    result = {"ok": True, "state": "caught_up"}
    monkeypatch.setattr(
        "pm_robot.ops.database_access_guard",
        lambda *args, **kwargs: RecordingGuard(),
    )
    monkeypatch.setattr(
        "pm_robot.ops._run_retention_cycle_locked",
        lambda *args, **kwargs: result,
    )

    def record_report_write(path, payload):
        assert events == ["lock_enter"]
        assert payload == result
        events.append("report_write")

    monkeypatch.setattr(
        "pm_robot.ops._atomic_write_retention_report",
        record_report_write,
    )

    assert run_retention_cycle(
        _settings(tmp_path / "robot.sqlite"),
        report_path=tmp_path / "retention.json",
    ) == result
    assert events == ["lock_enter", "report_write", "lock_exit"]


def test_previous_retention_cycle_rejects_missing_or_invalid_backlog_fields(tmp_path):
    report = tmp_path / "retention.json"
    base = {
        "ok": True,
        "dry_run": False,
        "finished_at": 1_000,
        "database_identity": {"database_id": "a" * 32, "mutation_generation": 2},
        "backlog_after": {
            "generated_at": 1_000,
            "terminal_wallets": 1,
            "terminal_activity_rows": 5,
            "needs_data_wallets": 0,
            "needs_data_activity_rows": 0,
            "total_wallets": 1,
            "total_activity_rows": 5,
        },
    }
    missing_total = json.loads(json.dumps(base))
    missing_total["backlog_after"].pop("total_activity_rows")
    report.write_text(json.dumps(missing_total), encoding="utf-8")
    assert _previous_retention_cycle(report)["ok"] is False

    invalid_generated_at = json.loads(json.dumps(base))
    invalid_generated_at["backlog_after"]["generated_at"] = "not-a-number"
    report.write_text(json.dumps(invalid_generated_at), encoding="utf-8")
    assert _previous_retention_cycle(report)["ok"] is False


def test_prune_evidence_cli_remains_compatible(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    captured: dict = {}

    def fake_prune(_settings, **kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr("pm_robot.cli.prune_low_value_evidence", fake_prune)
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "prune-evidence",
        ],
    )

    assert main() == 0
    assert "previous_report_path" not in captured
    assert captured["control_lock_timeout_seconds"] == 120.0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_retention_cycle_cli_forwards_previous_report(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    previous_report = tmp_path / "retention.json"
    report_path = tmp_path / "retention-next.json"
    captured: dict = {}

    def fake_cycle(_settings, **kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr("pm_robot.cli.run_retention_cycle", fake_cycle)
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "retention-cycle",
            "--previous-report",
            str(previous_report),
            "--report-path",
            str(report_path),
        ],
    )

    assert main() == 0
    assert captured["previous_report_path"] == previous_report
    assert captured["report_path"] == report_path
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_retention_cycle_cli_does_not_replace_report_when_cycle_is_active(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    monkeypatch.setattr(
        "pm_robot.cli.run_retention_cycle",
        lambda _settings, **kwargs: {
            "ok": True,
            "dry_run": False,
            "state": "already_running",
            "skipped": True,
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "retention-cycle",
            "--execute",
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["state"] == "already_running"


def test_execute_prune_counts_sqlite_changes_without_precount_scan(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    statements: list[str] = []
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True)
        conn.set_trace_callback(statements.append)

        deleted = _prune_wallet_evidence_batch(
            conn,
            [wallet],
            keep_recent_activity=0,
            dry_run=False,
        )

        assert deleted["wallet_activity"] == 5
        assert not any(
            "SELECT COUNT(*)" in statement.upper()
            for statement in statements
        )
    finally:
        conn.close()


def test_prune_selection_is_bounded_by_activity_rows(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallets = ["0x" + digit * 40 for digit in ("1", "2", "3")]
    conn = connect(db_path)
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_candidate(conn, wallet, materialized=True)
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                "INSERT INTO wallet_processing_state(wallet, activity_count) VALUES (?, 1)",
                (wallet,),
            )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        max_activity_rows=6,
        dry_run=True,
    )

    assert result["wallets"] == [wallets[0]]
    assert result["wallet_count"] == 1
    assert result["selected_activity_rows"] == 5
    assert result["max_activity_rows"] == 6
    assert result["activity_budget_exceeded"] is False


def test_prune_activity_budget_does_not_starve_one_oversized_wallet(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "8" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        max_activity_rows=3,
        dry_run=True,
    )

    assert result["wallets"] == [wallet]
    assert result["selected_activity_rows"] == 5
    assert result["activity_budget_exceeded"] is True


def test_prune_skips_terminal_wallet_with_running_pipeline_job(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "6" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        enqueue_pipeline_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet=wallet,
            subject_key="deep_pending",
            tier="l3_deep",
            now=2000,
        )
        conn.execute(
            "UPDATE pipeline_jobs SET status = 'running' WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(_settings(db_path), limit=10, dry_run=False)

    assert result["wallet_count"] == 0
    conn = connect(db_path)
    try:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
        job_status = conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()["status"]
    finally:
        conn.close()
    assert remaining == 5
    assert job_status == "running"


def test_prune_terminal_wallet_freezes_summary_and_stops_automatic_work(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "4" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True, roi=0.5)
        leader = "0x" + "5" * 40
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 3, 2, 4, 0.75, 1.0, 10, 1000, 2000, 1, 2000)
            """,
            (leader, wallet),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_HYGIENE.value, wallet),
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=wallet,
                leader_score=42,
                stage=CandidateStage.BLOCKED_HYGIENE,
                reason="hygiene_hard_block",
                components={"profitability": 60},
                penalties={"hygiene": 30},
            ),
            policy_version="test-2",
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, evidence_status, next_action, next_action_at, updated_at
            ) VALUES (?, 'queued', 'deep_pending', 0, 2000)
            """,
            (wallet,),
        )
        enqueue_pipeline_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet=wallet,
            subject_key="deep_pending",
            tier="l3_deep",
            now=2000,
        )
        enqueue_pipeline_job(
            conn,
            job_type="copyability_evidence",
            wallet=wallet,
            subject_key="copyability_backfill",
            tier="graph_v1",
            now=2000,
        )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(_settings(db_path), limit=10, dry_run=False)
    assert result["wallets"] == [wallet]
    assert result["deleted"]["wallet_activity"] == 5
    assert result["deleted"]["leader_scores"] == 1
    assert result["deleted"]["copy_pair_stats"] == 1

    conn = connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address = ?", (wallet,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT trade_count FROM wallet_activity_watermarks WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM copy_pair_stats WHERE follower_wallet = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM leader_scores WHERE address = ?", (wallet,)
        ).fetchone()[0] == 1
        latest = conn.execute(
            "SELECT review_stage, review_reason FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert latest["review_stage"] == CandidateStage.BLOCKED_HYGIENE.value
        assert latest["review_reason"] == "hygiene_hard_block"
        registry = conn.execute(
            """
            SELECT registry_status, raw_prune_version, activity_count, review_reason
            FROM wallet_registry
            WHERE address = ?
            """,
            (wallet,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
        assert registry["raw_prune_version"] == "v2_zero_raw"
        assert registry["activity_count"] == 5
        assert registry["review_reason"] == "hygiene_hard_block"
        state = conn.execute(
            "SELECT evidence_status, next_action FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert state["evidence_status"] == "summary_ready"
        assert state["next_action"] == ""
        jobs = conn.execute(
            "SELECT status, output_json FROM pipeline_jobs WHERE wallet = ? ORDER BY job_type",
            (wallet,),
        ).fetchall()
        assert [row["status"] for row in jobs] == ["done", "done"]
        assert all('"archived":true' in row["output_json"] for row in jobs)
    finally:
        conn.close()

    build_wallet_registry(_settings(db_path))
    conn = connect(db_path)
    try:
        frozen = conn.execute(
            "SELECT activity_count, review_reason FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert frozen["activity_count"] == 5
        assert frozen["review_reason"] == "hygiene_hard_block"
    finally:
        conn.close()
