import json
import sqlite3
from pathlib import Path

from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.pipeline_cycle import PipelineCycleOptions, run_pipeline_cycle
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    materialize_wallet_processing_state,
    enqueue_pipeline_job,
    persist_score,
    persist_wallet_activity,
    record_runtime_heartbeat,
    upsert_candidate,
    upsert_wallet_feature,
)


def _trade_events(wallet: str, count: int) -> list[dict]:
    return [
        {
            "proxyWallet": wallet,
            "timestamp": 10_000 + idx,
            "conditionId": f"condition-{idx % 5}",
            "eventSlug": f"event-{idx % 5}",
            "slug": f"market-{idx % 5}",
            "asset": f"asset-{idx % 5}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.55,
            "size": 20,
            "usdcSize": 11,
            "transactionHash": f"0x{idx:064x}",
        }
        for idx in range(count)
    ]


def _score(conn, wallet: str, *, score: float, stage: CandidateStage = CandidateStage.NEEDS_REVIEW) -> None:
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=score,
            stage=stage,
            reason="watchlist_score",
            components={"score": score},
            penalties={},
        ),
        policy_version="test",
    )


def test_pipeline_cycle_dry_run_is_read_only(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        _score(conn, wallet, score=72)
        conn.commit()

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(execute_plan=False, wallet_shard_count=1, min_score=40),
        )
        jobs = conn.execute("SELECT * FROM pipeline_jobs").fetchall()
        budgets = conn.execute("SELECT * FROM evidence_backfill_budget").fetchall()
        states = conn.execute("SELECT * FROM wallet_processing_state").fetchall()

        assert report["ok"] is True
        assert report["dry_run"] is True
        assert report["executed"] is False
        assert report["steps"][0]["name"] == "eligibility_repair_preview"
        assert report["steps"][0]["data"]["wallets_ineligible"] == 1
        assert jobs == []
        assert budgets == []
        assert states == []
    finally:
        conn.close()


def test_pipeline_cycle_routes_repairs_through_canonical_planners(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    thin = "0x" + "2" * 40
    copy_blocked = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=thin, sources="test_source"))
        _score(conn, thin, score=72)

        upsert_candidate(conn, CandidateAddress(address=copy_blocked, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=copy_blocked,
                hygiene_status="clean",
                copy_event_count=0,
                edge_retention_pct=80,
                walk_forward_consistency_pct=100,
            ),
        )
        _score(conn, copy_blocked, score=68)
        persist_wallet_activity(conn, copy_blocked, _trade_events(copy_blocked, 120), ingested_at=20_000)
        conn.commit()

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                wallet_shard_count=3,
                copyability_shard_count=1,
                min_score=40,
                feature_limit=0,
                run_scoring=False,
                policy_path=Path("config/leader_scoring_policy.json"),
            ),
        )
        thin_jobs = conn.execute(
            """
            SELECT * FROM pipeline_jobs
            WHERE wallet = ?
              AND job_type = 'wallet_evidence_backfill'
              AND subject_key = 'light_pending'
            """,
            (thin,),
        ).fetchall()
        copyability_jobs = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE wallet = ? AND job_type = 'copyability_evidence'",
            (copy_blocked,),
        ).fetchall()
        all_job_sources = [
            json.loads(row["input_json"]).get("source")
            for row in conn.execute("SELECT input_json FROM pipeline_jobs").fetchall()
        ]
        runs = conn.execute("SELECT * FROM ingest_runs").fetchall()
        steps = {step["name"]: step for step in report["steps"]}
        step_names = [step["name"] for step in report["steps"]]

        assert report["ok"] is True
        assert report["dry_run"] is False
        assert report["executed"] is True
        assert report["safety"]["runs_network_workers"] is False
        assert len(thin_jobs) == 1
        assert thin_jobs[0]["subject_key"] == "light_pending"
        assert thin_jobs[0]["tier"] == "l0_discovered"
        assert json.loads(thin_jobs[0]["input_json"])["source"] == "wallet_processing_state"
        assert len(copyability_jobs) == 1
        assert copyability_jobs[0]["shard"] == 0
        assert json.loads(copyability_jobs[0]["input_json"])["source"] == "copyability_planner"
        assert "eligibility_repair" not in all_job_sources
        assert steps["eligibility_repair_prepare"]["data"]["wallet_repairs_prepared"] == 1
        assert steps["eligibility_repair_prepare"]["data"]["copyability_repairs_ready"] == 1
        assert step_names.index("eligibility_repair_prepare") < step_names.index("wallet_pipeline_state_materialize")
        assert step_names.index("wallet_pipeline_state_materialize") < step_names.index("materialize_features")
        assert step_names.index("materialize_features") < step_names.index("incremental_score")
        assert step_names.index("incremental_score") < step_names.index("evidence_promotion")
        assert step_names.index("evidence_promotion") < step_names.index("wallet_pipeline_plan")
        assert step_names.index("wallet_pipeline_plan") < step_names.index("copyability_plan")
        assert runs == []
        assert report["after"]["queues"]["wallet_pipeline"]["statuses"]
        assert report["after"]["queues"]["copyability"]["statuses"] == [
            {"job_type": "copyability_evidence", "status": "queued", "count": 1}
        ]
    finally:
        conn.close()


def test_pipeline_cycle_can_materialize_only_stale_wallet_state(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.commit()
        materialize_wallet_processing_state(conn, stale_only=True)

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                state_stale_only=True,
                state_commit_every=1,
                wallet_shard_count=1,
                feature_limit=0,
                run_scoring=False,
                policy_path=Path("config/leader_scoring_policy.json"),
                include_diagnostics=False,
            ),
        )
        steps = {step["name"]: step for step in report["steps"]}

        assert steps["wallet_pipeline_state_materialize"]["data"]["wallets_materialized"] == 0
    finally:
        conn.close()


def test_pipeline_cycle_scoring_only_skips_repairs_and_queue_planning(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    calls: list[str] = []

    def record(name: str, result: dict):
        def operation(*args, **kwargs):
            calls.append(name)
            return result

        return operation

    def forbidden(name: str):
        def operation(*args, **kwargs):
            raise AssertionError(f"scoring-only unexpectedly ran {name}")

        return operation

    try:
        run_migrations(conn)
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.materialize_wallet_features",
            record("materialize_features", {"wallets_attempted": 3}),
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.score_database",
            record("incremental_score", {"score_candidates_considered": 2}),
        )
        for symbol in (
            "prepare_eligibility_repairs",
            "materialize_wallet_processing_state",
            "promote_wallet_evidence",
            "plan_wallet_pipeline_jobs",
            "plan_copyability_evidence_jobs",
        ):
            monkeypatch.setattr(
                f"pm_robot.orchestration.pipeline_cycle.{symbol}",
                forbidden(symbol),
            )

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                scoring_only=True,
                feature_limit=80,
                score_limit=300,
                policy_path=Path("config/leader_scoring_policy.json"),
                include_diagnostics=False,
            ),
        )

        executed = [step["name"] for step in report["steps"] if step["status"] == "executed"]
        skipped = {step["name"]: step["data"] for step in report["steps"] if step["status"] == "skipped"}

        assert report["ok"] is True
        assert report["mode"] == "scoring_only"
        assert calls == ["materialize_features", "incremental_score"]
        assert executed == ["materialize_features", "incremental_score"]
        assert skipped == {"post_score_planning": {"reason": "scoring_only"}}
        assert not {
            "eligibility_repair_prepare",
            "wallet_pipeline_state_materialize",
            "evidence_promotion",
            "wallet_pipeline_plan",
            "copyability_plan",
        }.intersection(step["name"] for step in report["steps"])
    finally:
        conn.close()


def test_pipeline_cycle_scoring_only_can_top_up_wallet_queue_without_full_planning(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    calls: list[str] = []
    planner_kwargs: dict = {}

    def record(name: str, result: dict):
        def operation(*args, **kwargs):
            calls.append(name)
            return result

        return operation

    def record_wallet_topup(*args, **kwargs):
        calls.append("wallet_pipeline_topup")
        planner_kwargs.update(kwargs)
        return {
            "targets_seen": 30,
            "jobs_enqueued": 30,
            "active_jobs": 0,
            "max_active_jobs": 60,
            "throttled": False,
        }

    def forbidden(name: str):
        def operation(*args, **kwargs):
            raise AssertionError(f"scoring-only top-up unexpectedly ran {name}")

        return operation

    try:
        run_migrations(conn)
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.materialize_wallet_features",
            record("materialize_features", {"wallets_attempted": 3}),
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.score_database",
            record("incremental_score", {"score_candidates_considered": 2}),
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.plan_wallet_pipeline_jobs",
            record_wallet_topup,
        )
        for symbol in (
            "prepare_eligibility_repairs",
            "materialize_wallet_processing_state",
            "promote_wallet_evidence",
            "plan_copyability_evidence_jobs",
        ):
            monkeypatch.setattr(
                f"pm_robot.orchestration.pipeline_cycle.{symbol}",
                forbidden(symbol),
            )

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                scoring_only=True,
                scoring_wallet_topup_max_active_jobs=60,
                wallet_light_limit=30,
                wallet_medium_limit=20,
                wallet_deep_limit=5,
                wallet_max_active_jobs=240,
                feature_limit=80,
                score_limit=300,
                policy_path=Path("config/leader_scoring_policy.json"),
                include_diagnostics=False,
            ),
        )

        steps = {step["name"]: step for step in report["steps"]}
        assert report["ok"] is True
        assert calls == [
            "materialize_features",
            "incremental_score",
            "wallet_pipeline_topup",
        ]
        assert planner_kwargs["light_limit"] == 30
        assert planner_kwargs["medium_limit"] == 20
        assert planner_kwargs["deep_limit"] == 5
        assert planner_kwargs["max_active_jobs"] == 60
        assert steps["wallet_pipeline_topup"]["status"] == "executed"
        assert steps["wallet_pipeline_topup"]["data"]["jobs_enqueued"] == 30
        assert "post_score_planning" not in steps
        assert not {
            "eligibility_repair_prepare",
            "wallet_pipeline_state_materialize",
            "evidence_promotion",
            "copyability_plan",
        }.intersection(steps)
    finally:
        conn.close()


def test_pipeline_cycle_scoring_topup_respects_real_queue_waterline(tmp_path, monkeypatch):
    from pm_robot.cli import _pipeline_cycle_step_rows_written

    conn = connect(tmp_path / "robot.sqlite")
    active_wallet = "0x" + "7" * 40
    pending_wallet = "0x" + "8" * 40

    def forbidden(name: str):
        def operation(*args, **kwargs):
            raise AssertionError(f"scoring-only top-up unexpectedly ran {name}")

        return operation

    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=pending_wallet, sources="test_source"))
        conn.commit()
        materialize_wallet_processing_state(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet=active_wallet,
            subject_key="light_pending",
            tier="l0_discovered",
            priority=1,
            shard=0,
            input_data={"stage": "light_pending"},
            now=30_000,
        )
        conn.commit()

        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.materialize_wallet_features",
            lambda *args, **kwargs: {"wallets_attempted": 0},
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.score_database",
            lambda *args, **kwargs: {"score_candidates_considered": 0},
        )
        for symbol in (
            "prepare_eligibility_repairs",
            "materialize_wallet_processing_state",
            "promote_wallet_evidence",
            "plan_copyability_evidence_jobs",
        ):
            monkeypatch.setattr(
                f"pm_robot.orchestration.pipeline_cycle.{symbol}",
                forbidden(symbol),
            )

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                scoring_only=True,
                scoring_wallet_topup_max_active_jobs=60,
                wallet_max_active_jobs=1,
                wallet_shard_count=1,
                feature_limit=0,
                score_limit=0,
                policy_path=Path("config/leader_scoring_policy.json"),
                include_diagnostics=False,
            ),
        )

        topup_step = next(step for step in report["steps"] if step["name"] == "wallet_pipeline_topup")
        queued_wallets = [
            row["wallet"]
            for row in conn.execute(
                """
                SELECT wallet
                FROM pipeline_jobs
                WHERE job_type = 'wallet_evidence_backfill'
                  AND status IN ('queued', 'running')
                ORDER BY wallet
                """
            ).fetchall()
        ]

        assert topup_step["data"]["max_active_jobs"] == 1
        assert topup_step["data"]["active_jobs"] == 1
        assert topup_step["data"]["throttled"] is True
        assert topup_step["data"]["reason"] == "active_queue_waterline"
        assert topup_step["data"]["jobs_enqueued"] == 0
        assert queued_wallets == [active_wallet]
        assert _pipeline_cycle_step_rows_written(topup_step) == 0
        assert _pipeline_cycle_step_rows_written(
            {"name": "wallet_pipeline_topup", "data": {"jobs_enqueued": 7}}
        ) == 7
    finally:
        conn.close()


def test_pipeline_cycle_isolates_failed_phase_and_continues_committed_work(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    reported_steps: list[dict] = []
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.commit()

        def fail_wallet_plan(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(
            "pm_robot.orchestration.pipeline_cycle.plan_wallet_pipeline_jobs",
            fail_wallet_plan,
        )
        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                continue_on_error=True,
                include_diagnostics=False,
                wallet_shard_count=1,
                copyability_shard_count=1,
                planner_lock_attempts=2,
                planner_lock_sleep_seconds=0.0,
                feature_limit=0,
                run_scoring=True,
                policy_path=Path("config/leader_scoring_policy.json"),
            ),
            step_reporter=reported_steps.append,
        )
        steps = {step["name"]: step for step in report["steps"]}

        assert report["ok"] is False
        assert report["partial"] is True
        assert report["failed_steps"] == ["wallet_pipeline_plan"]
        assert report["before"] == {}
        assert report["after"] == {}
        assert steps["wallet_pipeline_plan"]["status"] == "failed"
        assert steps["wallet_pipeline_plan"]["data"]["error_type"] == "OperationalError"
        assert steps["copyability_plan"]["status"] == "executed"
        assert steps["materialize_features"]["status"] == "executed"
        assert steps["incremental_score"]["status"] == "executed"
        assert [step["name"] for step in reported_steps] == [
            "eligibility_repair_prepare",
            "wallet_pipeline_state_materialize",
            "materialize_features",
            "incremental_score",
            "evidence_promotion",
            "wallet_pipeline_plan",
            "copyability_plan",
        ]
        assert all(int(step["finished_at"]) >= int(step["started_at"]) for step in reported_steps)
        assert all(float(step["duration_ms"]) >= 0 for step in reported_steps)
    finally:
        conn.close()


def test_pipeline_cycle_phase_heartbeats_preserve_failure_and_recovery(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40

    def report_step(step: dict) -> None:
        data = step.get("data") if isinstance(step.get("data"), dict) else {}
        record_runtime_heartbeat(
            conn,
            f"loop_research_control_step_{step['name']}",
            status="failed" if step.get("status") == "failed" else "ok",
            error=str(data.get("error") or ""),
            started_at=int(step.get("started_at") or 0) or None,
            finished_at=int(step.get("finished_at") or 0) or None,
        )

    options = PipelineCycleOptions(
        execute_plan=True,
        continue_on_error=True,
        include_diagnostics=False,
        wallet_shard_count=1,
        copyability_shard_count=1,
        planner_lock_attempts=1,
        planner_lock_sleep_seconds=0.0,
        feature_limit=0,
        run_scoring=False,
        policy_path=Path("config/leader_scoring_policy.json"),
    )
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.commit()

        with monkeypatch.context() as patcher:
            patcher.setattr(
                "pm_robot.orchestration.pipeline_cycle.plan_wallet_pipeline_jobs",
                lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
            )
            first = run_pipeline_cycle(conn, options, step_reporter=report_step)

        second = run_pipeline_cycle(conn, options, step_reporter=report_step)
        rows = conn.execute(
            """
            SELECT status, started_at, finished_at, error
            FROM ingest_runs
            WHERE ingest_type = 'loop_research_control_step_wallet_pipeline_plan'
            ORDER BY run_id DESC
            """
        ).fetchall()

        assert first["partial"] is True
        assert second["ok"] is True
        assert [row["status"] for row in rows] == ["ok", "failed"]
        assert rows[1]["error"] == "database is locked"
        assert all(int(row["finished_at"]) >= int(row["started_at"]) for row in rows)
    finally:
        conn.close()


def test_pipeline_cycle_cli_holds_control_lock_for_execute_cycle(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    events: list[str] = []

    class RecordingGuard:
        def __enter__(self):
            events.append("lock_enter")

        def __exit__(self, exc_type, exc, traceback):
            events.append("lock_exit")
            return False

    def fake_cycle(conn, options, *, step_reporter=None):
        assert events == ["lock_enter"]
        assert options.execute_plan is True
        events.append("cycle")
        return {"ok": True}

    monkeypatch.setattr(
        "pm_robot.cli.database_control_plane_guard",
        lambda *args, **kwargs: RecordingGuard(),
    )
    monkeypatch.setattr("pm_robot.cli.run_pipeline_cycle", fake_cycle)
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "pipeline-cycle",
            "--execute-plan",
            "--no-score",
        ],
    )

    assert main() == 0
    assert events == ["lock_enter", "cycle", "lock_exit"]
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_pipeline_cycle_cli_reports_control_lock_timeout(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    class BusyGuard:
        def __enter__(self):
            raise TimeoutError("retention batch still finishing")

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr(
        "pm_robot.cli.database_control_plane_guard",
        lambda *args, **kwargs: BusyGuard(),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "pipeline-cycle",
            "--execute-plan",
            "--control-lock-timeout-seconds",
            "0",
        ],
    )

    assert main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "control_plane_lock_timeout"
    assert "retention batch" in payload["error"]
