import inspect
import json
import sqlite3
from pathlib import Path

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
import pm_robot.orchestration.copyability_evidence as copyability_evidence
from pm_robot.orchestration.copyability_evidence import (
    JOB_TYPE,
    _claim_copyability_job,
    _copyability_worker_ingest_type,
    copyability_evidence_job_status,
    plan_copyability_evidence_jobs,
    run_copyability_evidence_worker,
)
from pm_robot.pipeline_terms import (
    COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON,
    COPYABILITY_OBSERVER_REVIEW_REASON,
)
from pm_robot.research.copy_backtest import TargetedCopyBacktestSummary
from pm_robot.research.copy_graph import TargetedCopyGraphSummary
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    get_wallet_features,
    persist_wallet_activity,
    rebuild_wallet_episodes,
    upsert_candidate,
    upsert_wallet_feature,
)


def test_copyability_worker_default_lease_matches_long_running_graph_refresh():
    default = inspect.signature(run_copyability_evidence_worker).parameters["lease_seconds"].default
    assert default == 7_200


def test_copyability_worker_keeps_completed_phases_when_completion_loses_lease(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        upsert_candidate(
            conn,
            CandidateAddress(address=wallet, sources="test", notes="original"),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'queued',
                NULL, 0, 0, 3, 0, '{}', '{}', '', 100, 100)
            """,
            (JOB_TYPE, wallet),
        )
        conn.commit()

        def fake_graph(
            conn_arg,
            policy,
            leaders,
            *,
            max_leader_events,
            max_followers_per_event,
            now,
            commit=True,
        ):
            assert commit is True
            conn_arg.execute(
                "UPDATE candidate_wallets SET notes = 'graph-write' WHERE address = ?",
                (wallet,),
            )
            conn_arg.commit()
            return TargetedCopyGraphSummary(1, 0, 0, 0, 0)

        def fake_backtest(
            conn_arg,
            policy,
            leaders,
            *,
            now=None,
            commit=True,
            preserve_existing_on_empty=False,
        ):
            assert commit is True
            return TargetedCopyBacktestSummary(1, 0, 0, 0)

        monkeypatch.setattr(copyability_evidence, "mine_copy_graph_for_leaders", fake_graph)
        monkeypatch.setattr(copyability_evidence, "backtest_copy_stream_for_leaders", fake_backtest)

        def fake_materialize(conn_arg, wallet_arg, **kwargs):
            assert kwargs["commit"] is False
            conn_arg.execute(
                "UPDATE candidate_wallets SET notes = 'final-write' WHERE address = ?",
                (wallet_arg,),
            )
            return True

        monkeypatch.setattr(copyability_evidence, "materialize_wallet_feature", fake_materialize)
        monkeypatch.setattr(
            copyability_evidence,
            "_score_wallet_after_copyability",
            lambda *args, **kwargs: False,
        )
        monkeypatch.setattr(
            copyability_evidence,
            "complete_pipeline_job",
            lambda *args, **kwargs: False,
        )

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="copy-lease-loss-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
        )
        candidate = conn.execute(
            "SELECT notes FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        job = conn.execute(
            "SELECT status, lease_owner, output_json FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.status == "partial"
        assert summary.jobs_failed == 1
        assert "lease was lost" in summary.error
        assert candidate["notes"] == "graph-write"
        assert job["status"] == "running"
        assert job["lease_owner"] == "copy-lease-loss-worker"
        assert json.loads(job["output_json"]) == {}
    finally:
        conn.close()


def test_copyability_worker_run_type_includes_worker_identity():
    first = _copyability_worker_ingest_type(0, "nas-copyability-0-host")
    second = _copyability_worker_ingest_type(0, "nas-copyability-worker-1")

    assert first == "copyability_evidence_worker_0_nas_copyability_0_host"
    assert second == "copyability_evidence_worker_0_nas_copyability_worker_1"
    assert first != second


def test_copyability_rescore_cannot_bypass_paper_evidence_gate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                cumulative_win_rate=0.72,
                recent_30d_volume_usdc=750_000,
                net_pnl_usdc=250_000,
                total_volume_usdc=5_000_000,
                event_win_rate=0.88,
                trade_win_rate=0.58,
                avg_dca_entries=25,
                sell_pct=2,
                bot_score=45,
                maker_fraction=0.1,
                leader_in_degree=8,
                copy_event_count=40,
                copy_market_count=12,
                containment_pct_median=0.95,
                copy_stream_roi=0.025,
                edge_retention_pct=70,
                walk_forward_consistency_pct=60,
                survival_score=70,
                single_market_pnl_share=0.2,
                net_to_gross_exposure=0.7,
                hygiene_status="clean",
                primary_category="politics",
                extra={"paper_roi_after_slippage": 0.08},
            ),
        )
        conn.commit()

        assert copyability_evidence._score_wallet_after_copyability(
            conn,
            wallet=wallet,
            policy=policy,
            policy_version=str(policy["version"]),
        )
        missing_evidence = conn.execute(
            """
            SELECT review_stage, review_reason
            FROM leader_scores
            WHERE address = ?
            ORDER BY score_id DESC
            LIMIT 1
            """,
            (wallet,),
        ).fetchone()

        assert missing_evidence["review_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert missing_evidence["review_reason"] == "paper_evidence_tier_incomplete"

        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                      'deep_done', 'score_wallet', 0, 1000, 100)
            """,
            (wallet,),
        )
        assert copyability_evidence._score_wallet_after_copyability(
            conn,
            wallet=wallet,
            policy=policy,
            policy_version=str(policy["version"]),
        )
        ready = conn.execute(
            """
            SELECT review_stage, review_reason
            FROM leader_scores
            WHERE address = ?
            ORDER BY score_id DESC
            LIMIT 1
            """,
            (wallet,),
        ).fetchone()

        assert ready["review_stage"] in {
            CandidateStage.PAPER_CANDIDATE.value,
            CandidateStage.PAPER_APPROVED.value,
        }
        assert ready["review_reason"] != "paper_evidence_tier_incomplete"
        next_action = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()["next_action"]
        assert next_action == ""
    finally:
        conn.close()


def test_copyability_rescore_reopens_no_signal_observer_when_signal_is_validated(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "f" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 60, 'blocked_copyability', ?, '{}', '{}', 'test', 100)
            """,
            (wallet, COPYABILITY_OBSERVER_REVIEW_REASON),
        )
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                cumulative_win_rate=0.72,
                recent_30d_volume_usdc=750_000,
                net_pnl_usdc=250_000,
                total_volume_usdc=5_000_000,
                event_win_rate=0.88,
                trade_win_rate=0.58,
                avg_dca_entries=25,
                sell_pct=2,
                bot_score=45,
                leader_in_degree=8,
                copy_event_count=40,
                copy_market_count=12,
                containment_pct_median=0.95,
                copy_stream_roi=0.025,
                edge_retention_pct=70,
                walk_forward_consistency_pct=60,
                survival_score=70,
                single_market_pnl_share=0.2,
                net_to_gross_exposure=0.7,
                hygiene_status="clean",
                primary_category="politics",
                extra={"copy_backtest_trade_count": 40},
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                      'deep_done', 'score_wallet', 0, 1000, 900, 20, 100)
            """,
            (wallet,),
        )

        assert copyability_evidence._score_wallet_after_copyability(
            conn,
            wallet=wallet,
            policy=policy,
            policy_version=str(policy["version"]),
            graph_scan_mode="deep",
            pair_stats_written=8,
            qualified_pairs=2,
        )
        latest = conn.execute(
            """
            SELECT cw.candidate_stage, ls.review_stage, ls.review_reason
            FROM candidate_wallets cw
            JOIN leader_latest_scores ls ON ls.address = cw.address
            WHERE cw.address = ?
            """,
            (wallet,),
        ).fetchone()

        assert latest["candidate_stage"] in {
            CandidateStage.PAPER_CANDIDATE.value,
            CandidateStage.PAPER_APPROVED.value,
        }
        assert latest["candidate_stage"] == latest["review_stage"]
        assert latest["review_reason"] != COPYABILITY_OBSERVER_REVIEW_REASON
    finally:
        conn.close()


def test_copyability_rescore_cannot_bypass_directional_history_gate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                cumulative_win_rate=0.72,
                recent_30d_volume_usdc=750_000,
                net_pnl_usdc=250_000,
                total_volume_usdc=5_000_000,
                event_win_rate=0.88,
                trade_win_rate=0.58,
                avg_dca_entries=25,
                sell_pct=2,
                bot_score=45,
                maker_fraction=0.1,
                leader_in_degree=8,
                copy_event_count=40,
                copy_market_count=12,
                containment_pct_median=0.95,
                copy_stream_roi=0.025,
                edge_retention_pct=70,
                walk_forward_consistency_pct=60,
                survival_score=70,
                single_market_pnl_share=0.2,
                net_to_gross_exposure=0.7,
                hygiene_status="clean",
                primary_category="politics",
                extra={"paper_roi_after_slippage": 0.08},
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, updated_at
            ) VALUES (?, 'l1_light', 'summary_ready', 10, 0.2, 10,
                      'light_done', 'score_wallet', 0, 10, 100)
            """,
            (wallet,),
        )

        assert copyability_evidence._score_wallet_after_copyability(
            conn,
            wallet=wallet,
            policy=policy,
            policy_version=str(policy["version"]),
        )
        latest = conn.execute(
            "SELECT review_stage, review_reason FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()
        action = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()["next_action"]

        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "insufficient_directional_trades:10<100"
        assert action == ""
    finally:
        conn.close()


def test_copyability_worker_retries_locked_finish_run(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    calls = {"finish": 0}
    original_finish = copyability_evidence.finish_ingest_run
    try:
        run_migrations(conn)

        def flaky_finish(*args, **kwargs):
            calls["finish"] += 1
            if calls["finish"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return original_finish(*args, **kwargs)

        monkeypatch.setattr(copyability_evidence, "finish_ingest_run", flaky_finish)
        monkeypatch.setattr(copyability_evidence, "LOCK_RETRY_SLEEP_SECONDS", 0)

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=0,
            worker_id="finish-lock-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
        )
        row = conn.execute(
            "SELECT status, wallets_attempted FROM ingest_runs WHERE run_id = ?",
            (summary.run_id,),
        ).fetchone()

        assert summary.status == "ok"
        assert calls["finish"] == 2
        assert row["status"] == "ok"
        assert row["wallets_attempted"] == 0
    finally:
        conn.close()


def test_copyability_claim_requeues_owned_running_leftovers(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    old_running = "0x" + "1" * 40
    queued = "0x" + "2" * 40
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', ?, 0, ?,
                ?, ?, 1, 3, 0, '{}', '{}', '', 100, ?)
            """,
            [
                (JOB_TYPE, old_running, 1, "running", "worker-a", 4_000_000_000, 100),
                (JOB_TYPE, queued, 10, "queued", None, 0, 90),
            ],
        )
        conn.commit()

        claimed = _claim_copyability_job(
            conn,
            shard=0,
            worker_id="worker-a",
            lease_seconds=7_200,
            now=2_000,
        )
        rows = {
            row["wallet"]: dict(row)
            for row in conn.execute(
                """
                SELECT wallet, status, lease_owner, lease_until, attempts, last_error
                FROM pipeline_jobs
                WHERE job_type = ?
                """,
                (JOB_TYPE,),
            )
        }
        running_for_owner = [
            row
            for row in rows.values()
            if row["status"] == "running" and row["lease_owner"] == "worker-a"
        ]

        assert claimed is not None
        assert claimed["wallet"] == old_running
        assert len(running_for_owner) == 1
        assert rows[old_running]["status"] == "running"
        assert rows[old_running]["lease_until"] == 9_200
        assert rows[old_running]["attempts"] == 2
        assert claimed["last_error"] == ""
        assert rows[old_running]["last_error"] == ""
        assert rows[queued]["status"] == "queued"
        assert rows[queued]["lease_owner"] is None
    finally:
        conn.close()


def test_copyability_claim_recovers_expired_running_job_before_queued_work(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    expired_wallet = "0x" + "3" * 40
    queued_wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', ?, 0, ?,
                ?, ?, 1, 3, 0, '{}', '{}', ?, 100, ?)
            """,
            [
                (JOB_TYPE, expired_wallet, 100, "running", "expired-worker", 1_000, "expired attempt", 2_000),
                (JOB_TYPE, queued_wallet, 1, "queued", None, 0, "", 1_000),
            ],
        )
        conn.commit()

        claimed = _claim_copyability_job(
            conn,
            shard=0,
            worker_id="recovery-worker",
            lease_seconds=7_200,
            now=2_500,
        )

        assert claimed is not None
        assert claimed["wallet"] == expired_wallet
        assert claimed["last_error"] == ""
    finally:
        conn.close()


def test_copyability_planner_reprioritizes_existing_queued_jobs_by_latest_score(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    high = "0x" + "1" * 40
    low = "0x" + "2" * 40
    running = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet, score in [(high, 68), (low, 42), (running, 70)]:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, ?, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 10000)
                """,
                (wallet, score),
            )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 5, 0, ?,
                'worker', 20000, 1, 3, 0, '{}', '{}', '', 10000, 10000)
            """,
            [
                (JOB_TYPE, high, "queued"),
                (JOB_TYPE, low, "queued"),
                (JOB_TYPE, running, "running"),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(conn, limit=0, shard_count=1, now=12_000)
        rows = {
            row["wallet"]: dict(row)
            for row in conn.execute(
                "SELECT wallet, status, priority, updated_at FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }

        assert summary.jobs_enqueued == 0
        assert summary.jobs_reprioritized == 2
        assert rows[high]["priority"] == 3
        assert rows[high]["updated_at"] == 12_000
        assert rows[low]["priority"] == 20
        assert rows[low]["updated_at"] == 12_000
        assert rows[running]["priority"] == 5
        assert rows[running]["updated_at"] == 10_000
    finally:
        conn.close()


def test_copyability_planner_queues_score_blocked_wallets_missing_copyability(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    missing_copyability = "0x" + "4" * 40
    materiality_blocked = "0x" + "5" * 40
    thin_missing_copyability = "0x" + "6" * 40
    try:
        run_migrations(conn)
        for wallet in [missing_copyability, materiality_blocked, thin_missing_copyability]:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_data' WHERE address = ?",
                (wallet,),
            )
        conn.executemany(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 0, 'needs_data', ?, '{}', '{}', 'test', 10000)
            """,
            [
                (
                    missing_copyability,
                    "missing_required_score_components:"
                    "leader_in_degree,copy_event_count,copy_market_count,copy_stream_roi",
                ),
                (
                    materiality_blocked,
                    "insufficient_net_pnl_usdc:20.00<50.00",
                ),
                (
                    thin_missing_copyability,
                    "missing_required_score_components:copy_event_count",
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, ?, 'summary_ready', ?, 1.0, 10, 'deep_done', 'score_wallet',
                0, ?, ?, ?, 10000)
            """,
            [
                (missing_copyability, "l3_deep", 1200, 1200, 900, 12),
                (materiality_blocked, "l3_deep", 1200, 1200, 900, 12),
                (thin_missing_copyability, "l3_deep", 10, 10, 8, 2),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=12_000,
        )
        jobs = conn.execute(
            """
            SELECT wallet, priority, input_json
            FROM pipeline_jobs
            WHERE job_type = ?
            ORDER BY wallet
            """,
            (JOB_TYPE,),
        ).fetchall()
        input_data = json.loads(jobs[0]["input_json"])

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert [row["wallet"] for row in jobs] == [missing_copyability]
        assert jobs[0]["priority"] == 14
        assert input_data["planner_reason"] == "missing_copyability_components"
        assert input_data["candidate_stage"] == "needs_data"
        assert input_data["activity_count"] == 1200
        assert input_data["distinct_markets"] == 12
        assert input_data["graph_scan_mode"] == "light_missing_copyability"
        assert input_data["graph_max_leader_events"] == 600
        assert input_data["graph_max_followers_per_event"] == 80
    finally:
        conn.close()


def test_copyability_planner_queues_manual_review_wallets_missing_copyability(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    manual_missing = "0x" + "a" * 40
    thin_manual = "0x" + "b" * 40
    try:
        run_migrations(conn)
        for wallet in [manual_missing, thin_manual]:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
                (wallet,),
            )
        conn.executemany(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, 'needs_manual_review', 'borderline_score', '{}', '{}', 'test', 10000)
            """,
            [
                (manual_missing, 38.0),
                (thin_manual, 55.0),
            ],
        )
        conn.executemany(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, ?, 'summary_ready', ?, 1.0, 10, ?, 'score_wallet',
                0, ?, ?, ?, 10000)
            """,
            [
                (manual_missing, "l3_deep", 1200, "deep_done", 1200, 900, 12),
                (thin_manual, "l1_light", 10, "light_done", 10, 8, 2),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=12_000,
        )
        jobs = conn.execute(
            """
            SELECT wallet, priority, input_json
            FROM pipeline_jobs
            WHERE job_type = ?
            ORDER BY wallet
            """,
            (JOB_TYPE,),
        ).fetchall()
        input_data = json.loads(jobs[0]["input_json"])

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert [row["wallet"] for row in jobs] == [manual_missing]
        assert jobs[0]["priority"] == 14
        assert input_data["planner_reason"] == "manual_missing_copyability"
        assert input_data["candidate_stage"] == "needs_manual_review"
        assert input_data["graph_scan_mode"] == "deep"
    finally:
        conn.close()


def test_copyability_planner_requires_new_activity_for_stale_deep_rescan(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stale_done = "0x" + "7" * 40
    active_backlog = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=stale_done, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (stale_done,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 65, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 10000)
            """,
            (stale_done,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 500, 1.0, 10,
                'deep_done', 'score_wallet', 0, 500, 300, 10, 10000)
            """,
            (stale_done,),
        )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, ?,
                NULL, 0, 1, 3, 0, ?, ?, '', 100, 100, ?)
            """,
            [
                (
                    JOB_TYPE,
                    stale_done,
                    "done",
                    json.dumps({"graph_scan_mode": "deep"}),
                    json.dumps({"graph_scan_mode": "deep"}),
                    100,
                ),
                (JOB_TYPE, active_backlog, "queued", "{}", "{}", None),
            ],
        )
        conn.commit()

        quiet_summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=2,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        quiet_status = conn.execute(
            "SELECT status FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (JOB_TYPE, stale_done),
        ).fetchone()["status"]

        assert quiet_summary.targets_seen == 0
        assert quiet_summary.jobs_enqueued == 0
        assert quiet_status == "done"

        conn.execute(
            """
            INSERT INTO wallet_activity_watermarks(
                address, newest_timestamp, newest_activity_key, updated_at
            ) VALUES (?, 1234, 'new-event', 200)
            """,
            (stale_done,),
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=2,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        status = conn.execute(
            "SELECT status, input_json FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (JOB_TYPE, stale_done),
        ).fetchone()
        input_data = json.loads(status["input_json"])

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert summary.active_jobs == 1
        assert summary.available_slots == 1
        assert status["status"] == "queued"
        assert input_data["planner_reason"] == "new_activity_after_deep_scan"
        assert input_data["activity_newest_timestamp"] == 1234
    finally:
        conn.close()


def test_copyability_planner_rescans_needs_data_near_miss_after_new_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_data' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 44, 'needs_data', ?, '{}', '{}', 'test', 10000)
            """,
            (wallet, COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 500, 1.0, 10,
                'deep_done', 'score_wallet', 0, 500, 300, 10, 10000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, '{"copy_candidate_event_count":12,"copy_candidate_market_count":5}', 10000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'done',
                NULL, 0, 1, 3, 0, ?, ?, '', 100, 100, 100)
            """,
            (
                JOB_TYPE,
                wallet,
                json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1234}),
                json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1234}),
            ),
        )
        conn.commit()

        quiet = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        assert quiet.jobs_enqueued == 0

        conn.execute(
            """
            INSERT INTO wallet_activity_watermarks(
                address, newest_timestamp, newest_activity_key, updated_at
            ) VALUES (?, 1235, 'new-event', 200)
            """,
            (wallet,),
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        job = conn.execute(
            "SELECT status, input_json FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (JOB_TYPE, wallet),
        ).fetchone()

        assert summary.jobs_enqueued == 1
        assert job["status"] == "queued"
        assert json.loads(job["input_json"])["planner_reason"] == "new_activity_after_deep_scan"
    finally:
        conn.close()


def test_copyability_planner_rescans_no_signal_observer_after_new_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    observer = "0x" + "7" * 40
    quiet_observer = "0x" + "8" * 40
    true_block = "0x" + "9" * 40
    fatal_paper_block = "0x" + "a" * 40
    try:
        run_migrations(conn)
        for wallet, reason in (
            (observer, COPYABILITY_OBSERVER_REVIEW_REASON),
            (quiet_observer, COPYABILITY_OBSERVER_REVIEW_REASON),
            (true_block, "hedge_or_arbitrage_exposure_too_low"),
            (fatal_paper_block, COPYABILITY_OBSERVER_REVIEW_REASON),
        ):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, 60, 'blocked_copyability', ?, '{}', '{}', 'test', 10000)
                """,
                (wallet, reason),
            )
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, non_fast_trade_count,
                    distinct_markets, updated_at
                ) VALUES (?, 'l3_deep', 'summary_ready', 500, 1.0, 10,
                    'deep_done', '', 0, 500, 300, 10, 10000)
                """,
                (wallet,),
            )
            conn.execute(
                "INSERT INTO wallet_features(address, hygiene_status, extra_json, updated_at) "
                "VALUES (?, 'clean', '{}', 10000)",
                (wallet,),
            )
            conn.execute(
                """
                INSERT INTO pipeline_jobs(
                    job_type, wallet, subject_key, tier, priority, shard, status,
                    lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                    input_json, output_json, last_error, created_at, updated_at, completed_at
                ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'done',
                    NULL, 0, 1, 3, 0, ?, ?, '', 100, 100, 100)
                """,
                (
                    JOB_TYPE,
                    wallet,
                    json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1000}),
                    json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1000}),
                ),
            )

        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      '["non_positive_total_roi"]', 100)
            """,
            (fatal_paper_block,),
        )
        conn.executemany(
            """
            INSERT INTO wallet_activity_watermarks(
                address, newest_timestamp, newest_activity_key, updated_at
            ) VALUES (?, ?, ?, 200)
            """,
            [
                (observer, 1001, "new-observer-event"),
                (quiet_observer, 1000, "same-observer-event"),
                (true_block, 1001, "new-blocked-event"),
                (fatal_paper_block, 1001, "new-paper-blocked-event"),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        rows = {
            row["wallet"]: dict(row)
            for row in conn.execute(
                "SELECT wallet, status, input_json FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert rows[observer]["status"] == "queued"
        assert json.loads(rows[observer]["input_json"])["planner_reason"] == (
            "new_activity_after_deep_scan"
        )
        assert rows[quiet_observer]["status"] == "done"
        assert rows[true_block]["status"] == "done"
        assert rows[fatal_paper_block]["status"] == "done"

        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'done',
                input_json = ?,
                output_json = ?,
                completed_at = 9950,
                updated_at = 9950
            WHERE job_type = ? AND wallet = ?
            """,
            (
                json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1001}),
                json.dumps({"graph_scan_mode": "deep", "activity_newest_timestamp": 1001}),
                JOB_TYPE,
                observer,
            ),
        )
        conn.commit()

        unchanged = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        assert unchanged.jobs_enqueued == 0

        conn.execute(
            """
            UPDATE wallet_activity_watermarks
            SET newest_timestamp = 1002, newest_activity_key = 'next-event', updated_at = 10000
            WHERE address = ?
            """,
            (observer,),
        )
        conn.commit()
        cooling_down = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        assert cooling_down.jobs_enqueued == 0

        ready_again = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_051,
        )
        assert ready_again.jobs_enqueued == 1
    finally:
        conn.close()


def test_migration_repairs_legacy_deep_near_miss_reason(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "0" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_data' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 44, 'needs_data', 'score_below_watchlist_after_evidence',
                      '{"score":44}', '{}', 'test', 100)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, '{"copy_candidate_pair_count":1,"copy_candidate_event_count":10}', 100)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0,
                      'done', NULL, 0, 1, 3, 0, '{"graph_scan_mode":"deep"}',
                      '{"graph_scan_mode":"deep","graph":{"pair_stats_written":2,"qualified_pairs":0}}',
                      '', 100, 100, 100)
            """,
            (wallet,),
        )
        conn.execute("DELETE FROM schema_migrations WHERE version = 52")
        conn.commit()

        assert run_migrations(conn) == [52]
        scores = conn.execute(
            """
            SELECT review_reason
            FROM leader_scores
            WHERE address = ?
            ORDER BY score_id
            """,
            (wallet,),
        ).fetchall()

        assert [row["review_reason"] for row in scores] == [
            "score_below_watchlist_after_evidence",
            COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON,
        ]
    finally:
        conn.close()


def test_copyability_planner_upgrades_high_score_light_no_signal_to_deep_scan(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    high_light = "0x" + "1" * 40
    low_light = "0x" + "2" * 40
    active_backlog = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet, score in [(high_light, 56.0), (low_light, 48.0)]:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, ?, 'needs_manual_review', 'borderline_score', '{}', '{}', 'test', 10000)
                """,
                (wallet, score),
            )
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, non_fast_trade_count,
                    distinct_markets, updated_at
                ) VALUES (?, 'l2_medium', 'summary_ready', 700, 1.0, 10,
                    'medium_done', 'score_wallet', 0, 700, 650, 10, 10000)
                """,
                (wallet,),
            )
            upsert_wallet_feature(
                conn,
                WalletFeatures(
                    address=wallet,
                    cumulative_win_rate=0.55,
                    recent_30d_volume_usdc=100_000,
                    net_pnl_usdc=10_000,
                    total_volume_usdc=500_000,
                    event_win_rate=0.55,
                    trade_win_rate=0.52,
                    avg_dca_entries=5,
                    sell_pct=10,
                    bot_score=30,
                    maker_fraction=0.1,
                    leader_in_degree=0,
                    copy_event_count=0,
                    copy_market_count=0,
                    containment_pct_median=0,
                    copy_stream_roi=0,
                    survival_score=50,
                    single_market_pnl_share=0.2,
                    net_to_gross_exposure=0.8,
                    hygiene_status="clean",
                    primary_category="politics",
                    extra={
                        "copy_candidate_pair_count": 0,
                        "copy_candidate_event_count": 0,
                        "copy_candidate_market_count": 0,
                        "copy_validated_pair_count": 0,
                    },
                ),
            )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'copyability', 'copyability', 16, 0, ?,
                NULL, 0, 1, 3, 0, ?, ?, '', 100, 100, ?)
            """,
            [
                (
                    JOB_TYPE,
                    high_light,
                    "done",
                    json.dumps({"graph_scan_mode": "light_missing_copyability"}),
                    json.dumps({"graph_scan_mode": "light_missing_copyability"}),
                    100,
                ),
                (
                    JOB_TYPE,
                    low_light,
                    "done",
                    json.dumps({"graph_scan_mode": "light_missing_copyability"}),
                    json.dumps({"graph_scan_mode": "light_missing_copyability"}),
                    100,
                ),
                (
                    JOB_TYPE,
                    active_backlog,
                    "queued",
                    "{}",
                    "{}",
                    None,
                ),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=21_600,
            now=10_000,
        )
        rows = {
            row["wallet"]: row
            for row in conn.execute(
                "SELECT wallet, status, priority, input_json FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            ).fetchall()
        }
        high_input = json.loads(rows[high_light]["input_json"])
        low_input = json.loads(rows[low_light]["input_json"])

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert rows[high_light]["status"] == "queued"
        assert rows[high_light]["priority"] == 8
        assert high_input["planner_reason"] == "light_no_signal_deep_rescan"
        assert high_input["graph_scan_mode"] == "deep"
        assert rows[low_light]["status"] == "done"
        assert low_input["graph_scan_mode"] == "light_missing_copyability"
    finally:
        conn.close()


def test_copyability_planner_reopens_failed_job_only_after_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 60, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 1000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l2_medium', 'summary_ready', 500, 1.0, 10,
                      'medium_done', 'score_wallet', 0, 500, 450, 8, 1000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'failed',
                      NULL, 0, 3, 3, 20000, '{}', '{}', 'graph refresh failed', 1000, 1000)
            """,
            (JOB_TYPE, wallet),
        )
        conn.commit()

        deferred = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=19_999,
        )
        before = conn.execute(
            "SELECT status, attempts, next_attempt_at, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert deferred.targets_seen == 0
        assert dict(before) == {
            "status": "failed",
            "attempts": 3,
            "next_attempt_at": 20_000,
            "last_error": "graph refresh failed",
        }

        reopened = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=20_000,
        )
        after = conn.execute(
            "SELECT status, attempts, next_attempt_at, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert reopened.targets_seen == 1
        assert reopened.jobs_enqueued == 1
        assert dict(after) == {
            "status": "queued",
            "attempts": 0,
            "next_attempt_at": 0,
            "last_error": "graph refresh failed",
        }
    finally:
        conn.close()


def test_copyability_planner_short_circuits_when_active_backlog_exceeds_limit(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stale_done = "0x" + "9" * 40
    active_one = "0x" + "a" * 40
    active_two = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=stale_done, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (stale_done,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 70, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 10000)
            """,
            (stale_done,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 500, 1.0, 10,
                'deep_done', 'score_wallet', 0, 500, 300, 10, 10000)
            """,
            (stale_done,),
        )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, ?,
                NULL, 0, 1, 3, 0, '{}', '{}', '', 100, 100, ?)
            """,
            [
                (JOB_TYPE, stale_done, "done", 100),
                (JOB_TYPE, active_one, "queued", None),
                (JOB_TYPE, active_two, "running", None),
            ],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=1,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            rescan_seconds=100,
            now=10_000,
        )
        status = conn.execute(
            "SELECT status FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (JOB_TYPE, stale_done),
        ).fetchone()["status"]

        assert summary.status == "backlog_active"
        assert summary.targets_seen == 0
        assert summary.jobs_enqueued == 0
        assert summary.active_jobs == 2
        assert summary.max_active_jobs == 1
        assert summary.available_slots == 0
        assert summary.throttled is True
        assert summary.reason == "active_queue_waterline"
        assert status == "done"
    finally:
        conn.close()


def test_copyability_planner_fills_only_remaining_active_queue_slots(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    first = "0x" + "1" * 40
    second = "0x" + "2" * 40
    active = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet, score in ((first, 70), (second, 69)):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, ?, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 10000)
                """,
                (wallet, score),
            )
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, non_fast_trade_count,
                    distinct_markets, updated_at
                ) VALUES (?, 'l3_deep', 'summary_ready', 500, 1.0, 10,
                    'deep_done', 'score_wallet', 0, 500, 300, 10, 10000)
                """,
                (wallet,),
            )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'queued',
                NULL, 0, 0, 3, 0, '{}', '{}', '', 100, 100)
            """,
            (JOB_TYPE, active),
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=2,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=20_000,
        )
        queued = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM pipeline_jobs
            WHERE job_type = ?
              AND status IN ('queued', 'running')
            """,
            (JOB_TYPE,),
        ).fetchone()["count"]

        assert summary.status == "ok"
        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        assert summary.active_jobs == 1
        assert summary.max_active_jobs == 2
        assert summary.available_slots == 1
        assert summary.throttled is False
        assert queued == 2
    finally:
        conn.close()


def test_copyability_planner_checks_capacity_inside_write_transaction(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    observed_transactions: list[bool] = []
    observed_selection_transactions: list[bool] = []
    real_active_count = copyability_evidence._active_copyability_job_count
    real_select_targets = copyability_evidence._select_copyability_targets

    def observing_active_count(conn_arg):
        observed_transactions.append(conn_arg.in_transaction)
        return real_active_count(conn_arg)

    def observing_select_targets(conn_arg, **kwargs):
        observed_selection_transactions.append(conn_arg.in_transaction)
        return real_select_targets(conn_arg, **kwargs)

    try:
        run_migrations(conn)
        monkeypatch.setattr(
            copyability_evidence,
            "_active_copyability_job_count",
            observing_active_count,
        )
        monkeypatch.setattr(
            copyability_evidence,
            "_select_copyability_targets",
            observing_select_targets,
        )

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=50,
            shard_count=1,
            now=20_000,
        )

        assert summary.status == "ok"
        assert observed_selection_transactions == [False]
        assert observed_transactions == [True]
        assert conn.in_transaction is False
    finally:
        conn.close()


def test_copyability_planner_revalidates_latest_score_before_enqueue(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    real_select_targets = copyability_evidence._select_copyability_targets
    selection_calls = 0

    def lower_score_after_prefetch(conn_arg, **kwargs):
        nonlocal selection_calls
        rows = real_select_targets(conn_arg, **kwargs)
        selection_calls += 1
        if selection_calls == 1:
            conn_arg.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, 10, 'needs_manual_review', 'score_dropped', '{}', '{}', 'test', 11000)
                """,
                (wallet,),
            )
            conn_arg.commit()
        return rows

    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 70, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 10000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l1_light', 'queued', 100, 0.7, 10,
                'light_done', 'medium_pending', 0, 100, 80, 4, 10000)
            """,
            (wallet,),
        )
        conn.commit()
        monkeypatch.setattr(
            copyability_evidence,
            "_select_copyability_targets",
            lower_score_after_prefetch,
        )

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=12_000,
        )
        queued = int(
            conn.execute(
                "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
                (JOB_TYPE, wallet),
            ).fetchone()[0]
        )

        assert selection_calls == 2
        assert summary.targets_seen == 0
        assert summary.jobs_enqueued == 0
        assert queued == 0
    finally:
        conn.close()


def test_copyability_worker_uses_per_job_light_scan_bounds(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    captured: dict[str, int] = {}
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'queued',
                NULL, 0, 0, 3, 0, ?, '{}', '', 100, 100)
            """,
            (
                JOB_TYPE,
                wallet,
                json.dumps(
                    {
                        "graph_scan_mode": "light_missing_copyability",
                        "graph_max_leader_events": 123,
                        "graph_max_followers_per_event": 17,
                    }
                ),
            ),
        )
        conn.commit()

        def fake_graph(
            conn_arg,
            policy,
            leaders,
            *,
            max_leader_events,
            max_followers_per_event,
            now,
            commit=True,
        ):
            captured["max_leader_events"] = max_leader_events
            captured["max_followers_per_event"] = max_followers_per_event
            return TargetedCopyGraphSummary(
                leaders_seen=1,
                links_written=0,
                pair_stats_written=0,
                leader_stats_written=0,
                qualified_pairs=0,
            )

        def fake_backtest(
            conn_arg,
            policy,
            leaders,
            *,
            now=None,
            commit=True,
            preserve_existing_on_empty=False,
        ):
            return TargetedCopyBacktestSummary(
                leaders_seen=1,
                trades_written=0,
                leader_performance_written=0,
                leaders_with_positive_net_roi=0,
            )

        monkeypatch.setattr(copyability_evidence, "mine_copy_graph_for_leaders", fake_graph)
        monkeypatch.setattr(copyability_evidence, "backtest_copy_stream_for_leaders", fake_backtest)
        monkeypatch.setattr(copyability_evidence, "materialize_wallet_feature", lambda *args, **kwargs: True)

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="test-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
            max_leader_events=3_000,
            max_followers_per_event=200,
        )
        job = conn.execute(
            "SELECT status, output_json FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (JOB_TYPE, wallet),
        ).fetchone()
        output = json.loads(job["output_json"])

        assert summary.status == "ok"
        assert summary.jobs_succeeded == 1
        assert captured == {"max_leader_events": 123, "max_followers_per_event": 17}
        assert job["status"] == "done"
        assert output["graph_scan_mode"] == "light_missing_copyability"
        assert output["graph_max_leader_events"] == 123
        assert output["graph_max_followers_per_event"] == 17
    finally:
        conn.close()


def test_copyability_worker_does_not_prefer_light_scan_over_higher_priority_jobs(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    legacy = "0x" + "8" * 40
    light = "0x" + "9" * 40
    captured: dict[str, str] = {}
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', ?, 0, 'queued',
                NULL, 0, 0, 3, 0, ?, '{}', '', 100, 100)
            """,
            [
                (JOB_TYPE, legacy, 1, "{}"),
                (
                    JOB_TYPE,
                    light,
                    20,
                    json.dumps({"graph_scan_mode": "light_missing_copyability"}),
                ),
            ],
        )
        conn.commit()

        def fake_graph(
            conn_arg,
            policy,
            leaders,
            *,
            max_leader_events,
            max_followers_per_event,
            now,
            commit=True,
        ):
            captured["wallet"] = leaders[0]
            return TargetedCopyGraphSummary(
                leaders_seen=1,
                links_written=0,
                pair_stats_written=0,
                leader_stats_written=0,
                qualified_pairs=0,
            )

        def fake_backtest(
            conn_arg,
            policy,
            leaders,
            *,
            now=None,
            commit=True,
            preserve_existing_on_empty=False,
        ):
            return TargetedCopyBacktestSummary(
                leaders_seen=1,
                trades_written=0,
                leader_performance_written=0,
                leaders_with_positive_net_roi=0,
            )

        monkeypatch.setattr(copyability_evidence, "mine_copy_graph_for_leaders", fake_graph)
        monkeypatch.setattr(copyability_evidence, "backtest_copy_stream_for_leaders", fake_backtest)
        monkeypatch.setattr(copyability_evidence, "materialize_wallet_feature", lambda *args, **kwargs: True)

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="test-light-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
            prefer_scan_mode="light_missing_copyability",
        )
        rows = {
            row["wallet"]: row["status"]
            for row in conn.execute(
                "SELECT wallet, status FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }

        assert summary.jobs_succeeded == 1
        assert captured["wallet"] == legacy
        assert rows == {legacy: "done", light: "queued"}
    finally:
        conn.close()


def test_copyability_worker_can_prefer_light_scan_with_equal_priority(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    legacy = "0x" + "8" * 40
    light = "0x" + "9" * 40
    captured: dict[str, str] = {}
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'queued',
                NULL, 0, 0, 3, 0, ?, '{}', '', 100, 100)
            """,
            [
                (JOB_TYPE, legacy, "{}"),
                (JOB_TYPE, light, json.dumps({"graph_scan_mode": "light_missing_copyability"})),
            ],
        )
        conn.commit()

        def fake_graph(
            conn_arg,
            policy,
            leaders,
            *,
            max_leader_events,
            max_followers_per_event,
            now,
            commit=True,
        ):
            captured["wallet"] = leaders[0]
            return TargetedCopyGraphSummary(
                leaders_seen=1,
                links_written=0,
                pair_stats_written=0,
                leader_stats_written=0,
                qualified_pairs=0,
            )

        def fake_backtest(
            conn_arg,
            policy,
            leaders,
            *,
            now=None,
            commit=True,
            preserve_existing_on_empty=False,
        ):
            return TargetedCopyBacktestSummary(
                leaders_seen=1,
                trades_written=0,
                leader_performance_written=0,
                leaders_with_positive_net_roi=0,
            )

        monkeypatch.setattr(copyability_evidence, "mine_copy_graph_for_leaders", fake_graph)
        monkeypatch.setattr(copyability_evidence, "backtest_copy_stream_for_leaders", fake_backtest)
        monkeypatch.setattr(copyability_evidence, "materialize_wallet_feature", lambda *args, **kwargs: True)

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="test-light-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
            prefer_scan_mode="light_missing_copyability",
        )
        rows = {
            row["wallet"]: row["status"]
            for row in conn.execute(
                "SELECT wallet, status FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }

        assert summary.jobs_succeeded == 1
        assert captured["wallet"] == light
        assert rows == {legacy: "queued", light: "done"}
    finally:
        conn.close()


def test_copyability_worker_blocks_completed_deep_scan_with_no_signal(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 60, 'blocked_copyability', ?, '{}', '{}', 'test', 100)
            """,
            (wallet, COPYABILITY_OBSERVER_REVIEW_REASON),
        )
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                cumulative_win_rate=0.72,
                recent_30d_volume_usdc=750_000,
                net_pnl_usdc=250_000,
                total_volume_usdc=5_000_000,
                event_win_rate=0.88,
                trade_win_rate=0.58,
                avg_dca_entries=25,
                sell_pct=2,
                bot_score=45,
                maker_fraction=0.1,
                leader_in_degree=0,
                copy_event_count=0,
                copy_market_count=0,
                containment_pct_median=0,
                copy_stream_roi=0,
                survival_score=70,
                single_market_pnl_share=0.2,
                net_to_gross_exposure=0.7,
                hygiene_status="clean",
                primary_category="politics",
                extra={
                    "copy_candidate_pair_count": 0,
                    "copy_candidate_event_count": 0,
                    "copy_candidate_market_count": 0,
                    "copy_validated_pair_count": 0,
                },
            ),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 0, 'queued',
                NULL, 0, 0, 3, 0, ?, '{}', '', 100, 100)
            """,
            (JOB_TYPE, wallet, json.dumps({"graph_scan_mode": "deep"})),
        )
        conn.commit()

        def fake_graph(
            conn_arg,
            policy,
            leaders,
            *,
            max_leader_events,
            max_followers_per_event,
            now,
            commit=True,
        ):
            return TargetedCopyGraphSummary(
                leaders_seen=1,
                links_written=0,
                pair_stats_written=0,
                leader_stats_written=0,
                qualified_pairs=0,
            )

        def fake_backtest(
            conn_arg,
            policy,
            leaders,
            *,
            now=None,
            commit=True,
            preserve_existing_on_empty=False,
        ):
            return TargetedCopyBacktestSummary(
                leaders_seen=1,
                trades_written=0,
                leader_performance_written=0,
                leaders_with_positive_net_roi=0,
            )

        monkeypatch.setattr(copyability_evidence, "mine_copy_graph_for_leaders", fake_graph)
        monkeypatch.setattr(copyability_evidence, "backtest_copy_stream_for_leaders", fake_backtest)
        monkeypatch.setattr(copyability_evidence, "materialize_wallet_feature", lambda *args, **kwargs: True)

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="test-no-signal-worker",
            policy_path=str(Path("config/leader_scoring_policy.json")),
        )
        latest = conn.execute(
            """
            SELECT cw.candidate_stage, ls.review_stage, ls.review_reason
            FROM candidate_wallets cw
            JOIN leader_scores ls
              ON ls.score_id = (
                  SELECT score_id
                  FROM leader_scores
                  WHERE address = cw.address
                  ORDER BY score_id DESC
                  LIMIT 1
              )
            WHERE cw.address = ?
            """,
            (wallet,),
        ).fetchone()
        output = json.loads(
            conn.execute(
                "SELECT output_json FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
                (JOB_TYPE, wallet),
            ).fetchone()["output_json"]
        )

        assert summary.jobs_succeeded == 1
        assert summary.scores_written == 1
        assert summary.no_signal_blocks == 1
        assert latest["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert latest["review_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert latest["review_reason"] == "copyability_scan_no_signal"
        assert output["score_written"] is True
        assert output["no_signal_blocked"] == 1
    finally:
        conn.close()


def test_deep_near_miss_score_uses_rescannable_reason(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        upsert_wallet_feature(conn, WalletFeatures(address=wallet, hygiene_status="clean"))
        monkeypatch.setattr(
            copyability_evidence,
            "score_candidate",
            lambda candidate, features, policy: ScoreBreakdown(
                address=wallet,
                leader_score=44,
                stage=CandidateStage.NEEDS_DATA,
                reason="score_below_watchlist_after_evidence",
                components={},
                penalties={},
            ),
        )
        monkeypatch.setattr(
            copyability_evidence,
            "apply_score_lifecycle_guards",
            lambda conn_arg, score, policy: score,
        )

        written = copyability_evidence._score_wallet_after_copyability(
            conn,
            wallet=wallet,
            policy={},
            policy_version="test",
            graph_scan_mode="deep",
            pair_stats_written=2,
            qualified_pairs=0,
        )
        latest = conn.execute(
            "SELECT review_stage, review_reason FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert written is True
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON
    finally:
        conn.close()


def test_copyability_queue_refreshes_targeted_graph_backtest_and_features(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "a" * 40
    follower = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (leader,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 56, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 20000)
            """,
            (leader,),
        )

        leader_events = []
        follower_events = []
        for idx in range(5):
            opened = {
                "timestamp": 10_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"market-{idx}",
                "asset": f"asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0xleaderbuy{idx}",
            }
            closed = {
                **opened,
                "timestamp": opened["timestamp"] + 50,
                "side": "SELL",
                "price": 0.8,
                "usdcSize": 8,
                "transactionHash": f"0xleadersell{idx}",
            }
            copied = {
                **opened,
                "timestamp": opened["timestamp"] + 5,
                "transactionHash": f"0xfollowerbuy{idx}",
            }
            leader_events.extend([opened, closed])
            follower_events.append(copied)

        persist_wallet_activity(conn, leader, leader_events, ingested_at=20_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=20_000)
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, current_stage,
                activity_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 'deep_done', 5, 20000)
            """,
            (follower,),
        )
        rebuild_wallet_episodes(conn, leader)
        conn.commit()

        plan = plan_copyability_evidence_jobs(
            conn,
            limit=10,
            min_score=40,
            min_activity_events=1,
            shard_count=1,
            now=30_000,
        )
        before = copyability_evidence_job_status(conn)
        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=2,
            policy_path=str(Path("config/leader_scoring_policy.json")),
        )
        after = copyability_evidence_job_status(conn)
        pair = conn.execute(
            "SELECT * FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()
        perf = conn.execute(
            "SELECT * FROM copy_leader_performance WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()
        features = get_wallet_features(conn)
        latest_score = conn.execute(
            """
            SELECT policy_version, review_stage, review_reason
            FROM leader_scores
            WHERE address = ?
            ORDER BY score_id DESC
            LIMIT 1
            """,
            (leader,),
        ).fetchone()
        job_output = json.loads(
            conn.execute(
                "SELECT output_json FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
                (JOB_TYPE, leader),
            ).fetchone()["output_json"]
        )

        assert plan.status == "ok"
        assert plan.jobs_enqueued == 1
        assert before["statuses"] == [
            {"job_type": "copyability_evidence", "status": "queued", "count": 1}
        ]
        assert summary.status == "ok"
        assert summary.jobs_succeeded == 1
        assert summary.links_written == 5
        assert summary.qualified_pairs == 1
        assert summary.backtest_trades_written == 5
        assert summary.features_materialized == 1
        assert summary.scores_written == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM copy_trade_links WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()[0] == 5
        assert pair["qualifies"] == 1
        assert perf["backtest_trade_count"] == 5
        assert features[leader].copy_event_count == 5
        assert features[leader].copy_market_count == 5
        assert features[leader].copy_stream_roi is not None
        assert features[leader].copy_stream_roi > 0
        assert latest_score["policy_version"] == load_policy(Path("config/leader_scoring_policy.json"))["version"]
        assert latest_score["review_stage"]
        assert latest_score["review_reason"] != "watchlist_score"
        assert job_output["score_written"] is True
        assert job_output["raw_links_pruned"] == 0
        assert after["statuses"] == [
            {"job_type": "copyability_evidence", "status": "done", "count": 1}
        ]
    finally:
        conn.close()


def test_copyability_single_worker_normalizes_old_shard_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 1, 'queued',
                NULL, 0, 0, 3, 0, '{}', '{}', '', 100, 100)
            """,
            (JOB_TYPE, "0x" + "c" * 40),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 2, 'running',
                'old-worker', 100, 1, 3, 0, '{}', '{}', '', 100, 100)
            """,
            (JOB_TYPE, "0x" + "d" * 40),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'copyability', 'copyability', 10, 2, 'done',
                NULL, 0, 1, 3, 0, '{}', '{}', '', 100, 100, 120)
            """,
            (JOB_TYPE, "0x" + "e" * 40),
        )
        conn.commit()

        summary = run_copyability_evidence_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=0,
            policy_path=str(Path("config/leader_scoring_policy.json")),
        )
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT wallet, shard, status, lease_owner, lease_until
                FROM pipeline_jobs
                WHERE job_type = ?
                ORDER BY wallet
                """,
                (JOB_TYPE,),
            )
        ]

        assert summary.status == "ok"
        assert rows == [
            {
                "wallet": "0x" + "c" * 40,
                "shard": 0,
                "status": "queued",
                "lease_owner": None,
                "lease_until": 0,
            },
            {
                "wallet": "0x" + "d" * 40,
                "shard": 0,
                "status": "queued",
                "lease_owner": None,
                "lease_until": 0,
            },
            {
                "wallet": "0x" + "e" * 40,
                "shard": 2,
                "status": "done",
                "lease_owner": None,
                "lease_until": 0,
            },
        ]
    finally:
        conn.close()
