import json
import sqlite3

from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.orchestration.pipeline_audit import pipeline_audit_report
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    enqueue_pipeline_job,
    materialize_wallet_processing_state,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


def _event(wallet: str, idx: int) -> dict:
    return {
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


def test_pipeline_audit_reports_missing_v2_tables_for_old_schema(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.sqlite")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_migrations(version) VALUES (25)")
        conn.execute(
            """
            CREATE TABLE candidate_wallets(
                address TEXT PRIMARY KEY,
                candidate_stage TEXT NOT NULL DEFAULT 'needs_data'
            )
            """
        )
        conn.commit()

        report = pipeline_audit_report(conn, now=50_000)

        assert report["schema"]["latest_migration"] == 25
        assert report["schema"]["v2_ready"] is False
        assert "wallet_processing_state" in report["schema"]["missing_tables"]
        assert report["issues"][0]["code"] == "schema_missing_v2_tables"
        assert report["next_steps"][0] == "run migrate before diagnosing current v2 wallet flow"
    finally:
        conn.close()


def test_pipeline_audit_uses_snapshot_without_leaving_transaction(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        assert conn.in_transaction is False

        report = pipeline_audit_report(conn, now=50_000)

        assert report["ok"] is True
        assert conn.in_transaction is False
    finally:
        conn.close()


def test_pipeline_audit_finds_pending_evidence_without_active_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        persist_wallet_activity(conn, wallet, [_event(wallet, idx) for idx in range(10)], ingested_at=20_000)
        materialize_wallet_processing_state(conn, limit=10, source="test")
        conn.commit()

        report = pipeline_audit_report(conn, top=5, now=50_000)

        assert report["schema"]["v2_ready"] is True
        assert report["funnel"]["candidates"]["total"] == 1
        assert report["funnel"]["evidence"]["pending_without_active_job"] == 1
        assert report["funnel"]["evidence"]["high_priority_pending_without_active_job"] == 0
        assert report["samples"]["pending_without_active_job"][0]["wallet"] == wallet
        assert not any(
            issue["code"] == "evidence_high_priority_pending_without_active_job"
            for issue in report["issues"]
        )

        enqueue_pipeline_job(
            conn,
                job_type="wallet_evidence_backfill",
                wallet=wallet,
                subject_key="light_pending",
                tier="l1_light",
            priority=10,
            shard=0,
            input_data={},
            now=50_010,
        )
        conn.commit()

        repaired = pipeline_audit_report(conn, top=5, now=50_020)
        assert repaired["funnel"]["evidence"]["pending_without_active_job"] == 0
    finally:
        conn.close()


def test_pipeline_audit_excludes_prefix_only_and_stale_policy_promotions(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    current_wallet = "0x" + "1" * 40
    old_policy_wallet = "0x" + "2" * 40
    prefix_only_wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for index, (wallet, policy_version, prefix_only) in enumerate(
            (
                (current_wallet, "current-policy", False),
                (old_policy_wallet, "old-policy", False),
                (prefix_only_wallet, "current-policy", True),
            )
        ):
            updated_at = 20_000 + index
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
            persist_wallet_activity(
                conn,
                wallet,
                [_event(wallet, event_index) for event_index in range(10)],
                ingested_at=updated_at,
            )
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, distinct_markets,
                    non_fast_trade_count, updated_at
                ) VALUES (?, 'l1_light', 'queued', 200, 0.7, 20, 'light_done',
                          'medium_pending', 0, 10, 5, 10, ?)
                """,
                (wallet, updated_at),
            )
            conn.execute(
                "INSERT INTO wallet_features(address, extra_json, updated_at) VALUES (?, ?, ?)",
                (
                    wallet,
                    json.dumps(
                        {
                            "feature_materializer_version": MATERIALIZER_VERSION,
                            "feature_materializer_activity_count": 10,
                        }
                    ),
                    updated_at,
                ),
            )
            stop_reason = (
                "promotion_approved:medium_pending:current-policy"
                if prefix_only
                else f"promotion_approved:medium_pending:{policy_version}:{updated_at}:10"
            )
            conn.execute(
                """
                INSERT INTO evidence_backfill_budget(
                    wallet, source, priority, stage, target_depth, current_depth,
                    next_attempt_at, stop_reason, evidence_json, created_at, updated_at
                ) VALUES (?, 'test', 20, 'medium_pending', 1000, 10, 0, ?, ?, ?, ?)
                """,
                (
                    wallet,
                    stop_reason,
                    json.dumps(
                        {
                            "promotion": {
                                "approved": True,
                                "job_action": "medium_pending",
                                "policy_version": policy_version,
                                "feature_updated_at": updated_at,
                                "activity_count": 10,
                                "materializer_version": MATERIALIZER_VERSION,
                            }
                        }
                    ),
                    updated_at,
                    updated_at,
                ),
            )
        conn.commit()

        report = pipeline_audit_report(
            conn,
            top=5,
            policy_version="current-policy",
            now=50_000,
        )

        assert report["funnel"]["evidence"]["pending_without_active_job"] == 1
        assert [
            row["wallet"] for row in report["samples"]["pending_without_active_job"]
        ] == [current_wallet]
    finally:
        conn.close()


def test_pipeline_audit_warns_on_high_priority_pending_without_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        persist_wallet_activity(conn, wallet, [_event(wallet, idx) for idx in range(10)], ingested_at=20_000)
        materialize_wallet_processing_state(conn, limit=10, source="test")
        conn.execute("UPDATE wallet_processing_state SET priority = 5 WHERE wallet = ?", (wallet,))
        conn.commit()

        report = pipeline_audit_report(conn, top=5, now=50_000)

        assert report["funnel"]["evidence"]["pending_without_active_job"] == 1
        assert report["funnel"]["evidence"]["high_priority_pending_without_active_job"] == 1
        assert report["samples"]["high_priority_pending_without_active_job"][0]["wallet"] == wallet
        assert any(
            issue["code"] == "evidence_high_priority_pending_without_active_job"
            for issue in report["issues"]
        )
    finally:
        conn.close()


def test_pipeline_audit_counts_latest_scores_for_current_candidates(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
            """,
            (wallet, 12.0, "needs_data", "old", 10),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
            """,
            (wallet, 72.0, "paper_candidate", "latest", 20),
        )
        conn.commit()

        report = pipeline_audit_report(conn, min_score=70.0, now=50_000)

        scoring = report["funnel"]["scoring"]
        assert scoring["latest_score_stage_counts"] == {"paper_candidate": 1}
        assert scoring["high_score_manual_review"] == 1
        assert scoring["candidate_stage_differs_latest_review"] == 1
    finally:
        conn.close()


def test_pipeline_audit_separates_review_and_paper_score_thresholds(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'needs_manual_review' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 50, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 20)
            """,
            (wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, min_score=40, paper_min_score=70, now=50_000)
        scoring = report["funnel"]["scoring"]

        assert report["review_min_score"] == 40
        assert report["paper_min_score"] == 70
        assert scoring["high_score_manual_review"] == 1
        assert scoring["paper_score_manual_review"] == 0
    finally:
        conn.close()


def test_pipeline_audit_flags_paper_stage_without_l3_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'live_eligible' WHERE address = ?",
            (wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, now=50_000)

        assert report["funnel"]["scoring"]["paper_stage_evidence_incomplete"] == 1
        assert report["samples"]["paper_stage_evidence_incomplete"][0]["wallet"] == wallet
        assert any(issue["code"] == "paper_stage_evidence_incomplete" for issue in report["issues"])

        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                      'score_wallet', 0, 1000, 20, 200, 20)
            """,
            (wallet,),
        )
        conn.commit()

        repaired = pipeline_audit_report(conn, now=50_020)
        assert repaired["funnel"]["scoring"]["paper_stage_evidence_incomplete"] == 0
    finally:
        conn.close()


def test_pipeline_audit_ignores_protected_stage_mismatch(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    rejected_wallet = "0x" + "9" * 40
    blocked_wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=rejected_wallet, sources="test_source"))
        upsert_candidate(conn, CandidateAddress(address=blocked_wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'rejected' WHERE address = ?",
            (rejected_wallet,),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
            (blocked_wallet,),
        )
        for wallet, stage in (
            (rejected_wallet, "needs_manual_review"),
            (blocked_wallet, "needs_data"),
        ):
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
                """,
                (wallet, 35.0, stage, "protected", 20),
            )
        conn.commit()

        report = pipeline_audit_report(conn, min_score=70.0, now=50_000)

        scoring = report["funnel"]["scoring"]
        assert scoring["candidate_stage_differs_latest_review"] == 0
    finally:
        conn.close()


def test_pipeline_audit_issues_only_actionable_candidate_missing_state(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    blocked_wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=blocked_wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (blocked_wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, now=50_000)

        candidates = report["funnel"]["candidates"]
        assert candidates["without_processing_state"] == 1
        assert candidates["active_without_processing_state"] == 0
        assert not any(issue["code"] == "candidate_missing_processing_state" for issue in report["issues"])
    finally:
        conn.close()


def test_pipeline_audit_grants_fresh_candidate_state_handoff_grace(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET first_seen_at = ?, updated_at = ? WHERE address = ?",
            (49_700, 49_700, wallet),
        )
        conn.commit()

        fresh_report = pipeline_audit_report(conn, now=50_000)
        fresh_candidates = fresh_report["funnel"]["candidates"]
        assert fresh_candidates["active_without_processing_state"] == 1
        assert fresh_candidates["active_without_processing_state_stale"] == 0
        assert fresh_candidates["handoff_grace_seconds"] == 600
        assert not any(
            issue["code"] == "candidate_missing_processing_state"
            for issue in fresh_report["issues"]
        )

        stale_report = pipeline_audit_report(conn, now=50_301)
        stale_candidates = stale_report["funnel"]["candidates"]
        assert stale_candidates["active_without_processing_state"] == 1
        assert stale_candidates["active_without_processing_state_stale"] == 1
        assert any(
            issue["code"] == "candidate_missing_processing_state" and issue["count"] == 1
            for issue in stale_report["issues"]
        )
    finally:
        conn.close()


def test_pipeline_audit_grants_fresh_score_handoff_grace_and_separates_stale_scores(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, current_stage,
                next_action, updated_at
            ) VALUES (?, 'l1_light', 'summary_ready', 'light_done', 'score_wallet', ?)
            """,
            (wallet, 49_700),
        )
        conn.commit()

        fresh_report = pipeline_audit_report(conn, now=50_000)
        fresh_evidence = fresh_report["funnel"]["evidence"]
        assert fresh_evidence["summary_ready_without_score"] == 1
        assert fresh_evidence["summary_ready_without_score_stale"] == 0
        assert fresh_evidence["summary_ready_score_stale"] == 0
        assert fresh_evidence["handoff_grace_seconds"] == 600
        assert not any(
            issue["code"] == "evidence_summary_ready_without_score"
            for issue in fresh_report["issues"]
        )

        unscored_report = pipeline_audit_report(conn, now=50_301)
        unscored_evidence = unscored_report["funnel"]["evidence"]
        assert unscored_evidence["summary_ready_without_score_stale"] == 1
        assert unscored_evidence["summary_ready_score_stale"] == 0
        assert any(
            issue["code"] == "evidence_summary_ready_without_score" and issue["count"] == 1
            for issue in unscored_report["issues"]
        )

        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 0, 'needs_data', 'test', '{}', '{}', 'test', ?)
            """,
            (wallet, 49_000),
        )
        conn.commit()

        scored_report = pipeline_audit_report(conn, now=50_301)
        scored_evidence = scored_report["funnel"]["evidence"]
        assert scored_evidence["summary_ready_without_score"] == 0
        assert scored_evidence["summary_ready_without_score_stale"] == 0
        assert scored_evidence["summary_ready_score_stale"] == 1
        assert not any(
            issue["code"] == "evidence_summary_ready_without_score"
            for issue in scored_report["issues"]
        )
        assert any(
            issue["code"] == "evidence_summary_ready_score_stale" and issue["count"] == 1
            for issue in scored_report["issues"]
        )
    finally:
        conn.close()


def test_pipeline_audit_ignores_stale_scores_for_blocked_candidates(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    blocked_wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=blocked_wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
            (blocked_wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, current_stage,
                next_action, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 'deep_done', 'score_wallet', ?)
            """,
            (blocked_wallet, 100),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
            """,
            (blocked_wallet, 25.0, "blocked_copyability", "protected", 50),
        )
        conn.commit()

        report = pipeline_audit_report(conn, now=50_000)

        evidence = report["funnel"]["evidence"]
        assert evidence["summary_ready_score_stale"] == 0
        assert report["samples"]["summary_ready_without_recent_score"] == []
        assert not any(issue["code"] == "evidence_summary_ready_score_stale" for issue in report["issues"])
    finally:
        conn.close()


def test_pipeline_audit_warns_only_after_score_stale_grace_window(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    recent_wallet = "0x" + "d" * 40
    stale_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        for wallet in (recent_wallet, stale_wallet):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_data' WHERE address = ?",
                (wallet,),
            )
        for wallet, updated_at, scored_at in (
            (recent_wallet, 950, 900),
            (stale_wallet, 100, 50),
        ):
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, current_stage,
                    next_action, updated_at
                ) VALUES (?, 'l3_deep', 'summary_ready', 'deep_done', 'score_wallet', ?)
                """,
                (wallet, updated_at),
            )
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
                """,
                (wallet, 0.0, "needs_data", "stale_test", scored_at),
            )
        conn.commit()

        report = pipeline_audit_report(conn, now=1_000)

        assert report["funnel"]["evidence"]["summary_ready_score_stale"] == 1
        assert report["samples"]["summary_ready_without_recent_score"][0]["wallet"] == stale_wallet
    finally:
        conn.close()


def test_pipeline_audit_excludes_summary_only_wallets_from_actionable_backlogs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    pending_wallet = "0x" + "1" * 40
    stale_wallet = "0x" + "2" * 40
    unscored_wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet in (pending_wallet, stale_wallet, unscored_wallet):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'needs_data' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                """
                INSERT INTO wallet_registry(
                    address, candidate_stage, registry_status, raw_retention_tier,
                    last_evaluated_at, updated_at
                ) VALUES (?, 'needs_data', 'archived_raw_pruned', 'summary_only', 100, 100)
                """,
                (wallet,),
            )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, current_stage,
                next_action, priority, updated_at
            ) VALUES (?, 'l1_light', 'needs_light', '', 'light_pending', 20, 200)
            """,
            (pending_wallet,),
        )
        for wallet in (stale_wallet, unscored_wallet):
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, current_stage,
                    next_action, updated_at
                ) VALUES (?, 'l3_deep', 'summary_ready', 'deep_done', 'score_wallet', 200)
                """,
                (wallet,),
            )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 0, 'needs_data', 'archived', '{}', '{}', 'test', 50)
            """,
            (stale_wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, now=1_000)

        evidence = report["funnel"]["evidence"]
        assert evidence["pending_without_active_job"] == 0
        assert evidence["summary_ready_without_score"] == 0
        assert evidence["summary_ready_score_stale"] == 0
        assert report["samples"]["pending_without_active_job"] == []
        assert report["samples"]["summary_ready_without_recent_score"] == []

        conn.execute(
            "UPDATE wallet_registry SET raw_retention_tier = 'summary_and_recent'"
        )
        conn.commit()

        reactivated_report = pipeline_audit_report(conn, now=1_000)
        reactivated_evidence = reactivated_report["funnel"]["evidence"]
        assert reactivated_evidence["pending_without_active_job"] == 1
        assert reactivated_evidence["summary_ready_without_score"] == 1
        assert reactivated_evidence["summary_ready_score_stale"] == 1
        assert len(reactivated_report["samples"]["pending_without_active_job"]) == 1
        assert len(reactivated_report["samples"]["summary_ready_without_recent_score"]) == 2
    finally:
        conn.close()


def test_pipeline_audit_flags_unvalidated_core_copy_credit(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                leader_in_degree=1,
                copy_event_count=18,
                copy_market_count=4,
                extra={
                    "copy_candidate_event_count": 18,
                    "copy_candidate_market_count": 4,
                    "copy_validated_pair_count": 0,
                },
            ),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 62, 'needs_manual_review', 'watchlist_score', '{}', '{}', 'test', 20)
            """,
            (wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, top=5, now=50_000)

        candidates = report["funnel"]["candidates"]
        assert candidates["unvalidated_core_copy_active"] == 1
        assert candidates["unvalidated_core_copy_all"] == 1
        assert any(issue["code"] == "candidate_unvalidated_core_copy_credit" for issue in report["issues"])
        assert report["samples"]["unvalidated_core_copy_credit"][0]["wallet"] == wallet
        assert any("copyability refresh" in step for step in report["next_steps"])

        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 1, 18, 4, 0.9, 10, 1, 20, 20)
            """,
            (wallet,),
        )
        conn.commit()

        repaired = pipeline_audit_report(conn, top=5, now=50_020)
        assert repaired["funnel"]["candidates"]["unvalidated_core_copy_active"] == 0
        assert not repaired["samples"]["unvalidated_core_copy_credit"]
    finally:
        conn.close()


def test_pipeline_audit_flags_non_standard_wallet_addresses(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    valid_wallet = "0x" + "9" * 40
    short_wallet = "0x" + "a" * 39
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=valid_wallet, sources="valid_source"))
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'bad_source', '', '', '', 'manual_research_seed', 'needs_data', 10, 10)
            """,
            (short_wallet,),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'bad_source', 'manual_research_seed', '', '', '', '{}', 10, 10)
            """,
            (short_wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard,
                status, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('wallet_evidence_backfill', ?, 'light_pending', 'l0_discovered', 10, 0,
                      'queued', 0, 0, 3, 0, '{}', '{}', '', 10, 10)
            """,
            (short_wallet,),
        )
        conn.commit()

        report = pipeline_audit_report(conn, top=5, now=50_000)

        quality = report["funnel"]["address_quality"]
        assert quality["invalid_address_rows"] == 3
        assert quality["by_table"]["candidate_wallets.address"] == 1
        assert quality["by_table"]["candidate_source_events.address"] == 1
        assert quality["by_table"]["pipeline_jobs.wallet"] == 1
        assert any(issue["code"] == "address_quality_invalid_wallet_rows" for issue in report["issues"])
        assert report["samples"]["invalid_address_rows"][0]["wallet"] == short_wallet
        assert any("repair or quarantine invalid wallet addresses" in step for step in report["next_steps"])
    finally:
        conn.close()
