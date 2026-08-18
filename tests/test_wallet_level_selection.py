from __future__ import annotations

import json
import sqlite3

import pytest

from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_level_selection import (
    PNL_METHODOLOGY_VERSION,
    SELECTION_POLICY_VERSION,
    reconcile_wallet_level_selections,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import (
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
)
from pm_robot.wallet_levels import WalletLevel


def _seed_wallet(conn, wallet: str, *, level: WalletLevel, source: str = "stream") -> None:
    record_wallet_sighting(
        conn,
        CandidateAddress(address=wallet, sources=source, labels="seed"),
        trusted_source=True,
        now=1_000,
    )
    current = WalletLevel.L1
    for target in (WalletLevel.L2, WalletLevel.L3, WalletLevel.L4):
        if target.value > level.value:
            break
        advance_wallet_level(conn, wallet, to_level=target, reason="test_seed", now=1_100)
        current = target
        if current is level:
            break


def _seed_summary(
    conn,
    wallet: str,
    *,
    depth: str,
    score: float,
    forward_score: float | None = None,
    strategy: str = "general",
    activity_count: int = 100,
    distinct_markets: int = 10,
    total_volume_usdc: float = 2_000,
    updated_at: int = 2_000,
) -> str:
    artifact_id = f"artifact-{wallet[-4:]}-{depth}"
    forward = score if forward_score is None else forward_score
    conn.execute(
        """
        INSERT INTO wallet_history_artifacts(
            artifact_id, wallet, history_depth, storage_version, relative_path,
            row_count, byte_size, checksum, status, created_at, updated_at
        ) VALUES (?, ?, ?, 'test', ?, 100, 10, 'checksum', 'active', ?, ?)
        """,
        (artifact_id, wallet, depth, f"test/{artifact_id}.parquet", updated_at, updated_at),
    )
    tags = [] if strategy == "general" else [strategy]
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count,
            distinct_markets, total_volume_usdc,
            strategy_tags_json, risk_flags_json, research_score,
            diagnostic_score, forward_selection_score,
            score_components_json, forward_score_components_json,
            methodology_version, computed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, '{}', '{}', ?, ?, ?)
        """,
        (
            wallet,
            artifact_id,
            depth,
            activity_count,
            distinct_markets,
            total_volume_usdc,
            json.dumps(tags),
            score,
            score,
            forward,
            METHODOLOGY_VERSION,
            updated_at,
            updated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO wallet_pnl_summaries(
            wallet, official_all_pnl_usdc, official_all_volume_usdc,
            official_profit_intensity, methodology_version, captured_at, updated_at
        ) VALUES (?, 100, 10000, 0.01, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            official_all_pnl_usdc = excluded.official_all_pnl_usdc,
            official_all_volume_usdc = excluded.official_all_volume_usdc,
            official_profit_intensity = excluded.official_profit_intensity,
            methodology_version = excluded.methodology_version,
            captured_at = excluded.captured_at,
            updated_at = excluded.updated_at
        """,
        (wallet, PNL_METHODOLOGY_VERSION, updated_at, updated_at),
    )
    return artifact_id


def _seed_selection_snapshot(
    conn,
    wallet: str,
    *,
    target_level: WalletLevel,
    score: float,
    selected: bool = True,
    decided_at: int = 1_500,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at,
            research_score, forward_selection_score, score_status
        ) VALUES (?, ?, ?, ?, ?, 1, 20, 'stream', 'general',
                  'test_transition_snapshot', ?, ?, ?, ?, 'valid')
        """,
        (
            wallet,
            target_level.value,
            f"transition-{target_level.value}-{wallet[-4:]}",
            SELECTION_POLICY_VERSION,
            int(selected),
            decided_at,
            decided_at,
            score,
            score,
        ),
    )


def _insert_selection_snapshot(
    conn,
    wallet: str,
    *,
    target_level: WalletLevel,
    artifact_id: str,
    score: float | None,
    forward_score: float | None | object = ...,
    decided_at: int,
) -> None:
    forward = score if forward_score is ... else forward_score
    status = "valid" if forward is not None else "legacy"
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at,
            research_score, forward_selection_score, score_status
        ) VALUES (?, ?, ?, ?, 1, 1, 20, 'stream', 'general',
                  'test_transition_snapshot', ?, ?, ?, ?, ?)
        """,
        (
            wallet,
            target_level.value,
            artifact_id,
            SELECTION_POLICY_VERSION,
            decided_at,
            decided_at,
            score,
            forward,
            status,
        ),
    )


def test_wallet_level_selection_reference_indexes_are_migrated(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        selection_index = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_wallet_level_selections_reference_wallet_latest",),
        ).fetchone()
        summary_index = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_wallet_history_summaries_method_rank",),
        ).fetchone()
        selection_columns = [
            row["name"]
            for row in conn.execute(
                "PRAGMA index_info(idx_wallet_level_selections_reference_wallet_latest)"
            )
        ]
        summary_columns = [
            row["name"]
            for row in conn.execute(
                "PRAGMA index_info(idx_wallet_history_summaries_method_rank)"
            )
        ]
    finally:
        conn.close()

    assert selection_columns == [
        "target_level",
        "policy_version",
        "wallet",
        "decided_at",
    ]
    assert "WHERE research_score IS NOT NULL" in str(selection_index["sql"])
    assert summary_columns == [
        "history_depth",
        "methodology_version",
        "research_score",
        "wallet",
    ]
    assert summary_index is not None


def test_historical_reference_rows_match_old_latest_decision_semantics_and_plan(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for index in range(250):
            wallet = "0x" + f"{index:040x}"
            _insert_selection_snapshot(
                conn,
                wallet,
                target_level=WalletLevel.L3,
                artifact_id=f"old-{index}",
                score=float(index),
                decided_at=1_000,
            )
            _insert_selection_snapshot(
                conn,
                wallet,
                target_level=WalletLevel.L3,
                artifact_id=f"same-ts-first-{index}",
                score=float(index + 1_000),
                decided_at=2_000,
            )
            _insert_selection_snapshot(
                conn,
                wallet,
                target_level=WalletLevel.L3,
                artifact_id=f"same-ts-rowid-wins-{index}",
                score=float(index + 2_000),
                decided_at=2_000,
            )
            _insert_selection_snapshot(
                conn,
                wallet,
                target_level=WalletLevel.L4,
                artifact_id=f"other-target-{index}",
                score=float(index + 3_000),
                decided_at=3_000,
            )
        null_score_wallet = "0x" + "f" * 40
        _insert_selection_snapshot(
            conn,
            null_score_wallet,
            target_level=WalletLevel.L3,
            artifact_id="null-score-latest",
            score=None,
            decided_at=9_000,
        )
        _insert_selection_snapshot(
            conn,
            null_score_wallet,
            target_level=WalletLevel.L3,
            artifact_id="nonnull-score-older",
            score=42.0,
            forward_score=None,
            decided_at=1_000,
        )
        conn.commit()

        import pm_robot.orchestration.wallet_level_selection as selection_module

        old_rows = conn.execute(
            """
            SELECT
                decision.wallet,
                decision.evidence_artifact_id AS artifact_id,
                decision.forward_selection_score,
                decision.research_score,
                '[]' AS strategy_tags_json,
                decision.updated_at,
                '' AS sources,
                decision.source_bucket,
                decision.strategy_bucket
            FROM wallet_level_selections AS decision
            WHERE decision.target_level = ?
              AND decision.policy_version = ?
              AND decision.forward_selection_score IS NOT NULL
              AND decision.score_status = 'valid'
              AND decision.rowid = (
                  SELECT prior.rowid
                  FROM wallet_level_selections AS prior
                  WHERE prior.wallet = decision.wallet
                    AND prior.target_level = decision.target_level
                    AND prior.policy_version = decision.policy_version
                    AND prior.forward_selection_score IS NOT NULL
                    AND prior.score_status = 'valid'
                  ORDER BY prior.decided_at DESC, prior.rowid DESC
                  LIMIT 1
              )
            ORDER BY decision.wallet
            """,
            (WalletLevel.L3.value, SELECTION_POLICY_VERSION),
        ).fetchall()
        new_rows = sorted(
            selection_module._historical_reference_rows(
                conn,
                target_level=WalletLevel.L3.value,
                policy_version=SELECTION_POLICY_VERSION,
            ),
            key=lambda row: row["wallet"],
        )
        plan = [
            str(row["detail"])
            for row in conn.execute(
                "EXPLAIN QUERY PLAN " + selection_module._REFERENCE_HISTORICAL_SQL,
                (WalletLevel.L3.value, SELECTION_POLICY_VERSION),
            )
        ]
    finally:
        conn.close()

    assert [dict(row) for row in new_rows] == [dict(row) for row in old_rows]
    assert len(new_rows) == 250
    assert all(
        str(row["artifact_id"]).startswith("same-ts-rowid-wins-")
        for row in new_rows
    )
    assert not any("CORRELATED" in detail.upper() for detail in plan)
    assert not any("TEMP B-TREE" in detail.upper() for detail in plan)
    assert any(
        "idx_wallet_level_selections_reference_wallet_latest" in detail
        or "idx_wallet_level_selections_forward_reference" in detail
        for detail in plan
    )


def test_empty_pending_cohort_does_not_load_reference_rows(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        # Large irrelevant summaries used to reproduce the production failure
        # mode where empty cohorts still paid reference/live scan cost.
        for index in range(1_000):
            wallet = "0x" + f"{index:040x}"
            conn.execute(
                """
                INSERT INTO wallet_history_summaries(
                    wallet, artifact_id, history_depth, activity_count,
                    distinct_markets, total_volume_usdc, strategy_tags_json,
                    risk_flags_json, research_score, diagnostic_score,
                    forward_selection_score, score_components_json,
                    forward_score_components_json, methodology_version,
                    computed_at, updated_at
                ) VALUES (?, ?, 'deep', 100, 5, 1000, '[]', '[]',
                          50, 50, 50, '{}', '{}', ?, 1_000, 1_000)
                """,
                (wallet, f"artifact-{index}", METHODOLOGY_VERSION),
            )
        conn.commit()

        import pm_robot.orchestration.wallet_level_selection as selection_module

        def fail_reference(*args, **kwargs):
            raise AssertionError("reference rows should not load for empty pending")

        monkeypatch.setattr(selection_module, "_reference_rows", fail_reference)
        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            now=10_000,
        )

        assert result.cohorts_processed == 0
        assert result.decisions_written == 0
    finally:
        conn.close()


def test_reconcile_commits_each_processed_cohort_so_other_level_writers_are_not_blocked(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    contender = connect(db_path, timeout_seconds=0.05)
    wallets = {
        "l4": ["0x" + f"{index:040x}" for index in range(1, 3)],
        "l3": ["0x" + f"{index:040x}" for index in range(10, 12)],
    }
    lock_errors: list[str] = []
    concurrent_writer_ok = False
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets["l4"], (90, 80)):
            _seed_wallet(conn, wallet, level=WalletLevel.L4)
            _seed_summary(conn, wallet, depth="deep", score=score)
        for wallet, score in zip(wallets["l3"], (70, 60)):
            _seed_wallet(conn, wallet, level=WalletLevel.L3)
            _seed_summary(conn, wallet, depth="deep", score=score)
        conn.commit()

        import pm_robot.orchestration.wallet_level_selection as selection_module

        original_selection_rows = selection_module._selection_rows
        calls = 0

        def selection_rows_with_contending_writer(*args, **kwargs):
            nonlocal calls, concurrent_writer_ok
            calls += 1
            if calls == 2:
                try:
                    ensure_wallet_level(
                        contender,
                        "0x" + "a" * 40,
                        reason="concurrent_discovery",
                        now=10_001,
                    )
                    contender.commit()
                    concurrent_writer_ok = True
                except sqlite3.OperationalError as exc:
                    lock_errors.append(str(exc))
                    contender.rollback()
            return original_selection_rows(*args, **kwargs)

        monkeypatch.setattr(
            selection_module,
            "_selection_rows",
            selection_rows_with_contending_writer,
        )

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l4_fraction=1.0,
            l5_fraction=1.0,
            l4_max_promotions=2,
            l5_max_promotions=2,
            now=10_000,
        )

        assert result.cohorts_processed == 2
        assert result.decisions_written == 4
        assert concurrent_writer_ok is True
        assert lock_errors == []
        assert get_wallet_level(contender, "0x" + "a" * 40).level is WalletLevel.L0
    finally:
        contender.close()
        conn.close()


def test_decision_batch_commit_allows_concurrent_discovery_before_cohort_end(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    contender = connect(db_path, timeout_seconds=0.05)
    wallets = ["0x" + f"{index:040x}" for index in range(1, 7)]
    concurrent_wallet = "0x" + "b" * 40
    lock_errors: list[str] = []
    concurrent_writer_ok = False
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets, (100, 90, 80, 70, 60, 50)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(conn, wallet, depth="light", score=score)
        conn.commit()

        import pm_robot.orchestration.wallet_level_selection as selection_module

        original_commit = selection_module._commit
        commit_calls = 0

        def commit_with_concurrent_discovery(commit_conn):
            nonlocal commit_calls, concurrent_writer_ok
            commit_calls += 1
            original_commit(commit_conn)
            if commit_calls == 1:
                try:
                    ensure_wallet_level(
                        contender,
                        concurrent_wallet,
                        reason="concurrent_discovery",
                        now=10_001,
                    )
                    contender.commit()
                    concurrent_writer_ok = True
                except sqlite3.OperationalError as exc:
                    lock_errors.append(str(exc))
                    contender.rollback()

        monkeypatch.setattr(selection_module, "_commit", commit_with_concurrent_discovery)

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=6,
            l3_fraction=0.5,
            l3_max_promotions=10,
            now=10_000,
            decision_commit_batch_size=2,
        )

        assert result.cohorts_processed == 1
        assert result.decisions_written == 6
        assert result.promoted_l3 == 3
        assert commit_calls == 4
        assert concurrent_writer_ok is True
        assert lock_errors == []
        assert get_wallet_level(contender, concurrent_wallet).level is WalletLevel.L0
        assert [get_wallet_level(conn, wallet).level for wallet in wallets] == [
            WalletLevel.L3,
            WalletLevel.L3,
            WalletLevel.L3,
            WalletLevel.L2,
            WalletLevel.L2,
            WalletLevel.L2,
        ]
        decisions = conn.execute(
            "SELECT wallet, selected, reason, rank_in_cohort, cohort_size "
            "FROM wallet_level_selections WHERE target_level = 'l3' "
            "ORDER BY rank_in_cohort",
        ).fetchall()
        assert [row["wallet"] for row in decisions] == wallets
        assert [row["selected"] for row in decisions] == [1, 1, 1, 0, 0, 0]
        assert [row["reason"] for row in decisions] == [
            "relative_rank_selected",
            "relative_rank_selected",
            "relative_rank_selected",
            "relative_rank_below_percentile",
            "relative_rank_below_percentile",
            "relative_rank_below_percentile",
        ]
        assert all(row["cohort_size"] == 6 for row in decisions)
    finally:
        contender.close()
        conn.close()


def test_l2_selection_promotes_relative_top_half_and_records_all_decisions(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + str(index) * 40 for index in range(1, 5)]
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets, (90, 80, 70, 60)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(conn, wallet, depth="light", score=score)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=4,
            l3_fraction=0.5,
            l3_max_promotions=10,
            now=10_000,
        )
        conn.commit()

        assert result.promoted_l3 == 2
        assert [get_wallet_level(conn, wallet).level for wallet in wallets] == [
            WalletLevel.L3,
            WalletLevel.L3,
            WalletLevel.L2,
            WalletLevel.L2,
        ]
        decisions = conn.execute(
            "SELECT wallet, selected, rank_in_cohort, cohort_size, reason "
            "FROM wallet_level_selections WHERE target_level = 'l3' "
            "ORDER BY rank_in_cohort",
        ).fetchall()
        assert len(decisions) == 4
        assert [row["selected"] for row in decisions] == [1, 1, 0, 0]
        assert all(row["cohort_size"] == 4 for row in decisions)
        assert decisions[0]["reason"] == "relative_rank_selected"
        assert decisions[-1]["reason"] == "relative_rank_below_percentile"
    finally:
        conn.close()


def test_l2_to_l3_uses_light_evidence_and_does_not_substitute_deep_summary(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    deep_only = "0x" + "8" * 40
    light_ready = "0x" + "9" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, deep_only, level=WalletLevel.L2)
        _seed_summary(conn, deep_only, depth="deep", score=99)
        _seed_wallet(conn, light_ready, level=WalletLevel.L2)
        _seed_summary(conn, light_ready, depth="light", score=90)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            timeout_min_cohort_size=1,
            l3_fraction=1.0,
            now=10_000,
        )

        assert result.promoted_l3 == 1
        assert get_wallet_level(conn, deep_only).level is WalletLevel.L2
        assert get_wallet_level(conn, light_ready).level is WalletLevel.L3
        decision = conn.execute(
            "SELECT evidence_artifact_id FROM wallet_level_selections "
            "WHERE wallet = ? AND target_level = 'l3'",
            (light_ready,),
        ).fetchone()
        assert decision["evidence_artifact_id"].endswith("-light")
        assert conn.execute(
            "SELECT 1 FROM wallet_level_selections "
            "WHERE wallet = ? AND target_level = 'l3'",
            (deep_only,),
        ).fetchone() is None
    finally:
        conn.close()


def test_relative_selection_can_surface_best_wallet_even_when_absolute_scores_are_low(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    high = "0x" + "a" * 40
    low = "0x" + "b" * 40
    try:
        run_migrations(conn)
        for wallet, score in ((high, 12), (low, 8)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(conn, wallet, depth="light", score=score)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l3_fraction=0.5,
            now=10_000,
        )

        assert result.promoted_l3 == 1
        assert get_wallet_level(conn, high).level is WalletLevel.L3
        assert get_wallet_level(conn, low).level is WalletLevel.L2
    finally:
        conn.close()


def test_relative_selection_defers_wallets_without_minimum_evidence_scale(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ("0x" + "7" * 40, "0x" + "8" * 40)
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets, (99, 98)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(
                conn,
                wallet,
                depth="light",
                score=score,
                activity_count=3,
                distinct_markets=1,
                total_volume_usdc=30,
            )
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l3_fraction=1.0,
            now=10_000,
        )

        assert result.cohorts_processed == 0
        assert result.decisions_written == 0
        assert result.promoted_l3 == 0
        assert all(get_wallet_level(conn, wallet).level is WalletLevel.L2 for wallet in wallets)
    finally:
        conn.close()


def test_source_and_strategy_buckets_receive_fair_relative_slots(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    entries = [
        ("0x" + "1" * 40, "polymarket_leaderboard", "multi_market", 90),
        ("0x" + "2" * 40, "polymarket_leaderboard", "multi_market", 80),
        ("0x" + "3" * 40, "polymarket_leaderboard", "multi_market", 70),
        ("0x" + "4" * 40, "stream", "fast_market_specialist", 60),
        ("0x" + "5" * 40, "stream", "fast_market_specialist", 50),
        ("0x" + "6" * 40, "stream", "fast_market_specialist", 40),
    ]
    try:
        run_migrations(conn)
        for wallet, source, strategy, score in entries:
            _seed_wallet(conn, wallet, level=WalletLevel.L2, source=source)
            _seed_summary(conn, wallet, depth="light", score=score, strategy=strategy)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=6,
            l3_fraction=0.5,
            l3_max_promotions=2,
            now=10_000,
        )

        assert result.promoted_l3 == 2
        selected = conn.execute(
            "SELECT source_bucket, strategy_bucket FROM wallet_level_selections "
            "WHERE target_level = 'l3' AND selected = 1 ORDER BY source_bucket"
        ).fetchall()
        assert [tuple(row) for row in selected] == [
            ("leaderboard", "multi_market"),
            ("stream", "fast_market_specialist"),
        ]
    finally:
        conn.close()


def test_l2_to_l3_trusted_slow_lane_is_budgeted_and_stream_excluded(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    entries = [
        ("0x" + "1" * 40, "manual", 90),
        ("0x" + "2" * 40, "manual", 80),
        ("0x" + "3" * 40, "polydata", 70),
        ("0x" + "4" * 40, "stream", 99),
    ]
    try:
        run_migrations(conn)
        for wallet, source, score in entries:
            _seed_wallet(conn, wallet, level=WalletLevel.L2, source=source)
            _seed_summary(
                conn,
                wallet,
                depth="light",
                score=score,
                activity_count=10,
                distinct_markets=1,
                total_volume_usdc=100,
            )
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=3,
            l3_fraction=1.0,
            l3_max_promotions=10,
            now=10_000,
        )

        selected = conn.execute(
            "SELECT wallet, source_bucket FROM wallet_level_selections "
            "WHERE target_level = 'l3' AND selected = 1 ORDER BY wallet"
        ).fetchall()
        assert result.promoted_l3 == 2
        assert [tuple(row) for row in selected] == [
            (entries[0][0], "curated"),
            (entries[2][0], "polydata"),
        ]
        assert get_wallet_level(conn, entries[1][0]).level is WalletLevel.L2
        assert get_wallet_level(conn, entries[3][0]).level is WalletLevel.L2
    finally:
        conn.close()


def test_l2_to_l3_slow_lane_excludes_real_time_stream_source_names(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    sources = ("polymarket_rtds_activity", "polymarket_trades_global")
    wallets = ("0x" + "6" * 40, "0x" + "7" * 40)
    try:
        run_migrations(conn)
        for wallet, source in zip(wallets, sources):
            _seed_wallet(conn, wallet, level=WalletLevel.L2, source=source)
            _seed_summary(
                conn,
                wallet,
                depth="light",
                score=99,
                activity_count=12,
                distinct_markets=8,
                total_volume_usdc=5_000,
            )
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            timeout_min_cohort_size=1,
            l3_fraction=1.0,
            l3_max_promotions=10,
            now=10_000,
        )

        assert result.promoted_l3 == 0
        assert result.decisions_written == 0
        assert all(
            get_wallet_level(conn, wallet).level is WalletLevel.L2
            for wallet in wallets
        )
    finally:
        conn.close()


def test_not_selected_wallet_cooldown_blocks_score_only_retry_and_reopens_on_new_evidence(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    high = "0x" + "a" * 40
    low = "0x" + "b" * 40
    try:
        run_migrations(conn)
        for wallet, score in ((high, 90), (low, 10)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(
                conn,
                wallet,
                depth="light",
                score=score,
                activity_count=25,
                distinct_markets=2,
                total_volume_usdc=500,
            )
        conn.commit()

        first = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l3_fraction=0.5,
            now=10_000,
        )
        assert first.promoted_l3 == 1
        cooldown = conn.execute(
            "SELECT review_state, cooldown_until, no_material_improvement_count "
            "FROM wallet_level_review_state WHERE wallet = ? AND target_level = 'l3'",
            (low,),
        ).fetchone()
        assert dict(cooldown) == {
            "review_state": "cooldown",
            "cooldown_until": 10_000 + 7 * 86_400,
            "no_material_improvement_count": 1,
        }

        conn.execute(
            """
            UPDATE wallet_history_summaries
            SET artifact_id = 'score-only-artifact',
                research_score = 95,
                diagnostic_score = 95,
                forward_selection_score = 95
            WHERE wallet = ?
            """,
            (low,),
        )
        conn.commit()
        retry = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l3_fraction=1.0,
            now=11_000,
        )
        assert retry.decisions_written == 0
        assert get_wallet_level(conn, low).level is WalletLevel.L2

        conn.execute(
            """
            UPDATE wallet_history_summaries
            SET artifact_id = 'improved-artifact',
                activity_count = 32,
                forward_selection_score = 95
            WHERE wallet = ?
            """,
            (low,),
        )
        conn.commit()
        reopened = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l3_fraction=1.0,
            now=12_000,
        )
        assert reopened.promoted_l3 == 1
        assert get_wallet_level(conn, low).level is WalletLevel.L3
    finally:
        conn.close()


def test_three_unimproved_reviews_archive_until_market_growth_reopens(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    high = "0x" + "c" * 40
    low = "0x" + "d" * 40
    try:
        run_migrations(conn)
        for wallet, score in ((high, 90), (low, 10)):
            _seed_wallet(conn, wallet, level=WalletLevel.L2)
            _seed_summary(
                conn,
                wallet,
                depth="light",
                score=score,
                activity_count=25,
                distinct_markets=2,
                total_volume_usdc=500,
            )
        conn.commit()

        for index, now in enumerate((10_000, 10_000 + 7 * 86_400 + 1, 10_000 + 14 * 86_400 + 2), start=1):
            conn.execute(
                "UPDATE wallet_history_summaries SET artifact_id = ? WHERE wallet = ?",
                (f"unimproved-{index}", low),
            )
            conn.commit()
            reconcile_wallet_level_selections(
                conn,
                min_cohort_size=1,
                l3_fraction=0.5,
                now=now,
            )

        archived = conn.execute(
            "SELECT review_state, no_material_improvement_count "
            "FROM wallet_level_review_state WHERE wallet = ? AND target_level = 'l3'",
            (low,),
        ).fetchone()
        assert dict(archived) == {
            "review_state": "archived",
            "no_material_improvement_count": 3,
        }

        conn.execute(
            """
            UPDATE wallet_history_summaries
            SET artifact_id = 'market-growth',
                distinct_markets = 3,
                forward_selection_score = 95
            WHERE wallet = ?
            """,
            (low,),
        )
        conn.commit()
        reopened = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l3_fraction=1.0,
            now=10_000 + 15 * 86_400,
        )
        assert reopened.promoted_l3 == 1
        assert get_wallet_level(conn, low).level is WalletLevel.L3
    finally:
        conn.close()


def test_l4_requires_global_baseline_in_addition_to_source_bucket_rank(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    strong = [
        ("0x" + f"{index:040x}", "polymarket_leaderboard", score)
        for index, score in enumerate((100, 90, 80, 70), start=1)
    ]
    weak = [
        ("0x" + f"{index:040x}", "stream", score)
        for index, score in enumerate((60, 50, 40), start=10)
    ]
    try:
        run_migrations(conn)
        for wallet, source, score in strong + weak:
            _seed_wallet(conn, wallet, level=WalletLevel.L3, source=source)
            _seed_summary(conn, wallet, depth="deep", score=score)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=7,
            l4_fraction=0.34,
            l4_max_promotions=10,
            now=10_000,
        )

        assert result.promoted_l4 == 2
        assert all(get_wallet_level(conn, wallet).level is WalletLevel.L3 for wallet, _, _ in weak)
        weak_best = conn.execute(
            "SELECT selected, reason FROM wallet_level_selections "
            "WHERE wallet = ? AND target_level = 'l4'",
            (weak[0][0],),
        ).fetchone()
        assert dict(weak_best) == {
            "selected": 0,
            "reason": "relative_rank_below_global_baseline",
        }
    finally:
        conn.close()


def test_l2_transition_ignores_higher_level_scores_without_transition_snapshots(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    high = "0x" + "e" * 40
    low = "0x" + "f" * 40
    peers = ["0x" + str(index) * 40 for index in range(1, 6)]
    try:
        run_migrations(conn)
        for wallet, score in zip(peers, (100, 90, 80, 70, 60)):
            _seed_wallet(conn, wallet, level=WalletLevel.L4, source="stream")
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L5,
                reason="existing_benchmark",
                now=1_200,
            )
            _seed_summary(conn, wallet, depth="deep", score=score, strategy="multi_market")
        _seed_wallet(conn, high, level=WalletLevel.L2, source="manual")
        _seed_summary(conn, high, depth="light", score=12, updated_at=1_000)
        _seed_wallet(conn, low, level=WalletLevel.L2, source="manual")
        _seed_summary(conn, low, depth="light", score=8, updated_at=1_000)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=2,
            l3_fraction=0.5,
            now=10_000,
        )

        assert result.promoted_l3 == 1
        assert get_wallet_level(conn, high).level is WalletLevel.L3
        assert get_wallet_level(conn, low).level is WalletLevel.L2
        decision = conn.execute(
            "SELECT rank_in_cohort, cohort_size FROM wallet_level_selections "
            "WHERE wallet = ? AND target_level = 'l3'",
            (high,),
        ).fetchone()
        assert dict(decision) == {"rank_in_cohort": 1, "cohort_size": 2}
    finally:
        conn.close()


def test_reconcile_advances_at_most_one_level_per_wallet_per_call(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet, level=WalletLevel.L3)
        _seed_summary(conn, wallet, depth="deep", score=90)
        conn.commit()

        first = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l4_fraction=1.0,
            l5_fraction=1.0,
            now=10_000,
        )
        conn.commit()
        assert first.promoted_l4 == 1
        assert first.promoted_l5 == 0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L4

        second = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            l4_fraction=1.0,
            l5_fraction=1.0,
            now=11_000,
        )
        assert second.promoted_l5 == 1
        assert get_wallet_level(conn, wallet).level is WalletLevel.L5
    finally:
        conn.close()


def test_l5_wallets_are_revalidated_on_new_deep_evidence_without_auto_demotion(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + str(index) * 40 for index in range(1, 6)]
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets, (100, 80, 60, 40, 20)):
            _seed_wallet(conn, wallet, level=WalletLevel.L4)
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L5,
                reason="previous_policy_selection",
                policy_version="relative_rank_v1",
                now=1_200,
            )
            _seed_summary(conn, wallet, depth="deep", score=score, updated_at=2_000)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=5,
            l5_fraction=0.2,
            l5_max_promotions=5,
            now=10_000,
        )

        decisions = conn.execute(
            "SELECT wallet, selected FROM wallet_level_selections "
            "WHERE target_level = 'l5' ORDER BY wallet"
        ).fetchall()
        assert result.promoted_l5 == 0
        assert len(decisions) == 5
        assert sum(int(row["selected"]) for row in decisions) == 1
        assert all(get_wallet_level(conn, wallet).level is WalletLevel.L5 for wallet in wallets)
    finally:
        conn.close()


def test_timeout_does_not_turn_a_single_wallet_into_an_automatic_winner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet, level=WalletLevel.L2)
        _seed_summary(conn, wallet, depth="light", score=5, updated_at=1_000)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=20,
            max_wait_seconds=3_600,
            now=10_000,
        )

        assert result.cohorts_processed == 0
        assert result.decisions_written == 0
        assert result.promoted_l3 == 0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L2
    finally:
        conn.close()


def test_late_wallet_is_ranked_against_transition_score_snapshots(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    poor = "0x" + "e" * 40
    peers = ["0x" + str(index) * 40 for index in range(1, 6)]
    try:
        run_migrations(conn)
        for wallet, score in zip(peers, (100, 90, 80, 70, 60)):
            _seed_wallet(conn, wallet, level=WalletLevel.L4)
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L5,
                reason="existing_benchmark",
                now=1_200,
            )
            _seed_summary(conn, wallet, depth="deep", score=score, updated_at=1_000)
            _seed_selection_snapshot(
                conn,
                wallet,
                target_level=WalletLevel.L3,
                score=score,
            )
        _seed_wallet(conn, poor, level=WalletLevel.L2)
        _seed_summary(conn, poor, depth="light", score=10, updated_at=1_000)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=20,
            max_wait_seconds=3_600,
            l3_fraction=0.25,
            now=10_000,
        )

        assert result.cohorts_processed == 1
        assert result.decisions_written == 1
        assert result.promoted_l3 == 0
        assert get_wallet_level(conn, poor).level is WalletLevel.L2
        decision = conn.execute(
            "SELECT selected, rank_in_cohort, cohort_size "
            "FROM wallet_level_selections WHERE wallet = ? AND target_level = 'l3'",
            (poor,),
        ).fetchone()
        assert dict(decision) == {
            "selected": 0,
            "rank_in_cohort": 6,
            "cohort_size": 6,
        }
    finally:
        conn.close()


def test_l5_selection_allows_positive_current_official_all_time_pnl(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + str(index) * 40 for index in range(1, 4)]
    try:
        run_migrations(conn)
        for wallet, score in zip(wallets, (90, 80, 70)):
            _seed_wallet(conn, wallet, level=WalletLevel.L4)
            _seed_summary(conn, wallet, depth="deep", score=score)
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=3,
            l5_fraction=1.0,
            l5_max_promotions=10,
            now=10_000,
        )
        conn.commit()

        assert result.promoted_l5 == 1
        assert get_wallet_level(conn, wallets[0]).level is WalletLevel.L5
        assert [get_wallet_level(conn, wallet).level for wallet in wallets[1:]] == [
            WalletLevel.L4,
            WalletLevel.L4,
        ]
        decision = conn.execute(
            "SELECT COUNT(*) FROM wallet_level_selections "
            "WHERE target_level = 'l5' AND selected = 1",
        ).fetchone()
        assert decision[0] == 1
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("pnl", "methodology_version", "captured_at"),
    (
        (None, PNL_METHODOLOGY_VERSION, 2_000),
        (0, PNL_METHODOLOGY_VERSION, 2_000),
        (-100, PNL_METHODOLOGY_VERSION, 2_000),
        (100, "old_pnl_methodology", 2_000),
        (100, PNL_METHODOLOGY_VERSION, 10_000 - 86_400 - 1),
    ),
)
def test_l5_selection_blocks_missing_stale_and_non_positive_official_pnl(
    tmp_path,
    pnl,
    methodology_version,
    captured_at,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet, level=WalletLevel.L4)
        _seed_summary(conn, wallet, depth="deep", score=99)
        conn.execute(
            """
            UPDATE wallet_pnl_summaries
            SET official_all_pnl_usdc = ?,
                official_all_volume_usdc = 10000,
                official_profit_intensity = ?,
                methodology_version = ?,
                captured_at = ?
            WHERE wallet = ?
            """,
            (
                pnl,
                None if pnl is None else float(pnl) / 10000,
                methodology_version,
                captured_at,
                wallet,
            ),
        )
        conn.commit()

        result = reconcile_wallet_level_selections(
            conn,
            min_cohort_size=1,
            timeout_min_cohort_size=1,
            l5_fraction=1.0,
            l5_max_promotions=10,
            now=10_000,
        )

        assert result.promoted_l5 == 0
        assert result.decisions_written == 0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L4
        assert conn.execute(
            "SELECT 1 FROM wallet_level_selections "
            "WHERE wallet = ? AND target_level = 'l5'",
            (wallet,),
        ).fetchone() is None
    finally:
        conn.close()
