import json

from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_level_selection import (
    SELECTION_POLICY_VERSION,
    reconcile_wallet_level_selections,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import advance_wallet_level, get_wallet_level
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
    strategy: str = "general",
    activity_count: int = 100,
    distinct_markets: int = 10,
    total_volume_usdc: float = 2_000,
    updated_at: int = 2_000,
) -> str:
    artifact_id = f"artifact-{wallet[-4:]}-{depth}"
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
            score_components_json, methodology_version, computed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, '[]', ?, '{}', 'test', ?, ?)
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
            updated_at,
            updated_at,
        ),
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
            strategy_bucket, reason, decided_at, updated_at, research_score
        ) VALUES (?, ?, ?, ?, ?, 1, 20, 'stream', 'general',
                  'test_transition_snapshot', ?, ?, ?)
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
        ),
    )


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
