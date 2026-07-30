import json
import sqlite3
from pathlib import Path

import duckdb
import pytest

from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress
from pm_robot.orchestration import wallet_history_pipeline as wallet_history_module
from pm_robot.orchestration.wallet_history_pipeline import (
    DEFAULT_PRIORITY_AGING_SECONDS,
    DEEP_ACTION,
    JOB_TYPE,
    LIGHT_ACTION,
    PNL_INCOMPLETE_REFRESH_SECONDS,
    PNL_METHODOLOGY_VERSION,
    _fetch_official_all_profit,
    plan_wallet_history_jobs,
    run_wallet_history_worker,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import advance_wallet_level, get_wallet_level
from pm_robot.wallet_levels import WalletLevel


class FakeHistoryClient:
    def __init__(
        self,
        rows,
        *,
        positions=None,
        closed=None,
        values=None,
        leaderboard=None,
    ):
        self.rows = rows
        self.positions_payload = positions or []
        self.closed_payload = closed or []
        self.values_payload = values or []
        self.leaderboard_payload = leaderboard or []
        self.calls = []
        self.closed_sort_calls = []

    def activity(self, wallet, *, limit, offset):
        self.calls.append(("activity", wallet, limit, offset))
        return self.rows[offset : offset + limit]

    def positions(self, wallet, *, size_threshold):
        self.calls.append(("positions", wallet, size_threshold))
        return self.positions_payload

    def closed_positions(
        self,
        wallet,
        *,
        limit,
        offset,
        size_threshold,
        sort_by=None,
        sort_direction=None,
    ):
        self.closed_sort_calls.append((sort_by, sort_direction))
        self.calls.append(("closed", wallet, limit, offset, size_threshold))
        return self.closed_payload[offset : offset + limit]

    def position_values(self, wallet):
        self.calls.append(("value", wallet))
        return self.values_payload

    def trader_leaderboard(self, **kwargs):
        return self.leaderboard_payload


class DeferredHistoryClient:
    def activity(self, wallet, *, limit, offset):
        raise HttpClientError(
            "shared upstream request budget is cooling down",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=180,
        )


def _rows(count: int) -> list[dict]:
    return [
        {
            "timestamp": 1_000 + index * 120,
            "conditionId": f"condition-{index % 5}",
            "eventSlug": f"event-{index % 5}",
            "slug": f"market-{index % 5}",
            "asset": f"asset-{index % 5}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY" if index % 2 == 0 else "SELL",
            "price": 0.5,
            "size": 20,
            "usdcSize": 10,
            "transactionHash": f"0x{index:064x}",
        }
        for index in range(count)
    ]


def _seed_level(conn, wallet: str, level: WalletLevel) -> None:
    record_wallet_sighting(
        conn,
        CandidateAddress(address=wallet, sources="manual", labels="seed"),
        trusted_source=True,
        now=1_000,
    )
    steps = {
        WalletLevel.L1: (),
        WalletLevel.L2: (WalletLevel.L2,),
        WalletLevel.L3: (WalletLevel.L2, WalletLevel.L3),
        WalletLevel.L4: (WalletLevel.L2, WalletLevel.L3, WalletLevel.L4),
        WalletLevel.L5: (WalletLevel.L2, WalletLevel.L3, WalletLevel.L4, WalletLevel.L5),
    }
    for index, target in enumerate(steps.get(level, ()), start=1):
        advance_wallet_level(
            conn,
            wallet,
            to_level=target,
            reason="test_level",
            now=1_000 + index * 100,
        )
    conn.commit()


def _seed_history_summary(
    conn,
    wallet: str,
    *,
    depth: str,
    updated_at: int,
    methodology_version: str = METHODOLOGY_VERSION,
) -> None:
    artifact_id = f"existing-{wallet[-4:]}-{depth}"
    conn.execute(
        """
        INSERT INTO wallet_history_artifacts(
            artifact_id, wallet, history_depth, storage_version, relative_path,
            row_count, byte_size, checksum, status, created_at, updated_at
        ) VALUES (?, ?, ?, 'test', ?, 100, 10, 'checksum', 'active', ?, ?)
        """,
        (
            artifact_id,
            wallet,
            depth,
            f"test/{artifact_id}.parquet",
            updated_at,
            updated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count,
            distinct_markets, total_volume_usdc, strategy_tags_json,
            risk_flags_json, research_score, score_components_json,
            methodology_version, computed_at, updated_at
        ) VALUES (?, ?, ?, 100, 10, 1000, '[]', '[]', 50, '{}', ?, ?, ?)
        """,
        (wallet, artifact_id, depth, methodology_version, updated_at, updated_at),
    )


@pytest.mark.parametrize(
    ("level", "depth"),
    [
        (WalletLevel.L2, "light"),
        (WalletLevel.L5, "deep"),
    ],
)
def test_history_planner_refreshes_old_methodology_without_age_or_new_sighting(
    tmp_path,
    level,
    depth,
):
    conn = connect(tmp_path / f"{level.value}.sqlite")
    wallet = "0x" + ("2" if level is WalletLevel.L2 else "5") * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, level)
        _seed_history_summary(
            conn,
            wallet,
            depth=depth,
            updated_at=1_900,
            methodology_version="wallet_history_summary_v1",
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=10,
            shard_count=1,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.commit()

        assert summary.jobs_enqueued == 1
        job = conn.execute(
            "SELECT job_action, job_scope, priority, input_json "
            "FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        action = LIGHT_ACTION if depth == "light" else DEEP_ACTION
        assert job["job_action"] == f"{action}:refresh:1900"
        assert job["job_scope"] == depth
        assert '"refresh_reason":"methodology_upgrade"' in job["input_json"]
        assert f'"methodology_version":"{METHODOLOGY_VERSION}"' in job["input_json"]
    finally:
        conn.close()


def test_history_planner_prioritizes_l5_methodology_upgrade_when_slots_are_limited(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        WalletLevel.L2: "0x" + "2" * 40,
        WalletLevel.L3: "0x" + "3" * 40,
        WalletLevel.L4: "0x" + "4" * 40,
        WalletLevel.L5: "0x" + "5" * 40,
    }
    try:
        run_migrations(conn)
        for level, wallet in wallets.items():
            _seed_level(conn, wallet, level)
            _seed_history_summary(
                conn,
                wallet,
                depth="light" if level is WalletLevel.L2 else "deep",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT wallet, priority FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()
        assert summary.jobs_enqueued == 1
        assert dict(queued) == {"wallet": wallets[WalletLevel.L5], "priority": 1}
    finally:
        conn.close()


def test_history_planner_reserves_capacity_for_first_evidence_during_rollout(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    stale_wallets = ["0x" + f"{index:040x}" for index in range(1, 5)]
    first_evidence_wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        for wallet in stale_wallets:
            _seed_level(conn, wallet, WalletLevel.L3)
            _seed_history_summary(
                conn,
                wallet,
                depth="deep",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        _seed_level(conn, first_evidence_wallet, WalletLevel.L2)
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=3,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT wallet, job_scope FROM pipeline_jobs WHERE status = 'queued' "
            "ORDER BY job_id"
        ).fetchall()
        assert summary.jobs_enqueued == 3
        assert any(
            row["wallet"] == first_evidence_wallet and row["job_scope"] == "light"
            for row in queued
        )
        assert sum(row["job_scope"] == "deep" for row in queued) == 2
    finally:
        conn.close()


def test_history_planner_preserves_first_evidence_inside_a_stale_light_pool(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    first_evidence_wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        for index in range(1, 61):
            wallet = "0x" + f"{index:040x}"
            _seed_level(conn, wallet, WalletLevel.L2)
            _seed_history_summary(
                conn,
                wallet,
                depth="light",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        _seed_level(conn, first_evidence_wallet, WalletLevel.L2)
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=3,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT wallet, input_json FROM pipeline_jobs WHERE status = 'queued' "
            "ORDER BY job_id"
        ).fetchall()
        assert summary.jobs_enqueued == 3
        assert any(
            row["wallet"] == first_evidence_wallet
            and '"refresh_reason":"required_depth"' in row["input_json"]
            for row in queued
        )
    finally:
        conn.close()


def test_history_planner_treats_missing_required_depth_as_initial_evidence(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L3)
        _seed_history_summary(
            conn,
            wallet,
            depth="light",
            updated_at=1_900,
            methodology_version="wallet_history_summary_v1",
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT job_action, job_scope, priority, max_attempts, input_json "
            "FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert summary.jobs_enqueued == 1
        assert queued["job_action"] == DEEP_ACTION
        assert queued["job_scope"] == "deep"
        assert queued["priority"] == 5
        assert queued["max_attempts"] == 3
        assert '"refresh_reason":"required_depth"' in queued["input_json"]
    finally:
        conn.close()


def test_history_planner_does_not_refresh_current_methodology_without_new_activity(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L5)
        _seed_history_summary(conn, wallet, depth="deep", updated_at=1_900)
        conn.execute(
            """
            INSERT INTO wallet_pnl_summaries(
                wallet, official_all_pnl_usdc, official_all_volume_usdc,
                official_profit_intensity, coverage, methodology_version,
                captured_at, updated_at
            ) VALUES (?, 100, 10000, 0.01, 'deep_recent_bounded', ?, 1900, 1900)
            """,
            (wallet, PNL_METHODOLOGY_VERSION),
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=10,
            shard_count=1,
            deep_refresh_seconds=10_000,
            now=2_000,
        )

        assert summary.jobs_enqueued == 0
    finally:
        conn.close()


def test_methodology_refresh_repairs_missing_artifact_with_authorized_network_fetch(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    client = FakeHistoryClient(_rows(2))
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L5)
        _seed_history_summary(
            conn,
            wallet,
            depth="deep",
            updated_at=1_900,
            methodology_version="wallet_history_summary_v1",
        )
        conn.commit()
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "missing-parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="missing-methodology-artifact",
            client=client,
        )

        assert result.jobs_succeeded == 1
        assert result.jobs_failed == 0
        assert ("activity", wallet, 100, 0) in client.calls
        completed_job = conn.execute(
            "SELECT status, output_json FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert completed_job["status"] == "done"
        output = json.loads(completed_job["output_json"])
        assert output["artifact_repaired"] is True
        assert output["artifact_reused"] is False
    finally:
        conn.close()


def test_failed_reuse_job_does_not_starve_a_healthy_methodology_target(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    blocked_wallets = ["0x" + f"{index:040x}" for index in range(1, 6)]
    healthy_wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        for wallet in (*blocked_wallets, healthy_wallet):
            _seed_level(conn, wallet, WalletLevel.L5)
            _seed_history_summary(
                conn,
                wallet,
                depth="deep",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, priority, shard,
                status, attempts, max_attempts, next_attempt_at, last_error,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'deep', 0, 0, 'failed', 1, 1, 999999, ?,
                      1900, 1900)
            """,
            [
                (
                    JOB_TYPE,
                    wallet,
                    f"{DEEP_ACTION}:refresh:1900",
                    "methodology_upgrade requires a valid active history artifact",
                )
                for wallet in blocked_wallets
            ],
        )
        conn.commit()

        plan = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=1,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE status = 'queued'"
        ).fetchall()
        assert plan.jobs_enqueued == 1
        assert [row["wallet"] for row in queued] == [healthy_wallet]
    finally:
        conn.close()


def test_failed_not_due_refresh_does_not_starve_a_healthy_methodology_target(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    blocked_wallets = ["0x" + f"{index:040x}" for index in range(1, 6)]
    healthy_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        for wallet in (*blocked_wallets, healthy_wallet):
            _seed_level(conn, wallet, WalletLevel.L5)
            _seed_history_summary(
                conn,
                wallet,
                depth="deep",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, priority, shard,
                status, attempts, max_attempts, next_attempt_at, last_error,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'deep', 0, 0, 'failed', 1, 1, 999999,
                      'HTTP 500 upstream', 1900, 1900)
            """,
            [
                (
                    JOB_TYPE,
                    wallet,
                    f"{DEEP_ACTION}:refresh:1900",
                )
                for wallet in blocked_wallets
            ],
        )
        conn.commit()

        plan = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=1,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE status = 'queued'"
        ).fetchall()
        assert plan.jobs_enqueued == 1
        assert [row["wallet"] for row in queued] == [healthy_wallet]
    finally:
        conn.close()


def test_missing_official_pnl_is_retried_with_parquet_reuse_only(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "e" * 40
    initial_now = 2_000
    retry_now = initial_now + PNL_INCOMPLETE_REFRESH_SECONDS + 1
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L4)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=initial_now)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: initial_now,
        )
        initial = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="initial-missing-official",
            client=FakeHistoryClient(_rows(80)),
        )
        conn.commit()

        plan = plan_wallet_history_jobs(
            conn,
            limit=1,
            max_active_jobs=10,
            shard_count=1,
            deep_refresh_seconds=10_000_000,
            now=retry_now,
        )
        conn.commit()
        job = conn.execute(
            "SELECT input_json FROM pipeline_jobs WHERE wallet = ? ORDER BY job_id DESC LIMIT 1",
            (wallet,),
        ).fetchone()
        job_input = json.loads(job["input_json"])
        assert job_input["refresh_reason"] == "pnl_evidence_refresh"
        assert job_input["job_action"] == f"{DEEP_ACTION}:pnl:{initial_now}"

        retry_client = FakeHistoryClient(
            [],
            leaderboard=[{"proxyWallet": wallet, "pnl": 250, "vol": 10_000}],
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: retry_now,
        )
        refreshed = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="retry-missing-official",
            client=retry_client,
        )

        pnl = conn.execute(
            "SELECT official_all_pnl_usdc FROM wallet_pnl_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert initial.jobs_succeeded == 1
        assert plan.jobs_enqueued == 1
        assert refreshed.jobs_succeeded == 1
        assert all(call[0] != "activity" for call in retry_client.calls)
        assert pnl["official_all_pnl_usdc"] == pytest.approx(250)
    finally:
        conn.close()


def test_official_pnl_crosscheck_rejects_a_different_wallet():
    wallet = "0x" + "a" * 40
    other = "0x" + "b" * 40
    client = FakeHistoryClient(
        [],
        leaderboard=[{"proxyWallet": other, "pnl": 1_000_000, "vol": 1}],
    )

    assert _fetch_official_all_profit(client, wallet) == (None, None)


def test_official_pnl_crosscheck_propagates_upstream_failure():
    wallet = "0x" + "a" * 40

    class FailedLeaderboardClient:
        def trader_leaderboard(self, **kwargs):
            del kwargs
            raise HttpClientError(
                "service unavailable",
                status_code=503,
                error_type="server_error",
            )

    with pytest.raises(HttpClientError, match="service unavailable"):
        _fetch_official_all_profit(FailedLeaderboardClient(), wallet)


def test_history_planner_maps_l2_to_light_and_ignores_l1(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    l2_wallet = "0x" + "1" * 40
    l1_wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, l2_wallet, WalletLevel.L2)
        _seed_level(conn, l1_wallet, WalletLevel.L1)

        summary = plan_wallet_history_jobs(
            conn,
            limit=10,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        job = conn.execute(
            "SELECT wallet, job_type, job_action, job_scope FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()
        assert dict(job) == {
            "wallet": l2_wallet,
            "job_type": JOB_TYPE,
            "job_action": LIGHT_ACTION,
            "job_scope": "light",
        }
    finally:
        conn.close()


def test_history_planner_waterline_ignores_exhausted_queued_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    target_wallet = "0x" + "b" * 40
    running_wallet = "0x" + "c" * 40
    claimable_wallet = "0x" + "d" * 40
    exhausted_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, target_wallet, WalletLevel.L2)
        for wallet, status, attempts, max_attempts in [
            (running_wallet, "running", 3, 3),
            (claimable_wallet, "queued", 2, 3),
            (exhausted_wallet, "queued", 3, 3),
        ]:
            conn.execute(
                """
                INSERT INTO pipeline_jobs(
                    job_type, wallet, job_action, job_scope, status,
                    attempts, max_attempts, created_at, updated_at
                ) VALUES (?, ?, ?, 'light', ?, ?, ?, 1000, 1000)
                """,
                (
                    JOB_TYPE,
                    wallet,
                    f"collect_light_history:v1:{wallet[-4:]}",
                    status,
                    attempts,
                    max_attempts,
                ),
            )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=10,
            max_active_jobs=3,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        assert summary.active_jobs == 2
        assert summary.jobs_enqueued == 1
        assert conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ? AND job_type = ?",
            (target_wallet, JOB_TYPE),
        ).fetchone()[0] == "queued"
    finally:
        conn.close()


def test_light_history_refresh_requires_both_staleness_and_a_new_sighting(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        _seed_history_summary(conn, wallet, depth="light", updated_at=1_500)
        conn.commit()

        without_new_sighting = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            light_refresh_seconds=1_000,
            now=3_000,
        )
        record_wallet_sighting(
            conn,
            CandidateAddress(address=wallet, sources="stream", labels="new_activity"),
            verified_trade=True,
            now=2_500,
        )
        with_new_sighting = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            light_refresh_seconds=1_000,
            now=3_000,
        )
        conn.commit()

        assert without_new_sighting.jobs_enqueued == 0
        assert with_new_sighting.jobs_enqueued == 1
        job = conn.execute(
            "SELECT job_action, job_scope FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(job) == {
            "job_action": f"{LIGHT_ACTION}:activity:2500",
            "job_scope": "light",
        }
    finally:
        conn.close()


def test_history_planner_rotates_level_and_source_buckets_to_avoid_starvation(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stream_l2_wallets = ["0x" + f"{index:040x}" for index in range(1, 5)]
    curated_l2_wallet = "0x" + "a" * 40
    deep_wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        for index, wallet in enumerate(stream_l2_wallets):
            _seed_level(conn, wallet, WalletLevel.L2)
            conn.execute(
                "UPDATE observed_wallets SET sources = 'stream', updated_at = ? WHERE wallet = ?",
                (2_000 - index, wallet),
            )
        _seed_level(conn, curated_l2_wallet, WalletLevel.L2)
        conn.execute(
            "UPDATE observed_wallets SET sources = 'manual_watchlist' WHERE wallet = ?",
            (curated_l2_wallet,),
        )
        _seed_level(conn, deep_wallet, WalletLevel.L3)
        conn.execute(
            "UPDATE observed_wallets SET sources = 'stream' WHERE wallet = ?",
            (deep_wallet,),
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=3,
            max_active_jobs=10,
            shard_count=1,
            now=3_000,
        )
        conn.commit()

        queued = {
            row["wallet"]: row["job_scope"]
            for row in conn.execute(
                "SELECT wallet, job_scope FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }
        assert summary.jobs_enqueued == 3
        assert queued[deep_wallet] == "deep"
        assert queued[curated_l2_wallet] == "light"
        assert len(set(queued).intersection(stream_l2_wallets)) == 1
    finally:
        conn.close()


def test_history_planner_does_not_starve_deep_targets_behind_light_backlog(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stream_l2_wallet = "0x" + "1" * 40
    curated_l2_wallet = "0x" + "a" * 40
    deep_wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, stream_l2_wallet, WalletLevel.L2)
        conn.execute(
            "UPDATE observed_wallets SET sources = 'stream' WHERE wallet = ?",
            (stream_l2_wallet,),
        )
        _seed_level(conn, curated_l2_wallet, WalletLevel.L2)
        conn.execute(
            "UPDATE observed_wallets SET sources = 'manual_watchlist' WHERE wallet = ?",
            (curated_l2_wallet,),
        )
        _seed_level(conn, deep_wallet, WalletLevel.L3)
        conn.execute(
            "UPDATE observed_wallets SET sources = 'stream' WHERE wallet = ?",
            (deep_wallet,),
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=2,
            max_active_jobs=10,
            shard_count=1,
            now=3_000,
        )
        conn.commit()

        queued = {
            row["wallet"]: row["job_scope"]
            for row in conn.execute(
                "SELECT wallet, job_scope FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }
        assert summary.jobs_enqueued == 2
        assert queued[deep_wallet] == "deep"
        assert any(
            scope == "light" for wallet, scope in queued.items() if wallet != deep_wallet
        )
    finally:
        conn.close()


def test_history_planner_reserves_light_candidates_before_sql_pool_truncation(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    light_wallet = "0x" + "f" * 40
    deep_wallets = ["0x" + f"{index:040x}" for index in range(1, 13)]
    try:
        run_migrations(conn)
        _seed_level(conn, light_wallet, WalletLevel.L2)
        for wallet in deep_wallets:
            _seed_level(conn, wallet, WalletLevel.L3)
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=2,
            max_active_jobs=10,
            shard_count=1,
            now=3_000,
        )
        conn.commit()

        queued = {
            row["wallet"]: row["job_scope"]
            for row in conn.execute(
                "SELECT wallet, job_scope FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }
        assert summary.jobs_enqueued == 2
        assert queued[light_wallet] == "light"
        assert list(queued.values()).count("deep") == 1
    finally:
        conn.close()


def test_history_worker_enables_priority_aging_when_claiming(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    captured = {}

    def fake_claim(*_args, **kwargs):
        captured.update(kwargs)
        return None

    try:
        monkeypatch.setattr(wallet_history_module, "claim_pipeline_job", fake_claim)
        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="aging-test",
            client=FakeHistoryClient([]),
        )

        assert result.jobs_attempted == 0
        assert captured["priority_aging_seconds"] == DEFAULT_PRIORITY_AGING_SECONDS
    finally:
        conn.close()


def test_history_worker_defers_sqlite_claim_contention_without_crashing(
    tmp_path, monkeypatch
):
    conn = connect(tmp_path / "robot.sqlite")

    def locked_claim(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    try:
        monkeypatch.setattr(wallet_history_module, "claim_pipeline_job", locked_claim)
        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="lock-test",
            client=FakeHistoryClient([]),
        )

        assert result.jobs_attempted == 0
        assert result.jobs_deferred == 1
        assert result.status == "partial"
        assert "writer contention" in result.error
        assert not conn.in_transaction
    finally:
        conn.close()


def test_light_history_worker_writes_parquet_and_compact_summary_only(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "3" * 40
    client = FakeHistoryClient(
        _rows(75),
        positions=[{"cashPnl": "5", "initialValue": "100"}],
        closed=[{"realizedPnl": "7", "totalBought": "50"}],
        values=[{"user": wallet, "value": 125}],
    )
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="history-test",
            client=client,
        )

        assert result.jobs_succeeded == 1
        assert result.light_completed == 1
        assert result.deep_completed == 0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L2
        summary = conn.execute(
            "SELECT * FROM wallet_history_summaries WHERE wallet = ?", (wallet,)
        ).fetchone()
        assert summary["history_depth"] == "light"
        assert summary["activity_count"] == 75
        assert summary["distinct_markets"] == 5
        assert summary["total_volume_usdc"] == pytest.approx(750)
        assert summary["research_score"] > 0
        artifact = conn.execute(
            "SELECT relative_path, status FROM wallet_history_artifacts WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        path = archive_dir / artifact["relative_path"]
        assert artifact["status"] == "active"
        assert path.is_file()
        with duckdb.connect(":memory:") as db:
            assert db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] == 75
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wallet_activity'"
        ).fetchone() is None
        feature = conn.execute(
            "SELECT net_pnl_usdc, total_volume_usdc, extra_json FROM wallet_features WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert feature["net_pnl_usdc"] == pytest.approx(12)
        assert feature["total_volume_usdc"] == pytest.approx(750)
        assert METHODOLOGY_VERSION in feature["extra_json"]
        pnl = conn.execute(
            "SELECT * FROM wallet_pnl_summaries WHERE wallet = ?", (wallet,)
        ).fetchone()
        assert pnl["total_estimated_pnl_usdc"] == pytest.approx(12)
        assert pnl["capital_basis_usdc"] == pytest.approx(150)
        assert pnl["cost_roi_estimate"] == pytest.approx(12 / 150)
        assert pnl["current_position_value_usdc"] == pytest.approx(125)
        assert client.calls == [
            ("activity", wallet, 100, 0),
            ("positions", wallet, 0.0),
            ("closed", wallet, 50, 0, 0.0),
            ("value", wallet),
        ]
    finally:
        conn.close()


def test_history_worker_commits_pnl_cache_before_parquet_io(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "8" * 40
    original_persist = wallet_history_module.persist_wallet_history_artifact
    transaction_states = []

    def assert_clean_transaction(*args, **kwargs):
        transaction_states.append(conn.in_transaction)
        return original_persist(*args, **kwargs)

    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            wallet_history_module,
            "persist_wallet_history_artifact",
            assert_clean_transaction,
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="transaction-boundary-test",
            client=FakeHistoryClient(_rows(10)),
        )

        assert result.jobs_succeeded == 1
        assert transaction_states == [False]
    finally:
        conn.close()


def test_l3_wallet_receives_deep_snapshot_that_supersedes_light(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="light-test",
            client=FakeHistoryClient(_rows(80)),
        )
        advance_wallet_level(conn, wallet, to_level=WalletLevel.L3, reason="selected", now=3_000)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=3_100)
        conn.commit()

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="deep-test",
            client=FakeHistoryClient(_rows(250)),
        )

        assert result.deep_completed == 1
        artifacts = conn.execute(
            "SELECT history_depth, status FROM wallet_history_artifacts "
            "WHERE wallet = ? ORDER BY rowid",
            (wallet,),
        ).fetchall()
        assert [dict(row) for row in artifacts] == [
            {"history_depth": "light", "status": "superseded"},
            {"history_depth": "deep", "status": "active"},
        ]
        assert conn.execute(
            "SELECT history_depth, activity_count FROM wallet_history_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()[:] == ("deep", 250)
    finally:
        conn.close()


def test_deep_history_refreshes_fresh_light_bounded_pnl_and_paginates(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "6" * 40
    closed = [
        {"realizedPnl": "1", "totalBought": "10", "asset": f"asset-{index}"}
        for index in range(120)
    ]
    client = FakeHistoryClient(_rows(250), closed=closed)
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L3)
        conn.execute(
            """
            INSERT INTO wallet_pnl_summaries(
                wallet, total_estimated_pnl_usdc, coverage,
                methodology_version, captured_at, updated_at
            ) VALUES (?, 999, 'light_bounded', 'test', 9_900, 9_900)
            """,
            (wallet,),
        )
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=9_950)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 10_000,
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="deep-pnl-test",
            client=client,
        )

        assert result.deep_completed == 1
        pnl = conn.execute(
            "SELECT closed_realized_pnl_usdc, closed_position_count, coverage "
            "FROM wallet_pnl_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(pnl) == {
            "closed_realized_pnl_usdc": pytest.approx(120),
            "closed_position_count": 120,
            "coverage": "complete",
        }
        assert [call for call in client.calls if call[0] == "closed"] == [
            ("closed", wallet, 50, 0, 0.0),
            ("closed", wallet, 50, 50, 0.0),
            ("closed", wallet, 50, 100, 0.0),
        ]
    finally:
        conn.close()


def test_l4_wallet_can_run_a_new_activity_driven_deep_refresh(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L3)
        _seed_history_summary(conn, wallet, depth="deep", updated_at=1_500)
        advance_wallet_level(conn, wallet, to_level=WalletLevel.L4, reason="selected", now=1_600)
        record_wallet_sighting(
            conn,
            CandidateAddress(address=wallet, sources="stream", labels="new_activity"),
            verified_trade=True,
            now=2_500,
        )
        plan = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            deep_refresh_seconds=1_000,
            now=3_000,
        )
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 3_000,
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="l4-refresh-test",
            client=FakeHistoryClient(_rows(250)),
        )

        assert plan.jobs_enqueued == 1
        assert result.deep_completed == 1
        assert get_wallet_level(conn, wallet).level is WalletLevel.L4
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_history_artifacts WHERE wallet = ? AND status = 'active'",
            (wallet,),
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_history_worker_defers_shared_cooldown_without_consuming_attempt(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 4_000,
        )

        summary = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=2,
            worker_id="history-deferred",
            client=DeferredHistoryClient(),
        )

        job = conn.execute(
            "SELECT status, attempts, next_attempt_at FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert summary.jobs_attempted == 1
        assert summary.jobs_failed == 0
        assert summary.jobs_deferred == 1
        assert summary.status == "partial"
        assert dict(job) == {
            "status": "queued",
            "attempts": 0,
            "next_attempt_at": 4_180,
        }
    finally:
        conn.close()


def test_history_worker_removes_uncatalogued_parquet_after_transaction_failure(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            wallet_history_module,
            "_persist_history_summary",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("summary write failed")),
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="rollback-test",
            client=FakeHistoryClient(_rows(25)),
        )

        assert result.jobs_failed == 1
        assert result.error == "summary write failed"
        assert list(archive_dir.rglob("*.parquet")) == []
        assert conn.execute("SELECT COUNT(*) FROM wallet_history_artifacts").fetchone()[0] == 0
        assert conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == "queued"
    finally:
        conn.close()


def test_bounded_recent_profit_cannot_override_negative_official_all_time_pnl(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "a" * 40
    closed = [
        {
            "realizedPnl": "100",
            "totalBought": "100",
            "timestamp": 10_000 - index,
            "asset": f"asset-{index}",
        }
        for index in range(250)
    ]
    client = FakeHistoryClient(
        _rows(250),
        closed=closed,
        leaderboard=[{"proxyWallet": wallet, "pnl": -1_000, "vol": 10_000}],
    )
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L3)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        result = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="negative-official-test",
            client=client,
        )

        assert result.deep_completed == 1
        pnl = conn.execute(
            """
            SELECT total_estimated_pnl_usdc, coverage, methodology_version,
                   official_all_pnl_usdc, official_all_volume_usdc,
                   official_profit_intensity
            FROM wallet_pnl_summaries
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchone()
        assert dict(pnl) == {
            "total_estimated_pnl_usdc": pytest.approx(20_000),
            "coverage": "deep_recent_bounded",
            "methodology_version": PNL_METHODOLOGY_VERSION,
            "official_all_pnl_usdc": pytest.approx(-1_000),
            "official_all_volume_usdc": pytest.approx(10_000),
            "official_profit_intensity": pytest.approx(-0.1),
        }
        summary = conn.execute(
            "SELECT risk_flags_json, score_components_json "
            "FROM wallet_history_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert "non_positive_official_all_time_pnl" in json.loads(
            summary["risk_flags_json"]
        )
        assert json.loads(summary["score_components_json"])["pnl"] < 50
        assert conn.execute(
            "SELECT net_pnl_usdc FROM wallet_features WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == pytest.approx(-1_000)
        assert client.closed_sort_calls == [("TIMESTAMP", "DESC")] * 4
    finally:
        conn.close()


def test_methodology_only_refresh_reuses_active_parquet_without_activity_refetch(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        first_client = FakeHistoryClient(
            _rows(60),
            leaderboard=[{"proxyWallet": wallet, "pnl": 100, "vol": 10_000}],
        )
        first = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="initial-history",
            client=first_client,
        )
        original = conn.execute(
            "SELECT artifact_id, relative_path FROM wallet_history_artifacts "
            "WHERE wallet = ? AND status = 'active'",
            (wallet,),
        ).fetchone()
        conn.execute(
            "UPDATE wallet_history_summaries SET methodology_version = 'old-method' "
            "WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
        plan = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=2_100,
        )
        conn.commit()

        no_fetch_client = FakeHistoryClient([])
        refreshed = run_wallet_history_worker(
            conn,
            archive_dir=archive_dir,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="methodology-refresh",
            client=no_fetch_client,
        )

        current = conn.execute(
            "SELECT artifact_id, relative_path FROM wallet_history_artifacts "
            "WHERE wallet = ? AND status = 'active'",
            (wallet,),
        ).fetchone()
        output = json.loads(
            conn.execute(
                "SELECT output_json FROM pipeline_jobs WHERE wallet = ? "
                "ORDER BY job_id DESC LIMIT 1",
                (wallet,),
            ).fetchone()[0]
        )
        assert first.jobs_succeeded == 1
        assert plan.jobs_enqueued == 1
        assert refreshed.jobs_succeeded == 1
        assert refreshed.rows_archived == 0
        assert dict(current) == dict(original)
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_history_artifacts WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == 1
        assert output["artifact_reused"] is True
        assert no_fetch_client.calls == []
    finally:
        conn.close()


def test_light_history_planner_prioritizes_stronger_screen_sample(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    weak = "0x" + "c" * 40
    strong = "0x" + "d" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, weak, WalletLevel.L2)
        _seed_level(conn, strong, WalletLevel.L2)
        conn.executemany(
            """
            INSERT INTO wallet_screen_summaries(
                wallet, sample_trade_count, sample_volume_usdc,
                sample_market_count, screen_complete, screen_qualified,
                computed_at, updated_at
            ) VALUES (?, 10, ?, ?, 1, 1, 1_900, 1_900)
            """,
            ((weak, 110, 1), (strong, 900, 5)),
        )
        conn.commit()

        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        assert conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()[0] == strong
    finally:
        conn.close()


def test_new_methodology_action_is_not_blocked_by_old_done_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, created_at, updated_at, completed_at
            ) VALUES (?, ?, 'collect_light_history:v1', 'light', 'done',
                      1, 3, 1_000, 1_000, 1_000)
            """,
            (JOB_TYPE, wallet),
        )
        conn.commit()

        plan = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        assert plan.jobs_enqueued == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT status FROM pipeline_jobs "
            "WHERE wallet = ? AND job_action = ?",
            (wallet, LIGHT_ACTION),
        ).fetchone()[0] == "queued"
    finally:
        conn.close()
