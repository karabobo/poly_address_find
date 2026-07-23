import pytest

from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_screening import (
    JOB_TYPE,
    plan_wallet_screen_jobs,
    run_wallet_screen_worker,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import WalletLevel


def _table_exists(conn, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


class FakeScreenClient:
    def __init__(self, *, trades, positions=None, closed=None, values=None):
        self.trades = trades
        self.positions_payload = positions or []
        self.closed_payload = closed or []
        self.values_payload = values or []
        self.calls = []

    def wallet_trades(self, wallet, *, limit, offset, taker_only):
        self.calls.append(("trades", wallet, limit, offset, taker_only))
        return self.trades[:limit]

    def positions(self, wallet, *, size_threshold):
        self.calls.append(("positions", wallet, size_threshold))
        return self.positions_payload

    def closed_positions(self, wallet, *, limit, offset, size_threshold):
        self.calls.append(("closed", wallet, limit, offset, size_threshold))
        return self.closed_payload[:limit]

    def position_values(self, wallet):
        self.calls.append(("value", wallet))
        return self.values_payload


class DeferredScreenClient:
    def wallet_trades(self, wallet, *, limit, offset, taker_only):
        raise HttpClientError(
            "shared upstream request budget is cooling down",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=120,
        )


def _seed_l1(conn, wallet: str, *, now: int = 1_000) -> None:
    record_wallet_sighting(
        conn,
        CandidateAddress(address=wallet, sources="manual", labels="seed"),
        trusted_source=True,
        now=now,
    )
    conn.commit()


def _trades(*amounts: float) -> list[dict]:
    return [
        {
            "transactionHash": f"0x{index:064x}",
            "timestamp": 2_000 + index,
            "slug": f"market-{index % 3}",
            "size": amount / 0.5,
            "price": 0.5,
        }
        for index, amount in enumerate(amounts)
    ]


def test_screen_planner_only_queues_l1_wallets_under_waterline(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    l1_wallet = "0x" + "1" * 40
    l0_wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed_l1(conn, l1_wallet)
        record_wallet_sighting(
            conn,
            CandidateAddress(address=l0_wallet, sources="stream"),
            verified_trade=True,
            allow_l1=False,
            now=1_000,
        )
        conn.commit()

        summary = plan_wallet_screen_jobs(
            conn,
            limit=10,
            max_active_jobs=1,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        assert summary.targets_seen == 1
        assert summary.jobs_enqueued == 1
        jobs = conn.execute(
            "SELECT wallet, job_type, job_action, job_scope, status FROM pipeline_jobs"
        ).fetchall()
        assert [dict(row) for row in jobs] == [
            {
                "wallet": l1_wallet,
                "job_type": JOB_TYPE,
                "job_action": "screen_recent:v1",
                "job_scope": "sample",
                "status": "queued",
            }
        ]
    finally:
        conn.close()


def test_screen_planner_waterline_ignores_exhausted_queued_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    target_wallet = "0x" + "b" * 40
    running_wallet = "0x" + "c" * 40
    claimable_wallet = "0x" + "d" * 40
    exhausted_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        _seed_l1(conn, target_wallet)
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
                ) VALUES (?, ?, ?, 'sample', ?, ?, ?, 1000, 1000)
                """,
                (
                    JOB_TYPE,
                    wallet,
                    f"screen_recent:v1:{wallet[-4:]}",
                    status,
                    attempts,
                    max_attempts,
                ),
            )
        conn.commit()

        summary = plan_wallet_screen_jobs(
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


def test_screen_worker_promotes_to_l2_at_100_usdc_with_one_bounded_trade_call(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    client = FakeScreenClient(trades=_trades(20, 30, 50))
    try:
        run_migrations(conn)
        _seed_l1(conn, wallet)
        plan_wallet_screen_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        summary = run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="screen-test",
            client=client,
        )

        assert summary.jobs_attempted == 1
        assert summary.jobs_succeeded == 1
        assert summary.promoted_l2 == 1
        assert get_wallet_level(conn, wallet).level is WalletLevel.L2
        screen = conn.execute(
            "SELECT * FROM wallet_screen_summaries WHERE wallet = ?", (wallet,)
        ).fetchone()
        assert screen["sample_trade_count"] == 3
        assert screen["sample_volume_usdc"] == pytest.approx(100)
        assert screen["sample_market_count"] == 3
        assert screen["screen_complete"] == 1
        assert screen["screen_qualified"] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_pnl_summaries WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wallet_activity'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ? AND job_type != ?",
            (wallet, JOB_TYPE),
        ).fetchone()[0] == 0
        assert client.calls == [("trades", wallet, 10, 0, False)]
    finally:
        conn.close()


def test_screen_worker_keeps_sub_100_usdc_wallet_at_l1_without_retry_loop(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    client = FakeScreenClient(trades=_trades(10, 20, 30))
    try:
        run_migrations(conn)
        _seed_l1(conn, wallet)
        plan_wallet_screen_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()

        summary = run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="screen-test",
            client=client,
        )
        second_plan = plan_wallet_screen_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=3_000,
        )

        assert summary.jobs_succeeded == 1
        assert summary.promoted_l2 == 0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L1
        screen = conn.execute(
            "SELECT screen_complete, screen_qualified, screen_reason "
            "FROM wallet_screen_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(screen) == {
            "screen_complete": 1,
            "screen_qualified": 0,
            "screen_reason": "sample_volume_below_100_usdc",
        }
        assert client.calls == [("trades", wallet, 10, 0, False)]
        assert second_plan.jobs_enqueued == 0
    finally:
        conn.close()


def test_failed_screen_requeues_only_after_new_sighting_and_cooldown(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    client = FakeScreenClient(trades=_trades(10, 20, 30))
    try:
        run_migrations(conn)
        _seed_l1(conn, wallet)
        plan_wallet_screen_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_screening.time.time",
            lambda: 2_100,
        )
        run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="screen-test",
            client=client,
        )

        before_new_activity = plan_wallet_screen_jobs(
            conn,
            limit=1,
            shard_count=1,
            rescreen_after_seconds=100,
            now=2_300,
        )
        record_wallet_sighting(
            conn,
            CandidateAddress(address=wallet, sources="stream", labels="new_activity"),
            verified_trade=True,
            now=2_400,
        )
        before_cooldown = plan_wallet_screen_jobs(
            conn,
            limit=1,
            shard_count=1,
            rescreen_after_seconds=500,
            now=2_500,
        )
        after_cooldown = plan_wallet_screen_jobs(
            conn,
            limit=1,
            shard_count=1,
            rescreen_after_seconds=500,
            now=2_700,
        )
        conn.commit()

        assert before_new_activity.jobs_enqueued == 0
        assert before_cooldown.jobs_enqueued == 0
        assert after_cooldown.jobs_enqueued == 1
        jobs = conn.execute(
            "SELECT job_action, status FROM pipeline_jobs WHERE wallet = ? ORDER BY job_id",
            (wallet,),
        ).fetchall()
        assert [dict(row) for row in jobs] == [
            {"job_action": "screen_recent:v1", "status": "done"},
            {"job_action": "screen_recent:v1:refresh:2100", "status": "queued"},
        ]
    finally:
        conn.close()


def test_screen_planner_rotates_source_buckets_to_avoid_stream_starvation(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stream_wallets = ["0x" + f"{index:040x}" for index in range(1, 5)]
    curated_wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        for index, wallet in enumerate(stream_wallets):
            record_wallet_sighting(
                conn,
                CandidateAddress(address=wallet, sources="stream"),
                trusted_source=True,
                now=2_000 - index,
            )
        record_wallet_sighting(
            conn,
            CandidateAddress(address=curated_wallet, sources="manual_watchlist"),
            trusted_source=True,
            now=1_000,
        )
        conn.commit()

        summary = plan_wallet_screen_jobs(
            conn,
            limit=2,
            max_active_jobs=10,
            shard_count=1,
            now=3_000,
        )
        conn.commit()

        queued = {
            row["wallet"]
            for row in conn.execute(
                "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
                (JOB_TYPE,),
            )
        }
        assert summary.jobs_enqueued == 2
        assert curated_wallet in queued
        assert len(queued.intersection(stream_wallets)) == 1
        assert _table_exists(conn, "pipeline_jobs")
    finally:
        conn.close()


def test_screen_worker_defers_shared_cooldown_without_consuming_attempt(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        _seed_l1(conn, wallet)
        plan_wallet_screen_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_screening.time.time",
            lambda: 3_000,
        )

        summary = run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=2,
            worker_id="screen-deferred",
            client=DeferredScreenClient(),
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
            "next_attempt_at": 3_120,
        }
    finally:
        conn.close()
