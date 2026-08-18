import random

import pytest

import pm_robot.orchestration.wallet_screening as wallet_screening_module
from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_screening import (
    DEFAULT_PRIORITY_AGING_SECONDS,
    JOB_TYPE,
    _candidate_bucket_sql,
    _fair_targets,
    _screen_job_action,
    _screen_priority,
    _wallet_shard,
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


def _legacy_screen_candidates(conn, *, cutoff: int) -> list[dict]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                levels.wallet,
                levels.last_seen_at,
                COALESCE(observed.sources, '') AS sources,
                COALESCE(observed.recent_usdc_total, 0) AS observed_usdc,
                COALESCE(observed.recent_trade_count, 0) AS observed_trades,
                COALESCE(screen.updated_at, 0) AS screen_updated_at
            FROM wallet_levels AS levels
            LEFT JOIN observed_wallets AS observed ON observed.wallet = levels.wallet
            LEFT JOIN wallet_screen_summaries AS screen ON screen.wallet = levels.wallet
            WHERE levels.level = 'l1'
              AND levels.hard_risk_block = 0
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_jobs AS active_job
                    WHERE active_job.job_type = ?
                      AND active_job.wallet = levels.wallet
                      AND (
                            active_job.status = 'running'
                         OR (active_job.status = 'queued' AND active_job.attempts < active_job.max_attempts)
                      )
              )
              AND (
                    screen.wallet IS NULL
                 OR screen.screen_complete = 0
                 OR (
                        screen.screen_qualified = 0
                    AND screen.updated_at <= ?
                    AND levels.last_seen_at > screen.updated_at
                 )
              )
            ORDER BY levels.last_seen_at DESC, levels.wallet ASC
            """,
            (JOB_TYPE, cutoff),
        ).fetchall()
    ]


def test_screen_worker_enables_priority_aging_when_claiming(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    captured = {}

    def fake_claim(*_args, **kwargs):
        captured.update(kwargs)
        return None

    try:
        monkeypatch.setattr(wallet_screening_module, "claim_pipeline_job", fake_claim)
        result = run_wallet_screen_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="aging-test",
            client=FakeScreenClient(trades=[]),
        )

        assert result.jobs_attempted == 0
        assert captured["priority_aging_seconds"] == DEFAULT_PRIORITY_AGING_SECONDS
    finally:
        conn.close()


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
                    "job_action": "screen_recent:v3",
                "job_scope": "sample",
                "status": "queued",
            }
        ]
    finally:
        conn.close()


def test_screen_planner_admits_qualifying_l0_overflow_before_queueing(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        record_wallet_sighting(
            conn,
            CandidateAddress(address=wallet, sources="stream"),
            recent_trades=[
                {
                    "transaction_hash": "0x" + "a" * 64,
                    "timestamp": 1_000,
                    "market": "market-a",
                    "side": "BUY",
                    "usdc_size": 60,
                },
                {
                    "transaction_hash": "0x" + "b" * 64,
                    "timestamp": 1_001,
                    "market": "market-b",
                    "side": "BUY",
                    "usdc_size": 60,
                },
            ],
            verified_trade=True,
            allow_l1=False,
            now=1_000,
        )
        conn.commit()
        assert get_wallet_level(conn, wallet).level is WalletLevel.L0
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone() is None

        summary = plan_wallet_screen_jobs(
            conn,
            limit=1,
            max_active_jobs=1,
            shard_count=1,
            now=2_000,
        )
        conn.commit()

        assert summary.wallets_admitted == 1
        assert summary.jobs_enqueued == 1
        assert get_wallet_level(conn, wallet).level is WalletLevel.L1
        observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert tuple(observed) == (2_000, "deferred_l0_admission")
        assert conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == "queued"
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
                    f"screen_recent:v2:{wallet[-4:]}",
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


def test_screen_worker_promotes_to_l2_after_a_material_bounded_recent_screen(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    client = FakeScreenClient(trades=_trades(100, 100, 100))
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
        assert screen["sample_volume_usdc"] == pytest.approx(300)
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
            "screen_reason": "sample_volume_below_300_usdc",
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
            {"job_action": "screen_recent:v3", "status": "done"},
            {"job_action": "screen_recent:v3:refresh:2100", "status": "queued"},
        ]
    finally:
        conn.close()


def test_screen_planner_rotates_source_buckets_to_avoid_stream_starvation(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    stream_wallets = ["0x" + f"{index:040x}" for index in range(1, 21)]
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


def test_screen_planner_bounded_bucket_queries_match_legacy_fair_targets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    rng = random.Random(20260811)
    sources = [
        "stream",
        "manual_watchlist",
        "bitget_import",
        "polydata",
        "leaderboard",
        "manual_watchlist,polydata",
        "leaderboard,manual_watchlist",
        "",
    ]
    now = 10_000
    limit = 11
    shard_count = 3
    cutoff = now - 500
    try:
        run_migrations(conn)
        for index in range(96):
            wallet = "0x" + f"{index + 1:040x}"
            record_wallet_sighting(
                conn,
                CandidateAddress(address=wallet, sources=rng.choice(sources)),
                trusted_source=True,
                now=1_000 + rng.randrange(0, 4_000),
            )
            if index % 17 == 0:
                conn.execute(
                    "UPDATE wallet_levels SET hard_risk_block = 1 WHERE wallet = ?",
                    (wallet,),
                )
            if index % 19 == 0:
                conn.execute(
                    "UPDATE wallet_levels SET level = 'l2' WHERE wallet = ?",
                    (wallet,),
                )
            if index % 13 == 0:
                conn.execute(
                    """
                    INSERT INTO pipeline_jobs(
                        job_type, wallet, job_action, job_scope, status,
                        attempts, max_attempts, created_at, updated_at
                    ) VALUES (?, ?, ?, 'sample', 'running', 1, 3, 8000, 8000)
                    """,
                    (JOB_TYPE, wallet, f"screen_recent:v2:active:{index}"),
                )
            if index % 7 == 0:
                conn.execute(
                    """
                    INSERT INTO wallet_screen_summaries(
                        wallet, screen_complete, screen_qualified, updated_at
                    ) VALUES (?, 1, 1, ?)
                    """,
                    (wallet, now - 100),
                )
            elif index % 5 == 0:
                conn.execute(
                    """
                    INSERT INTO wallet_screen_summaries(
                        wallet, screen_complete, screen_qualified, updated_at
                    ) VALUES (?, 1, 0, ?)
                    """,
                    (wallet, now - 700),
                )
            elif index % 11 == 0:
                conn.execute(
                    """
                    INSERT INTO wallet_screen_summaries(
                        wallet, screen_complete, screen_qualified, updated_at
                    ) VALUES (?, 0, 0, ?)
                    """,
                    (wallet, now - 20),
                )
        conn.commit()

        expected_targets = _fair_targets(
            _legacy_screen_candidates(conn, cutoff=cutoff),
            limit=limit,
        )
        summary = plan_wallet_screen_jobs(
            conn,
            limit=limit,
            max_active_jobs=72,
            shard_count=shard_count,
            admission_limit=0,
            rescreen_after_seconds=500,
            now=now,
        )
        conn.commit()

        planned_jobs = [
            dict(row)
            for row in conn.execute(
                """
                SELECT wallet, priority, shard, job_action
                FROM pipeline_jobs
                WHERE job_type = ?
                  AND created_at = ?
                ORDER BY job_id
                """,
                (JOB_TYPE, now),
            ).fetchall()
        ]
    finally:
        conn.close()

    assert summary.targets_seen == len(expected_targets)
    assert summary.jobs_enqueued == len(expected_targets)
    assert planned_jobs == [
        {
            "wallet": str(target["wallet"]),
            "priority": _screen_priority(target),
            "shard": _wallet_shard(str(target["wallet"]), shard_count),
            "job_action": _screen_job_action(target),
        }
        for target in expected_targets
    ]


def test_screen_planner_candidate_sql_is_bounded_and_not_correlated(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    statements = []
    try:
        run_migrations(conn)
        for index in range(20):
            record_wallet_sighting(
                conn,
                CandidateAddress(address="0x" + f"{index + 1:040x}", sources="stream"),
                trusted_source=True,
                now=1_000 + index,
            )
        conn.commit()

        conn.set_trace_callback(statements.append)
        plan_wallet_screen_jobs(
            conn,
            limit=3,
            max_active_jobs=72,
            shard_count=3,
            admission_limit=0,
            now=2_000,
        )
        conn.set_trace_callback(None)
    finally:
        conn.close()

    candidate_statements = [
        statement
        for statement in statements
        if "FROM wallet_levels AS levels" in statement
        and "LEFT JOIN pipeline_jobs AS active_job" in statement
    ]
    assert len(candidate_statements) == 4
    assert all(" LIMIT " in statement.upper() for statement in candidate_statements)
    assert all("NOT EXISTS" not in statement.upper() for statement in candidate_statements)


def test_screen_planner_candidate_query_uses_planner_indexes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for index in range(200):
            record_wallet_sighting(
                conn,
                CandidateAddress(
                    address="0x" + f"{index + 1:040x}",
                    sources="stream" if index % 3 else "manual_watchlist",
                ),
                trusted_source=True,
                now=1_000 + index,
            )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, created_at, updated_at
            ) VALUES (?, ?, 'screen_recent:v2:active', 'sample', 'queued', 0, 3, 1000, 1000)
            """,
            (JOB_TYPE, "0x" + f"{1:040x}"),
        )
        conn.commit()
        plan_details = [
            row[3]
            for row in conn.execute(
                f"EXPLAIN QUERY PLAN {_candidate_bucket_sql('stream')}",
                (0, 5),
            ).fetchall()
        ]
    finally:
        conn.close()

    assert any("idx_wallet_levels_l1_screen_plan" in detail for detail in plan_details)
    assert any(
        "idx_pipeline_jobs_wallet_screen_active_lookup" in detail
        for detail in plan_details
    )


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
