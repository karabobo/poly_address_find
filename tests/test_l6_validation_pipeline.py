from __future__ import annotations

import time
from pathlib import Path

import pytest

from pm_robot.clients.http import HttpClientError
from pm_robot.orchestration import l6_validation_pipeline as l6_validation_module
from pm_robot.orchestration.l6_validation_pipeline import (
    MAX_HISTORICAL_ACTIVITY_OFFSET,
    _fetch_activity_window,
    _fetch_leaderboard_cross_checks,
    plan_l6_validation_jobs,
    run_l6_validation_worker,
)
from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.current_elite import CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import ensure_wallet_level, get_wallet_level
from pm_robot.wallet_levels import WalletLevel


WALLET = "0x6666666666666666666666666666666666666666"


class FakeValidationClient:
    def __init__(
        self,
        *,
        now: int,
        concentrated: bool = False,
        thin: bool = False,
        official_pnl: float = 1_000,
    ):
        count = 2 if thin else 24
        self.official_pnl = official_pnl
        self.closed = [
            {
                "timestamp": now - (index * 3 + 2) * 86_400,
                "conditionId": "one-market" if concentrated else f"market-{index % 4}",
                "realizedPnl": 10,
                "totalBought": 100,
                "asset": f"asset-{index}",
            }
            for index in range(count)
        ]
        self.activity_rows = [
            {
                "timestamp": now - (index + 1) * 40_000,
                "type": "TRADE",
                "side": "BUY" if index % 2 == 0 else "SELL",
                "usdcSize": 100 + index,
                "transactionHash": f"0x{index:064x}",
            }
            for index in range(30)
        ]

    def positions(self, wallet, *, size_threshold, limit, offset):
        del wallet, size_threshold, limit
        return [] if offset == 0 else []

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
        del wallet, size_threshold
        assert sort_by == "TIMESTAMP"
        assert sort_direction == "DESC"
        return self.closed[offset : offset + limit]

    def activity(self, wallet, *, limit, offset, start, end):
        del wallet, start, end
        return self.activity_rows[offset : offset + limit]

    def trader_leaderboard(self, **kwargs):
        return [
            {
                "proxyWallet": kwargs["user"],
                "pnl": self.official_pnl,
                "vol": 10_000,
            }
        ]


class RangeLimitedActivityClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def activity(self, wallet, *, limit, offset, start, end):
        del wallet
        self.calls.append((offset, start, end))
        if offset > MAX_HISTORICAL_ACTIVITY_OFFSET:
            raise AssertionError("historical activity offset exceeded")
        selected = [row for row in self.rows if start <= row["timestamp"] <= end]
        selected.sort(key=lambda row: row["timestamp"], reverse=True)
        return selected[offset : offset + limit]


def _seed_current_l5(conn, *, now: int, wallet: str = WALLET) -> None:
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, policy_version, first_seen_at,
            last_seen_at, level_updated_at, updated_at
        ) VALUES (?, 'l5', 'relative_rank_selected', ?, ?, ?, ?, ?)
        """,
        (wallet, SELECTION_POLICY_VERSION, now - 10_000, now, now, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count, distinct_markets,
            non_fast_trade_count, fast_market_share, total_volume_usdc,
            buy_count, sell_count, market_volume_top_share,
            strategy_tags_json, risk_flags_json, research_score,
            diagnostic_score, forward_selection_score,
            score_components_json, forward_score_components_json,
            methodology_version, computed_at, updated_at
        ) VALUES (?, ?, 'deep', 500, 20, 500, 0, 50000,
                  300, 200, 0.2, '[]', '[]', 90, 90, 90, '{}', '{}',
                  ?, ?, ?)
        """,
        (wallet, f"deep-artifact-{wallet[-4:]}", METHODOLOGY_VERSION, now, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at,
            research_score, forward_selection_score, score_status
        ) VALUES (?, 'l5', ?, ?,
                  1, 1, 20, 'stream', 'general', 'relative_rank_selected',
                  ?, ?, 90, 90, 'valid')
        """,
        (wallet, f"deep-artifact-{wallet[-4:]}", SELECTION_POLICY_VERSION, now, now),
    )
    conn.commit()


def _plan(conn, *, now: int):
    return plan_l6_validation_jobs(
        conn,
        limit=5,
        max_active_jobs=10,
        shard_count=1,
        now=now,
    )


def test_l6_planner_commits_after_each_enqueue_to_release_sqlite_write_lock(
    tmp_path: Path, monkeypatch
):
    now = int(time.time())
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path, timeout_seconds=0.05)
    wallets = ["0x" + "1" * 40, "0x" + "2" * 40]
    probe_wallet = "0x" + "9" * 40
    calls = 0
    original_enqueue = l6_validation_module.enqueue_pipeline_job

    def enqueue_with_concurrent_write(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            other = connect(db_path, timeout_seconds=0.05)
            try:
                ensure_wallet_level(
                    other,
                    probe_wallet,
                    reason="concurrent_l6_planner_probe",
                    now=now + 1,
                )
                other.commit()
            finally:
                other.close()
        return original_enqueue(*args, **kwargs)

    monkeypatch.setattr(
        l6_validation_module,
        "enqueue_pipeline_job",
        enqueue_with_concurrent_write,
    )
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_current_l5(conn, now=now, wallet=wallet)
        conn.commit()

        summary = _plan(conn, now=now)

        assert summary.jobs_enqueued == 2
        assert calls == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ?",
            (l6_validation_module.JOB_TYPE,),
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_l6_planner_revalidates_existing_l6_before_new_l5_candidates(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    l5_wallet = "0x" + "1" * 40
    l6_wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now, wallet=l5_wallet)
        _seed_current_l5(conn, now=now, wallet=l6_wallet)
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6' WHERE wallet = ?",
            (l6_wallet,),
        )
        conn.commit()

        summary = plan_l6_validation_jobs(
            conn,
            limit=1,
            max_active_jobs=1,
            shard_count=1,
            now=now,
        )

        assert summary.jobs_enqueued == 1
        assert conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
            (l6_validation_module.JOB_TYPE,),
        ).fetchone()[0] == l6_wallet
    finally:
        conn.close()


def test_l6_worker_promotes_only_after_passing_independent_validation(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        plan = _plan(conn, now=now)
        conn.commit()

        summary = run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now),
            sleep_seconds=0,
            worker_id="test-l6-worker",
        )

        assert plan.jobs_enqueued == 1
        assert summary.validations_passed == 1
        assert summary.promoted_l6 == 1
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L6
        validation = conn.execute(
            "SELECT * FROM wallet_l6_validations WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
        assert validation["decision"] == "pass"
        assert validation["official_all_pnl_usdc"] == 1_000
        assert validation["official_all_volume_usdc"] == 10_000
        assert validation["official_profit_intensity"] == 0.1
        assert (tmp_path / "parquet" / validation["raw_relative_path"]).is_file()
    finally:
        conn.close()


def test_warning_or_fail_keeps_existing_l5_level(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        _plan(conn, now=now)
        conn.commit()

        summary = run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now, thin=True),
            sleep_seconds=0,
            worker_id="test-l6-warning",
        )

        assert summary.validations_warned == 1
        assert summary.promoted_l6 == 0
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L5
        assert conn.execute(
            "SELECT decision FROM wallet_l6_validations WHERE wallet = ?",
            (WALLET,),
        ).fetchone()[0] == "warning"
    finally:
        conn.close()


def test_recent_validation_prevents_duplicate_queueing_until_refresh(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        assert _plan(conn, now=now).jobs_enqueued == 1
        conn.commit()
        run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now),
            sleep_seconds=0,
            worker_id="test-l6-refresh",
        )

        second = _plan(conn, now=now + 60)

        assert second.targets_seen == 0
        assert second.jobs_enqueued == 0
    finally:
        conn.close()


def test_worker_skips_job_when_summary_becomes_stale_after_planning(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        assert _plan(conn, now=now).jobs_enqueued == 1
        conn.execute(
            "UPDATE wallet_history_summaries SET updated_at = ? WHERE wallet = ?",
            (now - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS - 60, WALLET),
        )
        conn.commit()

        summary = run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now),
            sleep_seconds=0,
            worker_id="test-l6-stale-evidence",
        )

        assert summary.jobs_succeeded == 1
        assert summary.validations_passed == 0
        assert summary.promoted_l6 == 0
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_l6_validations WHERE wallet = ?",
            (WALLET,),
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_failed_refresh_reclassifies_existing_l6_wallet_to_l5(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6', level_reason = 'prior_validation' WHERE wallet = ?",
            (WALLET,),
        )
        conn.commit()
        assert _plan(conn, now=now).jobs_enqueued == 1
        conn.commit()

        summary = run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now, concentrated=True),
            sleep_seconds=0,
            worker_id="test-l6-reclassification",
        )

        assert summary.validations_failed == 1
        assert summary.promoted_l6 == 0
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L5
        event = conn.execute(
            "SELECT from_level, to_level, reason FROM wallet_level_events "
            "WHERE wallet = ? ORDER BY event_id DESC LIMIT 1",
            (WALLET,),
        ).fetchone()
        assert tuple(event) == ("l6", "l5", "l6_refresh_not_currently_valid")
    finally:
        conn.close()


def test_base_pass_does_not_promote_when_current_quality_overlay_fails(tmp_path: Path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        _seed_current_l5(conn, now=now)
        _plan(conn, now=now)
        conn.commit()

        summary = run_l6_validation_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            client=FakeValidationClient(now=now, official_pnl=100),
            sleep_seconds=0,
            worker_id="test-l6-quality-overlay",
        )

        assert summary.validations_passed == 1
        assert summary.promoted_l6 == 0
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L5
        output = conn.execute(
            "SELECT output_json FROM pipeline_jobs WHERE wallet = ?",
            (WALLET,),
        ).fetchone()[0]
        assert "insufficient_official_all_time_pnl" in output
    finally:
        conn.close()


def test_activity_fetch_splits_dense_time_ranges_before_offset_limit():
    end = int(time.time())
    start = end - 90 * 86_400
    rows = [
        {
            "timestamp": start + index * 1_000,
            "type": "TRADE",
            "transactionHash": f"0x{index:064x}",
        }
        for index in range(6_000)
    ]
    client = RangeLimitedActivityClient(rows)

    fetched, complete = _fetch_activity_window(
        client,
        WALLET,
        start=start,
        end=end,
        sleep_seconds=0,
    )

    assert complete is True
    assert len(fetched) == len(rows)
    assert len({row["transactionHash"] for row in fetched}) == len(rows)
    assert max(offset for offset, _start, _end in client.calls) == MAX_HISTORICAL_ACTIVITY_OFFSET
    assert len({(call_start, call_end) for _offset, call_start, call_end in client.calls}) > 1


def test_l6_leaderboard_crosscheck_rejects_a_different_wallet():
    class MismatchedLeaderboardClient:
        def trader_leaderboard(self, **kwargs):
            del kwargs
            return [
                {
                    "proxyWallet": "0x" + "f" * 40,
                    "pnl": 1_000_000,
                    "vol": 1,
                }
            ]

    assert _fetch_leaderboard_cross_checks(
        MismatchedLeaderboardClient(),
        WALLET,
    ) == []


def test_l6_leaderboard_crosscheck_propagates_upstream_failure():
    class FailedLeaderboardClient:
        def trader_leaderboard(self, **kwargs):
            del kwargs
            raise HttpClientError(
                "service unavailable",
                status_code=503,
                error_type="server_error",
            )

    with pytest.raises(HttpClientError, match="service unavailable"):
        _fetch_leaderboard_cross_checks(FailedLeaderboardClient(), WALLET)
