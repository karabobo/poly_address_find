from __future__ import annotations

import time
from pathlib import Path

from pm_robot.orchestration.l6_validation_pipeline import (
    MAX_HISTORICAL_ACTIVITY_OFFSET,
    _fetch_activity_window,
    plan_l6_validation_jobs,
    run_l6_validation_worker,
)
from pm_robot.research.current_elite import CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import WalletLevel


WALLET = "0x6666666666666666666666666666666666666666"


class FakeValidationClient:
    def __init__(self, *, now: int, concentrated: bool = False, thin: bool = False):
        count = 2 if thin else 12
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

    def closed_positions(self, wallet, *, limit, offset, size_threshold):
        del wallet, size_threshold
        return self.closed[offset : offset + limit]

    def activity(self, wallet, *, limit, offset, start, end):
        del wallet, start, end
        return self.activity_rows[offset : offset + limit]

    def trader_leaderboard(self, **kwargs):
        return [
            {
                "proxyWallet": kwargs["user"],
                "pnl": 100,
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


def _seed_current_l5(conn, *, now: int) -> None:
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, policy_version, first_seen_at,
            last_seen_at, level_updated_at, updated_at
        ) VALUES (?, 'l5', 'relative_rank_selected', 'relative_rank_v3', ?, ?, ?, ?)
        """,
        (WALLET, now - 10_000, now, now, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count, distinct_markets,
            non_fast_trade_count, fast_market_share, total_volume_usdc,
            buy_count, sell_count, market_volume_top_share,
            strategy_tags_json, risk_flags_json, research_score,
            score_components_json, methodology_version, computed_at, updated_at
        ) VALUES (?, 'deep-artifact', 'deep', 500, 20, 500, 0, 50000,
                  300, 200, 0.2, '[]', '[]', 90, '{}',
                  'wallet_history_summary_v2', ?, ?)
        """,
        (WALLET, now, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at, research_score
        ) VALUES (?, 'l5', 'deep-artifact', 'relative_rank_v3',
                  1, 1, 20, 'stream', 'general', 'relative_rank_selected', ?, ?, 90)
        """,
        (WALLET, now, now),
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
        assert validation["official_all_pnl_usdc"] == 100
        assert validation["official_all_volume_usdc"] == 10_000
        assert validation["official_profit_intensity"] == 0.01
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


def test_failed_refresh_never_demotes_existing_l6_wallet(tmp_path: Path):
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
            worker_id="test-l6-no-demotion",
        )

        assert summary.validations_failed == 1
        assert summary.promoted_l6 == 0
        assert get_wallet_level(conn, WALLET).level is WalletLevel.L6
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
