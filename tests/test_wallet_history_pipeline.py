import json
import random
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
    WalletHistoryTerminalDataQualityError,
    _fetch_official_all_profit,
    _is_terminal_history_error,
    plan_wallet_history_jobs,
    run_wallet_history_worker,
)
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage import wallet_history_planner_state as planner_state_module
from pm_robot.storage.wallet_levels import (
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
)
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


class TerminalHistoryClient:
    def activity(self, wallet, *, limit, offset):
        raise ValueError("incompatible history data: local artifact depth replacement")


class DeferredHistoryClient:
    def activity(self, wallet, *, limit, offset):
        raise HttpClientError(
            "shared upstream request budget is cooling down",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=180,
        )


class TransientHistoryClient:
    def activity(self, wallet, *, limit, offset):
        raise HttpClientError(
            "upstream API exploded: gateway timeout",
            status_code=502,
            error_type="api_error",
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
    if level.value >= WalletLevel.L2.value:
        conn.execute(
            """
            INSERT INTO wallet_screen_summaries(
                wallet, sample_trade_count, sample_volume_usdc,
                sample_market_count, screen_complete, screen_qualified,
                source_snapshot_json, computed_at, updated_at
            ) VALUES (?, 25, 500, 2, 1, 1,
                      '{"policy_version":"v3","sample_max_trade_usdc":100}',
                      1000, 1000)
            ON CONFLICT(wallet) DO UPDATE SET
                sample_trade_count = excluded.sample_trade_count,
                sample_volume_usdc = excluded.sample_volume_usdc,
                sample_market_count = excluded.sample_market_count,
                screen_complete = excluded.screen_complete,
                screen_qualified = excluded.screen_qualified,
                source_snapshot_json = excluded.source_snapshot_json,
                updated_at = excluded.updated_at
            """,
            (wallet,),
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
            risk_flags_json, research_score, diagnostic_score,
            forward_selection_score, score_components_json,
            forward_score_components_json, methodology_version, computed_at, updated_at
        ) VALUES (?, ?, ?, 100, 10, 1000, '[]', '[]', 50, 50, 50, '{}', '{}', ?, ?, ?)
        """,
        (wallet, artifact_id, depth, methodology_version, updated_at, updated_at),
    )


_LEGACY_WALLET_HISTORY_PLAN_ELIGIBLE_CTE = """
        WITH evidence AS (
            SELECT
                levels.wallet,
                levels.level,
                levels.last_seen_at,
                COALESCE(summary.history_depth, '') AS current_depth,
                COALESCE(summary.methodology_version, '') AS current_methodology_version,
                COALESCE(summary.research_score, 0) AS research_score,
                COALESCE(summary.updated_at, 0) AS summary_updated_at,
                COALESCE(pnl.methodology_version, '') AS current_pnl_methodology_version,
                COALESCE(pnl.captured_at, 0) AS pnl_captured_at,
                COALESCE(screen.sample_trade_count, 0) AS sample_trade_count,
                COALESCE(screen.sample_volume_usdc, 0) AS sample_volume_usdc,
                COALESCE(screen.sample_market_count, 0) AS sample_market_count,
                CASE
                    WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN 'deep'
                    ELSE 'light'
                END AS target_depth,
                CASE
                    WHEN summary.wallet IS NOT NULL
                         AND COALESCE(summary.methodology_version, '') != ? THEN 1
                    ELSE 0
                END AS methodology_stale,
                CASE
                    WHEN levels.level IN ('l4', 'l5', 'l6')
                         AND COALESCE(summary.methodology_version, '') = ?
                         AND (
                                pnl.wallet IS NULL
                             OR COALESCE(pnl.methodology_version, '') != ?
                             OR (
                                    pnl.official_all_pnl_usdc IS NULL
                                AND COALESCE(pnl.captured_at, 0) <= ?
                             )
                         ) THEN 1
                    ELSE 0
                END AS pnl_refresh_needed,
                CASE
                    WHEN levels.level = 'l2'
                         AND summary.history_depth = 'light'
                         AND summary.updated_at <= ?
                         AND levels.last_seen_at > summary.updated_at THEN 1
                    WHEN levels.level IN ('l3', 'l4', 'l5', 'l6')
                         AND summary.history_depth = 'deep'
                         AND summary.updated_at <= ?
                         AND levels.last_seen_at > summary.updated_at THEN 1
                    ELSE 0
                END AS activity_refresh_needed
            FROM wallet_levels AS levels
            LEFT JOIN wallet_screen_summaries AS screen ON screen.wallet = levels.wallet
            LEFT JOIN wallet_history_summaries AS summary ON summary.wallet = levels.wallet
            LEFT JOIN wallet_pnl_summaries AS pnl ON pnl.wallet = levels.wallet
            WHERE levels.hard_risk_block = 0
              AND (
                    levels.level != 'l2'
                 OR (
                        screen.screen_complete = 1
                    AND screen.screen_qualified = 1
                    AND screen.sample_trade_count >= 3
                    AND screen.sample_market_count >= 2
                    AND screen.sample_volume_usdc >= 300
                    AND screen.source_snapshot_json LIKE '%"policy_version":"v3"%'
                 )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_jobs AS active_job
                    WHERE active_job.job_type = ?
                      AND active_job.wallet = levels.wallet
                      AND (
                            active_job.status = 'running'
                         OR (
                                active_job.status = 'queued'
                            AND active_job.attempts < active_job.max_attempts
                         )
                      )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_jobs AS terminal_job
                    WHERE terminal_job.job_type = ?
                      AND terminal_job.wallet = levels.wallet
                      AND terminal_job.status = ?
                      AND terminal_job.job_scope = CASE
                            WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN 'deep'
                            ELSE 'light'
                      END
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_jobs AS deferred_failure
                    WHERE deferred_failure.job_type = ?
                      AND deferred_failure.wallet = levels.wallet
                      AND deferred_failure.status = 'failed'
                      AND deferred_failure.next_attempt_at > ?
                      AND deferred_failure.job_scope = CASE
                            WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN 'deep'
                            ELSE 'light'
                      END
                      AND deferred_failure.job_action IN (
                            (
                                CASE
                                    WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN ?
                                    ELSE ?
                                END
                            ) || ':refresh:' || COALESCE(summary.updated_at, 0),
                            (
                                CASE
                                    WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN ?
                                    ELSE ?
                                END
                            ) || ':pnl:' || COALESCE(
                                NULLIF(pnl.captured_at, 0),
                                summary.updated_at,
                                0
                            )
                      )
              )
              AND (
                    (levels.level = 'l2' AND summary.wallet IS NULL)
                 OR (
                        levels.level = 'l2'
                    AND summary.history_depth = 'light'
                    AND (
                            COALESCE(summary.methodology_version, '') != ?
                         OR (
                                summary.updated_at <= ?
                            AND levels.last_seen_at > summary.updated_at
                         )
                    )
                 )
                 OR (
                        levels.level IN ('l3', 'l4', 'l5', 'l6')
                    AND (
                            summary.wallet IS NULL
                         OR COALESCE(summary.history_depth, '') != 'deep'
                         OR COALESCE(summary.methodology_version, '') != ?
                         OR (
                                summary.updated_at <= ?
                            AND levels.last_seen_at > summary.updated_at
                         )
                    )
                 )
                 OR (
                        levels.level IN ('l4', 'l5', 'l6')
                    AND COALESCE(summary.methodology_version, '') = ?
                    AND (
                           pnl.wallet IS NULL
                        OR COALESCE(pnl.methodology_version, '') != ?
                        OR (
                               pnl.official_all_pnl_usdc IS NULL
                           AND COALESCE(pnl.captured_at, 0) <= ?
                        )
                    )
                 )
              )
        ), eligible AS (
            SELECT
                evidence.*,
                CASE
                    WHEN current_depth != target_depth THEN 'required_depth'
                    WHEN methodology_stale = 1 THEN 'methodology_upgrade'
                    WHEN activity_refresh_needed = 1 THEN 'new_activity_after_refresh_window'
                    WHEN pnl_refresh_needed = 1 THEN 'pnl_evidence_refresh'
                    ELSE 'new_activity_after_refresh_window'
                END AS refresh_lane,
                CASE
                    WHEN methodology_stale = 1 AND level = 'l6' THEN 0
                    WHEN methodology_stale = 1 AND level = 'l5' THEN 1
                    WHEN methodology_stale = 1 AND level = 'l4' THEN 2
                    WHEN methodology_stale = 1 AND level = 'l3' THEN 3
                    WHEN pnl_refresh_needed = 1 AND level = 'l6' THEN 4
                    WHEN pnl_refresh_needed = 1 AND level = 'l5' THEN 5
                    WHEN pnl_refresh_needed = 1 AND level = 'l4' THEN 6
                    WHEN level IN ('l3', 'l4', 'l5', 'l6') AND current_depth != 'deep' THEN 7
                    WHEN methodology_stale = 1 THEN 8
                    WHEN level IN ('l3', 'l4', 'l5', 'l6') THEN 9
                    WHEN current_depth = '' THEN 10
                    ELSE 11
                END AS urgency
            FROM evidence
        )
"""


def _legacy_wallet_history_plan_candidate_params(
    *,
    light_refresh_seconds: int = wallet_history_module.DEFAULT_LIGHT_REFRESH_SECONDS,
    deep_refresh_seconds: int,
    now: int,
) -> tuple:
    return (
        METHODOLOGY_VERSION,
        METHODOLOGY_VERSION,
        PNL_METHODOLOGY_VERSION,
        now - PNL_INCOMPLETE_REFRESH_SECONDS,
        now - max(0, int(light_refresh_seconds)),
        now - max(0, int(deep_refresh_seconds)),
        JOB_TYPE,
        JOB_TYPE,
        wallet_history_module.PIPELINE_TERMINAL_FAILED_STATUS,
        JOB_TYPE,
        now,
        DEEP_ACTION,
        LIGHT_ACTION,
        DEEP_ACTION,
        LIGHT_ACTION,
        METHODOLOGY_VERSION,
        now - max(0, int(light_refresh_seconds)),
        METHODOLOGY_VERSION,
        now - max(0, int(deep_refresh_seconds)),
        METHODOLOGY_VERSION,
        PNL_METHODOLOGY_VERSION,
        now - PNL_INCOMPLETE_REFRESH_SECONDS,
    )


def _legacy_window_plan_candidates(
    conn,
    *,
    lane_limit: int,
    deep_refresh_seconds: int,
    now: int,
) -> list[dict]:
    sql = (
        _LEGACY_WALLET_HISTORY_PLAN_ELIGIBLE_CTE
        + """
        , ranked AS (
            SELECT
                eligible.*,
                ROW_NUMBER() OVER (
                    PARTITION BY target_depth, refresh_lane
                    ORDER BY urgency, research_score DESC,
                             sample_market_count DESC, sample_volume_usdc DESC,
                             sample_trade_count DESC, last_seen_at DESC, wallet ASC
                ) AS lane_rank
            FROM eligible
        )
        SELECT
            wallet, level, last_seen_at, current_depth,
            current_methodology_version, methodology_stale,
            current_pnl_methodology_version, pnl_captured_at, pnl_refresh_needed,
            activity_refresh_needed,
            research_score, summary_updated_at, target_depth, refresh_lane, urgency,
            sample_trade_count, sample_volume_usdc, sample_market_count, lane_rank
        FROM ranked
        WHERE lane_rank <= ?
        ORDER BY lane_rank, target_depth, refresh_lane, urgency,
                 research_score DESC, wallet ASC
        """
    )
    rows = [
        dict(row)
        for row in conn.execute(
            sql,
            (
                *_legacy_wallet_history_plan_candidate_params(
                    deep_refresh_seconds=deep_refresh_seconds,
                    now=now,
                ),
                lane_limit,
            ),
        ).fetchall()
    ]
    wallet_history_module._attach_wallet_history_plan_sources(conn, rows)
    return rows


def _candidate_identity(rows: list[dict]) -> list[tuple]:
    return [
        (
            row["wallet"],
            row["lane_rank"],
            row["target_depth"],
            row["refresh_lane"],
            row["urgency"],
            row["research_score"],
            row["sample_market_count"],
            row["sample_volume_usdc"],
            row["sample_trade_count"],
            row["last_seen_at"],
        )
        for row in rows
    ]


def test_history_planner_bounded_scan_matches_legacy_window_candidate_order(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        "l2_required": "0x" + "1" * 40,
        "l3_required": "0x" + "2" * 40,
        "l5_method": "0x" + "3" * 40,
        "l4_pnl": "0x" + "4" * 40,
        "l3_activity": "0x" + "5" * 40,
        "l6_method": "0x" + "6" * 40,
    }
    try:
        run_migrations(conn)
        _seed_level(conn, wallets["l2_required"], WalletLevel.L2)
        _seed_level(conn, wallets["l3_required"], WalletLevel.L3)
        _seed_level(conn, wallets["l5_method"], WalletLevel.L5)
        _seed_level(conn, wallets["l4_pnl"], WalletLevel.L4)
        _seed_level(conn, wallets["l3_activity"], WalletLevel.L3)
        _seed_level(conn, wallets["l6_method"], WalletLevel.L5)
        advance_wallet_level(
            conn,
            wallets["l6_method"],
            to_level=WalletLevel.L6,
            reason="test_level",
            now=1_600,
        )
        _seed_history_summary(
            conn,
            wallets["l5_method"],
            depth="deep",
            updated_at=1_900,
            methodology_version="wallet_history_summary_v1",
        )
        _seed_history_summary(conn, wallets["l4_pnl"], depth="deep", updated_at=1_900)
        _seed_history_summary(conn, wallets["l3_activity"], depth="deep", updated_at=900)
        _seed_history_summary(
            conn,
            wallets["l6_method"],
            depth="deep",
            updated_at=1_900,
            methodology_version="wallet_history_summary_v1",
        )
        conn.execute(
            "UPDATE wallet_history_summaries SET research_score = 77 WHERE wallet = ?",
            (wallets["l5_method"],),
        )
        conn.commit()

        legacy = _legacy_window_plan_candidates(
            conn,
            lane_limit=3,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
        lane_split = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=3,
            deep_refresh_seconds=1_000,
            now=2_000,
        )

        assert _candidate_identity(lane_split) == _candidate_identity(legacy)
    finally:
        conn.close()


def test_history_planner_snapshot_candidates_match_legacy_sql_on_random_data(tmp_path):
    rng = random.Random(20260811)
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallets = ["0x" + f"{index:040x}" for index in range(1, 181)]
        conn.executemany(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, first_seen_at, updated_at
            ) VALUES (?, ?, 'synthetic', 1000, ?)
            """,
            [
                (
                    wallet,
                    rng.choice(["stream", "manual_watchlist", "polydata", "leaderboard"]),
                    rng.randint(1_000, 4_000),
                )
                for wallet in wallets
            ],
        )
        conn.executemany(
            """
            INSERT INTO wallet_levels(
                wallet, level, hard_risk_block, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, ?, ?, 1000, ?, ?)
            """,
            [
                (
                    wallet,
                    rng.choice(["l1", "l2", "l3", "l4", "l5", "l6"]),
                    1 if rng.random() < 0.05 else 0,
                    rng.randint(1_100, 5_000),
                    rng.randint(1_100, 5_000),
                )
                for wallet in wallets
            ],
        )
        conn.executemany(
            """
                INSERT INTO wallet_screen_summaries(
                    wallet, sample_trade_count, sample_volume_usdc,
                    sample_market_count, screen_complete, screen_qualified,
                    source_snapshot_json, computed_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, 1,
                          '{"policy_version":"v3","sample_max_trade_usdc":1000}',
                          1000, 1000)
            """,
            [
                (
                    wallet,
                    rng.randint(0, 80),
                    rng.uniform(0, 5_000),
                    rng.randint(0, 12),
                )
                for wallet in wallets
            ],
        )
        for wallet in wallets:
            if rng.random() >= 0.65:
                continue
            level = conn.execute(
                "SELECT level FROM wallet_levels WHERE wallet = ?",
                (wallet,),
            ).fetchone()["level"]
            depth = rng.choice(["light", "deep"])
            if level in {"l3", "l4", "l5", "l6"} and rng.random() < 0.6:
                depth = "deep"
            if level == "l2" and rng.random() < 0.7:
                depth = "light"
            _seed_history_summary(
                conn,
                wallet,
                depth=depth,
                updated_at=rng.randint(1_000, 3_900),
                methodology_version=rng.choice(
                    [METHODOLOGY_VERSION, "wallet_history_summary_v1"]
                ),
            )
            conn.execute(
                "UPDATE wallet_history_summaries SET research_score = ? WHERE wallet = ?",
                (rng.uniform(0, 100), wallet),
            )
        for wallet in wallets:
            if rng.random() >= 0.4:
                continue
            conn.execute(
                """
                INSERT INTO wallet_pnl_summaries(
                    wallet, official_all_pnl_usdc, official_all_volume_usdc,
                    official_profit_intensity, coverage, methodology_version,
                    captured_at, updated_at
                ) VALUES (?, ?, 10000, 0.01, 'test', ?, ?, ?)
                """,
                (
                    wallet,
                    None if rng.random() < 0.5 else rng.uniform(-500, 1_000),
                    rng.choice([PNL_METHODOLOGY_VERSION, "old_pnl"]),
                    rng.randint(1_000, 3_900),
                    rng.randint(1_000, 3_900),
                ),
            )
        for wallet in wallets[::17]:
            level = conn.execute(
                "SELECT level FROM wallet_levels WHERE wallet = ?",
                (wallet,),
            ).fetchone()["level"]
            scope = "deep" if level in {"l3", "l4", "l5", "l6"} else "light"
            action = DEEP_ACTION if scope == "deep" else LIGHT_ACTION
            summary = conn.execute(
                "SELECT updated_at FROM wallet_history_summaries WHERE wallet = ?",
                (wallet,),
            ).fetchone()
            marker = int(summary["updated_at"]) if summary else 0
            status = rng.choice(["running", "queued", "terminal_failed", "failed"])
            next_attempt_at = 9_999 if status == "failed" else 0
            conn.execute(
                """
                INSERT INTO pipeline_jobs(
                    job_type, wallet, job_action, job_scope, status,
                    attempts, max_attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, 3, ?, 1000, 1000)
                """,
                (
                    JOB_TYPE,
                    wallet,
                    f"{action}:refresh:{marker}",
                    scope,
                    status,
                    next_attempt_at,
                ),
            )
        conn.commit()

        legacy = _legacy_window_plan_candidates(
            conn,
            lane_limit=9,
            deep_refresh_seconds=1_800,
            now=4_000,
        )
        snapshot = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=9,
            deep_refresh_seconds=1_800,
            now=4_000,
        )

        assert _candidate_identity(snapshot) == _candidate_identity(legacy)
    finally:
        conn.close()


def test_history_planner_lane_quota_is_independent_per_refresh_lane(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    required_wallets = ["0x" + f"{index:040x}" for index in range(1, 5)]
    stale_wallets = ["0x" + f"{index:040x}" for index in range(11, 15)]
    try:
        run_migrations(conn)
        for wallet in required_wallets:
            _seed_level(conn, wallet, WalletLevel.L3)
        for wallet in stale_wallets:
            _seed_level(conn, wallet, WalletLevel.L5)
            _seed_history_summary(
                conn,
                wallet,
                depth="deep",
                updated_at=1_900,
                methodology_version="wallet_history_summary_v1",
            )
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=2,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
        lanes = [(row["wallet"], row["refresh_lane"], row["lane_rank"]) for row in rows]

        assert sum(lane == "required_depth" for _, lane, _ in lanes) == 2
        assert sum(lane == "methodology_upgrade" for _, lane, _ in lanes) == 2
        assert {rank for _, lane, rank in lanes if lane == "required_depth"} == {1, 2}
        assert {rank for _, lane, rank in lanes if lane == "methodology_upgrade"} == {
            1,
            2,
        }
    finally:
        conn.close()


def test_history_planner_attaches_sources_after_bounded_lane_selection(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        "missing": "0x" + "1" * 40,
        "empty": "0x" + "2" * 40,
        "multi": "0x" + "3" * 40,
        "curated": "0x" + "4" * 40,
    }
    try:
        run_migrations(conn)
        for wallet in wallets.values():
            _seed_level(conn, wallet, WalletLevel.L2)
        conn.execute(
            "DELETE FROM observed_wallets WHERE wallet = ?",
            (wallets["missing"],),
        )
        conn.execute(
            "UPDATE observed_wallets SET sources = '' WHERE wallet = ?",
            (wallets["empty"],),
        )
        conn.execute(
            "UPDATE observed_wallets SET sources = 'stream,polydata' WHERE wallet = ?",
            (wallets["multi"],),
        )
        conn.execute(
            "UPDATE observed_wallets SET sources = 'manual_watchlist' WHERE wallet = ?",
            (wallets["curated"],),
        )
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=10,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
    finally:
        conn.close()

    sources = {row["wallet"]: row["sources"] for row in rows}
    assert sources == {
        wallets["missing"]: "",
        wallets["empty"]: "",
        wallets["multi"]: "stream,polydata",
        wallets["curated"]: "manual_watchlist",
    }


def test_history_planner_source_batching_is_bounded_and_parameterized(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    statements: list[tuple[str, tuple]] = []

    class SmallVariableLimitConnection:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def getlimit(self, category):
            assert category == sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER
            return 3

        def execute(self, sql, parameters=()):
            statements.append((sql, tuple(parameters)))
            return self.wrapped.execute(sql, parameters)

    try:
        run_migrations(conn)
        for index in range(10):
            wallet = "0x" + f"{index + 1:040x}"
            _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            SmallVariableLimitConnection(conn),
            lane_limit=10,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
    finally:
        conn.close()

    source_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM observed_wallets WHERE wallet IN" in statement
    ]
    assert len(rows) == 10
    assert len(source_reads) == 4
    for statement, parameters in source_reads:
        assert statement.count("?") == len(parameters)
        assert len(parameters) <= 3
        assert not any(wallet in statement for wallet in parameters)


def test_history_planner_30k_candidate_scan_eqp_has_no_temp_cte_or_sort(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, first_seen_at, updated_at
            ) VALUES (?, 'stream', 'synthetic', 1000, 1000)
            """,
            [("0x" + f"{index:040x}",) for index in range(30_000)],
        )
        conn.executemany(
            """
            INSERT INTO wallet_levels(
                wallet, level, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, 'l2', 1000, 1000, 1000)
            """,
            [("0x" + f"{index:040x}",) for index in range(30_000)],
        )
        conn.executemany(
            """
                INSERT INTO wallet_screen_summaries(
                    wallet, sample_trade_count, sample_volume_usdc,
                    sample_market_count, screen_complete, screen_qualified,
                    source_snapshot_json, computed_at, updated_at
                ) VALUES (?, 25, 500, 2, 1, 1,
                          '{"policy_version":"v3","sample_max_trade_usdc":100}',
                          1000, 1000)
            """,
            [("0x" + f"{index:040x}",) for index in range(30_000)],
        )
        conn.commit()

        scan_sql = wallet_history_module._wallet_history_plan_candidate_scan_sql()
        eqp = [
            row[3]
            for row in conn.execute(
                f"EXPLAIN QUERY PLAN {scan_sql}",
                wallet_history_module._wallet_history_plan_candidate_params(
                    deep_refresh_seconds=1_000,
                    now=2_000,
                ),
            ).fetchall()
        ]
        rows = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=8,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
        source = Path(wallet_history_module.__file__).read_text()

        assert rows == []
        assert "ROW_NUMBER" not in source
        assert "PARTITION BY" not in source
        assert "eligible AS MATERIALIZED" not in source
        assert "UNION ALL" not in scan_sql
        assert "wallet_history_planner_state" in scan_sql
        assert "wallet_levels" not in scan_sql
        assert "wallet_screen_summaries" not in scan_sql
        assert not any("MATERIALIZE eligible" in detail for detail in eqp)
        assert not any(detail == "SCAN eligible" for detail in eqp)
        assert not any("USE TEMP B-TREE" in detail for detail in eqp)
        assert not any("ranked" in detail.lower() for detail in eqp)
        assert not any("observed_wallets" in detail for detail in eqp)
    finally:
        conn.close()


def test_history_planner_scans_candidates_once_without_sql_temp_lanes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    statements: list[tuple[str, tuple]] = []

    class RecordingConnection:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def execute(self, sql, parameters=()):
            statements.append((sql, tuple(parameters)))
            return self.wrapped.execute(sql, parameters)

    try:
        run_migrations(conn)
        for index, level in enumerate(
            (WalletLevel.L2, WalletLevel.L3, WalletLevel.L4, WalletLevel.L5), start=1
        ):
            wallet = "0x" + f"{index:040x}"
            _seed_level(conn, wallet, level)
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            RecordingConnection(conn),
            lane_limit=2,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
    finally:
        conn.close()

    missing_state_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "LEFT JOIN wallet_history_planner_state AS state" in statement
        and "state.wallet IS NULL" in statement
    ]
    level_refresh_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM wallet_levels" in statement and "WHERE wallet IN" in statement
    ]
    state_lane_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM wallet_history_planner_state" in statement
        and "target_depth = ?" in statement
    ]
    snapshot_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if (
            "FROM wallet_screen_summaries" in statement
            or "FROM wallet_history_summaries" in statement
            or "FROM wallet_pnl_summaries" in statement
        )
    ]
    job_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM pipeline_jobs" in statement and "WHERE job_type = ?" in statement
    ]
    source_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM observed_wallets WHERE wallet IN" in statement
    ]
    temp_writes = [
        statement
        for statement, _parameters in statements
        if "FROM wallet_history_plan_candidates" in statement
    ]

    assert rows
    assert len(missing_state_reads) <= 3
    assert len(level_refresh_reads) == 1
    assert len(state_lane_reads) == 8
    assert len(snapshot_reads) == 3
    assert len(job_reads) == 1
    assert len(source_reads) == 1
    assert all("WHERE wallet IN" in statement for statement, _ in snapshot_reads)
    assert all(len(parameters) == 4 for _, parameters in snapshot_reads)
    assert all("wallet IN" in statement for statement, _ in job_reads)
    assert len(job_reads[0][1]) == 7
    assert all(
        "wallet_screen_summaries" not in statement
        and "wallet_history_summaries" not in statement
        and "wallet_pnl_summaries" not in statement
        and "pipeline_jobs" not in statement
        for statement, _ in state_lane_reads
    )
    assert not temp_writes
    planner_sql = "\n".join(statement for statement, _parameters in statements)
    assert "NOT EXISTS" not in planner_sql
    assert "eligible AS MATERIALIZED" not in planner_sql
    assert "UNION ALL" not in planner_sql
    assert "ROW_NUMBER" not in planner_sql
    assert "PARTITION BY" not in planner_sql
    assert all(
        "ORDER BY" in statement and "LIMIT ?" in statement
        for statement, _ in state_lane_reads
    )
    assert all("SELECT 1" not in statement for statement, _ in state_lane_reads)


def test_history_planner_lane_query_uses_compact_state_index(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for index in range(8):
            _seed_level(conn, "0x" + f"{index + 1:040x}", WalletLevel.L2)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=20,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.commit()

        eqp = [
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN "
                + wallet_history_module._wallet_history_plan_lane_sql(),
                ("light", "required_depth", 4),
            )
        ]
        due_eqp = [
            row[3]
            for row in conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT state.wallet
                FROM wallet_history_planner_state AS state
                WHERE state.next_refresh_at > 0
                  AND state.next_refresh_at <= ?
                ORDER BY state.next_refresh_at ASC, state.wallet ASC
                LIMIT ?
                """,
                (2_000, 4),
            )
        ]
        bootstrap_eqp = [
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN "
                + planner_state_module.WALLET_HISTORY_PLANNER_MISSING_STATE_SQL,
                (4,),
            )
        ]
        dirty_claim_eqp = [
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN "
                + planner_state_module.WALLET_HISTORY_PLANNER_DIRTY_CLAIM_SQL,
                (4,),
            )
        ]
        dirty_readiness_eqp = [
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN "
                + planner_state_module.WALLET_HISTORY_PLANNER_DIRTY_BACKLOG_SQL
            )
        ]
    finally:
        conn.close()

    assert any("idx_wallet_history_planner_state_lane" in detail for detail in eqp)
    assert not any("wallet_levels" in detail for detail in eqp)
    assert not any("wallet_screen_summaries" in detail for detail in eqp)
    assert not any("pipeline_jobs" in detail for detail in eqp)
    assert any("idx_wallet_history_planner_state_due" in detail for detail in due_eqp)
    assert not any("wallet_levels" in detail for detail in due_eqp)
    assert not any("wallet_screen_summaries" in detail for detail in due_eqp)
    assert not any("pipeline_jobs" in detail for detail in due_eqp)
    assert any(
        "idx_wallet_levels_history_planner_bootstrap" in detail
        for detail in bootstrap_eqp
    )
    assert not any("USE TEMP B-TREE" in detail for detail in bootstrap_eqp)
    assert any(
        "idx_wallet_history_planner_dirty_due" in detail
        for detail in dirty_claim_eqp
    )
    assert not any("USE TEMP B-TREE" in detail for detail in dirty_claim_eqp)
    assert not any("wallet_levels" in detail for detail in dirty_claim_eqp)
    assert not any(
        "wallet_history_planner_state" in detail for detail in dirty_claim_eqp
    )
    assert not any("USE TEMP B-TREE" in detail for detail in dirty_readiness_eqp)
    assert not any("wallet_levels" in detail for detail in dirty_readiness_eqp)
    assert not any(
        "wallet_history_planner_state" in detail for detail in dirty_readiness_eqp
    )


def test_history_planner_state_rebuild_is_bounded_and_restart_safe(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for index in range(12):
            _seed_level(conn, "0x" + f"{index + 1:040x}", WalletLevel.L2)
        conn.commit()

        refreshed = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=5,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.rollback()
        after_rollback = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state"
        ).fetchone()[0]
        dirty_after_rollback = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty"
        ).fetchone()[0]

        first = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=5,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.commit()
        second = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=5,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_001,
        )
        conn.commit()
        third = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=5,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_002,
        )
        conn.commit()
        state_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state"
        ).fetchone()[0]
        dirty_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty"
        ).fetchone()[0]
    finally:
        conn.close()

    assert refreshed == 5
    assert after_rollback == 0
    assert dirty_after_rollback == 12
    assert (first, second, third) == (5, 5, 2)
    assert state_count == 12
    assert dirty_count == 0


def test_history_planner_refresh_preserves_concurrent_dirty_generation(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    concurrent = None
    wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        observed_generation = conn.execute(
            "SELECT dirty_generation FROM wallet_history_planner_dirty "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        concurrent = connect(db_path)
        original_load = wallet_history_module._load_wallet_history_plan_snapshots
        wrote_concurrently = False

        def load_then_write(other_conn, *, wallets, now):
            nonlocal wrote_concurrently
            snapshots = original_load(other_conn, wallets=wallets, now=now)
            if not wrote_concurrently:
                wrote_concurrently = True
                concurrent.execute(
                    "UPDATE wallet_screen_summaries "
                    "SET sample_trade_count = 99, updated_at = 2001 "
                    "WHERE wallet = ?",
                    (wallet,),
                )
                concurrent.commit()
            return snapshots

        monkeypatch.setattr(
            wallet_history_module,
            "_load_wallet_history_plan_snapshots",
            load_then_write,
        )
        first = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=1,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.commit()
        stale_count = conn.execute(
            "SELECT sample_trade_count FROM wallet_history_planner_state "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        retained_generation = conn.execute(
            "SELECT dirty_generation FROM wallet_history_planner_dirty "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]

        second = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=1,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_002,
        )
        conn.commit()
        refreshed_count = conn.execute(
            "SELECT sample_trade_count FROM wallet_history_planner_state "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        dirty_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        if concurrent is not None:
            concurrent.close()
        conn.close()

    assert wrote_concurrently is True
    assert (first, second) == (1, 1)
    assert stale_count == 25
    assert retained_generation == observed_generation + 1
    assert refreshed_count == 99
    assert dirty_count == 0


def test_history_planner_cold_large_side_tables_are_loaded_by_level_batch(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    statements: list[tuple[str, tuple]] = []

    class SmallBatchConnection:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def getlimit(self, category):
            assert category == sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER
            return 5

        def execute(self, sql, parameters=()):
            statements.append((sql, tuple(parameters)))
            return self.wrapped.execute(sql, parameters)

    try:
        run_migrations(conn)
        wallets = ["0x" + f"{index:040x}" for index in range(40)]
        unrelated = ["0x" + f"{index + 10_000:040x}" for index in range(1_200)]
        conn.executemany(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, first_seen_at, updated_at
            ) VALUES (?, 'stream', 'synthetic', 1000, 1000)
            """,
            [(wallet,) for wallet in wallets + unrelated],
        )
        conn.executemany(
            """
            INSERT INTO wallet_levels(
                wallet, level, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, 'l2', 1000, 1000, 1000)
            """,
            [(wallet,) for wallet in wallets],
        )
        conn.executemany(
            """
            INSERT INTO wallet_screen_summaries(
                wallet, sample_trade_count, sample_volume_usdc,
                sample_market_count, screen_complete, screen_qualified,
                source_snapshot_json, computed_at, updated_at
            ) VALUES (?, 25, 500, 2, 1, 1,
                      '{"policy_version":"v3","sample_max_trade_usdc":100}',
                      1000, 1000)
            """,
            [(wallet,) for wallet in wallets + unrelated],
        )
        conn.executemany(
            """
            INSERT INTO wallet_history_summaries(
                wallet, artifact_id, history_depth, activity_count,
                distinct_markets, total_volume_usdc, strategy_tags_json,
                risk_flags_json, research_score, diagnostic_score,
                forward_selection_score, score_components_json,
                forward_score_components_json, methodology_version,
                computed_at, updated_at
            ) VALUES (?, ?, 'light', 100, 10, 1000, '[]', '[]',
                      50, 50, 50, '{}', '{}', ?, 1000, 1000)
            """,
            [
                (wallet, f"artifact-{index}", METHODOLOGY_VERSION)
                for index, wallet in enumerate(unrelated)
            ],
        )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'light', 'done', 1, 3, 1000, 1000)
            """,
            [(JOB_TYPE, wallet, f"{LIGHT_ACTION}:old") for wallet in unrelated],
        )
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            SmallBatchConnection(conn),
            lane_limit=4,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
    finally:
        conn.close()

    snapshot_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if (
            "FROM wallet_screen_summaries" in statement
            or "FROM wallet_history_summaries" in statement
            or "FROM wallet_pnl_summaries" in statement
        )
    ]
    job_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM pipeline_jobs" in statement and "WHERE job_type = ?" in statement
    ]
    source_reads = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM observed_wallets WHERE wallet IN" in statement
    ]

    assert len(rows) == 4
    assert len(snapshot_reads) == 24
    assert len(job_reads) == 24
    assert source_reads
    assert all("WHERE wallet IN" in statement for statement, _ in snapshot_reads)
    assert all(len(parameters) <= 5 for _, parameters in snapshot_reads)
    assert all("wallet IN" in statement for statement, _ in job_reads)
    assert all(len(parameters) <= 5 for _, parameters in job_reads)
    assert all(len(parameters) <= 5 for _, parameters in source_reads)


def test_history_planner_activity_due_time_rebuilds_without_source_write(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        _seed_history_summary(conn, wallet, depth="light", updated_at=1_000)
        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1500, updated_at = 1500 "
            "WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()

        early = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            light_refresh_seconds=1_000,
            now=1_999,
        )
        next_refresh_at = conn.execute(
            "SELECT next_refresh_at FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        due = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            light_refresh_seconds=1_000,
            now=2_000,
        )
        job = conn.execute(
            "SELECT job_action FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
    finally:
        conn.close()

    assert early.jobs_enqueued == 0
    assert next_refresh_at == 2_000
    assert due.jobs_enqueued == 1
    assert job["job_action"] == f"{LIGHT_ACTION}:activity:1500"


def test_history_planner_pnl_due_time_rebuilds_without_source_write(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "b" * 40
    captured_at = 1_000
    due_at = captured_at + PNL_INCOMPLETE_REFRESH_SECONDS
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L4)
        _seed_history_summary(conn, wallet, depth="deep", updated_at=1_000)
        conn.execute(
            """
            INSERT INTO wallet_pnl_summaries(
                wallet, official_all_pnl_usdc, official_all_volume_usdc,
                official_profit_intensity, coverage, methodology_version,
                captured_at, updated_at
            ) VALUES (?, NULL, 10000, NULL, 'deep_recent_bounded', ?, ?, ?)
            """,
            (wallet, PNL_METHODOLOGY_VERSION, captured_at, captured_at),
        )
        conn.commit()

        early = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            deep_refresh_seconds=10_000_000,
            now=due_at - 1,
        )
        next_refresh_at = conn.execute(
            "SELECT next_refresh_at FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        due = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            deep_refresh_seconds=10_000_000,
            now=due_at,
        )
        job_input = json.loads(
            conn.execute(
                "SELECT input_json FROM pipeline_jobs WHERE wallet = ?",
                (wallet,),
            ).fetchone()[0]
        )
    finally:
        conn.close()

    assert early.jobs_enqueued == 0
    assert next_refresh_at == due_at
    assert due.jobs_enqueued == 1
    assert job_input["refresh_reason"] == "pnl_evidence_refresh"


def test_history_planner_deferred_failure_due_time_rebuilds_without_source_write(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
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
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, 'deep', 'failed', 1, 3, 3000, 1900, 1900)
            """,
            (JOB_TYPE, wallet, f"{DEEP_ACTION}:refresh:1900"),
        )
        conn.commit()

        blocked = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=2_000,
        )
        next_refresh_at = conn.execute(
            "SELECT next_refresh_at FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        due = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=3_000,
        )
    finally:
        conn.close()

    assert blocked.jobs_enqueued == 0
    assert next_refresh_at == 3_000
    assert due.jobs_enqueued == 1


def test_history_planner_bootstrap_warms_until_relevant_state_is_complete(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    best_wallet = "0x" + f"{1001:040x}"
    try:
        run_migrations(conn)
        for index in range(1, 1002):
            wallet = "0x" + f"{index:040x}"
            _seed_level(conn, wallet, WalletLevel.L2)
            conn.execute(
                """
                UPDATE wallet_screen_summaries
                SET sample_trade_count = ?, sample_volume_usdc = ?,
                    sample_market_count = ?
                WHERE wallet = ?
                """,
                (
                    100 if wallet == best_wallet else 10,
                    10_000 if wallet == best_wallet else 500,
                    20 if wallet == best_wallet else 2,
                    wallet,
                ),
            )
        conn.commit()

        first = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        second = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_001)
        third = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_002)
        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchall()
    finally:
        conn.close()

    assert (first.status, first.jobs_enqueued) == ("warming_up", 0)
    assert (second.status, second.jobs_enqueued) == ("warming_up", 0)
    assert third.status == "ok"
    assert third.jobs_enqueued == 1
    assert [row["wallet"] for row in queued] == [best_wallet]


def test_history_planner_commits_zero_job_state_refresh_before_close(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "d" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        _seed_history_summary(conn, wallet, depth="light", updated_at=2_000)
        conn.commit()
        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            light_refresh_seconds=10_000,
            now=2_100,
        )
    finally:
        conn.close()

    reopened = connect(db_path)
    try:
        state_count = reopened.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        reopened.close()

    assert summary.jobs_enqueued == 0
    assert state_count == 1


def test_history_planner_persists_job_batch_before_return(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=2_000,
        )
        visible_before_close = conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()[0]
    finally:
        conn.close()

    reopened = connect(db_path)
    try:
        visible_after_reopen = reopened.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()[0]
    finally:
        reopened.close()

    assert summary.jobs_enqueued == 1
    assert visible_before_close == 1
    assert visible_after_reopen == 1


def test_history_planner_dirty_backlog_warms_until_stale_state_is_cleared(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for index in range(600):
            _seed_level(conn, "0x" + f"{index + 1:040x}", WalletLevel.L2)
        conn.commit()
        for offset in (0, 500):
            refreshed = wallet_history_module._refresh_wallet_history_planner_state(
                conn,
                limit=500,
                light_refresh_seconds=10_000,
                deep_refresh_seconds=10_000,
                now=2_000 + offset,
            )
            conn.commit()
            assert refreshed in {100, 500}

        conn.execute("DELETE FROM pipeline_jobs WHERE job_type = ?", (JOB_TYPE,))
        conn.execute(
            "UPDATE wallet_screen_summaries SET sample_trade_count = sample_trade_count + 1"
        )
        conn.commit()

        first = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=3_000)
        dirty_after_first = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty"
        ).fetchone()[0]
        second = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=3_001)
    finally:
        conn.close()

    assert (first.status, first.jobs_enqueued) == ("warming_up", 0)
    assert dirty_after_first == 100
    assert second.status == "ok"
    assert second.jobs_enqueued == 1


def test_history_planner_cleans_1600_dirty_generations_without_expression_limit(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + f"{index + 1:040x}" for index in range(1_601)]
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO wallet_levels(
                wallet, level, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, 'l2', 1000, 1000, 1000)
            """,
            ((wallet,) for wallet in wallets),
        )
        conn.executemany(
            """
            INSERT INTO wallet_screen_summaries(
                wallet, sample_trade_count, sample_volume_usdc,
                sample_market_count, screen_complete, screen_qualified,
                source_snapshot_json, computed_at, updated_at
            ) VALUES (?, 25, 500, 2, 1, 1,
                      '{"policy_version":"v3","sample_max_trade_usdc":100}',
                      1000, 1000)
            """,
            ((wallet,) for wallet in wallets),
        )
        conn.commit()

        first = plan_wallet_history_jobs(
            conn,
            limit=50,
            shard_count=1,
            now=2_000,
        )
        state_after_first = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state"
        ).fetchone()[0]
        dirty_after_first = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty"
        ).fetchone()[0]

        second = plan_wallet_history_jobs(
            conn,
            limit=50,
            shard_count=1,
            now=2_001,
        )
        state_after_second = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state"
        ).fetchone()[0]
        jobs_after_second = conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert (first.status, first.jobs_enqueued) == ("warming_up", 0)
    assert (state_after_first, dirty_after_first) == (1_600, 1)
    assert (second.status, second.jobs_enqueued) == ("ok", 50)
    assert state_after_second == 1_601
    assert jobs_after_second == 50


def test_history_planner_l0_l1_writes_stay_clean_until_l2_promotion(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    promoted_wallet = "0x" + f"{2:040x}"
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO wallet_levels(
                wallet, level, first_seen_at, last_seen_at, updated_at
            ) VALUES (?, ?, 1000, 1000, 1000)
            """,
            [
                (
                    "0x" + f"{index + 1:040x}",
                    "l0" if index % 2 == 0 else "l1",
                )
                for index in range(25_000)
            ],
        )
        conn.execute(
            """
                INSERT INTO wallet_screen_summaries(
                    wallet, sample_trade_count, sample_volume_usdc,
                    sample_market_count, screen_complete, screen_qualified,
                    source_snapshot_json, computed_at, updated_at
                ) VALUES (?, 77, 900, 5, 1, 1,
                          '{"policy_version":"v3","sample_max_trade_usdc":100}',
                          1000, 1000)
            """,
            (promoted_wallet,),
        )
        conn.commit()
        dirty_before_promotion = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty"
        ).fetchone()[0]

        conn.execute(
            "UPDATE wallet_levels SET level = 'l2', updated_at = 1100 "
            "WHERE wallet = ?",
            (promoted_wallet,),
        )
        conn.commit()
        promotion_dirty = conn.execute(
            "SELECT wallet, dirty_reason, dirty_generation "
            "FROM wallet_history_planner_dirty"
        ).fetchall()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=2_000,
        )
        state_row = conn.execute(
            "SELECT level, sample_trade_count "
            "FROM wallet_history_planner_state WHERE wallet = ?",
            (promoted_wallet,),
        ).fetchone()
        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchall()
    finally:
        conn.close()

    assert dirty_before_promotion == 0
    assert [tuple(row) for row in promotion_dirty] == [
        (promoted_wallet, "wallet_levels", 1)
    ]
    assert (summary.status, summary.jobs_enqueued) == ("ok", 1)
    assert tuple(state_row) == ("l2", 77)
    assert [row["wallet"] for row in queued] == [promoted_wallet]


def test_history_planner_last_seen_uses_state_only_sighting_queue(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    statements: list[str] = []

    class RecordingConnection:
        def execute(self, sql, parameters=()):
            statements.append(sql)
            return conn.execute(sql, parameters)

        def executemany(self, sql, parameters):
            statements.append(sql)
            return conn.executemany(sql, parameters)

    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        _seed_history_summary(conn, wallet, depth="light", updated_at=1_000)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_100,
        )
        conn.commit()

        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1500, updated_at = 1500 "
            "WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
        full_dirty = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        sighting_dirty = conn.execute(
            "SELECT last_seen_at, dirty_generation "
            "FROM wallet_history_planner_sighting_dirty WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        refreshed = wallet_history_module._refresh_wallet_history_planner_sightings(
            RecordingConnection(),
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_999,
        )
        conn.commit()
        state = conn.execute(
            "SELECT last_seen_at, next_refresh_at "
            "FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()
    finally:
        conn.close()

    assert full_dirty == 0
    assert tuple(sighting_dirty) == (1_500, 1)
    assert refreshed == 1
    assert tuple(state) == (1_500, 2_000)
    assert not any(
        table in statement
        for statement in statements
        for table in (
            "wallet_screen_summaries",
            "wallet_history_summaries",
            "wallet_pnl_summaries",
            "pipeline_jobs",
            "wallet_levels",
        )
    )


def test_history_planner_full_dirty_outranks_later_sighting(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_100,
        )
        conn.commit()

        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1200 WHERE wallet = ?",
            (wallet,),
        )
        conn.execute(
            "UPDATE wallet_screen_summaries "
            "SET sample_trade_count = sample_trade_count + 1 WHERE wallet = ?",
            (wallet,),
        )
        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1300 WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()

        queued_before = tuple(
            conn.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM wallet_history_planner_dirty), "
                "(SELECT COUNT(*) FROM wallet_history_planner_sighting_dirty)"
            ).fetchone()
        )
        skipped = wallet_history_module._refresh_wallet_history_planner_sightings(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_400,
        )
        full = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_400,
        )
        sighting = wallet_history_module._refresh_wallet_history_planner_sightings(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_400,
        )
        conn.commit()
        queued_after = tuple(
            conn.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM wallet_history_planner_dirty), "
                "(SELECT COUNT(*) FROM wallet_history_planner_sighting_dirty)"
            ).fetchone()
        )
        state_seen = conn.execute(
            "SELECT last_seen_at FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert queued_before == (1, 1)
    assert (skipped, full, sighting) == (0, 1, 1)
    assert queued_after == (0, 0)
    assert state_seen == 1_300


def test_history_planner_sighting_generation_survives_concurrent_update(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    concurrent = None
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_100,
        )
        conn.commit()
        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1200 WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
        claims = planner_state_module.select_wallet_history_planner_sightings(
            conn,
            limit=1,
        )
        conn.commit()

        concurrent = connect(db_path)
        concurrent.execute(
            "UPDATE wallet_levels SET last_seen_at = 1300 WHERE wallet = ?",
            (wallet,),
        )
        concurrent.commit()
        planner_state_module.apply_wallet_history_planner_sightings(
            conn,
            updates=[(1_200, 0, wallet)],
            claims=claims,
        )
        conn.commit()
        retained = conn.execute(
            "SELECT last_seen_at, dirty_generation "
            "FROM wallet_history_planner_sighting_dirty WHERE wallet = ?",
            (wallet,),
        ).fetchone()
    finally:
        if concurrent is not None:
            concurrent.close()
        conn.close()

    assert tuple(retained) == (1_300, 2)


def test_history_planner_stale_sighting_cannot_overwrite_full_rebuild(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    concurrent = None
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_100,
        )
        conn.commit()
        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 1200 WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
        claims = planner_state_module.select_wallet_history_planner_sightings(
            conn,
            limit=1,
        )
        conn.commit()

        concurrent = connect(db_path)
        concurrent.execute(
            "UPDATE wallet_screen_summaries "
            "SET sample_trade_count = sample_trade_count + 1 WHERE wallet = ?",
            (wallet,),
        )
        concurrent.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            concurrent,
            limit=10,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=1_300,
        )
        concurrent.commit()
        rebuilt = tuple(
            concurrent.execute(
                "SELECT last_seen_at, next_refresh_at "
                "FROM wallet_history_planner_state WHERE wallet = ?",
                (wallet,),
            ).fetchone()
        )

        planner_state_module.apply_wallet_history_planner_sightings(
            conn,
            updates=[(1_200, 9_999, wallet)],
            claims=claims,
        )
        conn.commit()
        after_stale_apply = tuple(
            conn.execute(
                "SELECT last_seen_at, next_refresh_at "
                "FROM wallet_history_planner_state WHERE wallet = ?",
                (wallet,),
            ).fetchone()
        )
    finally:
        if concurrent is not None:
            concurrent.close()
        conn.close()

    assert after_stale_apply == rebuilt


def test_history_planner_drains_10000_sightings_without_evidence_reads(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + f"{index + 1:040x}" for index in range(10_000)]
    statements: list[str] = []

    class RecordingConnection:
        def execute(self, sql, parameters=()):
            statements.append(sql)
            return conn.execute(sql, parameters)

        def executemany(self, sql, parameters):
            statements.append(sql)
            return conn.executemany(sql, parameters)

    try:
        run_migrations(conn)
        conn.executemany(
            "INSERT INTO wallet_levels(wallet, level, first_seen_at, "
            "last_seen_at, updated_at) VALUES (?, 'l2', 1000, 1000, 1000)",
            ((wallet,) for wallet in wallets),
        )
        conn.executemany(
            "INSERT INTO wallet_history_planner_state(wallet, level, "
            "last_seen_at, target_depth) VALUES (?, 'l2', 1000, 'light')",
            ((wallet,) for wallet in wallets),
        )
        conn.execute("DELETE FROM wallet_history_planner_dirty")
        conn.executemany(
            "UPDATE wallet_levels SET last_seen_at = 2000, updated_at = 2000 "
            "WHERE wallet = ?",
            ((wallet,) for wallet in wallets),
        )
        conn.commit()

        refreshed = wallet_history_module._refresh_wallet_history_planner_sightings(
            RecordingConnection(),
            limit=10_000,
            light_refresh_seconds=1_000,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
        conn.commit()
        remaining = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_sighting_dirty"
        ).fetchone()[0]
        updated = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state "
            "WHERE last_seen_at = 2000"
        ).fetchone()[0]
    finally:
        conn.close()

    assert refreshed == 10_000
    assert remaining == 0
    assert updated == 10_000
    assert not any(
        table in statement
        for statement in statements
        for table in (
            "wallet_screen_summaries",
            "wallet_history_summaries",
            "wallet_pnl_summaries",
            "pipeline_jobs",
            "wallet_levels",
        )
    )


def test_history_planner_sighting_backlog_does_not_starve_ready_candidates(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    sighted = ["0x" + f"{index + 1:040x}" for index in range(10_001)]
    ready_wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        conn.executemany(
            "INSERT INTO wallet_levels(wallet, level, first_seen_at, "
            "last_seen_at, updated_at) VALUES (?, 'l2', 1000, 1000, 1000)",
            ((wallet,) for wallet in sighted),
        )
        conn.executemany(
            "INSERT INTO wallet_history_planner_state(wallet, level, "
            "last_seen_at, target_depth) VALUES (?, 'l2', 1000, 'light')",
            ((wallet,) for wallet in sighted),
        )
        _seed_level(conn, ready_wallet, WalletLevel.L2)
        conn.execute("DELETE FROM wallet_history_planner_dirty")
        conn.execute(
            "INSERT INTO wallet_history_planner_state("
            "wallet, level, last_seen_at, target_depth, refresh_lane, urgency, "
            "is_eligible, sample_trade_count, sample_volume_usdc, "
            "sample_market_count) VALUES (?, 'l2', 1000, 'light', "
            "'required_depth', 10, 1, 25, 500, 2)",
            (ready_wallet,),
        )
        conn.executemany(
            "UPDATE wallet_levels SET last_seen_at = 2000, updated_at = 2000 "
            "WHERE wallet = ?",
            ((wallet,) for wallet in sighted),
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=1,
            shard_count=1,
            now=2_000,
        )
        remaining = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_sighting_dirty"
        ).fetchone()[0]
        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchall()
    finally:
        conn.close()

    assert summary.status == "ok"
    assert summary.jobs_enqueued == 1
    assert remaining == 1
    assert [row["wallet"] for row in queued] == [ready_wallet]


def test_history_planner_demoted_wallet_dirty_row_deletes_stale_state(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()
        wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_000,
        )
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == 1

        conn.execute(
            "UPDATE wallet_levels SET level = 'l1', updated_at = 2100 WHERE wallet = ?",
            (wallet,),
        )
        conn.commit()
        refreshed = wallet_history_module._refresh_wallet_history_planner_state(
            conn,
            limit=10,
            light_refresh_seconds=10_000,
            deep_refresh_seconds=10_000,
            now=2_100,
        )
        conn.commit()
        state_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
        dirty_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_dirty WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert refreshed == 1
    assert state_count == 0
    assert dirty_count == 0


def test_history_planner_candidate_scan_excludes_active_terminal_and_queued_jobs(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        "running": "0x" + "1" * 40,
        "queued": "0x" + "2" * 40,
        "exhausted_queued": "0x" + "3" * 40,
        "terminal": "0x" + "4" * 40,
        "eligible": "0x" + "5" * 40,
    }
    try:
        run_migrations(conn)
        for wallet in wallets.values():
            _seed_level(conn, wallet, WalletLevel.L2)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'light', ?, ?, 3, 1000, 1000)
            """,
            [
                (JOB_TYPE, wallets["running"], LIGHT_ACTION, "running", 1),
                (JOB_TYPE, wallets["queued"], LIGHT_ACTION, "queued", 1),
                (JOB_TYPE, wallets["exhausted_queued"], LIGHT_ACTION, "queued", 3),
                (JOB_TYPE, wallets["terminal"], LIGHT_ACTION, "terminal_failed", 3),
            ],
        )
        conn.commit()

        rows = wallet_history_module._select_wallet_history_plan_candidates(
            conn,
            lane_limit=10,
            deep_refresh_seconds=1_000,
            now=2_000,
        )
    finally:
        conn.close()

    assert {row["wallet"] for row in rows} == {
        wallets["exhausted_queued"],
        wallets["eligible"],
    }


def test_history_planner_commits_job_batch_to_release_sqlite_write_lock(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path, timeout_seconds=0.05)
    wallets = ["0x" + "1" * 40, "0x" + "2" * 40]
    probe_wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_level(conn, wallet, WalletLevel.L2)
        conn.commit()

        summary = plan_wallet_history_jobs(conn, limit=5, shard_count=1, now=2_000)
        other = connect(db_path, timeout_seconds=0.05)
        try:
            ensure_wallet_level(
                other,
                probe_wallet,
                reason="concurrent_history_planner_probe",
                now=2_100,
            )
            other.commit()
        finally:
            other.close()

        assert summary.jobs_enqueued == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ?",
            (JOB_TYPE,),
        ).fetchone()[0] == 2
    finally:
        conn.close()


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
            "SELECT job_action, job_scope, priority, max_attempts, input_json "
            "FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        action = LIGHT_ACTION if depth == "light" else DEEP_ACTION
        assert job["job_action"] == f"{action}:refresh:1900"
        assert job["job_scope"] == depth
        assert job["max_attempts"] == 3
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


def test_terminal_history_data_quality_error_fails_job_permanently(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 2_001,
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="terminal-history",
            client=TerminalHistoryClient(),
        )
        row = conn.execute(
            "SELECT status, attempts, max_attempts, last_error, terminal_reason, "
            "terminal_policy_version FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 30_000,
        )
        replanned = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=30_000)
        conn.commit()
        after_replan = conn.execute(
            "SELECT status, attempts, max_attempts FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        reclaimed = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="terminal-history-reclaim",
            client=FakeHistoryClient(_rows(5)),
        )

        policy_changed = plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=30_001)
        conn.commit()
        statuses = [
            dict(item)
            for item in conn.execute(
                "SELECT job_action, status, attempts FROM pipeline_jobs "
                "WHERE wallet = ? ORDER BY job_id",
                (wallet,),
            )
        ]
    finally:
        conn.close()

    assert result.jobs_failed == 1
    assert tuple(row) == (
        "terminal_failed",
        3,
        3,
        "incompatible history data: local artifact depth replacement",
        "wallet_history_data_quality",
        METHODOLOGY_VERSION,
    )
    assert replanned.jobs_enqueued == 0
    assert tuple(after_replan) == ("terminal_failed", 3, 3)
    assert reclaimed.jobs_attempted == 0
    assert policy_changed.jobs_enqueued == 0
    assert statuses == [
        {
            "job_action": LIGHT_ACTION,
            "status": "terminal_failed",
            "attempts": 3,
        },
    ]


def test_history_error_classifier_only_marks_explicit_data_quality_failures_terminal():
    assert _is_terminal_history_error(
        WalletHistoryTerminalDataQualityError("local data is unrecoverable")
    ) is True
    assert _is_terminal_history_error(
        ValueError("incompatible history data: old schema")
    ) is True
    assert _is_terminal_history_error(ValueError("artifact depth mismatch")) is True
    assert _is_terminal_history_error(
        ValueError("cannot replace deep history with light history")
    ) is True
    assert _is_terminal_history_error(ValueError("temporary upstream hiccup")) is False
    assert _is_terminal_history_error(RuntimeError("temporary upstream hiccup")) is False
    assert _is_terminal_history_error(sqlite3.OperationalError("database is locked")) is False
    assert _is_terminal_history_error(
        HttpClientError("HTTP 500", status_code=500, error_type="http_error")
    ) is False


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


def test_history_planner_freezes_l2_without_a_current_screen_policy(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        conn.execute(
            "UPDATE wallet_screen_summaries SET source_snapshot_json = ? WHERE wallet = ?",
            ('{"policy_version":"v2","sample_max_trade_usdc":100}', wallet),
        )
        conn.commit()

        summary = plan_wallet_history_jobs(
            conn,
            limit=10,
            max_active_jobs=10,
            shard_count=1,
            now=2_000,
        )
        state = conn.execute(
            "SELECT is_eligible FROM wallet_history_planner_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.jobs_enqueued == 0
        assert state["is_eligible"] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_history_worker_skips_queued_l2_job_when_screen_policy_becomes_stale(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    client = FakeHistoryClient(_rows(25))
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, max_active_jobs=10, shard_count=1, now=2_000)
        conn.execute(
            "UPDATE wallet_screen_summaries SET source_snapshot_json = ? WHERE wallet = ?",
            ('{"policy_version":"v2","sample_max_trade_usdc":100}', wallet),
        )
        conn.commit()

        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="stale-screen-test",
            client=client,
        )

        job = conn.execute(
            "SELECT status, output_json FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert result.jobs_attempted == 1
        assert result.jobs_succeeded == 1
        assert result.light_completed == 0
        assert client.calls == []
        assert job["status"] == "done"
        assert json.loads(job["output_json"])["status"] == "skipped_stale_l2_screen"
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_history_summaries WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == 0
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
            ) VALUES (?, 999, 'light_bounded', 'test', 9900, 9900)
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




def test_history_worker_keeps_transient_http_api_error_retryable_with_original_error(
    tmp_path, monkeypatch
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        _seed_level(conn, wallet, WalletLevel.L2)
        plan_wallet_history_jobs(conn, limit=1, shard_count=1, now=2_000)
        conn.commit()
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_history_pipeline.time.time",
            lambda: 4_000,
        )

        result = run_wallet_history_worker(
            conn,
            archive_dir=tmp_path / "parquet",
            shard_index=0,
            shard_count=1,
            limit=1,
            worker_id="transient-api-error",
            client=TransientHistoryClient(),
        )

        job = conn.execute(
            "SELECT status, attempts, max_attempts, next_attempt_at, last_error "
            "FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert result.jobs_attempted == 1
        assert result.jobs_failed == 1
        assert result.jobs_deferred == 0
        assert result.status == "partial"
        assert tuple(job) == (
            "queued",
            1,
            3,
            4_900,
            "upstream API exploded: gateway timeout",
        )
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
            ) VALUES (?, 10, ?, ?, 1, 1, 1900, 1900)
            ON CONFLICT(wallet) DO UPDATE SET
                sample_trade_count = excluded.sample_trade_count,
                sample_volume_usdc = excluded.sample_volume_usdc,
                sample_market_count = excluded.sample_market_count,
                updated_at = excluded.updated_at
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
                      1, 3, 1000, 1000, 1000)
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


def test_history_planner_active_count_uses_type_claim_index_when_capacity_is_full(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    target_wallet = "0x" + "f" * 40
    active_count_query = (
        "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ? "
        "AND (status = 'running' OR (status = 'queued' AND attempts < max_attempts))"
    )
    try:
        run_migrations(conn)
        _seed_level(conn, target_wallet, WalletLevel.L2)
        conn.execute(
            "INSERT OR REPLACE INTO wallet_history_planner_state("
            "wallet, level, last_seen_at, target_depth) "
            "VALUES (?, 'l2', 1000, 'light')",
            (target_wallet,),
        )
        conn.execute("DELETE FROM wallet_history_planner_dirty")
        conn.execute(
            "UPDATE wallet_levels SET last_seen_at = 2000, updated_at = 2000 "
            "WHERE wallet = ?",
            (target_wallet,),
        )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'light', ?, ?, ?, 1000, 1000)
            """,
            [
                (JOB_TYPE, "0x" + f"{index:040x}", f"active-{index}", "running", 0, 3)
                for index in range(1, 3)
            ]
            + [(JOB_TYPE, "0x" + "3" * 40, "active-queued", "queued", 2, 3)]
            + [(JOB_TYPE, "0x" + "4" * 40, "exhausted", "queued", 3, 3)]
            + [
                (
                    "wallet_recent_screen",
                    "0x" + f"{index + 1000:040x}",
                    f"completed-{index}",
                    "done",
                    0,
                    3,
                )
                for index in range(128)
            ],
        )
        conn.commit()

        plan_rows = conn.execute(
            f"EXPLAIN QUERY PLAN {active_count_query}",
            (JOB_TYPE,),
        ).fetchall()
        assert any("idx_pipeline_jobs_type_claim" in row[3] for row in plan_rows)
        assert conn.execute(active_count_query, (JOB_TYPE,)).fetchone()[0] == 3

        summary = plan_wallet_history_jobs(
            conn,
            limit=10,
            max_active_jobs=3,
            shard_count=1,
            now=2_000,
        )

        assert summary.active_jobs == 3
        assert summary.max_active_jobs == 3
        assert summary.throttled is True
        assert summary.jobs_enqueued == 0
        assert conn.execute(
            "SELECT last_seen_at FROM wallet_history_planner_state WHERE wallet = ?",
            (target_wallet,),
        ).fetchone()[0] == 2_000
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_history_planner_sighting_dirty WHERE wallet = ?",
            (target_wallet,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ? AND job_type = ?",
            (target_wallet, JOB_TYPE),
        ).fetchone()[0] == 0
    finally:
        conn.close()
