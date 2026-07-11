import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.research.scoring import score_candidate
from pm_robot.storage.db import connect, retry_sqlite_locked, run_migrations
from pm_robot.storage.repository import (
    SOURCE_EVENT_UPSERT_SOURCE,
    activity_coverage,
    activity_watermark,
    get_wallet_features,
    list_evidence_backfill_targets,
    latest_review_rows,
    list_activity_backfill_targets,
    list_candidates,
    persist_wallet_activity,
    persist_wallet_positions,
    record_runtime_heartbeat,
    persist_score,
    rebuild_wallet_episodes,
    seed_evidence_backfill_budget,
    upsert_candidate,
    upsert_gamma_market_cache,
    upsert_wallet_feature,
)


def test_current_schema_check_does_not_wait_for_database_write_lock(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    locker = sqlite3.connect(db_path, timeout=0)
    try:
        run_migrations(conn)
        conn.execute("PRAGMA busy_timeout = 1")
        locker.execute("BEGIN IMMEDIATE")

        assert run_migrations(conn) == []
    finally:
        locker.rollback()
        locker.close()
        conn.close()


def test_exhausted_sqlite_lock_retries_roll_back_final_attempt(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        conn.execute("CREATE TABLE test_rows (value INTEGER)")
        conn.commit()

        def write_then_lock() -> None:
            conn.execute("INSERT INTO test_rows(value) VALUES (1)")
            raise sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            retry_sqlite_locked(
                write_then_lock,
                rollback=conn.rollback,
                attempts=2,
                sleep_seconds=0,
            )

        assert conn.in_transaction is False
        assert conn.execute("SELECT COUNT(*) FROM test_rows").fetchone()[0] == 0
    finally:
        conn.close()


def test_concurrent_migration_runners_apply_each_version_once(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    start_gate = tmp_path / "start-migrations"
    script = textwrap.dedent(
        """
        import json
        import sys
        import time
        from pathlib import Path

        from pm_robot.storage.db import connect, run_migrations

        db_path = Path(sys.argv[1])
        start_gate = Path(sys.argv[2])
        while not start_gate.exists():
            time.sleep(0.01)
        conn = connect(db_path)
        try:
            print(json.dumps(run_migrations(conn)))
        finally:
            conn.close()
        """
    )
    env = os.environ.copy()
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (source_root, env.get("PYTHONPATH", ""))))
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(db_path), str(start_gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        for _index in range(4)
    ]
    start_gate.touch()
    outputs = [process.communicate(timeout=30) for process in processes]
    assert [process.returncode for process in processes] == [0, 0, 0, 0], outputs
    results = [json.loads(stdout) for stdout, _stderr in outputs]

    conn = connect(db_path)
    try:
        stored_versions = [int(row[0]) for row in conn.execute("SELECT version FROM schema_migrations")]
    finally:
        conn.close()

    assert sum(bool(result) for result in results) == 1
    assert len(stored_versions) == len(set(stored_versions))
    assert len(stored_versions) == max(len(result) for result in results)


def test_failed_migration_restores_foreign_keys_and_rolls_back(tmp_path, monkeypatch):
    import pm_robot.storage.db as db_module

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_failure.sql").write_text(
        """
        PRAGMA foreign_keys = OFF;
        BEGIN IMMEDIATE;
        CREATE TABLE partial_migration(id INTEGER PRIMARY KEY);
        INSERT INTO missing_table(id) VALUES (1);
        COMMIT;
        PRAGMA foreign_keys = ON;
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(db_module, "MIGRATIONS_DIR", migrations_dir)
    conn = connect(tmp_path / "robot.sqlite")
    try:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

        with pytest.raises(sqlite3.OperationalError, match="missing_table"):
            run_migrations(conn)

        assert conn.in_transaction is False
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'partial_migration'"
        ).fetchone() is None
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 0
    finally:
        conn.close()


def test_retention_migrations_backfill_counts_and_compact_copy_links(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        "existing": "0x" + "1" * 40,
        "missing": "0x" + "2" * 40,
        "non_trade": "0x" + "3" * 40,
        "empty": "0x" + "4" * 40,
    }
    try:
        conn.executescript(
            """
            CREATE TABLE schema_migrations(
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            );
            CREATE TABLE candidate_wallets(address TEXT PRIMARY KEY);
            CREATE TABLE wallet_activity(
                activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                activity_key TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                type TEXT NOT NULL
            );
            CREATE INDEX idx_wallet_activity_address_time
                ON wallet_activity(address, timestamp DESC);
            CREATE INDEX idx_wallet_activity_trade_address_time
                ON wallet_activity(type, address, timestamp);
            CREATE TABLE wallet_activity_watermarks(
                address TEXT PRIMARY KEY,
                newest_timestamp INTEGER NOT NULL DEFAULT 0,
                newest_activity_key TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL,
                last_full_backfill_at INTEGER
            );
            CREATE TABLE copy_trade_links(
                link_id INTEGER PRIMARY KEY AUTOINCREMENT,
                leader_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
                follower_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
                leader_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
                follower_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
                condition_id TEXT,
                market_slug TEXT,
                asset_id TEXT,
                outcome TEXT,
                side TEXT,
                leader_ts INTEGER NOT NULL,
                follower_ts INTEGER NOT NULL,
                lag_seconds INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(leader_activity_id, follower_activity_id)
            );
            CREATE INDEX idx_copy_trade_links_leader
                ON copy_trade_links(leader_wallet, follower_wallet, follower_ts DESC);
            CREATE INDEX idx_copy_trade_links_follower
                ON copy_trade_links(follower_wallet, follower_ts DESC);
            CREATE TABLE copy_pair_stats(
                leader_wallet TEXT NOT NULL,
                follower_wallet TEXT NOT NULL,
                qualifies INTEGER NOT NULL,
                PRIMARY KEY(leader_wallet, follower_wallet)
            );
            CREATE TABLE copy_backtest_trades(
                backtest_trade_id INTEGER PRIMARY KEY,
                leader_wallet TEXT NOT NULL,
                follower_wallet TEXT,
                link_id INTEGER REFERENCES copy_trade_links(link_id) ON DELETE SET NULL
            );
            """
        )
        conn.executemany(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, 1)",
            ((version,) for version in range(1, 46)),
        )
        conn.executemany(
            "INSERT INTO candidate_wallets(address) VALUES (?)",
            ((wallet,) for wallet in wallets.values()),
        )
        conn.executemany(
            """
            INSERT INTO wallet_activity(address, activity_key, timestamp, type)
            VALUES (?, ?, ?, ?)
            """,
            (
                (wallets["existing"], "existing-trade-1", 10, "TRADE"),
                (wallets["existing"], "existing-redeem", 20, "REDEEM"),
                (wallets["existing"], "existing-trade-2", 30, "TRADE"),
                (wallets["missing"], "missing-trade", 40, "TRADE"),
                (wallets["non_trade"], "non-trade-redeem", 50, "REDEEM"),
            ),
        )
        conn.executemany(
            """
            INSERT INTO wallet_activity_watermarks(
                address, newest_timestamp, newest_activity_key, updated_at
            ) VALUES (?, 0, 'stale', 1)
            """,
            ((wallets[name],) for name in ("existing", "empty")),
        )
        conn.executemany(
            """
            INSERT INTO copy_pair_stats(leader_wallet, follower_wallet, qualifies)
            VALUES (?, ?, ?)
            """,
            (
                (wallets["existing"], wallets["missing"], 1),
                (wallets["existing"], wallets["non_trade"], 0),
            ),
        )
        conn.executemany(
            """
            INSERT INTO copy_trade_links(
                link_id, leader_wallet, follower_wallet,
                leader_activity_id, follower_activity_id,
                condition_id, market_slug, asset_id, outcome, side,
                leader_ts, follower_ts, lag_seconds, created_at
            ) VALUES (?, ?, ?, ?, ?, '', '', '', '', 'BUY', ?, ?, 5, 60)
            """,
            (
                (101, wallets["existing"], wallets["missing"], 1, 4, 10, 15),
                (102, wallets["existing"], wallets["non_trade"], 3, 5, 30, 35),
            ),
        )
        conn.executemany(
            """
            INSERT INTO copy_backtest_trades(
                backtest_trade_id, leader_wallet, follower_wallet, link_id
            ) VALUES (?, ?, ?, ?)
            """,
            (
                (1, wallets["existing"], wallets["missing"], 101),
                (2, wallets["existing"], wallets["non_trade"], 102),
            ),
        )
        conn.commit()

        assert run_migrations(conn) == [46, 47, 48]
        counts = {
            row["address"]: int(row["trade_count"])
            for row in conn.execute(
                "SELECT address, trade_count FROM wallet_activity_watermarks"
            )
        }

        assert counts == {
            wallets["existing"]: 2,
            wallets["missing"]: 1,
            wallets["non_trade"]: 0,
            wallets["empty"]: 0,
        }
        assert [
            (int(row["link_id"]), str(row["follower_wallet"]))
            for row in conn.execute(
                "SELECT link_id, follower_wallet FROM copy_trade_links ORDER BY link_id"
            )
        ] == [(101, wallets["missing"])]
        assert [
            (int(row["backtest_trade_id"]), row["link_id"])
            for row in conn.execute(
                "SELECT backtest_trade_id, link_id FROM copy_backtest_trades ORDER BY backtest_trade_id"
            )
        ] == [(1, 101), (2, None)]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert run_migrations(conn) == []
    finally:
        conn.close()


def test_runtime_heartbeat_lock_failure_cannot_rollback_committed_wallet_data(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    locker = sqlite3.connect(db_path, timeout=0)
    wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.commit()
        conn.execute("PRAGMA busy_timeout = 1")
        locker.execute("BEGIN IMMEDIATE")

        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            record_runtime_heartbeat(conn, "loop_research_control_step_wallet_pipeline_plan")

        candidate_count = int(
            conn.execute("SELECT COUNT(*) FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()[0]
        )
        heartbeat_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ingest_runs WHERE ingest_type = ?",
                ("loop_research_control_step_wallet_pipeline_plan",),
            ).fetchone()[0]
        )

        assert candidate_count == 1
        assert heartbeat_count == 0
    finally:
        locker.rollback()
        locker.close()
        conn.close()


def test_sqlite_candidate_feature_score_roundtrip(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        candidate = CandidateAddress(address="0x" + "5" * 40, sources="test")
        upsert_candidate(conn, candidate)
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=candidate.address,
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
                copy_stream_roi=0.05,
                edge_retention_pct=70,
                walk_forward_consistency_pct=60,
                survival_score=70,
                single_market_pnl_share=0.2,
                net_to_gross_exposure=0.7,
                hygiene_status="clean",
                primary_category="politics",
                    extra={
                        "paper_roi_after_slippage": 0.08,
                        "maker_fraction_source": "verified_test_fixture",
                        "hygiene_evidence_source": "verified_test_fixture",
                    },
            ),
        )
        conn.commit()
        assert len(list_candidates(conn)) == 1
        features = get_wallet_features(conn)
        score = score_candidate(candidate, features[candidate.address], load_policy(Path("config/leader_scoring_policy.json")))
        persist_score(conn, score, policy_version="test")
        rows = latest_review_rows(conn)
        assert rows[0]["address"] == candidate.address
        assert rows[0]["review_stage"] in {
            CandidateStage.PAPER_CANDIDATE.value,
            CandidateStage.PAPER_APPROVED.value,
            CandidateStage.NEEDS_REVIEW.value,
        }
    finally:
        conn.close()


def test_candidate_source_events_append_by_default(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "a" * 40

        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual", notes="first observation"))
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual", notes="second observation"))
        conn.commit()

        rows = conn.execute(
            "SELECT notes FROM candidate_source_events WHERE address = ? AND source = ? ORDER BY event_id",
            (wallet, "manual"),
        ).fetchall()

        assert [row["notes"] for row in rows] == ["first observation", "second observation"]
    finally:
        conn.close()


def test_curated_candidate_source_events_upsert_by_wallet_source(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "b" * 40

        upsert_candidate(
            conn,
            CandidateAddress(
                address=wallet,
                sources="bitget_smart_money_20260407",
                labels="bitget | politics",
                notes="old wording",
                links="https://example.test/old",
                status="manual_research_seed",
            ),
            source_event_mode=SOURCE_EVENT_UPSERT_SOURCE,
        )
        upsert_candidate(
            conn,
            CandidateAddress(
                address=wallet,
                sources="bitget_smart_money_20260407",
                labels="manual_seed | politics",
                notes="canonical wording",
                links="https://example.test/new",
                status="manual_research_seed",
            ),
            source_event_mode=SOURCE_EVENT_UPSERT_SOURCE,
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT status, labels, notes, links
            FROM candidate_source_events
            WHERE address = ? AND source = ?
            """,
            (wallet, "bitget_smart_money_20260407"),
        ).fetchall()
        candidate = conn.execute(
            "SELECT sources, labels, notes, links, status FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert len(rows) == 1
        assert dict(rows[0]) == {
            "status": "manual_research_seed",
            "labels": "manual_seed | politics",
            "notes": "canonical wording",
            "links": "https://example.test/new",
        }
        assert candidate["sources"] == "bitget_smart_money_20260407"
        assert "old wording" in candidate["notes"]
        assert "canonical wording" in candidate["notes"]
    finally:
        conn.close()


def test_activity_backfill_targets_prioritize_undercovered_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        empty = CandidateAddress(address="0x" + "6" * 40, sources="test")
        covered = CandidateAddress(address="0x" + "7" * 40, sources="test")
        archived = CandidateAddress(address="0x" + "8" * 40, sources="test")
        upsert_candidate(conn, empty)
        upsert_candidate(conn, covered)
        upsert_candidate(conn, archived)
        conn.execute(
            """
            INSERT INTO wallet_registry(
                address, candidate_stage, registry_status, raw_retention_tier,
                last_evaluated_at, updated_at
            ) VALUES (?, 'needs_data', 'archived_raw_pruned', 'summary_only', 2000, 2000)
            """,
            (archived.address,),
        )
        persist_wallet_activity(
            conn,
            covered.address,
            [
                {
                    "timestamp": 1_000 + idx,
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
                    "transactionHash": f"0xhash{idx}",
                }
                for idx in range(3)
            ],
            ingested_at=2_000,
        )

        targets = list_activity_backfill_targets(conn, limit=2, target_events_per_wallet=3)
        coverage = activity_coverage(conn, limit=2)

        assert targets == [empty.address]
        assert archived.address not in targets
        assert coverage[0]["address"] == empty.address
        assert coverage[0]["activity_count"] == 0
    finally:
        conn.close()


def test_evidence_backfill_budget_targets_ready_wallets_by_priority(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        slow = CandidateAddress(address="0x" + "1" * 40, sources="polymarket_trades_global")
        fast = CandidateAddress(address="0x" + "2" * 40, sources="polymarket_trades_global")
        upsert_candidate(conn, slow)
        upsert_candidate(conn, fast)
        seed_evidence_backfill_budget(conn, slow.address, source="polymarket_trades_global", priority=50, now=1_000)
        seed_evidence_backfill_budget(conn, fast.address, source="polymarket_trades_global", priority=20, now=1_000)
        conn.commit()

        targets = list_evidence_backfill_targets(conn, stage="light_pending", limit=2, now=2_000)

        assert [row["wallet"] for row in targets] == [fast.address, slow.address]
        assert targets[0]["activity_count"] == 0
    finally:
        conn.close()


def test_persist_score_preserves_copyability_block(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        candidate = CandidateAddress(address="0x" + "8" * 40, sources="test")
        upsert_candidate(conn, candidate)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, candidate.address),
        )
        conn.commit()

        persist_score(
            conn,
            ScoreBreakdown(
                address=candidate.address,
                leader_score=55,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="borderline_score",
                components={},
                penalties={},
            ),
            policy_version="test",
        )
        row = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (candidate.address,),
        ).fetchone()
        events = conn.execute(
            "SELECT * FROM review_events WHERE address = ?",
            (candidate.address,),
        ).fetchall()

        assert row["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert events == []
    finally:
        conn.close()


def test_persist_wallet_activity_hashes_keys_and_skips_legacy_duplicates(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "9" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        event = {
            "timestamp": 1_000,
            "conditionId": "condition-1",
            "eventSlug": "event-1",
            "slug": "market-1",
            "asset": "asset-1",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "usdcSize": 5,
            "transactionHash": "0xhash",
        }
        legacy_key = "0xhash|1000|condition-1|event-1|market-1|asset-1|YES|TRADE|BUY|0.5|10.0|5.0"
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, activity_key, timestamp, condition_id, event_slug, market_slug,
                asset_id, outcome, type, side, price, size, usdc_size, transaction_hash,
                raw_json, ingested_at
            ) VALUES (?, ?, 1000, 'condition-1', 'event-1', 'market-1', 'asset-1',
                'YES', 'TRADE', 'BUY', 0.5, 10, 5, '0xhash', '{}', 1000)
            """,
            (wallet, legacy_key),
        )
        conn.commit()

        inserted = persist_wallet_activity(conn, wallet, [event], ingested_at=2_000)
        rows = conn.execute("SELECT activity_key FROM wallet_activity WHERE address = ?", (wallet,)).fetchall()

        assert inserted == 0
        assert len(rows) == 1
        assert rows[0]["activity_key"] == legacy_key

        second = dict(event)
        second["transactionHash"] = "0xhash2"
        inserted_second = persist_wallet_activity(conn, wallet, [second], ingested_at=2_100)
        key = conn.execute(
            "SELECT activity_key FROM wallet_activity WHERE transaction_hash = '0xhash2'"
        ).fetchone()["activity_key"]
        watermark = activity_watermark(conn, wallet)

        assert inserted_second == 1
        assert key.startswith("sha256:")
        assert watermark["newest_timestamp"] == 1_000
        assert watermark["newest_activity_key"].startswith("sha256:")
        assert watermark["trade_count"] == 2
    finally:
        conn.close()


def test_noop_activity_reingest_does_not_dirty_evidence_timestamps(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    event = {
        "timestamp": 1_000,
        "conditionId": "condition-1",
        "slug": "market-1",
        "asset": "asset-1",
        "type": "TRADE",
        "side": "BUY",
        "price": 0.5,
        "size": 10,
        "usdcSize": 5,
        "transactionHash": "0xnoop",
    }
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        assert persist_wallet_activity(conn, wallet, [event], ingested_at=2_000) == 1
        first_watermark_updated = conn.execute(
            "SELECT updated_at FROM wallet_activity_watermarks WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
        first_candidate_updated = conn.execute(
            "SELECT updated_at FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()[0]

        assert persist_wallet_activity(conn, wallet, [event], ingested_at=2_100) == 0
        watermark = conn.execute(
            "SELECT trade_count, updated_at FROM wallet_activity_watermarks WHERE address = ?",
            (wallet,),
        ).fetchone()
        candidate = conn.execute(
            "SELECT updated_at, last_ingested_at FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert watermark["trade_count"] == 1
        assert watermark["updated_at"] == first_watermark_updated
        assert candidate["updated_at"] == first_candidate_updated
        assert candidate["last_ingested_at"] == 2_100
    finally:
        conn.close()


def test_persist_wallet_activity_marks_source_without_overwriting_existing_source(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "a" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        event = {
            "timestamp": 1_000,
            "conditionId": "condition-1",
            "eventSlug": "event-1",
            "slug": "market-1",
            "asset": "asset-1",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "usdcSize": 5,
            "transactionHash": "0xsource1",
        }
        existing_source = {**event, "transactionHash": "0xsource2", "source": "polymarket_rtds_activity"}

        persist_wallet_activity(conn, wallet, [event], ingested_at=2_000, source="paper_wallet_activity")
        persist_wallet_activity(conn, wallet, [existing_source], ingested_at=2_001, source="paper_wallet_activity")
        rows = conn.execute(
            "SELECT transaction_hash, raw_json FROM wallet_activity WHERE address = ? ORDER BY transaction_hash",
            (wallet,),
        ).fetchall()

        sources = {row["transaction_hash"]: json.loads(row["raw_json"])["source"] for row in rows}
        assert sources["0xsource1"] == "paper_wallet_activity"
        assert sources["0xsource2"] == "polymarket_rtds_activity"
    finally:
        conn.close()


def test_rebuild_wallet_episodes_settles_closed_gamma_holders(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "b" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "token-yes",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.4,
                    "size": 10,
                    "usdcSize": 4,
                    "transactionHash": "0xbuy",
                }
            ],
            ingested_at=2_000,
        )
        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market={
                "conditionId": "condition-1",
                "closed": True,
                "clobTokenIds": ["token-yes", "token-no"],
                "outcomes": ["YES", "NO"],
                "outcomePrices": ["1", "0"],
            },
            fetched_at=3_000,
            ttl_seconds=60,
        )

        rebuilt = rebuild_wallet_episodes(conn, wallet)
        episode = conn.execute("SELECT * FROM wallet_episodes WHERE address = ?", (wallet,)).fetchone()
        features = get_wallet_features(conn)[wallet]

        assert rebuilt == 1
        assert episode["status"] == "closed"
        assert episode["realized_pnl_est"] == 6
        assert features.event_win_rate == 1
        assert features.net_pnl_usdc == 6
    finally:
        conn.close()


def test_persist_wallet_positions_replaces_wallet_snapshot(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "a" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))

        first = persist_wallet_positions(
            conn,
            wallet,
            [{"asset": "asset-1", "size": 10}, {"asset": "asset-2", "size": 20}],
            captured_at=1_000,
        )
        second = persist_wallet_positions(
            conn,
            wallet,
            [{"asset": "asset-1", "size": 30}],
            captured_at=2_000,
        )
        rows = conn.execute(
            "SELECT asset_id, size, captured_at FROM wallet_positions WHERE address = ?",
            (wallet,),
        ).fetchall()

        assert first == 2
        assert second == 1
        assert [(row["asset_id"], row["size"], row["captured_at"]) for row in rows] == [
            ("asset-1", 30, 2_000)
        ]
    finally:
        conn.close()
