from pm_robot.storage.db import MIGRATIONS_DIR, connect, run_migrations


WALLET = "0xabc0000000000000000000000000000000000062"


def _apply_migrations_through(conn, last_version: int) -> None:
    conn.execute(
        "CREATE TABLE schema_migrations ("
        "version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"
    )
    conn.commit()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        version = int(path.name.split("_", 1)[0])
        if version > last_version:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, 1000)",
            (version,),
        )
        conn.commit()


def _migration_versions_after(version: int) -> list[int]:
    return [
        int(path.name.split("_", 1)[0])
        for path in sorted(MIGRATIONS_DIR.glob("*.sql"))
        if int(path.name.split("_", 1)[0]) > version
    ]


def test_sighting_queue_migration_recovers_after_missing_marker(tmp_path):
    conn = connect(tmp_path / "sighting-recovery.sqlite")
    try:
        run_migrations(conn)
        conn.execute("DELETE FROM schema_migrations WHERE version = 76")
        conn.commit()

        applied = run_migrations(conn)
        triggers = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'trigger' AND name LIKE "
                "'trg_wallet_history_planner_%'"
            )
        }
    finally:
        conn.close()

    assert applied == [76]
    assert {
        "trg_wallet_history_planner_dirty_levels_update",
        "trg_wallet_history_planner_sighting_levels_update",
        "trg_wallet_history_planner_full_dirty_clears_sighting_insert",
        "trg_wallet_history_planner_full_dirty_clears_sighting_update",
    }.issubset(triggers)


def test_candidate_discovery_status_migration_removes_only_numeric_timestamps(tmp_path):
    conn = connect(tmp_path / "candidate-status.sqlite")
    wallet_dynamic = "0xabc0000000000000000000000000000000000078"
    wallet_manual = "0xabc0000000000000000000000000000000000079"
    try:
        _apply_migrations_through(conn, 77)
        for wallet, status in (
            (wallet_dynamic, "rtds_activity_discovered:1700000000"),
            (wallet_manual, "manual:review"),
        ):
            conn.execute(
                "INSERT INTO candidate_wallets(address, status, first_seen_at, updated_at) "
                "VALUES (?, ?, 1000, 2000)",
                (wallet, status),
            )
            conn.execute(
                "INSERT INTO candidate_source_events(address, source, status, observed_at, recorded_at) "
                "VALUES (?, 'test', ?, 1000, 2000)",
                (wallet, status),
            )
        conn.commit()

        applied = run_migrations(conn)
        candidate_rows = dict(conn.execute("SELECT address, status FROM candidate_wallets"))
        event_rows = dict(conn.execute("SELECT address, status FROM candidate_source_events"))
    finally:
        conn.close()

    assert applied == [78, 79, 80]
    assert candidate_rows[wallet_dynamic] == "rtds_activity_discovered"
    assert event_rows[wallet_dynamic] == "rtds_activity_discovered"
    assert candidate_rows[wallet_manual] == "manual:review"
    assert event_rows[wallet_manual] == "manual:review"


def test_observed_discovery_status_migration_preserves_manual_status(tmp_path):
    conn = connect(tmp_path / "observed-status.sqlite")
    wallet_dynamic = "0xabc0000000000000000000000000000000000080"
    wallet_manual = "0xabc0000000000000000000000000000000000081"
    try:
        _apply_migrations_through(conn, 79)
        for wallet, status in (
            (wallet_dynamic, "rtds_activity_discovered:1700000000"),
            (wallet_manual, "manual:review"),
        ):
            conn.execute(
                "INSERT INTO observed_wallets(wallet, status, first_seen_at, updated_at) "
                "VALUES (?, ?, 1000, 2000)",
                (wallet, status),
            )
        conn.commit()

        applied = run_migrations(conn)
        rows = dict(conn.execute("SELECT wallet, status FROM observed_wallets"))
    finally:
        conn.close()

    assert applied == [80]
    assert rows[wallet_dynamic] == "rtds_activity_discovered"
    assert rows[wallet_manual] == "manual:review"


def test_research_only_migration_preserves_current_state_and_drops_raw_legacy_tables(
    tmp_path,
):
    conn = connect(tmp_path / "upgrade.sqlite")
    try:
        _apply_migrations_through(conn, 61)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'leaderboard', 'seed', 'keep', '', 'active',
                      'legacy-stage', 100, 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, net_pnl_usdc, total_volume_usdc, extra_json, updated_at
            ) VALUES (?, 321.5, 4500, '{"source":"upgrade-test"}', 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO wallet_levels(
                wallet, level, level_reason, policy_version, first_seen_at,
                last_seen_at, level_updated_at, updated_at
            ) VALUES (?, 'l3', 'relative_rank_selected', 'relative_rank_v3',
                      100, 200, 200, 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version,
                relative_path, row_count, byte_size, checksum, status,
                created_at, updated_at
            ) VALUES ('artifact-upgrade', ?, 'deep', 'parquet-v1',
                      'wallet_history/deep/upgrade.parquet', 250, 4096,
                      'checksum', 'active', 190, 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_summaries(
                wallet, artifact_id, history_depth, activity_count,
                distinct_markets, total_volume_usdc, strategy_tags_json,
                risk_flags_json, research_score, score_components_json,
                methodology_version, computed_at, updated_at
            ) VALUES (?, 'artifact-upgrade', 'deep', 250, 12, 4500,
                      '[]', '[]', 77, '{}', 'wallet_history_summary_v2', 200, 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, status,
                attempts, max_attempts, created_at, updated_at
            ) VALUES ('wallet_history_collect', ?, 'collect_deep_history:v1',
                      'deep', 'queued', 1, 3, 180, 200)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status, rows_written
            ) VALUES ('loop_wallet_history', 180, 200, 'ok', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, timestamp, type, raw_json, ingested_at
            ) VALUES (?, 150, 'TRADE', '{}', 160)
            """,
            (WALLET,),
        )
        conn.executemany(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links, evidence_json,
                observed_at, recorded_at
            ) VALUES (?, 'legacy-source', ?, '', ?, '', '{}', ?, ?)
            """,
            (
                (WALLET, "older", "old snapshot", 120, 130),
                (WALLET, "latest", "latest snapshot", 140, 210),
            ),
        )
        for table in (
            "live_canary_events",
            "pipeline_metadata",
            "repair_score_overwrite_20260703",
            "tmp_write_probe",
        ):
            conn.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY)')
        conn.commit()

        applied = run_migrations(conn)

        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        candidate_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(candidate_wallets)")
        }
        feature = conn.execute(
            "SELECT net_pnl_usdc, total_volume_usdc, extra_json "
            "FROM wallet_features WHERE address = ?",
            (WALLET,),
        ).fetchone()
        job = conn.execute(
            "SELECT job_action, job_scope, status, attempts "
            "FROM pipeline_jobs WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
        heartbeat = conn.execute(
            "SELECT name, status, rows_written FROM runtime_heartbeats"
        ).fetchone()

        expected_applied = _migration_versions_after(61)
        assert 74 in expected_applied
        assert applied == expected_applied
        assert "wallet_l6_validations" in tables
        assert "official_all_pnl_usdc" in {
            row["name"] for row in conn.execute("PRAGMA table_info(wallet_l6_validations)")
        }
        assert "official_all_pnl_usdc" in {
            row["name"] for row in conn.execute("PRAGMA table_info(wallet_pnl_summaries)")
        }
        assert "candidate_stage" not in candidate_columns
        assert "wallet_activity" not in tables
        assert "ingest_runs" not in tables
        assert {
            "live_canary_events",
            "pipeline_metadata",
            "repair_score_overwrite_20260703",
            "tmp_write_probe",
        }.isdisjoint(tables)
        provenance = conn.execute(
            """
            SELECT status, notes, observed_at, recorded_at
            FROM candidate_source_events
            WHERE address = ? AND source = 'legacy-source'
            """,
            (WALLET,),
        ).fetchall()
        assert [tuple(row) for row in provenance] == [
            ("latest", "latest snapshot", 120, 210)
        ]
        assert conn.execute(
            "SELECT level FROM wallet_levels WHERE wallet = ?", (WALLET,)
        ).fetchone()[0] == "l3"
        observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
        assert dict(observed) == {
            "promoted_at": 100,
            "promotion_reason": "legacy_candidate_ingress_repair",
        }
        assert conn.execute(
            "SELECT research_score FROM wallet_history_summaries WHERE wallet = ?",
            (WALLET,),
        ).fetchone()[0] == 77
        assert dict(feature) == {
            "net_pnl_usdc": 321.5,
            "total_volume_usdc": 4500.0,
            "extra_json": '{"source":"upgrade-test"}',
        }
        assert dict(job) == {
            "job_action": "collect_deep_history:v1",
            "job_scope": "deep",
            "status": "queued",
            "attempts": 1,
        }
        assert dict(heartbeat) == {
            "name": "loop_wallet_history",
            "status": "ok",
            "rows_written": 1,
        }
        assert [
            row["name"]
            for row in conn.execute("PRAGMA index_info(idx_pipeline_jobs_type_claim)")
        ] == [
            "job_type",
            "status",
            "shard",
            "next_attempt_at",
            "priority",
            "updated_at",
        ]
        assert {"terminal_reason", "terminal_at", "terminal_policy_version"}.issubset(
            {row["name"] for row in conn.execute("PRAGMA table_info(pipeline_jobs)")}
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_terminal_failure_migration_marks_only_wallet_history_data_quality_failures(
    tmp_path,
):
    conn = connect(tmp_path / "terminal-upgrade.sqlite")
    try:
        _apply_migrations_through(conn, 71)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, job_action, job_scope, status,
                attempts, max_attempts, next_attempt_at, last_error,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'failed', 3, 3, 200, ?, 100, 150)
            """,
            (
                (
                    "wallet_history_collect",
                    "0x" + "1" * 40,
                    "collect_light_history:wallet_history_summary_v4",
                    "light",
                    "incompatible history data: malformed local rows",
                ),
                (
                    "wallet_history_collect",
                    "0x" + "2" * 40,
                    "collect_light_history:wallet_history_summary_v4",
                    "light",
                    "upstream gateway timeout",
                ),
                (
                    "wallet_recent_screen",
                    "0x" + "3" * 40,
                    "screen_recent:v2",
                    "recent",
                    "incompatible history data: unrelated queue",
                ),
            ),
        )
        conn.commit()

        applied = run_migrations(conn)
        rows = {
            row["wallet"]: dict(row)
            for row in conn.execute(
                "SELECT wallet, status, terminal_reason, terminal_at "
                "FROM pipeline_jobs ORDER BY wallet"
            )
        }
    finally:
        conn.close()

    expected_applied = _migration_versions_after(71)
    assert 75 in expected_applied
    assert applied == expected_applied
    assert rows["0x" + "1" * 40] == {
        "wallet": "0x" + "1" * 40,
        "status": "terminal_failed",
        "terminal_reason": "wallet_history_data_quality",
        "terminal_at": 150,
    }
    assert rows["0x" + "2" * 40]["status"] == "failed"
    assert rows["0x" + "2" * 40]["terminal_reason"] == ""
    assert rows["0x" + "3" * 40]["status"] == "failed"
    assert rows["0x" + "3" * 40]["terminal_reason"] == ""
