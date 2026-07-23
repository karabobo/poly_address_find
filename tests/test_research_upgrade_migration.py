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

        assert applied == [62, 63, 64, 65, 66, 67]
        assert "wallet_l6_validations" in tables
        assert "official_all_pnl_usdc" in {
            row["name"] for row in conn.execute("PRAGMA table_info(wallet_l6_validations)")
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
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        conn.close()
