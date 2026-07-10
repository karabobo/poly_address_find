from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import build_wallet_registry, prune_low_value_evidence
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import persist_score, persist_wallet_activity, upsert_candidate, upsert_wallet_feature


def _settings(db_path):
    return RobotSettings(db_path=db_path, execution_mode="research")


def _activity(idx: int) -> dict:
    return {
        "timestamp": 1_000 + idx,
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
        "transactionHash": f"0x{idx:064x}",
    }


def _seed_candidate(conn, wallet: str, *, materialized: bool, roi: float = -0.1) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            extra={"feature_materializer_version": "test"} if materialized else {},
        ),
    )
    persist_wallet_activity(conn, wallet, [_activity(idx) for idx in range(5)], ingested_at=2_000)
    conn.execute(
        """
        INSERT INTO paper_wallet_quality(
            wallet, orders, open_positions, settled_positions, gamma_marked_positions,
            fallback_marked_positions, mark_coverage, settled_cost_usd, settled_pnl_usd,
            settled_roi, total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
        ) VALUES (?, 5, 0, 2, 2, 0, 1.0, 100, ?, ?, ?, ?, 0, '[]', 2000)
        """,
        (wallet, roi * 100, roi, roi * 100, roi),
    )
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=0,
            stage=CandidateStage.NEEDS_DATA,
            reason="missing_required_score_components",
            components={},
            penalties={},
        ),
        policy_version="test",
    )
    conn.commit()


def test_prune_evidence_dry_run_and_execute_low_value_only(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    low = "0x" + "1" * 40
    high = "0x" + "2" * 40
    unmaterialized = "0x" + "3" * 40
    try:
        run_migrations(conn)
        _seed_candidate(conn, low, materialized=True, roi=-0.2)
        _seed_candidate(conn, high, materialized=True, roi=0.5)
        _seed_candidate(conn, unmaterialized, materialized=False, roi=-0.2)
    finally:
        conn.close()

    dry = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=True,
    )
    conn = connect(db_path)
    try:
        assert dry["wallets"] == [low]
        assert dry["deleted"]["wallet_activity"] == 3
        assert conn.execute("SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)).fetchone()[0] == 5
    finally:
        conn.close()

    executed = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=False,
    )
    conn = connect(db_path)
    try:
        assert executed["wallets"] == [low]
        assert conn.execute("SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)).fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (high,)).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (unmaterialized,),
        ).fetchone()[0] == 5
        registry = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()

    build_wallet_registry(_settings(db_path))
    conn = connect(db_path)
    try:
        rebuilt = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert rebuilt["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()
