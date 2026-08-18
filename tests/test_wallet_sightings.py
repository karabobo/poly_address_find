import pytest

from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_sightings import (
    _QUALIFIED_L0_ADMISSION_QUERY,
    admit_qualified_observed_wallets,
    record_wallet_sighting,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import upsert_candidate
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import WalletLevel


def _candidate(wallet: str, source: str = "polymarket_trades_global") -> CandidateAddress:
    return CandidateAddress(
        address=wallet,
        sources=source,
        labels="source_seed",
        notes="bounded ingress",
        links=f"https://polymarket.com/profile/{wallet}",
        status="discovered",
    )


def _trade(key: str, usdc: float) -> dict:
    return {
        "key": key,
        "timestamp": 1_000,
        "observed_at": 1_001,
        "market": "market-1",
        "side": "BUY",
        "usdc_size": usdc,
        "transaction_hash": key,
    }


def test_sighting_stays_l0_when_current_ingress_budget_is_full(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)

        result = record_wallet_sighting(
            conn,
            _candidate(wallet),
            recent_trades=[_trade("0xtrade", 25)],
            verified_trade=True,
            allow_l1=False,
            now=2_000,
        )
        conn.commit()

        assert result.level is WalletLevel.L0
        assert result.promoted is False
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?", (wallet,)
        ).fetchone() is None
        observed = conn.execute(
            "SELECT promoted_at, promotion_reason, recent_trade_count FROM observed_wallets "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(observed) == {
            "promoted_at": None,
            "promotion_reason": "",
            "recent_trade_count": 1,
        }
    finally:
        conn.close()


def test_sighting_normalizes_timestamped_discovery_status(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        candidate = CandidateAddress(
            address=wallet,
            sources="rtds",
            status="rtds_activity_discovered:1700000000",
        )

        record_wallet_sighting(conn, candidate, allow_l1=False, now=2_000)
        conn.commit()

        assert conn.execute(
            "SELECT status FROM observed_wallets WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == "rtds_activity_discovered"
    finally:
        conn.close()


def test_one_material_verified_trade_stays_l0_without_candidate_or_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)

        result = record_wallet_sighting(
            conn,
            _candidate(wallet),
            recent_trades=[_trade("0xtrade", 100)],
            verified_trade=True,
            now=2_000,
        )
        conn.commit()

        assert result.promoted is False
        assert result.level is WalletLevel.L0
        assert get_wallet_level(conn, wallet).level is WalletLevel.L0
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?", (wallet,)
        ).fetchone() is None
        observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(observed) == {
            "promoted_at": None,
            "promotion_reason": "",
        }
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'evidence_backfill_budget'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_unverified_trade_sample_cannot_be_admitted_later(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        result = record_wallet_sighting(
            conn,
            CandidateAddress(address=wallet, sources="unverified_feed"),
            recent_trades=[_trade("0xunverified", 500)],
            verified_trade=False,
            allow_l1=False,
            now=1_000,
        )
        conn.commit()

        observed = conn.execute(
            "SELECT recent_trade_count, recent_usdc_total FROM observed_wallets "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        admitted = admit_qualified_observed_wallets(conn, limit=10, now=2_000)

        assert result.level is WalletLevel.L0
        assert tuple(observed) == (0, 0.0)
        assert admitted == 0
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone() is None
    finally:
        conn.close()


def test_deferred_admission_prioritizes_larger_qualified_sample(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = {
        "oldest": "0x" + "a" * 40,
        "largest": "0x" + "b" * 40,
        "other": "0x" + "c" * 40,
    }
    try:
        run_migrations(conn)
        for name, volume, now in (
            ("oldest", 100, 1_000),
            ("largest", 300, 1_100),
            ("other", 200, 1_200),
        ):
            record_wallet_sighting(
                conn,
                _candidate(wallets[name]),
                recent_trades=[
                    _trade(f"0x{name}-a", volume * 0.4),
                    _trade(f"0x{name}-b", volume * 0.6),
                ],
                verified_trade=True,
                allow_l1=False,
                now=now,
            )
        conn.commit()

        admitted = admit_qualified_observed_wallets(conn, limit=1, now=2_000)

        assert admitted == 1
        assert get_wallet_level(conn, wallets["largest"]).level is WalletLevel.L1
        assert get_wallet_level(conn, wallets["oldest"]).level is WalletLevel.L0
        assert get_wallet_level(conn, wallets["other"]).level is WalletLevel.L0
        plan = conn.execute(
            f"EXPLAIN QUERY PLAN {_QUALIFIED_L0_ADMISSION_QUERY}",
            (100, 72),
        ).fetchall()
        plan_details = [str(row["detail"]) for row in plan]
        assert any("idx_observed_wallets_l0_admission" in detail for detail in plan_details)
        assert all("TEMP B-TREE" not in detail for detail in plan_details)
    finally:
        conn.close()


def test_cumulative_verified_trade_sample_promotes_to_l1_without_history_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)

        first = record_wallet_sighting(
            conn,
            _candidate(wallet),
            recent_trades=[_trade("0xtrade-1", 40)],
            verified_trade=True,
            now=2_000,
        )
        second = record_wallet_sighting(
            conn,
            _candidate(wallet),
            recent_trades=[_trade("0xtrade-2", 60)],
            verified_trade=True,
            now=2_100,
        )
        conn.commit()

        assert first.level is WalletLevel.L0
        assert first.promoted is False
        assert second.level is WalletLevel.L1
        assert second.promoted is True
        observed = conn.execute(
            "SELECT recent_trade_count, recent_usdc_total, promoted_at, promotion_reason "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(observed) == {
            "recent_trade_count": 2,
            "recent_usdc_total": 100.0,
            "promoted_at": 2_100,
            "promotion_reason": "observed_resource_gate",
        }
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?", (wallet,)
        ).fetchone() is not None
        assert conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_trusted_source_can_enter_l1_without_trade_history(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)

        result = record_wallet_sighting(
            conn,
            _candidate(wallet, source="manual_watchlist"),
            trusted_source=True,
            now=2_000,
        )
        conn.commit()

        assert result.level is WalletLevel.L1
        assert result.reason == "trusted_source"
        assert conn.execute(
            "SELECT recent_trade_count FROM observed_wallets WHERE wallet = ?", (wallet,)
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_existing_candidate_row_alone_does_not_promote_l0_to_l1(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(
            conn,
            CandidateAddress(
                address=wallet,
                sources="polymarket_trades_global",
                labels="source_seed",
            ),
            now=1_000,
        )
        conn.commit()

        result = record_wallet_sighting(
            conn,
            _candidate(wallet, source="polymarket_trades_global"),
            recent_trades=[_trade("0xtrade", 25)],
            verified_trade=True,
            now=2_000,
        )
        conn.commit()

        assert result.level is WalletLevel.L0
        assert result.reason == ""
        assert result.candidate_updated is True
        assert get_wallet_level(conn, wallet).level is WalletLevel.L0
        observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(observed) == {"promoted_at": None, "promotion_reason": ""}
    finally:
        conn.close()


def test_repeated_source_sighting_merges_provenance_without_duplicate_source_events(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        for now in (2_000, 3_000):
            record_wallet_sighting(
                conn,
                _candidate(wallet, source="manual_watchlist"),
                trusted_source=True,
                now=now,
            )
        conn.commit()

        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_source_events WHERE address = ? AND source = ?",
            (wallet, "manual_watchlist"),
        ).fetchone()[0] == 1
        assert get_wallet_level(conn, wallet).level is WalletLevel.L1
    finally:
        conn.close()


def test_invalid_sighting_address_is_rejected_before_any_write(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        with pytest.raises(ValueError, match="invalid wallet address"):
            record_wallet_sighting(
                conn,
                CandidateAddress(address="not-a-wallet", sources="manual"),
                trusted_source=True,
                now=2_000,
            )

        assert conn.execute("SELECT COUNT(*) FROM observed_wallets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM wallet_levels").fetchone()[0] == 0
    finally:
        conn.close()
