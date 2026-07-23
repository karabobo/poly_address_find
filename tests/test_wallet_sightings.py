import pytest

from pm_robot.models import CandidateAddress
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.db import connect, run_migrations
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


def test_small_verified_trade_stays_l0_without_candidate_or_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)

        result = record_wallet_sighting(
            conn,
            _candidate(wallet),
            recent_trades=[_trade("0xtrade", 5)],
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
            "promotion_reason": "observed_sample_volume_at_least_100_usdc",
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
