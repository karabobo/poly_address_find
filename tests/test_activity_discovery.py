from pm_robot.clients.http import HttpClientError
from pm_robot.orchestration.activity_discovery import discover_activity_candidates
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import get_wallet_features, upsert_candidate, upsert_wallet_feature
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.wallet_levels import WalletLevel


def _table_exists(conn, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


class FakeGlobalActivityClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def recent_trades(self, *, limit, offset, min_cash_usdc=0.0):
        self.calls.append((limit, offset, min_cash_usdc))
        return self.pages.get(offset, [])


class ForbiddenGlobalActivityClient:
    def recent_trades(self, *, limit, offset, min_cash_usdc=0.0):
        raise HttpClientError("forbidden", status_code=403, error_type="cloudflare_or_forbidden")


class PartialRateLimitedActivityClient:
    def __init__(self, first_page):
        self.first_page = first_page
        self.calls = 0

    def recent_trades(self, *, limit, offset, min_cash_usdc=0.0):
        self.calls += 1
        if offset == 0:
            return self.first_page
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


def _activity(wallet: str, tx: str, usdc: float, market: str = "market-1") -> dict:
    return {
        "proxyWallet": wallet,
        "timestamp": 1_000,
        "slug": market,
        "side": "BUY",
        "usdcSize": usdc,
        "transactionHash": tx,
        "type": "TRADE",
    }


def test_discover_activity_candidates_keeps_small_trade_at_l0(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient(
            {
                0: [
                    _activity(wallet, "0x1", 120),
                    _activity("0x" + "2" * 40, "0x3", 5),
                ]
            }
        )

        summary = discover_activity_candidates(
            conn,
            pages=1,
            page_limit=100,
            client=client,
        )
        row = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()

        assert summary.status == "ok"
        assert summary.wallets_seen == 2
        assert summary.candidates_inserted_or_updated == 1
        assert summary.observed_wallets == 2
        assert summary.promoted_wallets == 1
        assert row is not None
        assert observed["recent_trade_count"] == 1
        assert observed["recent_max_trade_usdc"] == 120
        assert observed["promotion_reason"] == "observed_sample_volume_at_least_100_usdc"
        assert not _table_exists(conn, "evidence_backfill_budget")
        assert wallet in get_wallet_features(conn)
    finally:
        conn.close()


def test_discover_activity_candidates_promotes_exceptionally_large_single_trade(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient({0: [_activity(wallet, "0xlarge", 6_000)]})

        summary = discover_activity_candidates(conn, pages=1, client=client)
        candidate = conn.execute(
            "SELECT sources FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        observed = conn.execute(
            "SELECT promotion_reason FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.promoted_wallets == 1
        assert candidate["sources"] == "polymarket_trades_global"
        assert observed["promotion_reason"] == "observed_sample_volume_at_least_100_usdc"
    finally:
        conn.close()


def test_discover_activity_candidates_only_marks_promoted_after_candidate_insert(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    promoted_wallet = "0x" + "1" * 40
    observed_only_wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient(
            {
                0: [
                    _activity(promoted_wallet, "0x1", 200),
                    _activity(promoted_wallet, "0x2", 200),
                    _activity(observed_only_wallet, "0x3", 180),
                    _activity(observed_only_wallet, "0x4", 180),
                ]
            }
        )

        summary = discover_activity_candidates(
            conn,
            pages=1,
            page_limit=100,
            max_candidates=1,
            client=client,
        )
        promoted = conn.execute(
            "SELECT * FROM observed_wallets WHERE wallet = ?",
            (promoted_wallet,),
        ).fetchone()
        observed_only = conn.execute(
            "SELECT * FROM observed_wallets WHERE wallet = ?",
            (observed_only_wallet,),
        ).fetchone()
        promoted_candidate = conn.execute(
            "SELECT * FROM candidate_wallets WHERE address = ?",
            (promoted_wallet,),
        ).fetchone()
        observed_only_candidate = conn.execute(
            "SELECT * FROM candidate_wallets WHERE address = ?",
            (observed_only_wallet,),
        ).fetchone()

        assert summary.promoted_wallets == 1
        assert promoted_candidate is not None
        assert promoted["promoted_at"] is not None
        assert observed_only_candidate is None
        assert observed_only["promotion_reason"] == ""
        assert observed_only["promoted_at"] is None
    finally:
        conn.close()


def test_discover_activity_candidates_keeps_subthreshold_sample_in_observation_pool(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient({0: [_activity(wallet, "0x1", 15), _activity(wallet, "0x2", 20)]})

        summary = discover_activity_candidates(conn, pages=1, client=client)
        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()

        assert summary.status == "ok"
        assert summary.observed_wallets == 1
        assert summary.promoted_wallets == 0
        assert summary.candidates_inserted_or_updated == 0
        assert candidate is None
        assert not _table_exists(conn, "evidence_backfill_budget")
        assert observed["recent_trade_count"] == 2
        assert observed["recent_usdc_total"] == 35
        assert observed["promotion_reason"] == ""
    finally:
        conn.close()


def test_discover_activity_candidates_passes_cash_filter_to_trades_api(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient({0: [_activity(wallet, "0x1", 600)]})

        summary = discover_activity_candidates(
            conn,
            pages=1,
            page_limit=25,
            min_trade_filter_usdc=500,
            client=client,
        )

        assert summary.status == "ok"
        assert client.calls == [(25, 0, 500)]
    finally:
        conn.close()


def test_discover_activity_candidates_promotes_cumulative_recent_observed_volume(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient(
            {
                0: [
                    _activity(wallet, f"0x{idx}", 30, market=f"market-{idx}")
                    for idx in range(10)
                ]
            }
        )

        summary = discover_activity_candidates(conn, pages=1, client=client)
        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()

        assert summary.promoted_wallets == 1
        assert candidate is not None
        assert observed["recent_trade_count"] == 10
        assert observed["recent_usdc_total"] == 300
        assert observed["recent_max_trade_usdc"] == 30
        assert observed["promotion_reason"] == "observed_sample_volume_at_least_100_usdc"
    finally:
        conn.close()


def test_discover_activity_candidates_merges_existing_candidate_source(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual", labels="watchlist"))
        conn.commit()
        client = FakeGlobalActivityClient({0: [_activity(wallet, "0x1", 15), _activity(wallet, "0x2", 20)]})

        discover_activity_candidates(conn, pages=1, client=client)
        row = conn.execute("SELECT sources, labels FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT promotion_reason FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()

        assert row["sources"] == "manual | polymarket_trades_global"
        assert row["labels"] == "watchlist | trade_activity_seed"
        assert observed["promotion_reason"] == "existing_candidate"
    finally:
        conn.close()


def test_existing_candidate_refresh_does_not_consume_new_promotion_limit(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    existing_wallet = "0x" + "3" * 40
    new_wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(
            conn,
            CandidateAddress(address=existing_wallet, sources="manual", labels="watchlist"),
        )
        conn.commit()
        client = FakeGlobalActivityClient(
            {
                0: [
                    _activity(existing_wallet, "0xexisting", 6_000),
                    _activity(new_wallet, "0xnew1", 200),
                    _activity(new_wallet, "0xnew2", 200),
                ]
            }
        )

        summary = discover_activity_candidates(
            conn,
            pages=1,
            max_candidates=1,
            client=client,
        )

        assert summary.candidates_inserted_or_updated == 2
        assert summary.promoted_wallets == 1
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (new_wallet,),
        ).fetchone() is not None
        assert conn.execute(
            "SELECT promotion_reason FROM observed_wallets WHERE wallet = ?",
            (existing_wallet,),
        ).fetchone()["promotion_reason"] == "existing_candidate"
    finally:
        conn.close()


def test_activity_discovery_ignores_legacy_summary_only_state_and_reenters_new_funnel(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="archived_source"))
        upsert_wallet_feature(conn, WalletFeatures(address=wallet, net_pnl_usdc=42))
        conn.commit()

        summary = discover_activity_candidates(
            conn,
            pages=1,
            client=FakeGlobalActivityClient({0: [_activity(wallet, "0xlarge", 500)]}),
        )

        candidate = conn.execute(
            "SELECT sources FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        observed = conn.execute(
            "SELECT recent_max_trade_usdc, promoted_at FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        feature = get_wallet_features(conn)[wallet]
        assert not _table_exists(conn, "wallet_registry")
        assert summary.observed_wallets == 1
        assert summary.promoted_wallets == 0
        assert summary.candidates_inserted_or_updated == 1
        assert candidate["sources"] == "archived_source | polymarket_trades_global"
        assert observed["recent_max_trade_usdc"] == 500
        assert observed["promoted_at"] is not None
        assert get_wallet_level(conn, wallet).level is WalletLevel.L1
        assert feature.net_pnl_usdc == 42
        assert not _table_exists(conn, "evidence_backfill_budget")
    finally:
        conn.close()


def test_discover_activity_candidates_reports_limited_when_global_activity_forbidden(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)

        summary = discover_activity_candidates(conn, pages=1, client=ForbiddenGlobalActivityClient())

        assert summary.status == "limited"
        assert summary.candidates_inserted_or_updated == 0
        assert "cloudflare_or_forbidden" in summary.error
    finally:
        conn.close()


def test_discovery_persists_successful_pages_before_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    client = PartialRateLimitedActivityClient([_activity(wallet, "0xpartial", 6_000)])
    try:
        run_migrations(conn)
        summary = discover_activity_candidates(
            conn,
            pages=2,
            page_limit=1,
            sleep_seconds=0,
            client=client,
        )

        assert summary.status == "partial"
        assert summary.pages_succeeded == 1
        assert summary.promoted_wallets == 1
        assert client.calls == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 1
    finally:
        conn.close()
