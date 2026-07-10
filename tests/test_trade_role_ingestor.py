from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.orchestration.trade_role_ingestor import (
    TRADE_SAMPLE_LIMIT,
    ingest_trade_role_evidence,
)
from pm_robot.orchestration.feature_materializer import materialize_wallet_features
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    get_wallet_features,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


class FakeTradeClient:
    def __init__(self, all_trades, taker_trades):
        self.all_trades = all_trades
        self.taker_trades = taker_trades

    def wallet_trades(self, wallet, *, limit, taker_only, offset=0):
        assert limit == TRADE_SAMPLE_LIMIT
        return self.taker_trades if taker_only else self.all_trades


class RateLimitedTradeClient:
    def __init__(self):
        self.calls = 0

    def wallet_trades(self, wallet, *, limit, taker_only, offset=0):
        self.calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


def _trades(count: int, *, prefix: str = "trade") -> list[dict]:
    return [
        {
            "transactionHash": f"{prefix}-{idx}",
            "asset": f"asset-{idx % 3}",
            "timestamp": 1_000 + idx,
            "side": "BUY",
            "size": "1",
            "price": "0.5",
        }
        for idx in range(count)
    ]


def _seed(conn, wallet: str) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    persist_wallet_activity(
        conn,
        wallet,
        [
            {
                "timestamp": 1_000,
                "conditionId": "condition",
                "eventSlug": "event",
                "slug": "market",
                "asset": "asset",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.5,
                "size": 2,
                "usdcSize": 1,
                "transactionHash": "0xseed",
            }
        ],
        ingested_at=2_000,
    )
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            bot_score=20,
            net_to_gross_exposure=0.8,
            single_market_pnl_share=0.2,
            hygiene_status="incomplete",
        ),
    )
    conn.commit()


def test_trade_role_evidence_can_screen_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        _seed(conn, wallet)
        all_trades = _trades(120)
        summary = ingest_trade_role_evidence(
            conn,
            limit=1,
            client=FakeTradeClient(all_trades, all_trades[:100]),
            now=10_000,
        )

        feature = get_wallet_features(conn)[wallet]
        evidence = conn.execute(
            "SELECT * FROM wallet_trade_role_evidence WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert summary.status == "ok"
        assert summary.wallets_screened == 1
        assert evidence["maker_trades"] == 20
        assert evidence["sample_complete"] == 1
        assert round(feature.maker_fraction, 4) == 0.1667
        assert feature.hygiene_status == "screened"

        materialize_wallet_features(conn, limit=1, min_activity_events=0, now=10_100)
        refreshed = get_wallet_features(conn)[wallet]
        assert round(refreshed.maker_fraction, 4) == 0.1667
        assert refreshed.hygiene_status == "screened"
        assert refreshed.extra["maker_fraction_source"].startswith(
            "polymarket_data_api_trades"
        )
    finally:
        conn.close()


def test_trade_role_ingestor_stops_batch_without_recording_wallet_error(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + "3" * 40, "0x" + "4" * 40]
    client = RateLimitedTradeClient()
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed(conn, wallet)

        summary = ingest_trade_role_evidence(
            conn,
            limit=2,
            client=client,
            now=10_000,
        )

        assert summary.status == "partial"
        assert summary.wallets_attempted == 1
        assert summary.wallets_succeeded == 0
        assert client.calls == 1
        assert conn.execute("SELECT COUNT(*) FROM wallet_trade_role_evidence").fetchone()[0] == 0
    finally:
        conn.close()


def test_trade_role_ingestor_skips_summary_only_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    client = FakeTradeClient(_trades(20), _trades(10, prefix="taker"))
    try:
        run_migrations(conn)
        _seed(conn, wallet)
        conn.execute(
            """
            INSERT INTO wallet_registry(
                address, candidate_stage, registry_status, raw_retention_tier,
                last_evaluated_at, updated_at
            ) VALUES (?, 'needs_data', 'archived_raw_pruned', 'summary_only', ?, ?)
            """,
            (wallet, 10_000, 10_000),
        )
        conn.commit()

        summary = ingest_trade_role_evidence(conn, limit=1, client=client, now=20_000)

        assert summary.wallets_attempted == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_trade_role_evidence WHERE wallet = ?",
            (wallet,),
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_capped_trade_sample_remains_incomplete(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed(conn, wallet)
        all_trades = _trades(TRADE_SAMPLE_LIMIT)
        summary = ingest_trade_role_evidence(
            conn,
            limit=1,
            client=FakeTradeClient(all_trades, all_trades[:9_000]),
            now=10_000,
        )

        feature = get_wallet_features(conn)[wallet]
        assert summary.wallets_screened == 0
        assert feature.hygiene_status == "incomplete"
    finally:
        conn.close()
