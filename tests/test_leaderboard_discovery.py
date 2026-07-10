from pm_robot.orchestration.leaderboard_discovery import discover_leaderboard_candidates
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import get_wallet_features


class FakeLeaderboardClient:
    def __init__(self):
        self.legacy_calls = []
        self.v1_calls = []

    def leaderboard(self, metric, *, window):
        self.legacy_calls.append((metric, window))
        return []

    def trader_leaderboard(self, *, category, time_period, order_by, limit, offset):
        self.v1_calls.append((category, time_period, order_by, limit, offset))
        if (category, time_period, order_by, offset) == ("OVERALL", "MONTH", "PNL", 0):
            return [
                {
                    "proxyWallet": "0x" + "1" * 40,
                    "rank": "1",
                    "userName": "sharp-one",
                    "vol": 12_000,
                    "pnl": 2_500,
                    "verifiedBadge": True,
                }
            ]
        if (category, time_period, order_by, offset) == ("OVERALL", "MONTH", "VOL", 0):
            return [
                {
                    "proxyWallet": "0x" + "1" * 40,
                    "rank": "3",
                    "userName": "sharp-one",
                    "vol": 18_000,
                    "pnl": 2_100,
                }
            ]
        return []


def test_discover_leaderboard_candidates_imports_official_v1_category_rankings(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        client = FakeLeaderboardClient()

        summary = discover_leaderboard_candidates(
            conn,
            metrics=(),
            windows=(),
            categories=("OVERALL",),
            time_periods=("MONTH",),
            order_bys=("PNL", "VOL"),
            v1_limit=50,
            v1_pages=1,
            client=client,
        )

        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        source_event = conn.execute(
            "SELECT * FROM candidate_source_events WHERE address = ? AND source LIKE '%polymarket_v1_leaderboard%'",
            (wallet,),
        ).fetchone()
        features = get_wallet_features(conn)[wallet]

        assert summary.status == "ok"
        assert summary.snapshots_attempted == 0
        assert summary.v1_snapshots_attempted == 2
        assert summary.v1_snapshots_succeeded == 2
        assert summary.candidates_seen == 1
        assert "polymarket_v1_leaderboard" in candidate["sources"]
        assert "category_leaderboard" in candidate["labels"]
        assert "OVERALL_MONTH_PNL" in candidate["notes"]
        assert "OVERALL_MONTH_VOL" in candidate["notes"]
        assert source_event is not None
        assert features.recent_30d_volume_usdc == 18_000
        assert features.net_pnl_usdc == 2_500
        assert "official_v1_leaderboard_discovery" in features.extra
        assert client.v1_calls == [
            ("OVERALL", "MONTH", "PNL", 50, 0),
            ("OVERALL", "MONTH", "VOL", 50, 0),
        ]
    finally:
        conn.close()
