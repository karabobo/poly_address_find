from pm_robot.clients.http import HttpClientError
import pm_robot.orchestration.activity_discovery as activity_module
from pm_robot.orchestration.activity_discovery import discover_activity_candidates, process_activity_rows
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import get_wallet_features, upsert_candidate, upsert_wallet_feature
from pm_robot.storage.wallet_levels import (
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
    set_wallet_hard_risk_block,
)
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
        assert summary.candidates_inserted_or_updated == 0
        assert summary.observed_wallets == 2
        assert summary.promoted_wallets == 0
        assert row is None
        assert observed["recent_trade_count"] == 1
        assert observed["recent_max_trade_usdc"] == 120
        assert observed["promotion_reason"] == ""
        assert not _table_exists(conn, "evidence_backfill_budget")
        assert wallet not in get_wallet_features(conn)
    finally:
        conn.close()


def test_activity_ingress_rejects_malformed_wallets_and_nonfinite_trade_values(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    valid_wallet = "0x" + "1" * 40
    invalid_wallet = "0x" + "g" * 40
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient(
            {
                0: [
                    _activity(invalid_wallet, "0xbad", 500),
                    _activity(valid_wallet, "0xinf", float("inf")),
                ]
            }
        )

        summary = discover_activity_candidates(conn, pages=1, client=client)

        assert summary.status == "ok"
        assert summary.wallets_seen == 0
        assert summary.observed_wallets == 0
        assert conn.execute("SELECT COUNT(*) FROM wallet_levels").fetchone()[0] == 0
    finally:
        conn.close()


def test_activity_ingress_counts_one_semantic_trade_once_across_pages(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    trade = _activity(wallet, "0xduplicate", 120)
    try:
        run_migrations(conn)
        client = FakeGlobalActivityClient(
            {
                0: [trade],
                100: [{**trade, "name": "enriched duplicate"}],
            }
        )

        summary = discover_activity_candidates(
            conn,
            pages=2,
            page_limit=100,
            client=client,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.wallets_seen == 1
        assert tuple(observed) == (1, 1, 120)
    finally:
        conn.close()


def test_discover_activity_candidates_keeps_exceptionally_large_single_trade_at_l0(tmp_path):
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

        assert summary.promoted_wallets == 0
        assert candidate is None
        assert observed["promotion_reason"] == ""
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
        assert observed["promotion_reason"] == "observed_resource_gate"
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
        assert observed["promotion_reason"] == "trusted_candidate_provenance"
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
        ).fetchone()["promotion_reason"] == "trusted_candidate_provenance"
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
        assert observed["promoted_at"] is None
        assert get_wallet_level(conn, wallet).level is WalletLevel.L0
        assert feature.net_pnl_usdc == 42
        assert not _table_exists(conn, "evidence_backfill_budget")
    finally:
        conn.close()


def test_activity_discovery_uses_chunk_snapshots_without_per_wallet_reads(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    target_wallets = ["0x" + f"{index:040x}" for index in range(7, 12)]
    unrelated_wallet = "0x" + "6" * 40
    statements = []
    try:
        run_migrations(conn)
        monkeypatch.setattr(activity_module, "DISCOVERY_WRITE_BATCH_SIZE", 2)
        monkeypatch.setattr(activity_module, "DISCOVERY_WRITE_YIELD_SECONDS", 0)
        for index, wallet in enumerate(target_wallets, start=1):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"))
            upsert_wallet_feature(
                conn,
                WalletFeatures(
                    address=wallet,
                    net_pnl_usdc=40 + index,
                    extra={"existing": {"wallet": index}},
                ),
            )
        upsert_candidate(
            conn,
            CandidateAddress(address=unrelated_wallet, sources="manual"),
        )
        upsert_wallet_feature(
            conn,
            WalletFeatures(address=unrelated_wallet, net_pnl_usdc=999),
        )
        conn.commit()
        conn.set_trace_callback(statements.append)

        summary = process_activity_rows(
            conn,
            [
                _activity(wallet, f"0xbatch{index}", 150 + index)
                for index, wallet in enumerate(target_wallets, start=1)
            ],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            now=1_700_000_000,
        )

        conn.set_trace_callback(None)
        feature_selects = [
            statement
            for statement in statements
            if "FROM wallet_features" in statement
        ]
        candidate_selects = [
            statement
            for statement in statements
            if "FROM candidate_wallets" in statement
        ]
        level_selects = [
            statement
            for statement in statements
            if "FROM wallet_levels" in statement
        ]
        observed_selects = [
            statement
            for statement in statements
            if "FROM observed_wallets" in statement
        ]
        features = get_wallet_features(conn)
        assert summary.candidates_inserted_or_updated == 5
        for index, wallet in enumerate(target_wallets, start=1):
            assert features[wallet].net_pnl_usdc == 40 + index
            assert features[wallet].extra["existing"] == {"wallet": index}
            assert features[wallet].extra["activity_discovery"]["trade_count"] == 1
        feature_snapshots = [
            statement
            for statement in feature_selects
            if "WHERE address IN" in statement
        ]
        candidate_snapshots = [
            statement
            for statement in candidate_selects
            if "WHERE address IN" in statement
        ]
        level_snapshots = [
            statement
            for statement in level_selects
            if "WHERE wallet IN" in statement
        ]
        observed_snapshots = [
            statement
            for statement in observed_selects
            if "WHERE wallet IN" in statement
        ]
        expected_chunks = 3
        assert len(feature_snapshots) == expected_chunks
        assert len(candidate_snapshots) == expected_chunks
        assert len(level_snapshots) == expected_chunks
        assert len(observed_snapshots) == expected_chunks
        assert all(
            any(wallet in statement for statement in feature_snapshots)
            for wallet in target_wallets
        )
        assert all(
            any(wallet in statement for statement in candidate_snapshots)
            for wallet in target_wallets
        )
        assert all(
            any(wallet in statement for statement in level_snapshots)
            for wallet in target_wallets
        )
        assert all(
            any(wallet in statement for statement in observed_snapshots)
            for wallet in target_wallets
        )
        assert all(unrelated_wallet not in statement for statement in feature_snapshots)
        assert all(unrelated_wallet not in statement for statement in candidate_snapshots)
        assert all(unrelated_wallet not in statement for statement in level_snapshots)
        assert all(unrelated_wallet not in statement for statement in observed_snapshots)
        assert all("WHERE address =" not in statement for statement in feature_selects)
        assert all("WHERE address =" not in statement for statement in candidate_selects)
        assert all("WHERE wallet =" not in statement for statement in level_selects)
        assert all("WHERE wallet =" not in statement for statement in observed_selects)
    finally:
        conn.set_trace_callback(None)
        conn.close()


def test_activity_chunk_snapshot_does_not_promote_wallet_blocked_between_chunks(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    blocker = connect(db_path)
    first_wallet = "0x" + "a" * 40
    blocked_wallet = "0x" + "b" * 40
    sleep_calls = 0

    def hard_block_next_wallet(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            set_wallet_hard_risk_block(blocker, blocked_wallet, blocked=True, now=1_500)
            blocker.commit()

    try:
        run_migrations(conn)
        monkeypatch.setattr(activity_module, "DISCOVERY_WRITE_BATCH_SIZE", 1)
        monkeypatch.setattr(activity_module.time, "sleep", hard_block_next_wallet)

        summary = process_activity_rows(
            conn,
            [
                _activity(first_wallet, "0xfirst", 500),
                _activity(first_wallet, "0xfirst-2", 100),
                _activity(blocked_wallet, "0xblocked", 200),
            ],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            now=2_000,
        )

        blocked_level = get_wallet_level(conn, blocked_wallet)
        assert sleep_calls == 1
        assert summary.promoted_wallets == 1
        assert blocked_level.level is WalletLevel.L0
        assert blocked_level.hard_risk_block is True
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_level_events WHERE wallet = ?",
            (blocked_wallet,),
        ).fetchone()[0] == 0
        assert tuple(
            conn.execute(
                "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
                (blocked_wallet,),
            ).fetchone()
        ) == (None, "")
    finally:
        blocker.close()
        conn.close()


def test_activity_chunk_snapshot_reads_concurrent_candidate_observed_feature_updates(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    updater = connect(db_path)
    first_wallet = "0x" + "c" * 40
    updated_wallet = "0x" + "d" * 40
    sleep_calls = 0

    def update_next_wallet(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            record_wallet_sighting(
                updater,
                CandidateAddress(address=updated_wallet, sources="external_stream"),
                recent_trades=[
                    {
                        "transaction_hash": "0xexternal",
                        "timestamp": 1_400,
                        "market": "market-external",
                        "side": "BUY",
                        "usdc_size": 30,
                    }
                ],
                verified_trade=True,
                allow_l1=False,
                now=1_500,
            )
            upsert_candidate(
                updater,
                CandidateAddress(
                    address=updated_wallet,
                    sources="manual",
                    labels="watchlist",
                ),
            )
            upsert_wallet_feature(
                updater,
                WalletFeatures(
                    address=updated_wallet,
                    net_pnl_usdc=42,
                    extra={"external": True},
                ),
            )
            updater.commit()

    try:
        run_migrations(conn)
        monkeypatch.setattr(activity_module, "DISCOVERY_WRITE_BATCH_SIZE", 1)
        monkeypatch.setattr(activity_module.time, "sleep", update_next_wallet)

        summary = process_activity_rows(
            conn,
            [
                _activity(first_wallet, "0xfirst", 500),
                _activity(first_wallet, "0xfirst-2", 100),
                _activity(updated_wallet, "0xrtds", 70, market="market-rtds"),
            ],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            now=2_000,
        )

        candidate = conn.execute(
            "SELECT sources, labels FROM candidate_wallets WHERE address = ?",
            (updated_wallet,),
        ).fetchone()
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total, "
            "promotion_reason FROM observed_wallets WHERE wallet = ?",
            (updated_wallet,),
        ).fetchone()
        feature = get_wallet_features(conn)[updated_wallet]

        assert sleep_calls == 1
        assert summary.promoted_wallets == 1
        assert candidate["sources"] == "manual | rtds_realtime_trades"
        assert candidate["labels"] == "watchlist | rtds_trade_activity_seed"
        assert dict(observed) == {
            "observed_trade_count": 2,
            "recent_trade_count": 2,
            "recent_usdc_total": 100.0,
            "promotion_reason": "trusted_candidate_provenance",
        }
        assert feature.net_pnl_usdc == 42
        assert feature.extra["external"] is True
        assert feature.extra["activity_discovery"]["trade_count"] == 1
        assert get_wallet_level(conn, updated_wallet).level is WalletLevel.L1
    finally:
        updater.close()
        conn.close()


def test_activity_preserves_caller_owned_transaction_until_outer_rollback(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    sleep_calls = 0

    def track_sleep(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1

    try:
        run_migrations(conn)
        monkeypatch.setattr(activity_module, "DISCOVERY_WRITE_BATCH_SIZE", 1)
        monkeypatch.setattr(activity_module.time, "sleep", track_sleep)
        conn.execute("CREATE TABLE caller_sentinel(id INTEGER PRIMARY KEY, note TEXT)")
        conn.commit()

        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO caller_sentinel(id, note) VALUES (1, 'outer')")
        assert conn.in_transaction is True

        summary = process_activity_rows(
            conn,
            [
                _activity(wallet, "0xouter-1", 75),
                _activity(wallet, "0xouter-2", 75),
                _activity("0x" + "f" * 40, "0xouter-3", 80),
                _activity("0x" + "f" * 40, "0xouter-4", 80),
            ],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            now=2_000,
        )

        assert conn.in_transaction is True
        assert sleep_calls == 0
        assert summary.promoted_wallets == 2
        assert conn.execute("SELECT COUNT(*) FROM caller_sentinel").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM observed_wallets").fetchone()[0] == 2

        conn.rollback()

        assert conn.in_transaction is False
        assert conn.execute("SELECT COUNT(*) FROM caller_sentinel").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM observed_wallets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM candidate_wallets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM wallet_levels").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM wallet_level_events").fetchone()[0] == 0
    finally:
        if conn.in_transaction:
            conn.rollback()
        conn.close()


def test_activity_discovery_batch_snapshot_preserves_mixed_wallet_semantics(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    gate_wallet = "0x" + "1" * 40
    existing_wallet = "0x" + "2" * 40
    blocked_wallet = "0x" + "3" * 40
    promoted_wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        process_activity_rows(
            conn,
            [_activity(gate_wallet, "0xgate-existing", 40)],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            max_candidates=0,
            now=1_000,
        )
        upsert_candidate(conn, CandidateAddress(address=existing_wallet, sources="manual"))
        ensure_wallet_level(conn, blocked_wallet, reason="risk_seed", now=1_000)
        set_wallet_hard_risk_block(conn, blocked_wallet, blocked=True, now=1_001)
        upsert_candidate(conn, CandidateAddress(address=promoted_wallet, sources="manual"))
        ensure_wallet_level(conn, promoted_wallet, reason="manual", now=1_000)
        advance_wallet_level(
            conn,
            promoted_wallet,
            to_level=WalletLevel.L1,
            reason="prior_promotion",
            now=1_100,
        )
        conn.commit()

        summary = process_activity_rows(
            conn,
            [
                _activity(gate_wallet, "0xgate-existing", 40),
                _activity(gate_wallet, "0xgate-new", 60),
                _activity(existing_wallet, "0xexisting-low", 10),
                _activity(blocked_wallet, "0xblocked", 200),
                _activity(promoted_wallet, "0xpromoted", 200),
            ],
            source="rtds_realtime_trades",
            labels="rtds_trade_activity_seed",
            status_prefix="rtds_activity_discovered",
            now=2_000,
        )

        gate_observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total, "
            "promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (gate_wallet,),
        ).fetchone()
        existing_observed = conn.execute(
            "SELECT recent_usdc_total, promoted_at, promotion_reason "
            "FROM observed_wallets WHERE wallet = ?",
            (existing_wallet,),
        ).fetchone()
        blocked_observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (blocked_wallet,),
        ).fetchone()
        promoted_observed = conn.execute(
            "SELECT promoted_at, promotion_reason FROM observed_wallets WHERE wallet = ?",
            (promoted_wallet,),
        ).fetchone()

        assert summary.wallets_seen == 4
        assert summary.observed_wallets == 4
        assert summary.candidates_inserted_or_updated == 3
        assert summary.promoted_wallets == 1
        assert dict(gate_observed) == {
            "observed_trade_count": 2,
            "recent_trade_count": 2,
            "recent_usdc_total": 100.0,
            "promoted_at": 2_000,
            "promotion_reason": "observed_resource_gate",
        }
        assert dict(existing_observed) == {
            "recent_usdc_total": 10.0,
            "promoted_at": 2_000,
            "promotion_reason": "trusted_candidate_provenance",
        }
        assert dict(blocked_observed) == {
            "promoted_at": None,
            "promotion_reason": "",
        }
        assert dict(promoted_observed) == {
            "promoted_at": 2_000,
            "promotion_reason": "trusted_candidate_provenance",
        }
        assert get_wallet_level(conn, gate_wallet).level is WalletLevel.L1
        assert get_wallet_level(conn, existing_wallet).level is WalletLevel.L1
        blocked_level = get_wallet_level(conn, blocked_wallet)
        assert blocked_level.level is WalletLevel.L0
        assert blocked_level.hard_risk_block is True
        assert get_wallet_level(conn, promoted_wallet).level is WalletLevel.L1
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
        assert summary.promoted_wallets == 0
        assert client.calls == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_activity_discovery_releases_writer_lock_between_wallet_batches(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    statements = []
    rows = [
        _activity(f"0x{index:040x}", f"0x{index}", 150)
        for index in range(1, 13)
    ]
    try:
        run_migrations(conn)
        conn.set_trace_callback(statements.append)

        summary = discover_activity_candidates(
            conn,
            pages=1,
            client=FakeGlobalActivityClient({0: rows}),
        )

        commits = [statement for statement in statements if statement == "COMMIT"]
        assert summary.observed_wallets == 12
        assert len(commits) >= 2
    finally:
        conn.close()
