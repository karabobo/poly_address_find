import sqlite3

import pytest

from pm_robot.storage.db import run_migrations
from pm_robot.storage.wallet_levels import (
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
    set_wallet_hard_risk_block,
)
from pm_robot.wallet_levels import WalletLevel


WALLET = "0x1111111111111111111111111111111111111111"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    run_migrations(conn)
    return conn


def test_wallet_level_storage_is_single_monotonic_truth():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="rtds_sighting", now=100)

    decision = advance_wallet_level(
        conn,
        WALLET,
        to_level=WalletLevel.L1,
        reason="verified_trade",
        policy_version="levels-v1",
        facts={"source": "rtds"},
        now=110,
    )

    assert decision.level is WalletLevel.L1
    assert get_wallet_level(conn, WALLET).level is WalletLevel.L1
    event = conn.execute("SELECT * FROM wallet_level_events WHERE wallet = ?", (WALLET,)).fetchone()
    assert event["from_level"] == "l0"
    assert event["to_level"] == "l1"
    assert event["policy_version"] == "levels-v1"


def test_wallet_level_storage_rejects_skips_and_downgrades():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="manual", now=100)
    advance_wallet_level(conn, WALLET, to_level=WalletLevel.L1, reason="trusted", now=101)

    with pytest.raises(ValueError, match="one level"):
        advance_wallet_level(conn, WALLET, to_level=WalletLevel.L3, reason="skip", now=102)
    with pytest.raises(ValueError, match="downgrade"):
        advance_wallet_level(conn, WALLET, to_level=WalletLevel.L0, reason="down", now=103)


def test_hard_risk_block_stops_future_promotion_without_demoting():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="manual", now=100)
    advance_wallet_level(conn, WALLET, to_level=WalletLevel.L1, reason="trusted", now=101)
    set_wallet_hard_risk_block(conn, WALLET, blocked=True, now=102)

    result = advance_wallet_level(conn, WALLET, to_level=WalletLevel.L2, reason="screen", now=103)

    assert result.level is WalletLevel.L1
    assert get_wallet_level(conn, WALLET).hard_risk_block is True


def test_same_level_write_is_idempotent_and_does_not_emit_event():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="first", now=100)
    ensure_wallet_level(conn, WALLET, reason="seen_again", now=110)

    row = get_wallet_level(conn, WALLET)
    assert row.level is WalletLevel.L0
    assert row.last_seen_at == 110
    assert conn.execute("SELECT COUNT(*) FROM wallet_level_events").fetchone()[0] == 0


def test_level_promotion_does_not_fabricate_a_new_wallet_sighting():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="rtds_sighting", now=100)

    advance_wallet_level(
        conn,
        WALLET,
        to_level=WalletLevel.L1,
        reason="verified_trade",
        now=500,
    )

    row = get_wallet_level(conn, WALLET)
    assert row.last_seen_at == 100
    assert row.level_updated_at == 500


def test_existing_l5_can_advance_once_to_l6_without_rewriting_prior_events():
    conn = _conn()
    ensure_wallet_level(conn, WALLET, reason="seed", now=100)
    for index, target in enumerate(
        (
            WalletLevel.L1,
            WalletLevel.L2,
            WalletLevel.L3,
            WalletLevel.L4,
            WalletLevel.L5,
        ),
        start=1,
    ):
        advance_wallet_level(conn, WALLET, to_level=target, reason="seed", now=100 + index)

    result = advance_wallet_level(
        conn,
        WALLET,
        to_level=WalletLevel.L6,
        reason="independent_validation_passed",
        policy_version="l6_independent_v1",
        now=200,
    )

    assert result.level is WalletLevel.L6
    assert get_wallet_level(conn, WALLET).level is WalletLevel.L6
    transitions = conn.execute(
        "SELECT from_level, to_level FROM wallet_level_events WHERE wallet = ? ORDER BY event_id",
        (WALLET,),
    ).fetchall()
    assert [(row["from_level"], row["to_level"]) for row in transitions][-2:] == [
        ("l4", "l5"),
        ("l5", "l6"),
    ]
