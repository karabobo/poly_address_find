CREATE TABLE IF NOT EXISTS wallet_dashboard_snapshot (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    candidate_stage TEXT NOT NULL DEFAULT '',
    activity_count INTEGER NOT NULL DEFAULT 0,
    discovery_tier TEXT NOT NULL DEFAULT '',
    next_action TEXT NOT NULL DEFAULT '',
    leader_score REAL NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

INSERT INTO wallet_dashboard_snapshot(
    address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
)
SELECT
    cw.address,
    cw.candidate_stage,
    COALESCE(wps.activity_count, eb.current_depth, 0) AS activity_count,
    COALESCE(wps.discovery_tier, '') AS discovery_tier,
    COALESCE(wps.next_action, '') AS next_action,
    COALESCE(ls.leader_score, 0) AS leader_score,
    MAX(
        COALESCE(cw.updated_at, 0),
        COALESCE(wps.updated_at, 0),
        COALESCE(eb.updated_at, 0),
        COALESCE(ls.scored_at, 0)
    ) AS updated_at
FROM candidate_wallets cw
LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
ON CONFLICT(address) DO UPDATE SET
    candidate_stage = excluded.candidate_stage,
    activity_count = excluded.activity_count,
    discovery_tier = excluded.discovery_tier,
    next_action = excluded.next_action,
    leader_score = excluded.leader_score,
    updated_at = excluded.updated_at;

CREATE INDEX IF NOT EXISTS idx_wallet_dashboard_snapshot_stage_score
    ON wallet_dashboard_snapshot(candidate_stage, leader_score DESC);

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_candidate_upsert
AFTER INSERT ON candidate_wallets
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.address
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_candidate_update
AFTER UPDATE ON candidate_wallets
BEGIN
    DELETE FROM wallet_dashboard_snapshot
    WHERE address = OLD.address
      AND OLD.address != NEW.address;

    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.address
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_candidate_delete
AFTER DELETE ON candidate_wallets
BEGIN
    DELETE FROM wallet_dashboard_snapshot WHERE address = OLD.address;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_processing_upsert
AFTER INSERT ON wallet_processing_state
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_processing_update
AFTER UPDATE ON wallet_processing_state
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_processing_delete
AFTER DELETE ON wallet_processing_state
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = OLD.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_budget_upsert
AFTER INSERT ON evidence_backfill_budget
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_budget_update
AFTER UPDATE ON evidence_backfill_budget
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_budget_delete
AFTER DELETE ON evidence_backfill_budget
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = OLD.wallet
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_score_upsert
AFTER INSERT ON leader_latest_scores
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.address
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_score_update
AFTER UPDATE ON leader_latest_scores
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = NEW.address
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_dashboard_snapshot_score_delete
AFTER DELETE ON leader_latest_scores
BEGIN
    INSERT INTO wallet_dashboard_snapshot(
        address, candidate_stage, activity_count, discovery_tier, next_action, leader_score, updated_at
    )
    SELECT
        cw.address,
        cw.candidate_stage,
        COALESCE(wps.activity_count, eb.current_depth, 0),
        COALESCE(wps.discovery_tier, ''),
        COALESCE(wps.next_action, ''),
        COALESCE(ls.leader_score, 0),
        MAX(COALESCE(cw.updated_at, 0), COALESCE(wps.updated_at, 0), COALESCE(eb.updated_at, 0), COALESCE(ls.scored_at, 0))
    FROM candidate_wallets cw
    LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
    LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
    LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
    WHERE cw.address = OLD.address
    ON CONFLICT(address) DO UPDATE SET
        candidate_stage = excluded.candidate_stage,
        activity_count = excluded.activity_count,
        discovery_tier = excluded.discovery_tier,
        next_action = excluded.next_action,
        leader_score = excluded.leader_score,
        updated_at = excluded.updated_at;
END;
