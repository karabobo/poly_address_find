CREATE TABLE IF NOT EXISTS wallet_history_planner_sighting_dirty (
    wallet TEXT PRIMARY KEY,
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    dirty_at INTEGER NOT NULL DEFAULT 0,
    dirty_generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_wallet_history_planner_sighting_dirty_due
    ON wallet_history_planner_sighting_dirty(dirty_at, wallet);

DROP TRIGGER IF EXISTS trg_wallet_history_planner_dirty_levels_update;
DROP TRIGGER IF EXISTS trg_wallet_history_planner_sighting_levels_update;
DROP TRIGGER IF EXISTS trg_wallet_history_planner_full_dirty_clears_sighting_insert;
DROP TRIGGER IF EXISTS trg_wallet_history_planner_full_dirty_clears_sighting_update;

CREATE TRIGGER trg_wallet_history_planner_dirty_levels_update
AFTER UPDATE OF level, hard_risk_block ON wallet_levels
WHEN NEW.wallet != ''
 AND (
       NEW.level != OLD.level
    OR NEW.hard_risk_block != OLD.hard_risk_block
 )
 AND (
       NEW.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
    OR OLD.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_levels', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER trg_wallet_history_planner_sighting_levels_update
AFTER UPDATE OF last_seen_at ON wallet_levels
WHEN NEW.wallet != ''
 AND NEW.last_seen_at > OLD.last_seen_at
 AND NEW.level = OLD.level
 AND NEW.hard_risk_block = OLD.hard_risk_block
 AND NEW.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
 AND EXISTS (
       SELECT 1 FROM wallet_history_planner_state
       WHERE wallet = NEW.wallet
 )
BEGIN
    INSERT INTO wallet_history_planner_sighting_dirty(
        wallet, last_seen_at, dirty_at
    ) VALUES (
        NEW.wallet,
        NEW.last_seen_at,
        CAST(strftime('%s', 'now') AS INTEGER)
    )
    ON CONFLICT(wallet) DO UPDATE SET
        last_seen_at = MAX(
            wallet_history_planner_sighting_dirty.last_seen_at,
            excluded.last_seen_at
        ),
        dirty_at = excluded.dirty_at,
        dirty_generation =
            wallet_history_planner_sighting_dirty.dirty_generation + 1;
END;

-- A structural invalidation supersedes sightings that happened before it.
CREATE TRIGGER trg_wallet_history_planner_full_dirty_clears_sighting_insert
AFTER INSERT ON wallet_history_planner_dirty
WHEN NEW.wallet != ''
BEGIN
    DELETE FROM wallet_history_planner_sighting_dirty
    WHERE wallet = NEW.wallet;
END;

CREATE TRIGGER trg_wallet_history_planner_full_dirty_clears_sighting_update
AFTER UPDATE OF dirty_generation ON wallet_history_planner_dirty
WHEN NEW.wallet != ''
BEGIN
    DELETE FROM wallet_history_planner_sighting_dirty
    WHERE wallet = NEW.wallet;
END;
