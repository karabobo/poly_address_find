CREATE TABLE IF NOT EXISTS wallet_history_planner_state (
    wallet TEXT PRIMARY KEY,
    level TEXT NOT NULL DEFAULT '',
    hard_risk_block INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    current_depth TEXT NOT NULL DEFAULT '',
    current_methodology_version TEXT NOT NULL DEFAULT '',
    methodology_stale INTEGER NOT NULL DEFAULT 0,
    current_pnl_methodology_version TEXT NOT NULL DEFAULT '',
    pnl_captured_at INTEGER NOT NULL DEFAULT 0,
    pnl_refresh_needed INTEGER NOT NULL DEFAULT 0,
    activity_refresh_needed INTEGER NOT NULL DEFAULT 0,
    research_score REAL NOT NULL DEFAULT 0,
    summary_updated_at INTEGER NOT NULL DEFAULT 0,
    sample_trade_count INTEGER NOT NULL DEFAULT 0,
    sample_volume_usdc REAL NOT NULL DEFAULT 0,
    sample_market_count INTEGER NOT NULL DEFAULT 0,
    target_depth TEXT NOT NULL DEFAULT '',
    refresh_lane TEXT NOT NULL DEFAULT '',
    urgency INTEGER NOT NULL DEFAULT 99,
    is_eligible INTEGER NOT NULL DEFAULT 0,
    next_refresh_at INTEGER NOT NULL DEFAULT 0,
    refreshed_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_history_planner_state_lane
    ON wallet_history_planner_state(
        target_depth, refresh_lane, is_eligible, urgency,
        research_score DESC, sample_market_count DESC, sample_volume_usdc DESC,
        sample_trade_count DESC, last_seen_at DESC, wallet
    );

CREATE INDEX IF NOT EXISTS idx_wallet_history_planner_state_rebuild
    ON wallet_history_planner_state(wallet, refreshed_at);

CREATE INDEX IF NOT EXISTS idx_wallet_history_planner_state_due
    ON wallet_history_planner_state(next_refresh_at, wallet)
    WHERE next_refresh_at > 0;

CREATE INDEX IF NOT EXISTS idx_wallet_levels_history_planner_bootstrap
    ON wallet_levels(level, wallet)
    WHERE level IN ('l2', 'l3', 'l4', 'l5', 'l6');

CREATE TABLE IF NOT EXISTS wallet_history_planner_dirty (
    wallet TEXT PRIMARY KEY,
    dirty_reason TEXT NOT NULL DEFAULT '',
    dirty_at INTEGER NOT NULL DEFAULT 0,
    dirty_generation INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_wallet_history_planner_dirty_due
    ON wallet_history_planner_dirty(dirty_at, wallet);

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_levels_insert
AFTER INSERT ON wallet_levels
WHEN NEW.wallet != ''
 AND NEW.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_levels', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_levels_update
AFTER UPDATE OF level, hard_risk_block, last_seen_at ON wallet_levels
WHEN NEW.wallet != ''
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

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_levels_delete
AFTER DELETE ON wallet_levels
WHEN OLD.wallet != ''
 AND OLD.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'wallet_levels_delete', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_screen_insert
AFTER INSERT ON wallet_screen_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_screen_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_screen_update
AFTER UPDATE OF sample_trade_count, sample_volume_usdc, sample_market_count ON wallet_screen_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_screen_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_screen_delete
AFTER DELETE ON wallet_screen_summaries
WHEN OLD.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = OLD.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = OLD.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'wallet_screen_summaries_delete', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_history_insert
AFTER INSERT ON wallet_history_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_history_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_history_update
AFTER UPDATE OF history_depth, methodology_version, research_score, updated_at ON wallet_history_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_history_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_history_delete
AFTER DELETE ON wallet_history_summaries
WHEN OLD.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = OLD.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = OLD.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'wallet_history_summaries_delete', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_pnl_insert
AFTER INSERT ON wallet_pnl_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_pnl_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_pnl_update
AFTER UPDATE OF methodology_version, captured_at, official_all_pnl_usdc ON wallet_pnl_summaries
WHEN NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'wallet_pnl_summaries', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_pnl_delete
AFTER DELETE ON wallet_pnl_summaries
WHEN OLD.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = OLD.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = OLD.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'wallet_pnl_summaries_delete', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_jobs_insert
AFTER INSERT ON pipeline_jobs
WHEN NEW.job_type = 'wallet_history_collect'
 AND NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'pipeline_jobs', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_jobs_update_new
AFTER UPDATE OF wallet, job_type, job_scope, job_action, status, attempts, max_attempts, next_attempt_at ON pipeline_jobs
WHEN NEW.job_type = 'wallet_history_collect'
 AND NEW.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = NEW.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = NEW.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (NEW.wallet, 'pipeline_jobs', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_jobs_update_old
AFTER UPDATE OF wallet, job_type, job_scope, job_action, status, attempts, max_attempts, next_attempt_at ON pipeline_jobs
WHEN OLD.job_type = 'wallet_history_collect'
 AND OLD.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = OLD.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = OLD.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'pipeline_jobs_old', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_jobs_delete
AFTER DELETE ON pipeline_jobs
WHEN OLD.job_type = 'wallet_history_collect'
 AND OLD.wallet != ''
 AND (
       EXISTS (
           SELECT 1 FROM wallet_levels
           WHERE wallet = OLD.wallet
             AND level IN ('l2', 'l3', 'l4', 'l5', 'l6')
       )
    OR EXISTS (
           SELECT 1 FROM wallet_history_planner_state
           WHERE wallet = OLD.wallet
       )
 )
BEGIN
    INSERT INTO wallet_history_planner_dirty(wallet, dirty_reason, dirty_at)
    VALUES (OLD.wallet, 'pipeline_jobs_delete', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;
