-- v3 makes the compact L1 screen the only route into L2 history collection.
-- Freeze legacy L2 planner rows until a new v3 screen marks them eligible.
UPDATE wallet_history_planner_state
SET is_eligible = 0,
    refresh_lane = '',
    next_refresh_at = 0,
    refreshed_at = CAST(strftime('%s', 'now') AS INTEGER)
WHERE level = 'l2';

-- A policy-only screen rewrite must refresh planner state even if metrics match.
CREATE TRIGGER IF NOT EXISTS trg_wallet_history_planner_dirty_screen_policy_update
AFTER UPDATE OF screen_complete, screen_qualified, source_snapshot_json
ON wallet_screen_summaries
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
    VALUES (NEW.wallet, 'wallet_screen_policy', CAST(strftime('%s', 'now') AS INTEGER))
    ON CONFLICT(wallet) DO UPDATE SET
        dirty_reason = excluded.dirty_reason,
        dirty_at = excluded.dirty_at,
        dirty_generation = wallet_history_planner_dirty.dirty_generation + 1;
END;
