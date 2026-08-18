CREATE INDEX IF NOT EXISTS idx_wallet_level_events_recent
    ON wallet_level_events(created_at DESC, event_id DESC);
