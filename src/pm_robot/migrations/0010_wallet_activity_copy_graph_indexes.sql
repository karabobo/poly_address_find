CREATE INDEX IF NOT EXISTS idx_wallet_activity_copy_match
    ON wallet_activity(type, asset_id, side, timestamp);

CREATE INDEX IF NOT EXISTS idx_wallet_activity_trade_address_time
    ON wallet_activity(type, address, timestamp);
