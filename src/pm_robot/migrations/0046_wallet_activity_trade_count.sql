ALTER TABLE wallet_activity_watermarks
    ADD COLUMN trade_count INTEGER NOT NULL DEFAULT 0;
