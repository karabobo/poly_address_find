BEGIN IMMEDIATE;

ALTER TABLE wallet_activity_watermarks
    ADD COLUMN activity_count INTEGER NOT NULL DEFAULT 0;

COMMIT;
