ALTER TABLE wallet_activity ADD COLUMN activity_key TEXT;

UPDATE wallet_activity
SET activity_key =
    COALESCE(LOWER(transaction_hash), '') || '|' ||
    COALESCE(CAST(timestamp AS TEXT), '') || '|' ||
    COALESCE(condition_id, '') || '|' ||
    COALESCE(event_slug, '') || '|' ||
    COALESCE(market_slug, '') || '|' ||
    COALESCE(asset_id, '') || '|' ||
    COALESCE(outcome, '') || '|' ||
    COALESCE(type, '') || '|' ||
    COALESCE(side, '') || '|' ||
    COALESCE(CAST(price AS TEXT), '') || '|' ||
    COALESCE(CAST(size AS TEXT), '') || '|' ||
    COALESCE(CAST(usdc_size AS TEXT), '')
WHERE activity_key IS NULL;

DELETE FROM wallet_activity
WHERE activity_id IN (
    SELECT activity_id
    FROM (
        SELECT
            activity_id,
            ROW_NUMBER() OVER (
                PARTITION BY address, activity_key
                ORDER BY activity_id ASC
            ) AS duplicate_rank
        FROM wallet_activity
    )
    WHERE duplicate_rank > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_activity_address_key
    ON wallet_activity(address, activity_key)
    WHERE activity_key IS NOT NULL;
