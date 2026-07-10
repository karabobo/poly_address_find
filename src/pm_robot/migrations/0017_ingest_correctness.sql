CREATE TABLE IF NOT EXISTS wallet_activity_watermarks (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    newest_timestamp INTEGER NOT NULL DEFAULT 0,
    newest_activity_key TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL,
    last_full_backfill_at INTEGER
);

INSERT OR IGNORE INTO wallet_activity_watermarks(
    address, newest_timestamp, newest_activity_key, updated_at, last_full_backfill_at
)
SELECT
    address,
    COALESCE(MAX(timestamp), 0) AS newest_timestamp,
    COALESCE(
        (
            SELECT activity_key
            FROM wallet_activity wa2
            WHERE wa2.address = wallet_activity.address
            ORDER BY timestamp DESC, activity_id DESC
            LIMIT 1
        ),
        ''
    ) AS newest_activity_key,
    strftime('%s','now') AS updated_at,
    NULL AS last_full_backfill_at
FROM wallet_activity
GROUP BY address;

DELETE FROM wallet_positions
WHERE position_id IN (
    SELECT position_id
    FROM (
        SELECT
            position_id,
            ROW_NUMBER() OVER (
                PARTITION BY address, asset_id
                ORDER BY captured_at DESC, position_id DESC
            ) AS duplicate_rank
        FROM wallet_positions
    )
    WHERE duplicate_rank > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_positions_address_asset
    ON wallet_positions(address, asset_id);
