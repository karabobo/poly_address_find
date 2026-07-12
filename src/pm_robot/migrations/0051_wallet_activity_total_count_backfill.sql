BEGIN IMMEDIATE;

DROP TABLE IF EXISTS migration_0051_activity_counts;

CREATE TEMP TABLE migration_0051_activity_counts (
    address TEXT PRIMARY KEY,
    activity_count INTEGER NOT NULL,
    newest_timestamp INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO migration_0051_activity_counts(
    address,
    activity_count,
    newest_timestamp
)
SELECT
    address,
    COUNT(*),
    COALESCE(MAX(timestamp), 0)
FROM wallet_activity INDEXED BY idx_wallet_activity_address_time
GROUP BY address;

INSERT OR IGNORE INTO wallet_activity_watermarks(
    address,
    newest_timestamp,
    newest_activity_key,
    updated_at,
    last_full_backfill_at,
    trade_count,
    activity_count
)
SELECT
    counts.address,
    counts.newest_timestamp,
    COALESCE(
        (
            SELECT activity.activity_key
            FROM wallet_activity activity INDEXED BY idx_wallet_activity_address_time
            WHERE activity.address = counts.address
            ORDER BY activity.timestamp DESC, activity.activity_id DESC
            LIMIT 1
        ),
        ''
    ),
    CAST(strftime('%s', 'now') AS INTEGER),
    NULL,
    (
        SELECT COUNT(*)
        FROM wallet_activity trades INDEXED BY idx_wallet_activity_trade_address_time
        WHERE trades.type = 'TRADE'
          AND trades.address = counts.address
    ),
    counts.activity_count
FROM migration_0051_activity_counts counts
JOIN candidate_wallets candidate
  ON candidate.address = counts.address;

UPDATE wallet_activity_watermarks
SET activity_count = COALESCE(
    (
        SELECT counts.activity_count
        FROM migration_0051_activity_counts counts
        WHERE counts.address = wallet_activity_watermarks.address
    ),
    0
);

DROP TABLE migration_0051_activity_counts;

COMMIT;
