CREATE TEMP TABLE migration_0047_trade_counts (
    address TEXT PRIMARY KEY,
    trade_count INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO migration_0047_trade_counts(address, trade_count)
SELECT address, COUNT(*)
FROM wallet_activity INDEXED BY idx_wallet_activity_trade_address_time
WHERE type = 'TRADE'
GROUP BY address;

INSERT OR IGNORE INTO wallet_activity_watermarks(
    address, newest_timestamp, newest_activity_key, updated_at,
    last_full_backfill_at, trade_count
)
SELECT
    candidate.address,
    COALESCE(
        (
            SELECT wa.timestamp
            FROM wallet_activity wa INDEXED BY idx_wallet_activity_address_time
            WHERE wa.address = candidate.address
            ORDER BY wa.timestamp DESC, wa.activity_id DESC
            LIMIT 1
        ),
        0
    ),
    COALESCE(
        (
            SELECT wa.activity_key
            FROM wallet_activity wa INDEXED BY idx_wallet_activity_address_time
            WHERE wa.address = candidate.address
            ORDER BY wa.timestamp DESC, wa.activity_id DESC
            LIMIT 1
        ),
        ''
    ),
    CAST(strftime('%s', 'now') AS INTEGER),
    NULL,
    COALESCE(
        (
            SELECT counts.trade_count
            FROM migration_0047_trade_counts counts
            WHERE counts.address = candidate.address
        ),
        0
    )
FROM candidate_wallets candidate
WHERE NOT EXISTS (
    SELECT 1
    FROM wallet_activity_watermarks watermarks
    WHERE watermarks.address = candidate.address
);

UPDATE wallet_activity_watermarks
SET trade_count = COALESCE(
    (
        SELECT counts.trade_count
        FROM migration_0047_trade_counts counts
        WHERE counts.address = wallet_activity_watermarks.address
    ),
    0
);

DROP TABLE migration_0047_trade_counts;
