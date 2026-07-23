-- Repair legacy candidates that predate the single wallet-sighting ingress.
-- Current code creates the observation and L0/L1 level before deeper work.
BEGIN IMMEDIATE;

INSERT OR IGNORE INTO wallet_levels(
    wallet, level, level_reason, first_seen_at, last_seen_at,
    level_updated_at, updated_at
)
SELECT
    wallet, 'l0', 'legacy_sighting_invariant_repair', first_seen_at, updated_at,
    first_seen_at, updated_at
FROM observed_wallets;

INSERT OR IGNORE INTO observed_wallets(
    wallet, sources, labels, notes, links, status,
    observed_trade_count, recent_trade_count, recent_usdc_total,
    recent_max_trade_usdc, recent_trades_json, promoted_at,
    promotion_reason, first_seen_at, updated_at
)
SELECT
    address, sources, labels, notes, links, status,
    0, 0, 0, 0, '[]', first_seen_at,
    'legacy_candidate_ingress_repair', first_seen_at, updated_at
FROM candidate_wallets;

UPDATE observed_wallets
SET promoted_at = COALESCE(
        promoted_at,
        (SELECT candidate.first_seen_at
         FROM candidate_wallets AS candidate
         WHERE candidate.address = observed_wallets.wallet)
    ),
    promotion_reason = CASE
        WHEN promotion_reason = '' THEN 'legacy_candidate_ingress_repair'
        ELSE promotion_reason
    END
WHERE EXISTS (
    SELECT 1
    FROM candidate_wallets AS candidate
    WHERE candidate.address = observed_wallets.wallet
);

INSERT OR IGNORE INTO wallet_levels(
    wallet, level, level_reason, first_seen_at, last_seen_at,
    level_updated_at, updated_at
)
SELECT
    address, 'l1', 'legacy_candidate_invariant_repair', first_seen_at, updated_at,
    updated_at, updated_at
FROM candidate_wallets;

COMMIT;
