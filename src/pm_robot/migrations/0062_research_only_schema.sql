-- Collapse the historical all-in-one schema into the wallet-research control plane.
-- Raw wallet history lives in Parquet; SQLite retains only compact state and metadata.
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TABLE candidate_wallets_next (
    address TEXT PRIMARY KEY,
    sources TEXT NOT NULL DEFAULT '',
    labels TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    links TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    first_seen_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO candidate_wallets_next(
    address, sources, labels, notes, links, status, first_seen_at, updated_at
)
SELECT
    address, sources, labels, notes, links, status, first_seen_at, updated_at
FROM candidate_wallets;

CREATE TEMP TABLE wallet_features_keep AS
SELECT
    address,
    cumulative_win_rate,
    recent_30d_volume_usdc,
    net_pnl_usdc,
    total_volume_usdc,
    event_win_rate,
    trade_win_rate,
    avg_dca_entries,
    sell_pct,
    bot_score,
    trades_per_day,
    median_gap_sec,
    survival_score,
    single_market_pnl_share,
    hygiene_status,
    primary_category,
    last_active_days_ago,
    extra_json,
    updated_at
FROM wallet_features;

CREATE TABLE pipeline_jobs_next (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    wallet TEXT NOT NULL DEFAULT '',
    job_action TEXT NOT NULL DEFAULT '',
    job_scope TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 100,
    shard INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    lease_owner TEXT,
    lease_until INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    input_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT NOT NULL DEFAULT '{}',
    last_error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    completed_at INTEGER,
    UNIQUE(job_type, wallet, job_scope, job_action)
);

INSERT INTO pipeline_jobs_next(
    job_id, job_type, wallet, job_action, job_scope, priority, shard, status,
    lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
    input_json, output_json, last_error, created_at, updated_at, completed_at
)
SELECT
    job_id, job_type, wallet, subject_key, tier, priority, shard, status,
    lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
    input_json, output_json, last_error, created_at, updated_at, completed_at
FROM pipeline_jobs
WHERE job_type IN ('wallet_recent_screen', 'wallet_history_collect');

CREATE TABLE runtime_heartbeats_next (
    heartbeat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    rows_written INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);

INSERT INTO runtime_heartbeats_next(
    heartbeat_id, name, started_at, finished_at, status, rows_written, error
)
SELECT
    run_id, ingest_type, started_at, COALESCE(finished_at, started_at),
    status, rows_written, error
FROM ingest_runs
WHERE ingest_type IN (
    'loop_discovery_activity',
    'loop_discovery_leaderboard',
    'loop_maintenance',
    'loop_rtds_discovery',
    'loop_wallet_history',
    'loop_wallet_level_control',
    'loop_wallet_screen'
);

DROP TRIGGER IF EXISTS trg_candidate_source_wallet_first_after_insert;
DROP TRIGGER IF EXISTS trg_candidate_source_wallet_latest_after_delete;
DROP TRIGGER IF EXISTS trg_candidate_source_wallet_latest_after_insert;
DROP TRIGGER IF EXISTS trg_candidate_source_wallet_latest_after_update;
DROP TRIGGER IF EXISTS trg_leader_scores_latest_after_delete;
DROP TRIGGER IF EXISTS trg_leader_scores_latest_after_insert;
DROP TRIGGER IF EXISTS trg_leader_scores_latest_after_update;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_budget_delete;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_budget_update;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_budget_upsert;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_candidate_delete;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_candidate_update;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_candidate_upsert;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_processing_delete;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_processing_update;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_processing_upsert;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_score_delete;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_score_update;
DROP TRIGGER IF EXISTS trg_wallet_dashboard_snapshot_score_upsert;

DROP TABLE IF EXISTS candidate_decision_state;
DROP TABLE IF EXISTS candidate_source_wallet_first;
DROP TABLE IF EXISTS candidate_source_wallet_latest;
DROP TABLE IF EXISTS copy_backtest_trades;
DROP TABLE IF EXISTS copy_leader_performance;
DROP TABLE IF EXISTS copy_leader_stats;
DROP TABLE IF EXISTS copy_pair_stats;
DROP TABLE IF EXISTS copy_trade_links;
DROP TABLE IF EXISTS data_artifacts;
DROP TABLE IF EXISTS evidence_archive_files;
DROP TABLE IF EXISTS evidence_archive_runs;
DROP TABLE IF EXISTS evidence_archive_scope;
DROP TABLE IF EXISTS evidence_archive_wallets;
DROP TABLE IF EXISTS evidence_backfill_budget;
DROP TABLE IF EXISTS evidence_backfill_jobs;
DROP TABLE IF EXISTS gamma_market_cache;
DROP TABLE IF EXISTS leader_latest_scores;
DROP TABLE IF EXISTS leader_publish;
DROP TABLE IF EXISTS leader_scores;
DROP TABLE IF EXISTS paper_fills;
DROP TABLE IF EXISTS paper_marks;
DROP TABLE IF EXISTS paper_observer_trials;
DROP TABLE IF EXISTS paper_orders;
DROP TABLE IF EXISTS paper_positions;
DROP TABLE IF EXISTS paper_readiness_observations;
DROP TABLE IF EXISTS paper_settlements;
DROP TABLE IF EXISTS paper_signal_evaluations;
DROP TABLE IF EXISTS paper_wallet_performance;
DROP TABLE IF EXISTS paper_wallet_quality;
DROP TABLE IF EXISTS pipeline_scheduler_state;
DROP TABLE IF EXISTS retention_cycle_state;
DROP TABLE IF EXISTS review_events;
DROP TABLE IF EXISTS wallet_activity;
DROP TABLE IF EXISTS wallet_activity_watermarks;
DROP TABLE IF EXISTS wallet_dashboard_snapshot;
DROP TABLE IF EXISTS wallet_episodes;
DROP TABLE IF EXISTS wallet_evidence_summary;
DROP TABLE IF EXISTS wallet_positions;
DROP TABLE IF EXISTS wallet_processing_state;
DROP TABLE IF EXISTS wallet_registry;
DROP TABLE IF EXISTS wallet_trade_role_evidence;
DROP TABLE IF EXISTS wallet_validation_summaries;

DROP TABLE pipeline_jobs;
ALTER TABLE pipeline_jobs_next RENAME TO pipeline_jobs;
CREATE INDEX idx_pipeline_jobs_claim
    ON pipeline_jobs(status, shard, next_attempt_at, priority, updated_at);
CREATE INDEX idx_pipeline_jobs_wallet
    ON pipeline_jobs(wallet, job_type, status);

DROP TABLE ingest_runs;
ALTER TABLE runtime_heartbeats_next RENAME TO runtime_heartbeats;
CREATE INDEX idx_runtime_heartbeats_name_time
    ON runtime_heartbeats(name, finished_at DESC, heartbeat_id DESC);

DROP TABLE wallet_features;
DROP TABLE candidate_wallets;
ALTER TABLE candidate_wallets_next RENAME TO candidate_wallets;

CREATE TABLE wallet_features (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    cumulative_win_rate REAL,
    recent_30d_volume_usdc REAL,
    net_pnl_usdc REAL,
    total_volume_usdc REAL,
    event_win_rate REAL,
    trade_win_rate REAL,
    avg_dca_entries REAL,
    sell_pct REAL,
    bot_score REAL,
    trades_per_day REAL,
    median_gap_sec REAL,
    survival_score REAL,
    single_market_pnl_share REAL,
    hygiene_status TEXT NOT NULL DEFAULT '',
    primary_category TEXT NOT NULL DEFAULT '',
    last_active_days_ago REAL,
    extra_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);

INSERT INTO wallet_features(
    address, cumulative_win_rate, recent_30d_volume_usdc, net_pnl_usdc,
    total_volume_usdc, event_win_rate, trade_win_rate, avg_dca_entries,
    sell_pct, bot_score, trades_per_day, median_gap_sec, survival_score,
    single_market_pnl_share, hygiene_status, primary_category,
    last_active_days_ago, extra_json, updated_at
)
SELECT
    address, cumulative_win_rate, recent_30d_volume_usdc, net_pnl_usdc,
    total_volume_usdc, event_win_rate, trade_win_rate, avg_dca_entries,
    sell_pct, bot_score, trades_per_day, median_gap_sec, survival_score,
    single_market_pnl_share, hygiene_status, primary_category,
    last_active_days_ago, extra_json, updated_at
FROM wallet_features_keep;

DROP TABLE wallet_features_keep;
COMMIT;
PRAGMA foreign_keys = ON;
