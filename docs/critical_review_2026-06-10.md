# Critical Review 2026-06-10

This is a critical audit of the current `pm_robot` framework as a production Polymarket copy-trading robot. The standard is not "does the code run"; the standard is whether the system can safely discover leaders, prove copyability, run paper trading, and later graduate to automated execution without data leakage or false confidence.

## Executive Verdict

The project has a useful skeleton: SQLite state, candidate stages, public API ingestors, copy-graph mining, paper order recording, safe deployment defaults, backups, maintenance, and systemd scheduling.

It is not yet a production trading robot. It is currently a research and monitoring pipeline with a ledger-only paper signal recorder. The largest gap is that the system still scores wallets mostly from indirect and partially stale features, while the paper ledger does not yet measure the realized economics of our own copy trades.

## Current Strengths

- Safe execution default: `PM_ROBOT_MODE=paper` and `PM_ROBOT_LIVE_ENABLED=false`; live broker is intentionally disabled.
- Production state is in SQLite, not CSV; CSV is now import/export only.
- Server deployment preserves DB/logs/backups and has timers for ingestion, scoring, Gamma cache, copy graph, backtest, paper, backup, and maintenance.
- Writer services use a shared `flock`, reducing transient SQLite writer collisions.
- Paper runner only accepts `paper_candidate`, `paper_approved`, and `live_eligible` stages, and ignores stale historical activity by default.
- API request logging and storage reporting exist, which helps detect Gamma/data API pressure and disk growth.

## Critical Findings

### P0: Paper ledger is not yet a paper portfolio

Evidence:
- `paper_orders` only stores one accepted signal row: wallet, market, asset, side, price, stake, route, reason, timestamp.
- `PaperBroker.submit` records the row and optional JSONL; it does not model open position, current mark, exit, settlement, slippage, rejected order state, or portfolio exposure.
- `paper_runner` consumes wallet activity rows, not a real order book execution snapshot.

Impact:
- You cannot yet use `paper_orders` to prove captured edge, drawdown, fill quality, exposure, or live eligibility.
- A wallet may look good externally while the bot's own executable copy stream is unprofitable.

Required fix:
- Add a paper portfolio lifecycle:
  - `paper_signals`
  - `paper_orders`
  - `paper_fills`
  - `paper_positions`
  - `paper_marks`
  - `paper_settlements`
  - `paper_performance_by_wallet`
- Record best bid/ask and executable price at signal time, not only the leader's reported activity price.
- Use paper results, not external wallet PnL, for `paper_approved` and `live_eligible`.

### P0: Copy backtest overstates copyability

Evidence:
- `copy_backtest.py` computes `leader_roi = realized_pnl_est / bought_usdc` from the leader's whole episode.
- It then applies that ROI to our fixed stake, subtracting fixed friction.
- It does not calculate whether our delayed entry price was worse, whether size was available, whether the market was still active, or whether the leader's later DCA/exit behavior can be replicated.

Impact:
- The backtest can make a wallet appear copyable when the profit came from early entry, private timing, or non-replicable later sizing.
- This directly conflicts with the project goal of building an automated copy bot.

Required fix:
- Replace episode-level ROI proxy with copy-stream simulation:
  - signal timestamp;
  - target order book at signal time;
  - executable bid/ask depth;
  - delayed entry price;
  - mark/exit/settlement rule;
  - realized paper PnL after slippage and fees.

### P0: Risk gates are mostly placeholders

Evidence:
- `risk/gates.py` blocks only string statuses (`wash`, `routing_operator`, etc.), maker fraction, and `net_to_gross_exposure`.
- There is no implemented on-chain maker/taker classifier, wash trading detector, hedge-pair detector, negative-risk bundle detector, sybil/entity clustering, or market-maker routing filter.

Impact:
- A wallet can pass scoring because the evidence is missing, not because the risk is clean.
- This is especially dangerous for Polymarket, where hedged/negative-risk/maker strategies may be profitable but not single-leg copyable.

Required fix:
- Treat missing hygiene/copyability evidence as incomplete, not clean.
- Add explicit feature provenance and freshness.
- Build detectors for:
  - both-side exposure by condition;
  - net-to-gross by market group;
  - negative-risk bundles;
  - high maker/taker or routing-like behavior;
  - repeated self-similar counterparty/wash patterns;
  - cluster/entity duplication.

### P1: Scoring can promote with incomplete validation

Evidence:
- `_stage` returns `paper_candidate` when score exceeds the threshold even if edge retention and walk-forward consistency are missing.
- `_components` gives default execution copyability value `0.5` when `edge_retention_pct` is absent.
- Required score components in policy include fields that are often absent, but scoring does not require them before high stages.

Impact:
- Missing evidence becomes a neutral or mildly positive assumption.
- Paper stage is protected by requiring high score, but the score can still be inflated by external Polydata features plus incomplete copyability features.

Required fix:
- Split stages:
  - `needs_data`: missing required raw data;
  - `research_candidate`: external score only;
  - `paper_candidate`: copy graph plus hygiene evidence present;
  - `paper_approved`: enough closed paper trades after friction;
  - `live_eligible`: paper PnL, drawdown, exposure, and kill-switch gates pass.
- Score should have evidence completeness gates before score thresholds.

### P1: Copy graph uses timestamp proxy instead of block-order evidence

Evidence:
- `copy_graph.py` explicitly states that public rows do not carry block numbers, so it uses `max_copy_lag_seconds`.
- Leader/follower links match same `asset_id` and side within a seconds window.

Impact:
- False positives are likely during active markets where many wallets buy the same token in the same short time window.
- False negatives are also possible if data API timestamp precision is coarse or delayed.

Required fix:
- Add chain transaction metadata where available:
  - tx hash;
  - block number;
  - log index/order;
  - taker/maker info;
  - CTF exchange event type.
- Use the paper's one-block rule when the data source supports it.

### P1: Episode reconstruction underestimates resolution outcomes

Evidence:
- `rebuild_wallet_episodes` marks an episode closed only when buys and sells net out.
- It estimates realized PnL from sell proceeds minus average cost.
- A resolution holder that buys and holds to settlement remains `open` unless there is a sell-like activity row.

Impact:
- The smart-money paper's strongest behavior, resolution-edge holding, is exactly the behavior this reconstruction cannot fully settle.
- Event win rate, resolution holding quality, and copy backtest results can be materially wrong.

Required fix:
- Join Gamma/market resolution and token outcome data.
- Mark episodes closed on market resolution even without sells.
- Compute settlement PnL for held shares.

### P1: Feature merging can preserve stale values

Evidence:
- `upsert_wallet_feature` uses `COALESCE(excluded.value, wallet_features.value)` for most numeric fields.
- Modules that intend to clear invalid/obsolete values must manually `UPDATE ... SET field = NULL` first.

Impact:
- Future ingestors can accidentally keep stale metrics when a newer calculation has no valid result.
- This is risky for `copy_stream_roi`, hygiene, edge retention, and walk-forward fields.

Required fix:
- Add update modes:
  - merge-missing for import enrichment;
  - replace-owned-fields for computed feature families;
  - clear-owned-fields when a computation has no valid output.

### P1: Systemd schedule is operationally safe but not strategically coherent

Evidence:
- Paper loop runs every minute.
- Activity ingestion runs every 15 minutes for only 5 wallets and up to 1000 events each.
- Copy graph/backtest/scoring run every 15 minutes.

Impact:
- Paper may run much more frequently than fresh wallet activity arrives.
- Scoring may repeatedly process mostly stale features.
- A newly promoted wallet may not get timely activity ingestion unless it is selected by target ordering.

Required fix:
- Split schedules into:
  - fast watchlist/paper wallets;
  - slow candidate backfill;
  - heavy research recomputation;
  - daily maintenance.
- Add per-stage ingestion priority.

### P2: Health checks verify availability, not trading readiness

Evidence:
- `ops.health_check` validates DB, directories, mode guard, and counts.
- It does not assert data freshness thresholds, API error budgets, stalled timers, stale paper ledger, low disk trajectory, or failed promotion invariants.

Impact:
- `ok: true` can hide that the system is not finding paper candidates, Gamma cache is low coverage, or backtests are producing no trades.

Required fix:
- Add readiness metrics:
  - activity freshness by stage;
  - Gamma cache coverage target;
  - copy graph qualified pair count trend;
  - paper orders per day;
  - API error budget;
  - DB growth projection;
  - systemd failed units.

### P2: Deployment is VPS-local and lacks off-host durability

Evidence:
- SQLite and backups live under `/opt/pm-robot` on the same VPS.
- Backup retention exists, but no off-host copy is configured.

Impact:
- VPS disk loss or account failure loses research state and paper evidence.

Required fix:
- Add daily off-host backup to Supabase Storage, S3-compatible storage, or a private GitHub release/artifact path.
- Keep SQLite local for runtime latency; offload compressed backups and reports.

## Recommended Repair Order

1. Paper portfolio schema and lifecycle.
2. CLOB order book snapshot at signal time.
3. Paper performance aggregation by copied wallet.
4. Evidence-completeness gates before `paper_candidate`.
5. Resolution-aware episode settlement.
6. Real copy-stream backtest using entry/exit prices, not leader episode ROI.
7. Hedge/wash/maker-taker detectors.
8. Stage-aware scheduler.
9. Health/readiness metrics.
10. Off-host backups.

## Current Server Snapshot

Latest observed server state after deployment:

```text
mode: paper
systemd failed units: 0
paper_orders: 0
candidate_wallets: 118
wallet_features: 90
wallet_positions: 1754
wallet_activity: 26047
wallet_episodes: 2429
copy_trade_links: 4
copy_pair_stats: 4
copy_backtest_trades: 0
gamma_market_cache: 55
db_size_mb: 80.27
free_disk_gb: 23.16
```

The zero `paper_orders` count is correct because no wallets are currently in `paper_candidate`, `paper_approved`, or `live_eligible`.
