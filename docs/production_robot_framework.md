# Wallet Research Framework

This is the research-only architecture after splitting wallet discovery from live copy-trading execution.

## Module Boundaries

```text
src/pm_robot/
  clients/        Polymarket public data clients
  research/       wallet discovery, leader scoring, copy-graph analysis
  risk/           hygiene, hedge/arbitrage, drawdown, paper-readiness gates
  execution/      paper-evaluation ledger only
  orchestration/  pipelines that connect research -> risk -> paper evaluation
```

## Source Roles

From `polywhale`, the new project borrows the module ideas:

- `whale_discovery`: leaderboard candidate discovery;
- `whale_vet`: historical episode quality gate;
- `whale_review`: review by our captured copy PnL;
- `friction_observer`: paper-to-real edge retention;
- `walk_forward`: train/test validation;
- `whale_sizing`: fractional Kelly with shrinkage and caps;
- `ws_dispatcher`: WebSocket-triggered targeted resnapshot.

From `nautilus-trading`, the research project keeps only non-live ideas:

- crash guards and process isolation;
- dashboard/service deployment shape;
- richer paper validation and reporting ideas.

From the papers, the new project adds:

- copy-leader graph evidence;
- routing/wash/market-maker taker hygiene;
- event win rate vs trade win rate gap;
- DCA and low-sell resolution-holder scoring;
- anti-hedge and anti-arbitrage copyability gates.

## Promotion Funnel

```text
imported address
  -> needs_data
  -> scored candidate
      -> needs_data
      -> needs_manual_review
      -> paper_candidate -> paper_approved -> live_eligible
      -> blocked_hygiene / blocked_copyability / rejected
```

`needs_manual_review` is a candidate stage, not a processing module. It is the
conservative holding state after scoring when a wallet is interesting enough to
keep, but still lacks score, hygiene, copyability, or evidence strength for
paper eligibility. Automated evidence workers may keep improving wallets in this
stage, but the stage itself must never grant paper access. A strong wallet can
skip this holding state and move directly from scoring to `paper_candidate` when
all paper gates pass.

`paper_candidate` requires both a high score and validated copyability evidence.
Raw follower links, default-zero copy ROI, or unverified copy-candidate pairs are
discovery signals only; they can keep a wallet in review, but they must not grant
copyability or paper eligibility credit.

`live_eligible` is a research output meaning "publishable to a separate execution system." This project does not place or prepare real orders.

## Current Implementation

Implemented now:

- unified domain models in `src/pm_robot/models.py`;
- policy loading in `src/pm_robot/config.py`;
- SQLite state database with migrations in `src/pm_robot/storage/`;
- rate-limited HTTP client, retry, and API request logging in `src/pm_robot/clients/http.py`;
- candidate and wallet-feature CSV loading as import/export adapters in `src/pm_robot/io.py`;
- Polydata JSON feature ingestion in `src/pm_robot/research/polydata_features.py`;
- Polymarket positions ingestion in `src/pm_robot/orchestration/positions_ingestor.py`;
- Polymarket activity ingestion and episode reconstruction in `src/pm_robot/orchestration/activity_ingestor.py`;
- copy-leader graph mining in `src/pm_robot/research/copy_graph.py`;
- copy-stream ROI backtesting in `src/pm_robot/research/copy_backtest.py`;
- paper-driven leader scoring in `src/pm_robot/research/scoring.py`;
- hygiene and hedge gates in `src/pm_robot/risk/gates.py`;
- review queue orchestration in `src/pm_robot/orchestration/review_pipeline.py`;
- auditable paper-evaluation ledger;
- CLI: `migrate`, `import-addresses`, `import-features`, `import-polydata`, `build-review`.
- CLI: `ingest-positions`, `ingest-activity`, `activity-coverage`, `mine-copy-graph`, `backtest-copy-stream`, `paper-run`, `paper-settle`, `paper-readiness`, `health`, `status`, `backup`.

Current state database:

```text
data/pm_robot.sqlite
```

CSV files are now only import sources or exported reports.

## Next Implementation Blocks

1. Feature store ingestors:
   - Gamma metadata cache;
   - public liquidity snapshots for research;
   - on-chain maker/taker and wash flags.

2. Copy graph:
   - replace timestamp-window lag proxy with block-number lag when available;
   - replace closed-episode ROI proxy with settlement-aware copied-stream ROI;
   - add longer historical backfill on production data.

3. Historical episode reconstruction:
   - DCA entries;
   - sell rate;
   - event-level and trade-level win rates;
   - resolution-holder classification.

4. Research publication boundary:
   - define a small `leader_publish` export table or report;
   - include wallet, stage, paper quality, blockers, source evidence, and expiry;
   - keep real-time order execution in a separate project.
