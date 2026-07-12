# Wallet Research Pipeline Architecture

This document is the canonical architecture reference for the repository. The default NAS deployment is
`research/scoring`: it discovers wallets, builds evidence, scores candidates, and observes paper-stage signals.
It does not submit real orders or start the opt-in execution profile.

## End-to-End Flow

```text
leaderboards / large trades / RTDS / curated imports
  -> observed_wallets
  -> promotion gate
  -> candidate_wallets
  -> wallet_processing_state (L0-L3 evidence truth)
  -> pipeline_jobs[job_type=wallet_evidence_backfill]
  -> wallet_activity + wallet_positions + wallet_evidence_summary
  -> wallet_features
  -> leader_scores
  -> candidate_wallets.candidate_stage
  -> wallet_registry (durable decision summary)
  -> verified Parquet archive before low-value raw evidence leaves SQLite
  -> paper observer / read-only handoff
  -> leader_publish for explicit downstream consumption
```

Copyability is a separate evidence lane:

```text
promising scored wallet
  -> pipeline_jobs[job_type=copyability_evidence]
  -> copy graph + copied-stream backtest
  -> wallet feature refresh
  -> score refresh
```

## State Ownership

| Store | Responsibility | Must not be used as |
| --- | --- | --- |
| `observed_wallets` | Cheap recent sightings and promotion inputs | A scored candidate registry |
| `candidate_wallets` | Canonical wallet registry and current `candidate_stage` | L1/L2/L3 evidence state |
| `wallet_processing_state` | Current evidence tier, evidence status, priority, and next action | A worker queue |
| `pipeline_jobs` | Leased execution tasks, retries, errors, and dedupe scope | Wallet evidence truth |
| `wallet_evidence_summary` | Materialized historical evidence summary | Candidate-stage authority |
| `wallet_features` | Current scoring inputs | Score history |
| `leader_scores` | Append-only scoring/review decisions | Worker execution state |
| `wallet_registry` | Durable wallet decision, retention policy, and archive pointer | Raw event storage |
| `evidence_archive_*` | Archive run, wallet, file, checksum, and recovery state | Candidate or queue authority |
| Parquet files | Compressed cold evidence removed from the SQLite hot store | Mutable workflow state |
| `leader_publish` | Explicit read-only output for downstream consumers | Permission to trade from this repository |

The historical database column `wallet_processing_state.discovery_tier` is called `evidence_tier` in code.
`pipeline_jobs.tier` is a job scope/dedupe field, not the wallet's real evidence tier.

The NAS research/scoring stack has one control-plane owner: `research-control-loop.sh`. It executes the ordered
`pipeline-cycle` handoff across eligibility preparation, stale wallet state, wallet/copyability queue admission,
feature materialization, incremental scoring, and paper handoff export. Discovery and queue workers remain
asynchronous. The control loop materializes only wallets with changed candidate metadata, evidence budgets, or
activity watermarks.
Each database phase commits independently. In NAS mode, a failed phase is rolled back and recorded, while later
phases continue against the latest committed data. Paper handoff export runs after every cycle attempt, including
partial cycles, so planner contention cannot make an otherwise valid handoff stale.
The frequent control loop skips full smoothness diagnostics; those scans remain available through the explicit
audit/report commands instead of being repeated during every scheduling pass.
The NAS control loop uses a short SQLite busy timeout and a bounded planner retry budget. Lock contention therefore
fails one isolated phase and yields to the next control pass instead of blocking workers for minutes. Every phase
records its own start time, finish time, result count, and error; the system-health panel shows the latest six phase
heartbeats only after phase data exists.
An executing `pipeline-cycle` holds the shared control-plane priority lock for its complete ordered pass. Retention
takes that lock only around one bounded prune batch and releases it before the inter-batch delay. If research-control
already owns the lock, retention reports `yielded_to_research` and retries later instead of competing for SQLite's
single writer slot. Direct `prune-evidence --execute` follows the same lock order.
Wallet and copyability planners run candidate selection and copyability priority calculation before opening their
short queue-admission write transaction. Under the write lock they recheck mutable eligibility, exact-scope dedupe,
retry cooldown, and active-queue capacity before enqueueing.
Eligibility repair only prepares evidence budgets and planner-ready actions; it never writes `pipeline_jobs`.
Queue admission remains delegated to the canonical wallet and copyability planners, and unchanged repair budgets
are not rewritten on every control pass.
The files under `deploy/systemd` describe the older non-NAS deployment and do not share this scheduler. Do not run
those timers beside the NAS Compose stack. The NAS Compose control loop is the supported scheduling architecture
for this repository's current research/scoring deployment.

## Evidence Tiers

- `l0_discovered`: candidate registered, no useful history yet.
- `l1_light`: light activity history for a fast continue/stop decision.
- `l2_medium`: enough history to assess market diversity, fast-market concentration, and strategy shape.
- `l3_deep`: deep evidence suitable for scoring, paper-stage observation, and publication review.

There is no L4. `candidate_stage` is a separate scoring/research lifecycle and must not be used to infer an
evidence tier.

## Candidate Stages

- `needs_data`: evidence or scoring inputs are incomplete.
- `needs_manual_review`: conservative holding stage; automated evidence work may continue.
- `paper_candidate`: research gates passed for paper-stage observation.
- `paper_approved`: paper evidence passed the configured policy.
- `live_eligible`: publishable research label for a separate execution system.
- `blocked_hygiene`, `blocked_copyability`, `rejected`: explicit blocking outcomes.

Missing maker/taker evidence is not a hard gate in the current research pipeline. Reliable role evidence may
adjust risk, but role inference remains a future enhancement. Hygiene uses conservative risk signals and can
block only when configured evidence is strong enough.

## NAS Runtime

The default Compose stack runs:

- proxy tunnel and web console;
- polling and RTDS discovery;
- one ordered research control loop for eligibility, queue admission, features, scoring, and handoff export;
- sharded wallet and copyability workers;
- the read-only paper-observer loop;
- lightweight maintenance with bounded, archive-aware evidence pruning.

Full SQLite backups are manual in the current development phase. The default research stack does not start the
backup loop. Cold Parquet evidence is not a database backup: it preserves reusable historical rows while SQLite
remains the only workflow truth source.

The opt-in execution profile contains paper-run, paper-settle, and publish loops. It is not started by the
default `up`, `restart`, `runtime-ensure`, or watchdog commands.

## Reliability Rules

- Queue claims use SQLite write serialization and leases.
- Evidence pruning is a state machine: wallets are frozen, jobs are closed, Parquet files are written to partial
  paths, row counts and SHA-256 checksums are verified, manifests are atomically promoted, and only then are raw
  SQLite rows deleted. Failed exports keep the SQLite rows and resume the same archive run on the next pass.
- Archive batches are written per table rather than per wallet to avoid a NAS tiny-file explosion. The default
  batch remains five wallets per hour.
- `parquet-wallet://<address>` is the stable wallet-level archive locator. It resolves every verified, partial,
  and completed archive run for that wallet; an individual archive run id is audit metadata, not a restore pointer.
- Workers renew leases around long work and can only complete or retry jobs they still own.
- Maintenance requeues expired leases and stale runtime records.
- Lightweight maintenance retains `loop_*` runtime heartbeats for 30 days by default even when broad database
  cleanup is skipped. It does not delete worker audit runs or wallet evidence in that path.
- Maintenance marks expired or queued jobs failed once their attempt budget is exhausted, so unclaimable jobs
  cannot occupy planner queue capacity indefinitely. Failed jobs respect `next_attempt_at`; after the cooldown,
  planners may reopen them with a fresh attempt budget while retaining the previous error for diagnosis.
- Retention catch-up is bounded. Maintenance runs up to four short passes only when newly classified eligible raw
  rows outpace completed deletion or when retention yielded to research-control; every batch still releases the
  control lock so research remains the priority.
- Planner backpressure limits queued/running wallet evidence and copyability jobs. Copyability planning keeps
  its per-pass batch limit separate from the active-queue waterline and only fills currently available slots.
- Research control keeps feature and scoring transactions bounded. A cycle that fills either batch schedules
  the next cycle on the shorter active interval; idle, failed, or malformed summaries retain the conservative
  interval so backlog drains without turning the control plane into a tight loop.
- When queue capacity is tight, the planner allocates slots across light, medium, and deep evidence stages by
  configured weight, current active-job share, and a persistent smooth weighted round-robin cursor. Priority
  ordering remains intact within each stage, while fully drained planner cycles cannot reset stage fairness.
- Wallet and copyability queue capacity checks and job admission run in one SQLite write transaction, so
  concurrent planners cannot reserve the same high-waterline slot.
- Workers normally claim by wallet priority, then promote the oldest queued job after the configured aging
  threshold. This preserves urgent-wallet ordering without allowing low-priority L2/L3 work to wait forever.
- `wallet-pipeline-jobs` and the research console expose the same per-stage queue counts, configured weights,
  scheduler cursor, and aged-job totals. Queue age is measured from `pipeline_jobs.updated_at` only when
  `attempts < max_attempts` and `next_attempt_at` is due, matching the worker claim rule and avoiding false
  alerts during retry backoff or after attempts are exhausted.
- Public Polymarket HTTP clients reserve global and endpoint request slots atomically through the shared
  `api_rate_limit_state` table, while retaining the existing per-process limiter.
- HTTP `429 Retry-After` cooldowns are shared across containers. Short waits are handled in the HTTP client;
  waits longer than 30 seconds return to the queue scheduler so workers do not hold leases while sleeping.
- Upstream cooldown and coordination deferrals do not consume a wallet job's failure-attempt budget, and the
  worker stops the current batch instead of churning through more wallets during the same cooldown. Run
  summaries count only wallets actually attempted and do not classify scheduler deferrals as job failures.
- Shared limiter lock contention defers the caller instead of allowing an uncoordinated request. The
  coordination transaction uses a short timeout and never contains network I/O or sleep.
- Optional SQLite recovery points are integrity-checked when explicitly created. Their freshness is not part of
  default runtime health while scheduled backups are paused.
- Paper handoff and paper eligibility share the same accepted hygiene statuses; the UI does not maintain a
  second status vocabulary that can contradict the scoring gate.
- `paper_candidate` and `live_eligible` are research states, never implicit permission to place real orders.

## Current Follow-Up Work

- Verify proxy reachability and logical loop liveness from the NAS runtime, not only container presence.
- Measure shared request-budget write latency under the real NAS worker count before increasing concurrency.
- Measure Parquet archive latency and compression on the real NAS before increasing the five-wallet batch size.
- Measure net retention backlog movement after the research-priority lock has run through normal discovery load.
