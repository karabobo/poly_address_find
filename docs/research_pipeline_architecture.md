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
| `leader_publish` | Explicit read-only output for downstream consumers | Permission to trade from this repository |

The historical database column `wallet_processing_state.discovery_tier` is called `evidence_tier` in code.
`pipeline_jobs.tier` is a job scope/dedupe field, not the wallet's real evidence tier.

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
- evidence planner and sharded wallet workers;
- copyability planner and workers;
- scoring and paper-observer loops;
- maintenance and verified SQLite backup loops.

The opt-in execution profile contains paper-run, paper-settle, and publish loops. It is not started by the
default `up`, `restart`, `runtime-ensure`, or watchdog commands.

## Reliability Rules

- Queue claims use SQLite write serialization and leases.
- Workers renew leases around long work and can only complete or retry jobs they still own.
- Maintenance requeues expired leases and stale runtime records.
- Planner backpressure limits queued/running wallet evidence jobs.
- Daily online SQLite backups are integrity-checked; the dashboard warns when no backup exists or the latest
  backup is older than the configured freshness window.
- `paper_candidate` and `live_eligible` are research states, never implicit permission to place real orders.

## Current Follow-Up Work

- Verify proxy reachability and logical loop liveness from the NAS runtime, not only container presence.
- Show failed queue samples and per-wallet job history in the web console.
- Add a shared cross-container upstream API budget and cooldown for rate-limit responses.
- Validate the complete Compose stack and backup schedule on the NAS after network access is restored.
