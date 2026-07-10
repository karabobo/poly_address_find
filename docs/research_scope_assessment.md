# Research Scope Assessment

## Target

This repository is now scoped to profitable-wallet discovery, copyability research, and paper evaluation.
Live copy-trading execution should live in a separate system that consumes this project's published wallet outputs.

## Keep In This Project

- Candidate import and discovery.
- Polydata, leaderboard, positions, activity, and Gamma metadata ingestion.
- Wallet feature scoring and review queue generation.
- Hygiene, hedge/arbitrage, and copyability gates.
- Copy-graph mining and copied-stream backtesting as research evidence.
- Paper evaluation ledger, fills, positions, marks, settlements, wallet quality, and readiness observations.
- Server health, backup, retention, and research timers.

## Move Out

- Nautilus/live broker adapters.
- Canary order preflight.
- Live/canary environment configuration.
- Live order intent tables.
- Exchange signing, CLOB authenticated order submission, and real order tracking.
- Account credentials, private keys, API key derivation, and balance/allowance checks.

## Boundary

The strongest output of this project is a wallet-level research decision:

```text
wallet -> candidate_stage -> paper_quality -> readiness blockers -> leader_publish
```

`live_eligible` remains a research label. It does not imply this repository can submit orders.

`leader_publish` is the local filtered wallet library for read-only consumers. It contains only currently
active, unexpired wallets that are `live_eligible`, production-ready, stable, and blocker-free. The
deployment also writes `/opt/pm-robot/reports/published_leaders.json` for read-only consumption.

Any future execution bot should treat this repository as read-only input and apply its own:

- real-time signal listener;
- orderbook and liquidity preflight;
- account and balance checks;
- order signing and submission;
- order status reconciliation;
- kill switch and capital limits.
