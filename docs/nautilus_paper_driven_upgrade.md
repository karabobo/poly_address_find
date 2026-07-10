# Nautilus Paper-Driven Upgrade

This note translates the two Polymarket papers into concrete upgrades for a whale-following system such as `tentenone1/nautilus-trading`.

Sources:

- `ssrn-6624899.pdf`: smart-money wallet anatomy, Polydata snapshot, April 2026.
- `ssrn-6670318.pdf`: copy-trading identification and returns, full Polymarket transaction sample through January 2026.

## Current Gap

The Nautilus-style project already has the right downstream shape: monitor whales, publish signals, paper trade, validate fills, and watch operations.

The weak layer is upstream selection. A static whale list or a simple `alpha_score >= 70` can overfit to high PnL, famous wallets, or one-off wins. The papers suggest that the source of edge is more specific:

- profitable wallets often look like resolution-edge holders, not pure arbitrage bots;
- copy leaders are selected for cumulative win rate and recent activity, not lifetime size or headline wins;
- copy trades earn a measurable but modest premium, so execution lag and slippage can erase the edge.

## Target Architecture

```text
Candidate Sources
  -> Address Library
  -> Data Hygiene Filters
  -> Wallet Feature Store
  -> Leader Graph
  -> Paper-Driven Scoring
  -> Review Queue
  -> Paper Trading Validation
  -> Live Eligibility
```

## Upgrade 1: Replace Static Whales With Candidate Sources

Candidate sources should be explicitly tracked:

- manually curated addresses, such as `data/candidate_addresses.csv`;
- Polydata analyzed traders;
- Polymarket public leaderboards;
- wallets repeatedly copied by other wallets;
- leaders found from transaction-level co-trading;
- social or research addresses, with source notes.

Every address needs provenance. Do not mix a manually tagged `btc` address with a statistically identified copy leader without preserving why it entered the system.

Minimum address-library columns:

```text
address
sources
labels
notes
links
status
first_seen_at
last_reviewed_at
candidate_stage
```

## Upgrade 2: Add Data Hygiene Before Scoring

The copy-trading paper removes three major sources of false signal before identifying leaders:

- wrap / unwrap routing operator trades;
- wash trades;
- market-maker taker activity.

For Nautilus, this should become a required pre-score gate:

```text
eligible_for_scoring =
  not routing_operator
  and not wash_cluster_flag
  and maker_fraction <= maker_fraction_max
  and taker_directional_volume >= min_directional_volume
```

Market makers can remain as counterparties when they provide liquidity. They should be excluded when they are the taker being evaluated as a directional trader.

## Upgrade 3: Score Leaders, Not Just Profitable Wallets

The first paper says high-quality smart money often has:

- high event-level win rate;
- trade-level win rate that can be lower because of DCA;
- repeated DCA entries per market;
- low sell rate / resolution holding;
- non-pure-bot execution;
- concentration in politics or other slow-resolution markets.

The second paper says copied leaders are more likely to have:

- high cumulative win rate;
- high recent 30-day volume;
- not merely high lifetime volume;
- no dependence on a single large market win;
- no recent headline one-off win.

The score should be split into interpretable blocks:

```text
leader_score =
  skill_quality
  + recent_activity
  + resolution_edge
  + copy_graph_evidence
  + execution_copyability
  - hygiene_penalties
  - salience_penalties
  - bot_hft_penalties
```

Avoid one opaque `alpha_score`. Store every component so failed candidates can be debugged.

## Upgrade 4: Detect Copy-Graph Leaders

The copy-trading paper identifies a leader/follower pair when:

- follower trades the same market and direction after the leader;
- window is 1 Polygon block, about 2 seconds;
- at least 5 matched co-trades;
- at least 5 distinct shared markets;
- leader always precedes follower in every shared market;
- during the active pair window, at least 90% of follower taker-buys fall inside the leader footprint.

These rules are valuable even if the system does not trade on two-second signals yet. They can identify wallets that other participants already consider worth copying.

Leader graph features:

```text
leader_in_degree
copy_event_count
copy_market_count
median_copy_lag_blocks
median_copy_cost_price
copy_stream_roi
followers_positive_copy_roi_pct
containment_pct_median
```

## Upgrade 5: Add Anti-Hedge and Anti-Arbitrage Flags

The first paper does not fully rule out hedging. Nautilus should add an explicit hedge/arbitrage detector before following:

```text
gross_exposure = sum(abs(position_value))
net_exposure = abs(sum(direction_adjusted_position_value))
net_to_gross = net_exposure / gross_exposure
```

Flag a wallet or market when:

- same event has offsetting YES/NO or multi-outcome positions;
- correlated markets carry opposite exposures;
- net-to-gross exposure is low;
- most PnL comes from convergence/spread capture rather than resolution;
- trade-level win rate is high, event-level edge is weak, and positions are rapidly closed.

This does not make a wallet bad. It makes it less copyable for a simple follower unless the bot can reproduce both legs.

## Upgrade 6: Paper-Trade Eligibility Gates

A signal should enter paper trading only when:

- candidate is not blocked by hygiene filters;
- leader score exceeds the review threshold;
- wallet is recently active;
- signal is not too late relative to leader fill;
- expected edge exceeds estimated slippage and copy cost;
- market has enough depth for the configured stake;
- market is not close to resolution unless the strategy explicitly supports late entries.

Live eligibility should require out-of-sample paper evidence:

```text
min_paper_signals
positive_copy_roi
max_drawdown_within_limit
slippage_within_limit
no unresolved hygiene flags
```

## Practical Integration Points

For a Nautilus project, these map cleanly:

- `data/candidate_addresses.csv`: upstream address library.
- `whale_discovery.db`: feature store and candidate state.
- `run_paper.py`: consume only `review_status in ('paper_approved', 'live_eligible')`.
- signal validator: add slippage, lag, market-depth, hedge, and hygiene gates.
- dashboard: show score components, not only total score.
- watchdog: alert when copied leader degrades, stops trading, or gets wash/MM flagged.

## Review Queue States

```text
imported
needs_data
needs_manual_review
paper_candidate
paper_approved
live_eligible
rejected
blocked_hygiene
blocked_copyability
```

Default policy: new addresses from manual files are `needs_data`, not tradable.
