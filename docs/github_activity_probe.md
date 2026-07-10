# GitHub Activity Probe

GitHub Actions is a supplemental discovery probe, not the primary robot runtime.

The VPS remains the source of truth for:

- SQLite writes
- wallet activity and position backfill
- scoring
- paper evaluation
- published leader handoff

The GitHub workflow scans recent Polymarket `/trades` pages and exports candidate wallets as JSON.

## Workflow

File:

```text
.github/workflows/polymarket-activity-probe.yml
```

Schedule:

```text
every 15 minutes
```

Default probe:

```bash
python -m pm_robot.cli \
  --db /tmp/pm_robot_probe.sqlite \
  discover-activity \
  --pages 8 \
  --page-limit 100 \
  --min-trades 2 \
  --min-usdc-volume 20 \
  --max-candidates 500 \
  --no-db-write \
  --out artifacts/polymarket-candidates.json
```

## Output

The workflow uploads:

```text
polymarket-candidates
```

containing:

```text
artifacts/polymarket-candidates.json
```

## Optional Push

If these GitHub repository secrets are configured, the workflow POSTs the JSON payload to the VPS:

```text
PM_ROBOT_PROBE_PUSH_URL
PM_ROBOT_PROBE_TOKEN
```

The current project does not expose a VPS ingest endpoint yet, so artifact-only mode is the safe default.
