# GitHub Activity Probe

GitHub Actions provides a manual supplemental discovery probe, not a runtime.

The NAS research/scoring deployment remains the source of truth for:

- SQLite writes
- wallet activity and position backfill
- scoring
- paper-candidate observation

The GitHub workflow scans recent Polymarket `/trades` pages and exports candidate wallets as JSON. It does not write to the NAS database or feed the production queue automatically.

## Workflow

File:

```text
.github/workflows/polymarket-activity-probe.yml
```

Trigger:

```text
manual workflow_dispatch only
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

The artifact is diagnostic output only. Importing it into the candidate registry requires a separate, explicit review step.
