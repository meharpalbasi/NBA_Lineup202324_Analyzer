# NBA Lineup Data Pipeline

Automated data pipeline that fetches NBA lineup and supplementary statistics via the [nba_api](https://github.com/swar/nba_api). Runs on a Railway cron job every 2 days and auto-commits updated CSVs to this repo.

This data powers the [NBA Lineup Analytics](https://github.com/meharpalbasi/nbalineup) dashboard.

## Architecture

```
run_pipeline.py          ← entry point
└── pipeline/
    ├── config.py        ← all tunable constants (measure types, delays, etc.)
    ├── fetch_lineups.py ← core lineup data (LeagueDashLineups)
    ├── fetch_supplementary.py ← 7 supplementary data sources
    ├── main.py          ← orchestrator with CLI args & summary reporting
    └── utils.py         ← retry logic, health check, API call tracking
```

### Legacy Script

`fetchlineups.py` is the original single-file pipeline (Base + Advanced only, team-by-team fetching with 30 API calls). Kept for reference — use `run_pipeline.py` instead.

## What It Fetches

### Core Lineups

Uses `LeagueDashLineups` (1 API call per combo vs 30 for the old team-by-team approach).

| Dimension | Values |
|-----------|--------|
| **Measure types** | Base, Advanced, Four Factors, Misc, Scoring, Opponent |
| **Group quantities** | 5-man, 3-man, 2-man |
| **Per modes** | Totals, Per100Possessions |
| **Season types** | Regular Season, Playoffs |

All measure types are fetched separately and merged into a single wide CSV per group quantity.

### Supplementary Data

| Source | Description | Output |
|--------|-------------|--------|
| **On/Off Court** | Player impact when on vs off the court | `on_off_{season}.csv` |
| **Clutch** | Performance in clutch situations (≤5 min, ≤5 pts) | `clutch_{season}.csv` |
| **Play Types** | Synergy play type breakdowns (11 types: Transition, Isolation, PnR, Postup, Spotup, etc.) | `play_types_{season}.csv` |
| **Hustle** | Deflections, loose balls, contested shots, charges | `hustle_players_{season}.csv`, `hustle_teams_{season}.csv` |
| **Player Tracking** | Speed/distance, catch & shoot, drives, passing, defense, etc. (12 categories) | `tracking_{season}.csv` |
| **Defense Tracking** | Defensive matchup stats by shot distance | `defense_tracking_{season}.csv` |
| **Estimated Metrics** | NBA's estimated offensive/defensive/net rating | `estimated_metrics_{season}.csv` |

## Usage

### Quick Start

```bash
pip install nba_api pandas

# Run full pipeline (lineups + supplementary)
python run_pipeline.py

# Override season
python run_pipeline.py --season 2024-25
```

### CLI Options

```
python run_pipeline.py [OPTIONS]

--season SEASON     NBA season (default: 2025-26, or NBA_SEASON env var)
--lineups-only      Fetch only lineup data, skip supplementary
--supplementary-only  Fetch only supplementary data, skip lineups
--dry-run           Test API connectivity only
--verbose           Enable debug logging
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NBA_SEASON` | Season to fetch (e.g. `2024-25`) | `2025-26` |
| `GITHUB_TOKEN` | For Railway auto-commit workflow | — |
| `GITHUB_REPO` | Repo slug for auto-commit | — |

## Output Files

### Lineup CSVs

| File | Description |
|------|-------------|
| `data/lineups_5man_{season}.csv` | 5-man lineup combinations |
| `data/lineups_3man_{season}.csv` | 3-man lineup combinations |
| `data/lineups_2man_{season}.csv` | 2-man lineup combinations |

Each contains merged columns from all 6 measure types × 2 per modes, with metadata (team, season type, group name, player list).

### Legacy CSVs

| File | Description |
|------|-------------|
| `data/NBALineup{season}_RegSeason_Playoffs_BaseAdvanced.csv` | Old format (Base + Advanced only) |

## Railway Deployment

See [RAILWAY_SETUP.md](RAILWAY_SETUP.md) for full deployment guide.

**Quick summary:** Runs `update_and_commit.sh` on a cron schedule (`0 0 */2 * *` — every 2 days at midnight UTC).

## Reliability

- Exponential backoff retry (5 retries, 2× multiplier, starting at 3s)
- 1.5s delay between API calls, 3s between endpoint types
- Health check before full fetch
- Per-section error isolation (one failure doesn't stop the rest)
- Summary report with success/failure counts, row totals, and API call count
