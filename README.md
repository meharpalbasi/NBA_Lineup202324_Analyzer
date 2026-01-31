# NBA Lineup Data Pipeline

Automated data pipeline that fetches 5-man lineup statistics (Base + Advanced) for all NBA teams via the [nba_api](https://github.com/swar/nba_api). Runs on a Railway cron job every 2 days and auto-commits updated CSVs to this repo.

This data powers the [NBA Lineup Analytics](https://github.com/meharpalbasi/nbalineup) dashboard.

## What It Does

1. Fetches lineup data for all 30 NBA teams (Regular Season + Playoffs)
2. Merges Base and Advanced stats per lineup
3. Saves to `data/NBALineup{season}_RegSeason_Playoffs_BaseAdvanced.csv`
4. Commits and pushes changes to GitHub automatically

## Data

| File | Description |
|------|-------------|
| `data/NBALineup202324_RegSeason_Playoffs_BaseAdvanced.csv` | 2023-24 season |
| `data/NBALineup202425_RegSeason_Playoffs_BaseAdvanced.csv` | 2024-25 season |
| `data/NBALineup202526_RegSeason_Playoffs_BaseAdvanced.csv` | 2025-26 season (current) |

Each CSV contains 5-man lineup combinations with:
- **Base stats:** MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, TOV, STL, BLK, PF, PTS, PLUS_MINUS
- **Advanced stats:** OFF_RATING, DEF_RATING, NET_RATING, AST_PCT, AST_TO, AST_RATIO, OREB_PCT, DREB_PCT, REB_PCT, EFG_PCT, TS_PCT, PACE, PIE
- **Metadata:** team, team_id, SEASON_TYPE, GROUP_NAME, players_list

## Local Usage

```bash
# Install dependencies
pip install nba_api pandas

# Fetch data (defaults to 2025-26 season)
python fetchlineups.py

# Override season
NBA_SEASON=2024-25 python fetchlineups.py
```

## Railway Deployment

See [RAILWAY_SETUP.md](RAILWAY_SETUP.md) for full deployment guide.

**Quick summary:** Runs `update_and_commit.sh` on a cron schedule (`0 0 */2 * *` — every 2 days at midnight UTC). Requires `GITHUB_TOKEN` and `GITHUB_REPO` environment variables.

## Features

- Exponential backoff retry logic (handles NBA API rate limits)
- API health check before full fetch
- Handles both Regular Season and Playoffs data
- Merges Base + Advanced stats per lineup
- Success rate reporting after each run
