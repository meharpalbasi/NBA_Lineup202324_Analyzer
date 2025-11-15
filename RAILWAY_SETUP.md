# Railway Deployment Guide

This guide will help you deploy the NBA Lineup data fetcher as a cron job on Railway.

## Prerequisites

1. A [Railway](https://railway.app) account
2. A GitHub Personal Access Token (PAT) with `repo` permissions

## Setup Steps

### 1. Create GitHub Personal Access Token

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a name like "Railway NBA Lineup Updater"
4. Select scope: **`repo`** (Full control of private repositories)
5. Generate and **copy the token** (you won't see it again!)

### 2. Deploy to Railway

1. Go to [Railway Dashboard](https://railway.app/dashboard)
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Choose this repository (`nbalineup`)
5. Railway will auto-detect the `railway.json` configuration

### 3. Configure Environment Variables

In your Railway project, go to **Variables** and add the following:

#### Required Variables

| Variable | Value | Description |
|----------|-------|-------------|
| `GITHUB_TOKEN` | `ghp_xxxxx...` | Your GitHub Personal Access Token (from Step 1) |
| `GITHUB_REPO` | `username/nbalineup` | Your GitHub repository in format `username/repo` |

#### Optional Variables

| Variable | Value | Description |
|----------|-------|-------------|
| `GIT_USER_NAME` | `Railway Bot` | Name for git commits (default: Railway Bot) |
| `GIT_USER_EMAIL` | `bot@railway.app` | Email for git commits (default: bot@railway.app) |
| `NBA_SEASON` | `2025-26` | NBA season to fetch (default: 2025-26) |

**Example Configuration:**
- `GITHUB_TOKEN`: `ghp_abc123xyz...` (your actual PAT)
- `GITHUB_REPO`: `meharpalbasi/nbalineup`
- `NBA_SEASON`: `2025-26`

### 4. Cron Schedule

The cron is configured in `railway.json`:
```json
"cronSchedule": "0 0 */2 * *"
```

This runs at **midnight (UTC) every 2 days**.

To modify the schedule, edit `railway.json`:
- `0 0 */2 * *` - Every 2 days at midnight
- `0 0 * * *` - Daily at midnight
- `0 */12 * * *` - Every 12 hours
- `0 0 * * 0` - Weekly (every Sunday)

### 5. Monitor the Job

- View logs in Railway dashboard under your service
- Check your GitHub repository for automated commits with message: `chore: update NBA lineup data - YYYY-MM-DD`
- CSV files will be updated in the `data/` folder

## How It Works

1. Railway runs `update_and_commit.sh` on the cron schedule
2. The script runs `fetchlineups.py` to fetch NBA lineup data
3. Data is saved to `data/NBALineup{season}_RegSeason_Playoffs_BaseAdvanced.csv`
4. Git detects changes and commits them
5. Changes are pushed back to your GitHub repository

## Cost Estimate

- **Compute**: ~2-5 minutes per run
- **Frequency**: Every 2 days = ~15 runs/month
- **Total**: ~75 minutes/month
- **Cost**: $0-1/month (well within Railway's free tier)

## Troubleshooting

### "Error: GITHUB_TOKEN and GITHUB_REPO environment variables are required"
- Ensure both `GITHUB_TOKEN` and `GITHUB_REPO` are set in Railway Variables
- Check that `GITHUB_REPO` is in the format `username/repository`
- Verify `GITHUB_TOKEN` is a valid Personal Access Token

### No data being pushed to GitHub
- Check Railway logs for git errors
- Verify GitHub PAT has `repo` scope permissions
- Ensure repository name in `GITHUB_REPO` is correct
- Check that the token hasn't expired

### API rate limiting or fetch errors
- NBA API has rate limits; the script includes delays (0.6s between calls)
- If you encounter 429 errors, the delays may need to be increased
- Check Railway logs for specific API error messages

### Season data not found or empty CSV
- Update `NBA_SEASON` environment variable to current season
- Format: `YYYY-YY` (e.g., `2025-26`)
- Note: Playoff data only available after playoffs start

## Manual Trigger

To run the job manually (outside the cron schedule):
1. Go to Railway dashboard
2. Select your service
3. Click "Deploy" → "Redeploy"
